use std::fmt;

use crate::codec::checksum;
use crate::files;
use crate::manifest::{
    check_condition, condition_matches, sequence, FragmentAddress, FragmentReceipt,
    MigrationAttempt, MigrationState, ObjectGeneration, RepairState, StripeManifest,
};
use crate::placement::PlacementPlanner;
use crate::readpath::StripeReader;
use crate::{Boundary, Catalog, Cluster, Condition, DomainPolicy, Error, Media, NodeState, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TierOutcome {
    /// The move was planned and every stripe was copied and switched.
    Published,
    /// The object already lives on the requested media; nothing was reserved.
    InPlace,
    /// A plan existed or was created but its copies could not all be verified
    /// yet; the original placement stays the live mapping.
    Pending,
    /// The object generation changed or was deleted while the move was paused.
    Changed,
    /// The target media has no viable three-node placement for some stripe.
    NoTarget,
}

impl fmt::Display for TierOutcome {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(match self {
            Self::Published => "published",
            Self::InPlace => "in-place",
            Self::Pending => "pending",
            Self::Changed => "changed",
            Self::NoTarget => "no-target",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TierReport {
    pub outcome: TierOutcome,
    pub attempt: Option<MigrationAttempt>,
}

impl Cluster {
    /// Move an object to another media class. The object generation, size,
    /// geometry and per-shard CRCs are preserved; every stripe switches media
    /// together in the final catalog transaction. A failed or interrupted
    /// move never changes the live mapping and can be resumed or cancelled.
    pub fn tier_object(
        &self,
        key: &str,
        target_media: Media,
        condition: Condition,
    ) -> Result<TierReport> {
        let attempt = match self.plan_tier(key, target_media, condition)? {
            Some(attempt) => attempt,
            None => {
                return Ok(TierReport {
                    outcome: TierOutcome::InPlace,
                    attempt: None,
                })
            }
        };
        let attempt = self.run_migration(attempt.id)?;
        let outcome = match attempt.state {
            MigrationState::Published => TierOutcome::Published,
            MigrationState::Stale => TierOutcome::Changed,
            MigrationState::Planned => TierOutcome::Pending,
        };
        Ok(TierReport {
            outcome,
            attempt: Some(attempt),
        })
    }

    /// Resume one planned move, reusing target fragments already verified.
    pub fn run_migration(&self, id: u64) -> Result<MigrationAttempt> {
        // The pause must sit outside the reader pin: another process may need
        // the exclusive cleanup lock while the copy is suspended.
        let precheck = self.catalog()?;
        let attempt = precheck
            .migrations
            .get(&id)
            .ok_or_else(|| Error::NotFound(format!("migration attempt {id}")))?;
        if attempt.state != MigrationState::Planned {
            return Ok(attempt.clone());
        }
        self.checkpoint(Boundary::BeforeTierCopy, id.to_string())?;

        // Re-validate under the pin so a generation switch during the pause is
        // observed before any fragment is read or written.
        let _pin = self.pin_fragments()?;
        let catalog = self.catalog()?;
        let attempt = catalog
            .migrations
            .get(&id)
            .ok_or_else(|| Error::NotFound(format!("migration attempt {id}")))?;
        if attempt.state != MigrationState::Planned {
            return Ok(attempt.clone());
        }
        let Some((object, stripes)) = live_mapping(&catalog, attempt) else {
            return self.finish_migration(id, MigrationState::Stale);
        };

        for (stripe, targets) in stripes.iter().zip(&attempt.targets) {
            let mut reader = StripeReader::new(self, &catalog, attempt.generation, stripe);
            for target in targets {
                if self.migration_copy_present(attempt, stripe.index, target)? {
                    continue;
                }
                let bytes = match reader.direct(target.shard) {
                    Some(bytes) => bytes.to_vec(),
                    None => match reader.rebuild(target.shard) {
                        Ok(bytes) => bytes,
                        // A damaged source stripe is healable later; keep the
                        // plan and the source mapping intact for a retry.
                        Err(_) => {
                            return Ok(self
                                .catalog()?
                                .migrations
                                .get(&id)
                                .expect("planned migration retained")
                                .clone())
                        }
                    },
                };
                self.write_fragment(attempt.generation, stripe.index, target, &bytes)?;
                if !self.migration_copy_present(attempt, stripe.index, target)? {
                    return Err(Error::Corrupt(format!(
                        "migration {id} failed read-back verification before publication"
                    )));
                }
            }
        }
        self.publish_migration(id, object)
    }

    /// Resume every planned move, returning the terminal or still-pending
    /// journal entries in identifier order.
    pub fn replay_migrations(&self) -> Result<Vec<MigrationAttempt>> {
        let ids: Vec<_> = self
            .catalog()?
            .migrations
            .values()
            .filter(|attempt| attempt.state == MigrationState::Planned)
            .map(|attempt| attempt.id)
            .collect();
        ids.into_iter().map(|id| self.run_migration(id)).collect()
    }

    /// Abandon a planned move. Copied-but-unpublished target fragments become
    /// retired and are reclaimed by `gc`; the source mapping is never touched.
    pub fn cancel_migration(&self, id: u64) -> Result<MigrationAttempt> {
        let mut txn = self.transaction()?;
        let mut attempt = txn
            .catalog
            .migrations
            .remove(&id)
            .ok_or_else(|| Error::NotFound(format!("migration attempt {id}")))?;
        if attempt.state != MigrationState::Planned {
            txn.catalog.migrations.insert(id, attempt.clone());
            return Ok(attempt);
        }
        attempt.state = MigrationState::Stale;
        attempt.detail = "cancelled before publication".into();
        txn.catalog.retire_migration_targets(&attempt);
        txn.catalog.migrations.insert(id, attempt.clone());
        txn.commit("migration-cancel")?;
        Ok(attempt)
    }

    pub fn migration_attempt(&self, id: u64) -> Result<MigrationAttempt> {
        self.catalog()?
            .migrations
            .remove(&id)
            .ok_or_else(|| Error::NotFound(format!("migration attempt {id}")))
    }

    pub fn prune_migrations(&self, through_id: u64) -> Result<usize> {
        let mut txn = self.transaction()?;
        let old_len = txn.catalog.migrations.len();
        txn.catalog
            .migrations
            .retain(|id, attempt| *id > through_id || attempt.state == MigrationState::Planned);
        let count = old_len - txn.catalog.migrations.len();
        if count > 0 {
            txn.commit("migration-prune")?;
        }
        Ok(count)
    }

    fn plan_tier(
        &self,
        key: &str,
        target_media: Media,
        condition: Condition,
    ) -> Result<Option<MigrationAttempt>> {
        let mut txn = self.transaction()?;
        let object = txn
            .catalog
            .objects
            .get(key)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("object {key:?}")))?;
        check_condition(key, condition, Some(&object))?;
        if object.media == target_media {
            return Ok(None);
        }
        if txn
            .catalog
            .migrations
            .values()
            .any(|attempt| attempt.state == MigrationState::Planned && attempt.generation == object.generation)
        {
            let existing = txn
                .catalog
                .migrations
                .values()
                .find(|attempt| {
                    attempt.state == MigrationState::Planned
                        && attempt.generation == object.generation
                })
                .expect("planned migration observed under writer lock");
            return Ok(Some(existing.clone()));
        }
        if txn.catalog.repairs.values().any(|attempt| {
            attempt.state == RepairState::Planned && attempt.generation == object.generation
        }) {
            return Err(Error::Conflict(format!(
                "object {key:?} has a planned repair; replay it before changing media"
            )));
        }

        // Placement planning runs against the live ledger, which already
        // reserves existing uploads, repairs and other planned migrations.
        // A failure here leaves every catalog reservation untouched.
        let plans = PlacementPlanner::new(&txn.catalog, None)?.plan(
            key,
            object.size,
            target_media,
            object.domain_policy,
        )?;

        let mut targets = Vec::with_capacity(plans.len());
        for (stripe, plan) in plans.iter().enumerate() {
            let mut stripe_targets: [Option<FragmentReceipt>; 3] =
                std::array::from_fn(|_| None);
            for shard in 0..3 {
                let revision = sequence(&mut txn.catalog)?;
                let source = &object.stripes[stripe].fragments[shard];
                stripe_targets[shard] = Some(FragmentReceipt {
                    shard: source.shard,
                    node: plan.nodes[shard].clone(),
                    revision,
                    bytes: source.bytes,
                    crc32c: source.crc32c,
                });
            }
            targets.push(stripe_targets.map(Option::unwrap));
        }
        let id = sequence(&mut txn.catalog)?;
        let attempt = MigrationAttempt {
            id,
            key: key.to_owned(),
            generation: object.generation,
            from_media: object.media,
            target_media,
            condition,
            targets,
            state: MigrationState::Planned,
            detail: String::new(),
        };
        txn.catalog.migrations.insert(id, attempt.clone());
        txn.commit("migration-plan")?;
        Ok(Some(attempt))
    }

    fn migration_copy_present(
        &self,
        attempt: &MigrationAttempt,
        stripe: u32,
        receipt: &FragmentReceipt,
    ) -> Result<bool> {
        let address = FragmentAddress::from_receipt(attempt.generation, stripe, receipt);
        let path = self.fragment_path(&address)?;
        match files::read_limited(&path, receipt.bytes as usize) {
            Ok(bytes) => {
                Ok(bytes.len() == receipt.bytes as usize && checksum(&bytes) == receipt.crc32c)
            }
            Err(Error::Corrupt(_)) => Ok(false),
            Err(error) if error.is_not_found_io() => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Re-validate the plan under the writer lock and switch the whole object
    /// in one catalog replacement.
    fn publish_migration(
        &self,
        id: u64,
        snapshot: &ObjectGeneration,
    ) -> Result<MigrationAttempt> {
        let mut txn = self.transaction()?;
        let attempt = txn
            .catalog
            .migrations
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("migration attempt {id}")))?;
        if attempt.state != MigrationState::Planned {
            return Ok(attempt);
        }
        if !mapping_is_live(&txn.catalog, &attempt) {
            return self.finish_migration(id, MigrationState::Stale);
        }
        let object = txn
            .catalog
            .objects
            .get(&attempt.key)
            .expect("live mapping checked");
        if object.generation != snapshot.generation || object.stripes != snapshot.stripes {
            return self.finish_migration(id, MigrationState::Stale);
        }
        // The target layout must still fit the target media and policy.
        for targets in &attempt.targets {
            let nodes = targets.each_ref().map(|receipt| receipt.node.as_str());
            if !target_layout_valid(&txn.catalog, &attempt, object, nodes) {
                return self.finish_migration(id, MigrationState::Stale);
            }
        }

        // Snapshot the source receipts before mutating so retirement and the
        // mapping switch do not alias the mutable object borrow.
        let sources: Vec<[FragmentReceipt; 3]> = {
            let object = txn
                .catalog
                .objects
                .get(&attempt.key)
                .expect("live mapping checked");
            object
                .stripes
                .iter()
                .map(|stripe| stripe.fragments.clone())
                .collect()
        };
        for (stripe, (sources, targets)) in
            sources.iter().zip(&attempt.targets).enumerate()
        {
            for (shard, target) in targets.iter().enumerate() {
                txn.catalog
                    .retire_fragment(attempt.generation, stripe as u32, &sources[shard]);
                txn.catalog.objects.get_mut(&attempt.key).expect("object present").stripes[stripe]
                    .fragments[shard] = target.clone();
            }
        }
        txn.catalog
            .objects
            .get_mut(&attempt.key)
            .expect("object present")
            .media = attempt.target_media;
        let mut finished = attempt;
        finished.state = MigrationState::Published;
        finished.detail = "all stripes copied and switched together".into();
        txn.catalog.migrations.insert(id, finished.clone());
        txn.commit("migration-publish")?;
        Ok(finished)
    }

    fn finish_migration(&self, id: u64, state: MigrationState) -> Result<MigrationAttempt> {
        let mut txn = self.transaction()?;
        let mut attempt = txn
            .catalog
            .migrations
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("migration attempt {id}")))?;
        if attempt.state != MigrationState::Planned {
            return Ok(attempt);
        }
        attempt.state = state;
        attempt.detail = match state {
            MigrationState::Stale => {
                "source mapping or target eligibility changed".into()
            }
            _ => String::new(),
        };
        // The target fragments were never published; reclaim their budget.
        txn.catalog.retire_migration_targets(&attempt);
        txn.catalog.migrations.insert(id, attempt.clone());
        txn.commit("migration-stale")?;
        Ok(attempt)
    }
}

/// The object and its stripe mapping the plan was built against, provided the
/// key, generation, condition and per-stripe source receipts still match.
fn live_mapping<'a>(
    catalog: &'a Catalog,
    attempt: &MigrationAttempt,
) -> Option<(&'a ObjectGeneration, Vec<&'a StripeManifest>)> {
    let object = mapping_object(catalog, attempt)?;
    let stripes = object.stripes.iter().collect();
    Some((object, stripes))
}

/// Whether the plan's object generation, media, condition and source receipts
/// are still the live mapping.
fn mapping_is_live(catalog: &Catalog, attempt: &MigrationAttempt) -> bool {
    mapping_object(catalog, attempt).is_some()
}

fn mapping_object<'a>(
    catalog: &'a Catalog,
    attempt: &MigrationAttempt,
) -> Option<&'a ObjectGeneration> {
    let object = catalog
        .objects
        .get(&attempt.key)
        .filter(|object| object.generation == attempt.generation)?;
    if !condition_matches(attempt.condition, Some(object))
        || object.media != attempt.from_media
        || object.stripes.len() != attempt.targets.len()
    {
        return None;
    }
    for (stripe, targets) in object.stripes.iter().zip(&attempt.targets) {
        for (shard, target) in targets.iter().enumerate() {
            let source = &stripe.fragments[shard];
            if source.shard != target.shard
                || source.bytes != target.bytes
                || source.crc32c != target.crc32c
            {
                return None;
            }
        }
    }
    Some(object)
}

fn target_layout_valid(
    catalog: &Catalog,
    attempt: &MigrationAttempt,
    object: &ObjectGeneration,
    nodes: [&str; 3],
) -> bool {
    let mut domains = std::collections::BTreeSet::new();
    for id in nodes {
        let Some(node) = catalog.nodes.get(id) else {
            return false;
        };
        if node.media != attempt.target_media || node.state == NodeState::Removed {
            return false;
        }
        domains.insert(node.domain.as_str());
    }
    if nodes[0] == nodes[1] || nodes[0] == nodes[2] || nodes[1] == nodes[2] {
        return false;
    }
    object.domain_policy != DomainPolicy::Strict || domains.len() == 3
}
