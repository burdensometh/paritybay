use crate::codec::checksum;
use crate::files;
use crate::manifest::{sequence, FragmentAddress};
use crate::placement::{replacement, replacement_valid, replacement_with_ledger, Ledger, ReplacementPolicy};
use crate::readpath::StripeReader;
use crate::{
    Boundary, Catalog, Cluster, Error, ObjectGeneration, RepairAttempt, RepairKind, RepairState,
    Result, StripeManifest,
};

use super::budget::RepairBudget;

pub(super) enum Planning {
    Ready(RepairAttempt),
    Busy(u64),
    Changed,
    NoTarget(String),
}

impl Cluster {
    pub(super) fn plan_repair(
        &self,
        object: &ObjectGeneration,
        stripe: &StripeManifest,
        shard: u8,
        kind: RepairKind,
        policy: ReplacementPolicy,
        mut budget: Option<&mut RepairBudget>,
    ) -> Result<Planning> {
        let mut txn = self.transaction()?;
        let Some(current) = txn
            .catalog
            .objects
            .get(&object.key)
            .filter(|current| current.generation == object.generation)
        else {
            return Ok(Planning::Changed);
        };
        if current.stripes.get(stripe.index as usize) != Some(stripe) {
            return Ok(Planning::Changed);
        }
        if let Some(pending) = pending_for_stripe(&txn.catalog, current.generation, stripe.index) {
            return Ok(Planning::Busy(pending.id));
        }
        let node = match budget.as_mut() {
            Some(round_budget) => {
                let ledger = round_budget.sync(&txn.catalog)?;
                replacement_with_ledger(
                    &txn.catalog,
                    ledger,
                    current,
                    stripe,
                    shard,
                    policy,
                )
            }
            None => replacement(&txn.catalog, current, stripe, shard, policy),
        };
        let node = match node {
            Ok(node) => node,
            Err(Error::Capacity(message)) => return Ok(Planning::NoTarget(message)),
            Err(error) => return Err(error),
        };

        let from = stripe.fragments[usize::from(shard)].clone();
        let id = sequence(&mut txn.catalog)?;
        let mut to = from.clone();
        to.node = node.clone();
        to.revision = id;
        let attempt = RepairAttempt {
            id,
            key: object.key.clone(),
            generation: object.generation,
            stripe: stripe.index,
            from,
            to,
            kind,
            state: RepairState::Planned,
            detail: String::new(),
        };
        txn.catalog.repairs.insert(id, attempt.clone());
        txn.commit("repair-plan")?;
        if let Some(round_budget) = budget.as_mut() {
            // The plan now owns a repair reservation on the target node.
            round_budget
                .ledger_mut()
                .book_repair(&node, attempt.to.bytes)?;
            round_budget.adopt(&self.catalog()?);
        }
        Ok(Planning::Ready(attempt))
    }

    pub fn replay_repairs(&self) -> Result<Vec<RepairAttempt>> {
        let catalog = self.catalog()?;
        let ids: Vec<_> = catalog
            .repairs
            .values()
            .filter(|attempt| attempt.state == RepairState::Planned)
            .map(|attempt| attempt.id)
            .collect();
        ids.into_iter()
            .map(|id| self.run_repair_with_budget(id, None))
            .collect()
    }

    pub fn repair_attempt(&self, id: u64) -> Result<RepairAttempt> {
        self.catalog()?
            .repairs
            .remove(&id)
            .ok_or_else(|| no_repair(id))
    }

    pub fn run_repair(&self, id: u64) -> Result<RepairAttempt> {
        self.run_repair_with_budget(id, None)
    }

    pub(super) fn run_repair_with_budget(
        &self,
        id: u64,
        budget: Option<&mut RepairBudget>,
    ) -> Result<RepairAttempt> {
        let _pin = self.pin_fragments()?;
        let catalog = self.catalog()?;
        let attempt = catalog.repairs.get(&id).ok_or_else(|| no_repair(id))?;
        if attempt.state != RepairState::Planned {
            return Ok(attempt.clone());
        }
        let Some((_, stripe)) = expected_stripe(&catalog, attempt) else {
            return self.finish_repair(id, CopyStatus::Stale, budget);
        };

        self.checkpoint(Boundary::BeforeRepairCopy, id.to_string())?;
        if !self.copy_already_present(attempt)? {
            let mut reader = StripeReader::new(self, &catalog, attempt.generation, stripe);
            let bytes = match reader.direct(attempt.from.shard) {
                Some(bytes) => bytes.to_vec(),
                None => match reader.rebuild(attempt.from.shard) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        return self.finish_repair(
                            id,
                            CopyStatus::BadSources(error.to_string()),
                            budget,
                        )
                    }
                },
            };
            self.write_fragment(attempt.generation, attempt.stripe, &attempt.to, &bytes)?;
            if !self.copy_already_present(attempt)? {
                return Err(Error::Corrupt(format!(
                    "repair {id} failed read-back verification before publication"
                )));
            }
        }
        self.finish_repair(id, CopyStatus::Verified, budget)
    }

    fn copy_already_present(&self, attempt: &RepairAttempt) -> Result<bool> {
        let address =
            FragmentAddress::from_receipt(attempt.generation, attempt.stripe, &attempt.to);
        let path = self.fragment_path(&address)?;
        match files::read_limited(&path, attempt.to.bytes as usize) {
            Ok(bytes) => {
                Ok(bytes.len() == attempt.to.bytes as usize
                    && checksum(&bytes) == attempt.to.crc32c)
            }
            Err(Error::Corrupt(_)) => Ok(false),
            Err(error) if error.is_not_found_io() => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn finish_repair(
        &self,
        id: u64,
        copy: CopyStatus,
        mut budget: Option<&mut RepairBudget>,
    ) -> Result<RepairAttempt> {
        let mut txn = self.transaction()?;
        let mut attempt = txn
            .catalog
            .repairs
            .get(&id)
            .cloned()
            .ok_or_else(|| no_repair(id))?;
        if attempt.state != RepairState::Planned {
            return Ok(attempt);
        }

        // Re-derive capacity and placement eligibility against the catalog as
        // it stands at publication time. Reservations started by other
        // processes between planning and this point are visible here.
        let eligible = expected_stripe(&txn.catalog, &attempt).is_some();
        let target = txn
            .catalog
            .nodes
            .get(&attempt.to.node)
            .ok_or_else(|| {
                Error::Corrupt(format!(
                    "repair {} references unknown node {}",
                    attempt.id, attempt.to.node
                ))
            })?;
        let target_capacity = target.capacity;
        let capacity_ok = match budget.as_mut() {
            Some(round_budget) => {
                round_budget.note_recheck();
                let ledger = round_budget.sync(&txn.catalog)?;
                ledger.fits(&attempt.to.node, target_capacity, attempt.to.bytes)
            }
            None => {
                let ledger = Ledger::from_catalog(&txn.catalog, None)?;
                ledger.fits(&attempt.to.node, target_capacity, attempt.to.bytes)
            }
        };
        let still_expected = eligible && capacity_ok;
        let retire_old = match (still_expected, copy) {
            (true, CopyStatus::Verified) => {
                let object = txn
                    .catalog
                    .objects
                    .get_mut(&attempt.key)
                    .expect("checked generation");
                object.stripes[attempt.stripe as usize].fragments
                    [usize::from(attempt.from.shard)] = attempt.to.clone();
                attempt.state = RepairState::Published;
                attempt.detail = "replacement verified and published".into();
                true
            }
            (true, CopyStatus::BadSources(detail)) => {
                attempt.state = RepairState::Unrecoverable;
                attempt.detail = clip(detail);
                false
            }
            _ => {
                attempt.state = RepairState::Stale;
                attempt.detail = if eligible {
                    "target capacity no longer fits at publication".into()
                } else {
                    "source mapping or target eligibility changed".into()
                };
                false
            }
        };
        let retired = if retire_old {
            &attempt.from
        } else {
            &attempt.to
        };
        txn.catalog
            .retire_fragment(attempt.generation, attempt.stripe, retired);
        txn.catalog.repairs.insert(id, attempt.clone());
        txn.commit("repair-publish")?;
        if let Some(budget) = budget {
            apply_publication_delta(budget, &attempt)?;
            budget.adopt(&self.catalog()?);
        }
        Ok(attempt)
    }

    pub fn prune_repairs(&self, through_id: u64) -> Result<usize> {
        let mut txn = self.transaction()?;
        let old_len = txn.catalog.repairs.len();
        txn.catalog
            .repairs
            .retain(|id, attempt| *id > through_id || attempt.state == RepairState::Planned);
        let count = old_len - txn.catalog.repairs.len();
        if count > 0 {
            txn.commit("repair-prune")?;
        }
        Ok(count)
    }
}

enum CopyStatus {
    Verified,
    Stale,
    BadSources(String),
}

/// Mirror a publication decision into the round ledger so later stripes keep
/// deciding against the reservations already made in this round.
fn apply_publication_delta(
    budget: &mut RepairBudget,
    attempt: &RepairAttempt,
) -> Result<()> {
    let ledger = budget.ledger_mut();
    match attempt.state {
        RepairState::Published => {
            // Current fragment moves off the source node; the target's repair
            // reservation converts into a current fragment and the source file
            // is retired but keeps charging its node until collection.
            ledger.shift_repair_to_current(&attempt.to.node, attempt.to.bytes)?;
            ledger.shift_current_to_retired(&attempt.from.node, attempt.from.bytes)?;
        }
        RepairState::Stale => {
            // The reserved copy never became current and is itself retired;
            // its bytes keep charging the target node until collection runs.
            ledger.shift_repair_to_retired(&attempt.to.node, attempt.to.bytes)?;
        }
        RepairState::Unrecoverable => {
            // No durable copy was produced, so the reservation is released.
            ledger.release_repair(&attempt.to.node, attempt.to.bytes)?;
        }
        RepairState::Planned => {}
    }
    Ok(())
}

fn no_repair(id: u64) -> Error {
    Error::NotFound(format!("repair attempt {id}"))
}

fn pending_for_stripe(catalog: &Catalog, generation: u64, stripe: u32) -> Option<&RepairAttempt> {
    catalog.repairs.values().find(|attempt| {
        attempt.state == RepairState::Planned
            && attempt.generation == generation
            && attempt.stripe == stripe
    })
}

fn expected_stripe<'a>(
    catalog: &'a Catalog,
    attempt: &RepairAttempt,
) -> Option<(&'a ObjectGeneration, &'a StripeManifest)> {
    let object = catalog
        .objects
        .get(&attempt.key)
        .filter(|entry| entry.generation == attempt.generation)?;
    let stripe = object.stripes.get(attempt.stripe as usize)?;
    let shard = attempt.from.shard;
    if stripe.fragments[usize::from(shard)] != attempt.from
        || !replacement_valid(catalog, object, stripe, shard, &attempt.to.node)
    {
        return None;
    }
    Some((object, stripe))
}

fn clip(mut detail: String) -> String {
    if detail.len() > 1024 {
        let limit = (0..=1024)
            .rev()
            .find(|position| detail.is_char_boundary(*position))
            .unwrap();
        detail.truncate(limit);
    }
    detail
}
