use crate::manifest::sequence;
use crate::placement::{replacement, replacement_valid, ReplacementPolicy};
use crate::{
    Boundary, Catalog, Cluster, Error, ObjectGeneration, RepairAttempt, RepairKind, RepairState,
    Result, StripeManifest,
};

use super::stream::{verify_existing_copy, CopyFailure, REPAIR_WINDOW};

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
        let node = match replacement(&txn.catalog, current, stripe, shard, policy) {
            Ok(node) => node,
            Err(Error::Capacity(message)) => return Ok(Planning::NoTarget(message)),
            Err(error) => return Err(error),
        };

        let from = stripe.fragments[usize::from(shard)].clone();
        let id = sequence(&mut txn.catalog)?;
        let mut to = from.clone();
        to.node = node;
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
        ids.into_iter().map(|id| self.run_repair(id)).collect()
    }

    pub fn repair_attempt(&self, id: u64) -> Result<RepairAttempt> {
        self.catalog()?
            .repairs
            .remove(&id)
            .ok_or_else(|| no_repair(id))
    }

    pub fn run_repair(&self, id: u64) -> Result<RepairAttempt> {
        let _pin = self.pin_fragments()?;
        let catalog = self.catalog()?;
        let attempt = catalog.repairs.get(&id).ok_or_else(|| no_repair(id))?;
        if attempt.state != RepairState::Planned {
            return Ok(attempt.clone());
        }
        let Some((_, stripe)) = expected_stripe(&catalog, attempt) else {
            return self.finish_repair(id, CopyStatus::Stale);
        };

        self.checkpoint(Boundary::BeforeRepairCopy, id.to_string())?;
        if !self.copy_already_present(attempt)? {
            let outcome = self.stream_repair_fragment(
                &catalog,
                attempt.generation,
                stripe,
                attempt.from.shard,
                &attempt.to,
                REPAIR_WINDOW,
            );
            if let Err(failure) = outcome {
                match failure {
                    CopyFailure::Io(error) => return Err(error),
                    source_failure => {
                        return self
                            .finish_repair(id, CopyStatus::BadSources(source_failure.detail()))
                    }
                }
            }
            // Re-read the draft through rolling windows before any catalog
            // change, so a torn write can never publish a new mapping.
            if !self.copy_already_present(attempt)? {
                return Err(Error::Corrupt(format!(
                    "repair {id} failed read-back verification before publication"
                )));
            }
        }
        self.finish_repair(id, CopyStatus::Verified)
    }

    fn copy_already_present(&self, attempt: &RepairAttempt) -> Result<bool> {
        match verify_existing_copy(self, attempt, REPAIR_WINDOW) {
            Ok(verified) => Ok(verified),
            Err(error) if error.is_not_found_io() => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn finish_repair(&self, id: u64, copy: CopyStatus) -> Result<RepairAttempt> {
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

        let still_expected = expected_stripe(&txn.catalog, &attempt).is_some();
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
                attempt.detail = "source mapping or target eligibility changed".into();
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
