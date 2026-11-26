use std::fmt;

use crate::codec::ChecksumAlgorithm;
use crate::manifest::checked_key;
use crate::placement::ReplacementPolicy;
use crate::readpath::{StripeInspection, StripeReader, StripeVerdict};
use crate::{
    Catalog, Cluster, Error, ObjectGeneration, RepairAttempt, RepairKind, RepairState, Result,
};

use super::{CollectionReport, Planning};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ScanTotals {
    pub objects: usize,
    pub healthy: usize,
    pub recoverable: usize,
    pub unrecoverable: usize,
    pub parity_mismatch: usize,
}

impl ScanTotals {
    fn count(&mut self, verdict: StripeVerdict) {
        match verdict {
            StripeVerdict::Healthy => self.healthy += 1,
            StripeVerdict::Recoverable => self.recoverable += 1,
            StripeVerdict::Unrecoverable => self.unrecoverable += 1,
            StripeVerdict::ParityMismatch => self.parity_mismatch += 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceOutcome {
    Published,
    Unrecoverable,
    NoTarget,
    Pending,
    Changed,
}

impl fmt::Display for MaintenanceOutcome {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(match self {
            Self::Published => "published",
            Self::Unrecoverable => "unrecoverable",
            Self::NoTarget => "no-target",
            Self::Pending => "pending",
            Self::Changed => "changed",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceEvent {
    pub key: String,
    pub generation: u64,
    pub stripe: u32,
    pub shard: Option<u8>,
    pub outcome: MaintenanceOutcome,
    pub attempt: Option<u64>,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceReport {
    pub kind: RepairKind,
    pub scan: ScanTotals,
    pub events: Vec<MaintenanceEvent>,
    pub collection: Option<CollectionReport>,
}

impl Cluster {
    pub fn scan(&self, key: Option<&str>) -> Result<ScanTotals> {
        let _pin = self.pin_fragments()?;
        let catalog = self.catalog()?;
        let objects = selected(&catalog, key)?;
        let mut totals = ScanTotals {
            objects: objects.len(),
            ..Default::default()
        };
        for object in objects {
            for stripe in &object.stripes {
                totals.count(
                    self.inspect_stripe(&catalog, object.generation, stripe)
                        .verdict,
                );
            }
        }
        Ok(totals)
    }

    pub fn heal(&self, key: Option<&str>) -> Result<MaintenanceReport> {
        self.maintain(key, RepairKind::Heal)
    }

    pub fn rebalance(&self, key: Option<&str>) -> Result<MaintenanceReport> {
        let mut report = self.maintain(key, RepairKind::Rebalance)?;
        report.collection = Some(self.collect()?);
        Ok(report)
    }

    fn maintain(&self, key: Option<&str>, kind: RepairKind) -> Result<MaintenanceReport> {
        let _snapshot_pin = self.pin_fragments()?;
        let catalog = self.catalog()?;
        let objects = selected(&catalog, key)?;
        let mut report = MaintenanceReport {
            kind,
            scan: ScanTotals {
                objects: objects.len(),
                ..Default::default()
            },
            events: Vec::new(),
            collection: None,
        };

        for snapshot in objects {
            for original_stripe in &snapshot.stripes {
                let inspected = self.inspect_stripe(&catalog, snapshot.generation, original_stripe);
                report.scan.count(inspected.verdict);
                self.maintain_stripe(snapshot, &catalog, inspected, &mut report)?;
            }
        }
        Ok(report)
    }

    fn maintain_stripe(
        &self,
        snapshot: &ObjectGeneration,
        initial: &Catalog,
        inspected: StripeInspection,
        report: &mut MaintenanceReport,
    ) -> Result<()> {
        let kind = report.kind;
        let index = inspected.manifest.index;
        let mut first_inspection = Some(inspected);
        let mut refreshed = None;
        // A stripe can evacuate at most three placements and improve at most twice.
        // The bound also caps retries if other writers keep changing node states.
        for _ in 0..5 {
            let catalog = refreshed.as_ref().unwrap_or(initial);
            let Some(object) = catalog
                .objects
                .get(&snapshot.key)
                .filter(|object| object.generation == snapshot.generation)
            else {
                report.events.push(event(
                    snapshot,
                    index,
                    None,
                    MaintenanceOutcome::Changed,
                    None,
                    "object changed",
                ));
                return Ok(());
            };
            let stripe = &object.stripes[index as usize];
            let inspected = first_inspection
                .take()
                .unwrap_or_else(|| self.inspect_stripe(catalog, object.generation, stripe));
            if matches!(
                inspected.verdict,
                StripeVerdict::Unrecoverable | StripeVerdict::ParityMismatch
            ) {
                report.events.push(event(
                    object,
                    index,
                    None,
                    MaintenanceOutcome::Unrecoverable,
                    None,
                    inspected
                        .detail
                        .unwrap_or_else(|| inspected.verdict.to_string()),
                ));
                return Ok(());
            }

            let choices = choices(catalog, &inspected, kind);
            let mut no_target = None;
            let mut advanced = false;
            for (shard, policy) in choices {
                match self.plan_repair(object, stripe, shard, kind, policy, None)? {
                    Planning::Ready(attempt) => {
                        let attempt = self.run_repair(attempt.id)?;
                        advanced = attempt.state == RepairState::Published;
                        report.events.push(attempt_event(object, &attempt));
                        break;
                    }
                    Planning::Changed => {
                        report.events.push(event(
                            object,
                            index,
                            Some(shard),
                            MaintenanceOutcome::Changed,
                            None,
                            "stripe changed",
                        ));
                        return Ok(());
                    }
                    Planning::Busy(id) => {
                        report.events.push(event(
                            object,
                            index,
                            Some(shard),
                            MaintenanceOutcome::Pending,
                            Some(id),
                            "replay the existing plan",
                        ));
                        return Ok(());
                    }
                    Planning::NoTarget(detail)
                        if !matches!(policy, ReplacementPolicy::ImproveDomains) =>
                    {
                        no_target = Some(event(
                            object,
                            index,
                            Some(shard),
                            MaintenanceOutcome::NoTarget,
                            None,
                            detail,
                        ));
                    }
                    Planning::NoTarget(_) => {}
                }
            }
            if !advanced {
                if let Some(blocked) = no_target {
                    report.events.push(blocked);
                }
                return Ok(());
            }
            if kind == RepairKind::Heal {
                return Ok(());
            }
            refreshed = Some(self.catalog()?);
        }
        Ok(())
    }

    /// Rewrite legacy CRC-32C fragments with SHA-256 through the durable
    /// repair journal. Each target is fully written and read-back verified
    /// before the catalog mapping switches, so an interrupted run resumes via
    /// [`Cluster::replay_repairs`] after reopen. Returns the maintenance events
    /// produced; pass `None` for every object or `Some(KEY)` for one object.
    pub fn upgrade_checksums(&self, key: Option<&str>) -> Result<MaintenanceReport> {
        // New writes after the upgrade use the stronger algorithm too.
        self.enable_strong_checksums()?;
        let _snapshot_pin = self.pin_fragments()?;
        let catalog = self.catalog()?;
        let objects = selected(&catalog, key)?;
        let mut report = MaintenanceReport {
            kind: RepairKind::Heal,
            scan: ScanTotals {
                objects: objects.len(),
                ..Default::default()
            },
            events: Vec::new(),
            collection: None,
        };

        // Each published repair converts exactly one fragment; bound the loop
        // by the total fragment count plus a small refresh allowance.
        let fragment_count: usize = catalog
            .objects
            .values()
            .map(|object| object.stripes.iter().map(|s| s.fragments.len()).sum::<usize>())
            .sum();
        for _ in 0..=fragment_count + 4 {
            let catalog = self.catalog()?;
            let Some((object_key, generation, stripe_index, shard)) =
                first_legacy_fragment(&catalog, key)?
            else {
                break;
            };
            let object = catalog
                .objects
                .get(&object_key)
                .expect("legacy fragment key present");
            let stripe = &object.stripes[stripe_index as usize];

            let mut reader = StripeReader::new(self, &catalog, generation, stripe);
            let direct = reader.direct(shard).map(<[u8]>::to_vec);
            let bytes = match direct {
                Some(bytes) => bytes,
                None => match reader.rebuild(shard) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        report.events.push(event(
                            object,
                            stripe_index,
                            Some(shard),
                            MaintenanceOutcome::Unrecoverable,
                            None,
                            error.to_string(),
                        ));
                        return Ok(report);
                    }
                },
            };
            let digest = ChecksumAlgorithm::Sha256.compute(&bytes);

            match self.plan_repair(
                object,
                stripe,
                shard,
                RepairKind::Heal,
                ReplacementPolicy::Heal,
                Some(digest),
            )? {
                Planning::Ready(attempt) => {
                    let attempt = self.run_repair(attempt.id)?;
                    let published = attempt.state == RepairState::Published;
                    report.events.push(attempt_event(object, &attempt));
                    if !published {
                        break;
                    }
                }
                Planning::Busy(id) => {
                    let attempt = self.run_repair(id)?;
                    report.events.push(attempt_event(object, &attempt));
                }
                Planning::Changed => {
                    report.events.push(event(
                        object,
                        stripe_index,
                        Some(shard),
                        MaintenanceOutcome::Changed,
                        None,
                        "stripe changed",
                    ));
                }
                Planning::NoTarget(detail) => {
                    report.events.push(event(
                        object,
                        stripe_index,
                        Some(shard),
                        MaintenanceOutcome::NoTarget,
                        None,
                        detail,
                    ));
                    break;
                }
            }
        }
        Ok(report)
    }

    /// Alias for [`Cluster::upgrade_checksums`].
    pub fn upgrade_fragment_checksums(
        &self,
        key: Option<&str>,
    ) -> Result<MaintenanceReport> {
        self.upgrade_checksums(key)
    }

    /// Alias for [`Cluster::upgrade_checksums`].
    pub fn rechecksum(&self, key: Option<&str>) -> Result<MaintenanceReport> {
        self.upgrade_checksums(key)
    }

    /// Alias for [`Cluster::upgrade_checksums`].
    pub fn migrate_checksums(&self, key: Option<&str>) -> Result<MaintenanceReport> {
        self.upgrade_checksums(key)
    }

    /// Short alias for [`Cluster::upgrade_checksums`].
    pub fn upgrade(&self, key: Option<&str>) -> Result<MaintenanceReport> {
        self.upgrade_checksums(key)
    }

    /// Noun-first alias for [`Cluster::upgrade_checksums`].
    pub fn checksum_upgrade(&self, key: Option<&str>) -> Result<MaintenanceReport> {
        self.upgrade_checksums(key)
    }

    /// Upgrade every object; convenience wrapper used by the CLI.
    pub fn upgrade_all_checksums(&self) -> Result<MaintenanceReport> {
        self.upgrade_checksums(None)
    }
}

/// Locate the next CRC-32C fragment eligible for a strong-checksum upgrade.
fn first_legacy_fragment(
    catalog: &Catalog,
    key: Option<&str>,
) -> Result<Option<(String, u64, u32, u8)>> {
    let objects = selected(catalog, key)?;
    for object in objects {
        for stripe in &object.stripes {
            for fragment in &stripe.fragments {
                if fragment.algorithm == ChecksumAlgorithm::Crc32c {
                    return Ok(Some((
                        object.key.clone(),
                        object.generation,
                        stripe.index,
                        fragment.shard,
                    )));
                }
            }
        }
    }
    Ok(None)
}

pub(super) fn selected<'a>(
    catalog: &'a Catalog,
    key: Option<&str>,
) -> Result<Vec<&'a ObjectGeneration>> {
    if let Some(key) = key {
        checked_key(key)?;
        let object = catalog
            .objects
            .get(key)
            .ok_or_else(|| Error::NotFound(format!("object {key:?}")))?;
        return Ok(vec![object]);
    }
    Ok(catalog.objects.values().collect())
}

fn choices(
    catalog: &Catalog,
    inspection: &StripeInspection,
    kind: RepairKind,
) -> Vec<(u8, ReplacementPolicy)> {
    let faults = &inspection.faults;
    if kind == RepairKind::Heal {
        return faults
            .iter()
            .map(|fault| (fault.address.shard, ReplacementPolicy::Heal))
            .collect();
    }
    let mut evacuations: Vec<_> = inspection
        .manifest
        .fragments
        .iter()
        .filter(|receipt| !catalog.nodes[&receipt.node].state.can_place())
        .map(|receipt| (receipt.shard, ReplacementPolicy::Evacuate))
        .collect();
    if !evacuations.is_empty() {
        return evacuations;
    }
    if !faults.is_empty() || inspection.domains.survives_any_single_domain_loss() {
        return Vec::new();
    }
    for receipt in &inspection.manifest.fragments {
        let domain = &catalog.nodes[&receipt.node].domain;
        if inspection.domains.domains[domain] > 1 {
            evacuations.push((receipt.shard, ReplacementPolicy::ImproveDomains));
        }
    }
    evacuations
}

fn event(
    object: &ObjectGeneration,
    stripe: u32,
    shard: Option<u8>,
    outcome: MaintenanceOutcome,
    attempt: Option<u64>,
    detail: impl Into<String>,
) -> MaintenanceEvent {
    MaintenanceEvent {
        key: object.key.clone(),
        generation: object.generation,
        stripe,
        shard,
        outcome,
        attempt,
        detail: detail.into(),
    }
}

fn attempt_event(object: &ObjectGeneration, attempt: &RepairAttempt) -> MaintenanceEvent {
    let outcome = match attempt.state {
        RepairState::Planned => MaintenanceOutcome::Pending,
        RepairState::Published => MaintenanceOutcome::Published,
        RepairState::Stale => MaintenanceOutcome::Changed,
        RepairState::Unrecoverable => MaintenanceOutcome::Unrecoverable,
    };
    event(
        object,
        attempt.stripe,
        Some(attempt.from.shard),
        outcome,
        Some(attempt.id),
        attempt.detail.clone(),
    )
}
