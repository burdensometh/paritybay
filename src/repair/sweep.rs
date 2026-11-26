use std::fmt;

use crate::manifest::checked_key;
use crate::placement::ReplacementPolicy;
use crate::readpath::{StripeInspection, StripeVerdict};
use crate::{
    Catalog, Cluster, Error, ObjectGeneration, RepairAttempt, RepairKind, RepairState, Result,
};

use super::budget::RepairBudget;
use super::{CollectionReport, Planning};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ScanTotals {
    pub objects: usize,
    pub healthy: usize,
    pub recoverable: usize,
    pub unrecoverable: usize,
    pub parity_mismatch: usize,
}

/// Capacity-accounting work performed by one maintenance round. A round that
/// repairs several stripes builds the full per-node totals once and then keeps
/// a single ledger current incrementally.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BudgetReport {
    /// Full-directory ledger constructions, including the initial build.
    pub ledger_builds: usize,
    /// Rebuilds forced because another writer changed the catalog mid-round.
    pub ledger_refreshes: usize,
    /// Publication-time capacity and eligibility rechecks.
    pub publication_rechecks: usize,
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
    pub budget: BudgetReport,
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
            budget: BudgetReport::default(),
        };

        // One budget serves the whole round: each planned copy is booked into
        // the shared ledger, so repairing many stripes costs one full catalog
        // aggregation instead of one per replacement search.
        let mut budget = None;
        for snapshot in objects {
            for original_stripe in &snapshot.stripes {
                let inspected = self.inspect_stripe(&catalog, snapshot.generation, original_stripe);
                report.scan.count(inspected.verdict);
                self.maintain_stripe(
                    snapshot,
                    &catalog,
                    inspected,
                    &mut budget,
                    &mut report,
                )?;
            }
        }
        if let Some(budget) = &budget {
            report.budget = BudgetReport {
                ledger_builds: budget.full_builds,
                ledger_refreshes: budget.refreshes,
                publication_rechecks: budget.rechecks,
            };
        }
        Ok(report)
    }

    fn maintain_stripe(
        &self,
        snapshot: &ObjectGeneration,
        initial: &Catalog,
        inspected: StripeInspection,
        budget: &mut Option<RepairBudget>,
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
                let round_budget = match budget.as_mut() {
                    Some(round_budget) => round_budget,
                    None => budget.get_or_insert(RepairBudget::open(catalog)?),
                };
                match self.plan_repair(
                    object,
                    stripe,
                    shard,
                    kind,
                    policy,
                    Some(round_budget),
                )? {
                    Planning::Ready(attempt) => {
                        let attempt = self.run_repair_with_budget(
                            attempt.id,
                            Some(budget.as_mut().expect("opened above")),
                        )?;
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
}

fn selected<'a>(catalog: &'a Catalog, key: Option<&str>) -> Result<Vec<&'a ObjectGeneration>> {
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
