#[cfg(test)]
use std::collections::BTreeMap;

use crate::manifest::Catalog;
#[cfg(test)]
use crate::placement::Allocation;
use crate::placement::Ledger;
use crate::{NodeState, RepairState, Result};

/// Cheap summary of every catalog input that can move a node's byte budget
/// or placement eligibility. A matching fingerprint means the cached ledger
/// still describes the catalog; a changed one forces a single full rebuild.
/// Walking this summary stays proportional to header records, never to the
/// total fragment count that dominates `Ledger::from_catalog`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BudgetFingerprint {
    next_id: u64,
    nodes: Vec<(String, NodeState, u64, u8)>,
    objects: Vec<(String, u64, usize)>,
    uploads: Vec<(u64, u64, Vec<[String; 3]>)>,
    planned_repairs: Vec<(u64, String, String, u32)>,
    retired: Vec<(String, u32)>,
}

impl BudgetFingerprint {
    fn of(catalog: &Catalog) -> Self {
        Self {
            next_id: catalog.next_id,
            nodes: catalog
                .nodes
                .values()
                .map(|node| {
                    (
                        node.id.clone(),
                        node.state,
                        node.capacity,
                        node.media as u8,
                    )
                })
                .collect(),
            objects: catalog
                .objects
                .values()
                .map(|object| {
                    (
                        object.key.clone(),
                        object.generation,
                        object.stripes.len(),
                    )
                })
                .collect(),
            uploads: catalog
                .uploads
                .values()
                .map(|upload| {
                    (
                        upload.id,
                        upload.expected_bytes,
                        upload.placements.iter().map(|plan| plan.nodes.clone()).collect(),
                    )
                })
                .collect(),
            planned_repairs: catalog
                .repairs
                .values()
                .filter(|attempt| attempt.state == RepairState::Planned)
                .map(|attempt| {
                    (
                        attempt.id,
                        attempt.from.node.clone(),
                        attempt.to.node.clone(),
                        attempt.to.bytes,
                    )
                })
                .collect(),
            retired: catalog
                .retired
                .iter()
                .map(|entry| (entry.address.node.clone(), entry.bytes))
                .collect(),
        }
    }
}

/// Per-node totals observed by one maintenance round. The ledger is built at
/// most once per catalog generation: every plan and publication made by this
/// round is mirrored incrementally, while other processes invalidate it
/// through the fingerprint.
#[derive(Debug)]
pub(super) struct RepairBudget {
    ledger: Ledger,
    fingerprint: BudgetFingerprint,
    pub(super) full_builds: usize,
    pub(super) refreshes: usize,
    pub(super) rechecks: usize,
}

impl RepairBudget {
    pub(super) fn open(catalog: &Catalog) -> Result<Self> {
        let ledger = Ledger::from_catalog(catalog, None)?;
        Ok(Self {
            ledger,
            fingerprint: BudgetFingerprint::of(catalog),
            full_builds: 1,
            refreshes: 0,
            rechecks: 0,
        })
    }

    /// Rebuild only when another writer may have changed the inputs; local
    /// plan commits refresh the fingerprint from the committed catalog.
    pub(super) fn sync(&mut self, catalog: &Catalog) -> Result<&Ledger> {
        let now = BudgetFingerprint::of(catalog);
        if now != self.fingerprint {
            self.ledger = Ledger::from_catalog(catalog, None)?;
            self.fingerprint = now;
            self.full_builds += 1;
            self.refreshes += 1;
        }
        Ok(&self.ledger)
    }

    pub(super) fn ledger_mut(&mut self) -> &mut Ledger {
        &mut self.ledger
    }

    pub(super) fn note_recheck(&mut self) {
        self.rechecks += 1;
    }

    /// Adopt the fingerprint of a catalog this round just committed, so its
    /// own publications never look like foreign invalidations.
    pub(super) fn adopt(&mut self, catalog: &Catalog) {
        self.fingerprint = BudgetFingerprint::of(catalog);
    }

    #[cfg(test)]
    pub(super) fn allocations(&self) -> BTreeMap<String, Allocation> {
        self.ledger.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{
        FragmentAddress, FragmentReceipt, ObjectGeneration, RepairAttempt, RepairKind,
        RetiredFragment, StripeManifest,
    };
    use crate::{DomainPolicy, Media, NodeDomain, NodeState};

    const BLOCK: u32 = 8;

    fn node(id: &str, domain: &str) -> NodeDomain {
        NodeDomain {
            id: id.into(),
            domain: domain.into(),
            media: Media::Hot,
            capacity: 4096,
            state: NodeState::Online,
        }
    }

    fn catalog() -> Catalog {
        Catalog {
            block_bytes: BLOCK,
            staging_limit: 1_048_576,
            next_id: 100,
            nodes: [
                node("n0", "d0"),
                node("n1", "d1"),
                node("n2", "d2"),
                node("n3", "d3"),
            ]
            .into_iter()
            .map(|entry| (entry.id.clone(), entry))
            .collect(),
            uploads: BTreeMap::new(),
            objects: BTreeMap::new(),
            repairs: BTreeMap::new(),
            retired: Vec::new(),
        }
    }

    fn receipt(shard: u8, node: &str, bytes: u32) -> FragmentReceipt {
        FragmentReceipt {
            shard,
            node: node.into(),
            revision: 7,
            bytes,
            crc32c: 0,
        }
    }

    fn object(catalog: &mut Catalog, key: &str, nodes: [&str; 3], bytes: u32) {
        let stripe = StripeManifest {
            index: 0,
            offset: 0,
            data_bytes: [bytes, bytes],
            fragments: [
                receipt(0, nodes[0], bytes),
                receipt(1, nodes[1], bytes),
                receipt(2, nodes[2], bytes),
            ],
        };
        catalog.objects.insert(
            key.into(),
            ObjectGeneration {
                key: key.into(),
                generation: 7,
                size: u64::from(bytes) * 2,
                media: Media::Hot,
                domain_policy: DomainPolicy::Strict,
                stripes: vec![stripe],
            },
        );
    }

    fn planned(id: u64, from: &str, to_node: &str, bytes: u32) -> RepairAttempt {
        let from = receipt(0, from, bytes);
        let mut to = from.clone();
        to.node = to_node.into();
        to.revision = id;
        RepairAttempt {
            id,
            key: "obj".into(),
            generation: 7,
            stripe: 0,
            from,
            to,
            kind: RepairKind::Heal,
            state: RepairState::Planned,
            detail: String::new(),
        }
    }

    #[test]
    fn unchanged_catalog_skips_full_rebuild() {
        let base = catalog();
        let mut budget = RepairBudget::open(&base).unwrap();
        assert_eq!(budget.full_builds, 1);
        budget.sync(&base).unwrap();
        assert_eq!(budget.full_builds, 1);
        assert_eq!(budget.refreshes, 0);
    }

    #[test]
    fn foreign_catalog_change_forces_one_rebuild_then_matches_again() {
        let base = catalog();
        let mut budget = RepairBudget::open(&base).unwrap();
        let mut changed = base.clone();
        changed.next_id += 1;
        budget.sync(&changed).unwrap();
        assert_eq!(budget.full_builds, 2);
        assert_eq!(budget.refreshes, 1);
        // The adopted fingerprint means another identical view stays cached.
        budget.sync(&changed).unwrap();
        assert_eq!(budget.full_builds, 2);
    }

    #[test]
    fn local_plan_books_incrementally_and_tracks_a_fresh_ledger() {
        let mut view = catalog();
        object(&mut view, "obj", ["n0", "n1", "n2"], 8);
        let mut budget = RepairBudget::open(&view).unwrap();

        // Simulate committing a planned repair onto n3.
        let attempt = planned(20, "n0", "n3", 8);
        view.repairs.insert(attempt.id, attempt.clone());
        view.next_id = 21;
        budget
            .ledger_mut()
            .book_repair(&attempt.to.node, attempt.to.bytes)
            .unwrap();
        budget.adopt(&view);

        let reference = Ledger::from_catalog(&view, None).unwrap();
        assert_eq!(budget.allocations(), reference.snapshot());
        let target = budget.allocations()["n3"];
        assert_eq!((target.repairs, target.current), (8, 0));

        // Simulate publication: target reservation becomes current and the
        // source current fragment moves into the retired bucket.
        budget
            .ledger_mut()
            .shift_repair_to_current("n3", 8)
            .unwrap();
        budget
            .ledger_mut()
            .shift_current_to_retired("n0", 8)
            .unwrap();
        let mut published = view.clone();
        let entry = published.repairs.get_mut(&20).unwrap();
        entry.state = RepairState::Published;
        published.objects.get_mut("obj").unwrap().stripes[0].fragments[0] =
            attempt.to.clone();
        published.retired.push(RetiredFragment {
            address: FragmentAddress::from_receipt(7, 0, &attempt.from),
            bytes: 8,
        });
        budget.adopt(&published);

        let reference = Ledger::from_catalog(&published, None).unwrap();
        assert_eq!(budget.allocations(), reference.snapshot());
        let n0 = budget.allocations()["n0"];
        let n3 = budget.allocations()["n3"];
        assert_eq!((n0.current, n0.retired), (0, 8));
        assert_eq!((n3.current, n3.repairs), (8, 0));
    }

    #[test]
    fn multiple_local_reservations_accumulate_like_a_full_aggregation() {
        let mut view = catalog();
        object(&mut view, "a", ["n0", "n1", "n2"], 8);
        object(&mut view, "b", ["n3", "n1", "n2"], 4);
        let mut budget = RepairBudget::open(&view).unwrap();

        let first = planned(20, "n0", "n3", 8);
        view.repairs.insert(20, first.clone());
        view.next_id = 21;
        budget.ledger_mut().book_repair("n3", 8).unwrap();
        budget.adopt(&view);

        let second = planned(21, "n3", "n0", 4);
        view.repairs.insert(21, second.clone());
        view.next_id = 22;
        budget.ledger_mut().book_repair("n0", 4).unwrap();
        budget.adopt(&view);

        let reference = Ledger::from_catalog(&view, None).unwrap();
        assert_eq!(budget.allocations(), reference.snapshot());
        assert_eq!(budget.full_builds, 1);
    }

    #[test]
    fn stale_and_unrecoverable_decay_match_reference_buckets() {
        let mut view = catalog();
        object(&mut view, "obj", ["n0", "n1", "n2"], 8);
        let mut budget = RepairBudget::open(&view).unwrap();
        let attempt = planned(20, "n0", "n3", 8);
        view.repairs.insert(20, attempt.clone());
        budget.ledger_mut().book_repair("n3", 8).unwrap();
        budget.adopt(&view);

        // Stale: reserved target copy is retired and keeps charging.
        budget.ledger_mut().shift_repair_to_retired("n3", 8).unwrap();
        let mut stale = view.clone();
        stale.repairs.get_mut(&20).unwrap().state = RepairState::Stale;
        stale.retired.push(RetiredFragment {
            address: FragmentAddress::from_receipt(7, 0, &attempt.to),
            bytes: 8,
        });
        budget.adopt(&stale);
        let reference = Ledger::from_catalog(&stale, None).unwrap();
        assert_eq!(budget.allocations(), reference.snapshot());
        assert_eq!(budget.allocations()["n3"].retired, 8);

        // Unrecoverable on a fresh round: reservation is released outright.
        let mut other = catalog();
        object(&mut other, "obj", ["n0", "n1", "n2"], 8);
        other.repairs.insert(21, planned(21, "n0", "n3", 8));
        let mut budget = RepairBudget::open(&other).unwrap();
        budget.ledger_mut().release_repair("n3", 8).unwrap();
        other.repairs.get_mut(&21).unwrap().state = RepairState::Unrecoverable;
        budget.adopt(&other);
        let reference = Ledger::from_catalog(&other, None).unwrap();
        assert_eq!(budget.allocations(), reference.snapshot());
        assert_eq!(budget.allocations()["n3"].total(), 0);
    }
}
