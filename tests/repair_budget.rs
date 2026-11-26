mod common;

use std::fs;
use std::sync::mpsc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use paritybay::readpath::RangeRequest;
use paritybay::repair::MaintenanceOutcome;
use paritybay::{
    Boundary, Checkpoint, Cluster, Config, Error, Media, NodeDomain, NodeState, RepairState,
    Result,
};

use common::{get, options, put, Workspace};

const WAIT: Duration = Duration::from_secs(15);

fn six_nodes() -> Vec<NodeDomain> {
    vec![
        node("hot-a", "az1"),
        node("hot-b", "az2"),
        node("hot-c", "az3"),
        node("hot-d", "az1"),
        node("hot-e", "az2"),
        node("hot-f", "az3"),
    ]
}

fn node(id: &str, domain: &str) -> NodeDomain {
    NodeDomain {
        id: id.into(),
        domain: domain.into(),
        media: Media::Hot,
        capacity: 64 * 1024,
        state: NodeState::Online,
    }
}

fn cluster_config(nodes: Vec<NodeDomain>) -> Config {
    Config {
        block_bytes: 8,
        staging_limit: 4 * 1024 * 1024,
        nodes,
    }
}

fn usage(cluster: &Cluster, id: &str) -> paritybay::placement::Allocation {
    cluster
        .node_usage()
        .unwrap()
        .into_iter()
        .find(|entry| entry.node.id == id)
        .unwrap()
        .allocation
}

struct Pause {
    boundary: Boundary,
    reached: mpsc::Sender<()>,
    resume: Mutex<mpsc::Receiver<()>>,
}

impl Checkpoint for Pause {
    fn reach(&self, boundary: Boundary, _: &str) -> Result<()> {
        if boundary == self.boundary {
            self.reached
                .send(())
                .map_err(|error| Error::Conflict(error.to_string()))?;
            self.resume
                .lock()
                .unwrap()
                .recv_timeout(WAIT)
                .map_err(|error| Error::Conflict(error.to_string()))?;
        }
        Ok(())
    }
}

fn paused(cluster: &Cluster, boundary: Boundary) -> (Cluster, mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (reached_tx, reached_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let cluster = cluster.clone().with_checkpoints(Pause {
        boundary,
        reached: reached_tx,
        resume: Mutex::new(resume_rx),
    });
    (cluster, reached_rx, resume_tx)
}

#[test]
fn one_heal_round_aggregates_the_full_directory_once() {
    let dir = Workspace::new();
    let cluster = dir.create(cluster_config(six_nodes()));
    // Three objects, each one stripe: deleting a shard from each makes three
    // independent repairs that previously paid for three full ledgers.
    for key in ["alpha", "bravo", "charlie"] {
        put(&cluster, key, b"01234567abcdefgh");
    }
    for key in ["alpha", "bravo", "charlie"] {
        let address = common::address(&cluster, key, 0, 0);
        fs::remove_file(cluster.fragment_path(&address).unwrap()).unwrap();
    }

    let report = cluster.heal(None).unwrap();
    let published = report
        .events
        .iter()
        .filter(|event| event.outcome == MaintenanceOutcome::Published)
        .count();
    assert_eq!(published, 3);
    assert_eq!(report.budget.ledger_builds, 1);
    assert_eq!(report.budget.ledger_refreshes, 0);
    assert_eq!(report.budget.publication_rechecks, 3);

    // A second round with nothing to repair never opens a budget at all.
    let idle = cluster.heal(None).unwrap();
    assert_eq!(idle.budget.ledger_builds, 0);
    assert!(idle.events.is_empty());

    for key in ["alpha", "bravo", "charlie"] {
        assert_eq!(
            get(&cluster, key, RangeRequest::Full).0,
            b"01234567abcdefgh"
        );
        let inspection = cluster.inspect(key).unwrap();
        assert_eq!(inspection.stripes[0].verdict.to_string(), "healthy");
    }
}

#[test]
fn reservations_planned_repairs_and_retired_bytes_all_charge_the_budget() {
    let dir = Workspace::new();
    let cluster = dir.create(cluster_config(six_nodes()));
    put(&cluster, "obj", b"01234567abcdefgh");

    // A staged multipart upload reserves every stripe placement up front.
    let session = cluster
        .begin_upload(
            "staged",
            options(16),
        )
        .unwrap();
    let reserved_node = session.placements[0].nodes[0].clone();
    let before = usage(&cluster, &reserved_node);
    assert!(before.uploads > 0, "upload reservations charge the ledger");

    // A planned repair charges its target node before the copy runs.
    let address = common::address(&cluster, "obj", 0, 0);
    fs::remove_file(cluster.fragment_path(&address).unwrap()).unwrap();
    let (worker, reached, resume) = paused(&cluster, Boundary::BeforeRepairCopy);
    let handle = thread::spawn(move || worker.heal(Some("obj")).unwrap());
    reached.recv_timeout(WAIT).unwrap();

    let catalog = cluster.catalog().unwrap();
    let planned = catalog
        .repairs
        .values()
        .find(|attempt| attempt.state == RepairState::Planned)
        .unwrap();
    let planned_target = planned.to.node.clone();
    let after_plan = usage(&cluster, &planned_target);
    assert!(after_plan.repairs >= planned.to.bytes as u64);

    resume.send(()).unwrap();
    let report = handle.join().unwrap();
    assert_eq!(
        report.events[0].outcome,
        MaintenanceOutcome::Published
    );

    // The source fragment is retired and keeps charging until collection.
    let source = planned.from.node.clone();
    let source_usage = usage(&cluster, &source);
    assert!(source_usage.retired >= planned.from.bytes as u64);
    let target_usage = usage(&cluster, &target_for(&cluster, "obj"));
    assert!(target_usage.current >= planned.to.bytes as u64);
    assert_eq!(target_usage.repairs, 0);

    // Collection clears the retired charge without disturbing current data.
    let collected = cluster.collect().unwrap();
    assert!(collected.retired_entries >= 1);
    assert_eq!(usage(&cluster, &source).retired, 0);

    cluster.abort_upload(session.id).unwrap();
    assert_eq!(usage(&cluster, &reserved_node).uploads, before.uploads - 8);
}

fn target_for(cluster: &Cluster, key: &str) -> String {
    cluster.object(key).unwrap().stripes[0].fragments[0]
        .node
        .clone()
}

#[test]
fn publication_recheck_marks_the_repair_stale_when_capacity_disappears() {
    let dir = Workspace::new();
    let cluster = dir.create(cluster_config(six_nodes()));
    put(&cluster, "obj", b"01234567abcdefgh");
    let address = common::address(&cluster, "obj", 0, 0);
    fs::remove_file(cluster.fragment_path(&address).unwrap()).unwrap();

    let (worker, reached, resume) = paused(&cluster, Boundary::BeforeRepairCopy);
    let handle = thread::spawn(move || worker.heal(Some("obj")).unwrap());
    reached.recv_timeout(WAIT).unwrap();

    // Another process shrinks the target budget after the plan was booked.
    let planned = cluster
        .catalog()
        .unwrap()
        .repairs
        .values()
        .find(|attempt| attempt.state == RepairState::Planned)
        .unwrap()
        .clone();
    let allocated = cluster
        .node_usage()
        .unwrap()
        .into_iter()
        .find(|entry| entry.node.id == planned.to.node)
        .unwrap()
        .allocation
        .total();
    cluster
        .set_node_capacity(&planned.to.node, allocated)
        .unwrap();
    resume.send(()).unwrap();

    let report = handle.join().unwrap();
    assert_eq!(report.events[0].outcome, MaintenanceOutcome::Changed);
    assert_eq!(report.budget.publication_rechecks, 1);

    let attempt = cluster
        .repair_attempt(report.events[0].attempt.unwrap())
        .unwrap();
    assert_eq!(attempt.state, RepairState::Stale);
    assert!(attempt.detail.contains("capacity"));
    // The mapping was never published and the reserved copy is retired.
    let generation = cluster.object("obj").unwrap().generation;
    assert_eq!(
        common::address(&cluster, "obj", 0, 0),
        paritybay::manifest::FragmentAddress::from_receipt(generation, 0, &planned.from)
    );

    // Restoring the budget lets a fresh round heal the stripe.
    cluster.set_node_capacity(&planned.to.node, 64 * 1024).unwrap();
    let retry = cluster.heal(Some("obj")).unwrap();
    assert_eq!(retry.events[0].outcome, MaintenanceOutcome::Published);
    assert_eq!(
        get(&cluster, "obj", RangeRequest::Full).0,
        b"01234567abcdefgh"
    );
}

#[test]
fn foreign_reservation_between_repairs_forces_one_refresh_and_stays_correct() {
    let dir = Workspace::new();
    let cluster = dir.create(cluster_config(six_nodes()));
    // Two stripes with stable, distinct node sets per stripe.
    let payload = b"01234567abcdefghABCDEFGH87654321";
    put(&cluster, "twin", payload);
    assert_eq!(cluster.object("twin").unwrap().stripes.len(), 2);

    // Drain one node that hosts one shard on each stripe. Evacuating it
    // produces two repairs inside a single rebalance round.
    let drain_node = common::address(&cluster, "twin", 0, 0).node.clone();
    let other_stripe_node = common::address(&cluster, "twin", 1, 0).node.clone();
    assert_ne!(drain_node, other_stripe_node);
    cluster
        .set_node_state(&drain_node, NodeState::Draining)
        .unwrap();

    // Pause the round at each repair copy. The first pause lets another
    // process start an upload while the cached ledger exists; a helper thread
    // then releases every further pause so the same round continues.
    let (worker, reached, resume) = paused(&cluster, Boundary::BeforeRepairCopy);
    let handle = thread::spawn(move || worker.rebalance(Some("twin")).unwrap());
    reached.recv_timeout(WAIT).unwrap();
    let session = cluster
        .begin_upload("interloper", options(16))
        .unwrap();
    resume.send(()).unwrap();
    let releaser = thread::spawn(move || {
        while reached.recv_timeout(WAIT).is_ok() && resume.send(()).is_ok() {}
    });

    let report = handle.join().unwrap();
    releaser.join().unwrap();
    let published = report
        .events
        .iter()
        .filter(|event| event.outcome == MaintenanceOutcome::Published)
        .count();
    assert_eq!(published, 2);
    // Initial build plus exactly one foreign-invalidation rebuild.
    assert_eq!(report.budget.ledger_builds, 2);
    assert_eq!(report.budget.ledger_refreshes, 1);

    // The cached ledger after the rebuild matches a fresh full aggregation.
    let catalog = cluster.catalog().unwrap();
    let fresh = paritybay::placement::Ledger::from_catalog(&catalog, None).unwrap();
    for entry in cluster.node_usage().unwrap() {
        assert_eq!(entry.allocation, fresh.for_node(&entry.node.id));
    }
    cluster.abort_upload(session.id).unwrap();
    assert_eq!(
        get(&cluster, "twin", RangeRequest::Full).0,
        payload
    );
}

#[test]
fn healing_preserves_domain_and_media_selection_behavior() {
    let dir = Workspace::new();
    let cluster = dir.create(cluster_config(six_nodes()));
    put(&cluster, "obj", b"01234567abcdefgh");
    let address = common::address(&cluster, "obj", 0, 0);
    fs::remove_file(cluster.fragment_path(&address).unwrap()).unwrap();

    let report = cluster.heal(Some("obj")).unwrap();
    assert_eq!(report.events[0].outcome, MaintenanceOutcome::Published);
    let after = cluster.inspect("obj").unwrap();
    let catalog = cluster.catalog().unwrap();
    let domains: std::collections::BTreeSet<_> = after.stripes[0]
        .manifest
        .fragments
        .iter()
        .map(|fragment| catalog.nodes[&fragment.node].domain.as_str())
        .collect();
    assert_eq!(domains.len(), 3, "strict policy keeps three domains");
    let media: std::collections::BTreeSet<_> = after.stripes[0]
        .manifest
        .fragments
        .iter()
        .map(|fragment| catalog.nodes[&fragment.node].media)
        .collect();
    assert_eq!(media, std::collections::BTreeSet::from([Media::Hot]));
    // In-place healing on an eligible original node is an allowed baseline
    // outcome; the rebuilt stripe is healthy either way.
    assert_eq!(after.stripes[0].verdict.to_string(), "healthy");
    assert_eq!(
        get(&cluster, "obj", RangeRequest::Full).0,
        b"01234567abcdefgh"
    );
}
