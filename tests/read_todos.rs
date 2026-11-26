mod common;

use std::fs;
use std::thread;

use paritybay::readpath::{FaultKind, RangeRequest};
use paritybay::repair::MaintenanceOutcome;
use paritybay::{Boundary, Checkpoint, Error, NodeState, RepairKind, RepairState, Result};

use common::{address, config, get, path, put, Workspace};

struct FailAt(Boundary, &'static str);

impl Checkpoint for FailAt {
    fn reach(&self, boundary: Boundary, detail: &str) -> Result<()> {
        if boundary == self.0 && detail == self.1 {
            return Err(Error::Conflict(format!("forced failure at {detail}")));
        }
        Ok(())
    }
}

#[test]
fn small_range_reads_recover_and_file_a_single_todo() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let data = b"01234567abcdefgh";
    let generation = put(&cluster, "todos", data);
    fs::remove_file(path(&cluster, "todos", 0, 0)).unwrap();

    let mut todo = None;
    for (range, expected) in [
        (RangeRequest::Span { start: 0, end: 4 }, &data[0..4]),
        (RangeRequest::Span { start: 2, end: 6 }, &data[2..6]),
        (RangeRequest::Span { start: 4, end: 8 }, &data[4..8]),
    ] {
        let (bytes, report) = get(&cluster, "todos", range);
        assert_eq!(bytes, expected);
        assert_eq!(report.stripes[0].reconstructed, vec![0]);
        assert_eq!(report.repairs.len(), 1);
        assert_eq!(report.repairs[0].stripe, 0);
        assert_eq!(report.repairs[0].shard, 0);
        match todo {
            None => todo = Some(report.repairs[0].attempt),
            Some(id) => assert_eq!(report.repairs[0].attempt, id),
        }
    }
    let todo = todo.unwrap();

    // One durable planned heal attempt, keyed to the lost fragment revision.
    let repairs = cluster.catalog().unwrap().repairs;
    assert_eq!(repairs.len(), 1);
    let attempt = &repairs[&todo];
    assert_eq!(attempt.state, RepairState::Planned);
    assert_eq!(attempt.kind, RepairKind::Heal);
    assert_eq!(attempt.generation, generation);
    assert_eq!(attempt.stripe, 0);
    assert_eq!(attempt.from.shard, 0);
    assert_eq!(attempt.from.revision, generation);

    // The later inspection finds the queued todo and executes it.
    let report = cluster.heal(Some("todos")).unwrap();
    assert!(report.events.iter().any(|event| {
        event.outcome == MaintenanceOutcome::Published && event.attempt == Some(todo)
    }));
    assert_eq!(get(&cluster, "todos", RangeRequest::Full).0, data);
    let scan = cluster.scan(None).unwrap();
    assert_eq!(scan.healthy, 1);
    assert_eq!(scan.recoverable, 0);
}

#[test]
fn concurrent_degraded_reads_file_one_todo() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let data = b"01234567abcdefgh";
    put(&cluster, "racy", data);
    fs::remove_file(path(&cluster, "racy", 0, 1)).unwrap();

    let mut handles = Vec::new();
    for _ in 0..4 {
        let reader = cluster.clone();
        handles.push(thread::spawn(move || get(&reader, "racy", RangeRequest::Full)));
    }
    let mut attempts = Vec::new();
    for handle in handles {
        let (bytes, report) = handle.join().unwrap();
        assert_eq!(bytes, data);
        assert_eq!(report.repairs.len(), 1);
        attempts.push(report.repairs[0].attempt);
    }
    assert!(attempts.iter().all(|id| *id == attempts[0]));
    let repairs = cluster.catalog().unwrap().repairs;
    assert_eq!(repairs.len(), 1);
    assert_eq!(repairs[&attempts[0]].state, RepairState::Planned);
}

#[test]
fn unreadable_node_is_reported_inaccessible_without_a_todo() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let data = b"01234567abcdefgh";
    put(&cluster, "offline", data);
    let address = address(&cluster, "offline", 0, 0);
    cluster
        .set_node_state(&address.node, NodeState::Offline)
        .unwrap();

    let (bytes, report) = get(&cluster, "offline", RangeRequest::Full);
    assert_eq!(bytes, data);
    assert_eq!(report.stripes[0].reconstructed, vec![0]);
    assert!(report.repairs.is_empty());
    assert_eq!(report.inaccessible.len(), 1);
    assert_eq!(report.inaccessible[0].kind, FaultKind::NodeUnavailable);
    assert_eq!(report.inaccessible[0].address.node, address.node);
    assert!(cluster.catalog().unwrap().repairs.is_empty());

    cluster
        .set_node_state(&address.node, NodeState::Online)
        .unwrap();
    let (bytes, report) = get(&cluster, "offline", RangeRequest::Full);
    assert_eq!(bytes, data);
    assert!(report.stripes[0].reconstructed.is_empty());
    assert!(report.repairs.is_empty());
    assert!(report.inaccessible.is_empty());
}

#[test]
fn corrupt_fragment_files_a_todo_and_replay_republishes_it() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let data = b"01234567abcdefgh";
    put(&cluster, "corrupt", data);
    let original = address(&cluster, "corrupt", 0, 0);
    fs::write(cluster.fragment_path(&original).unwrap(), b"0123456x").unwrap();

    let (bytes, report) = get(&cluster, "corrupt", RangeRequest::Span { start: 0, end: 8 });
    assert_eq!(bytes, &data[..8]);
    assert_eq!(report.repairs.len(), 1);
    let fault = &report.stripes[0].faults[0];
    assert_eq!(fault.kind, FaultKind::InvalidChecksum);

    let finished = cluster.replay_repairs().unwrap();
    assert_eq!(finished.len(), 1);
    assert_eq!(finished[0].state, RepairState::Published);
    assert_eq!(finished[0].id, report.repairs[0].attempt);
    assert_ne!(address(&cluster, "corrupt", 0, 0), original);
    assert_eq!(get(&cluster, "corrupt", RangeRequest::Full).0, data);
}

#[test]
fn todo_goes_stale_after_generation_change_without_rewriting() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let original = put(&cluster, "stale", b"01234567abcdefgh");
    fs::remove_file(path(&cluster, "stale", 0, 0)).unwrap();
    let (_, report) = get(&cluster, "stale", RangeRequest::Full);
    let todo = report.repairs[0].attempt;

    let current = put(&cluster, "stale", b"AAAAAAAAAAAAAAAA");
    assert_ne!(original, current);

    let finished = cluster.replay_repairs().unwrap();
    assert_eq!(finished.len(), 1);
    assert_eq!(finished[0].id, todo);
    assert_eq!(finished[0].state, RepairState::Stale);

    // The new generation's mapping is untouched and reads back.
    let object = cluster.object("stale").unwrap();
    assert_eq!(object.generation, current);
    assert_eq!(object.stripes[0].fragments[0].revision, current);
    assert_eq!(get(&cluster, "stale", RangeRequest::Full).0, b"AAAAAAAAAAAAAAAA");
    assert_eq!(cluster.scan(None).unwrap().healthy, 1);
}

#[test]
fn todo_persistence_failure_keeps_the_successful_read() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let data = b"01234567abcdefgh";
    put(&cluster, "durable", data);
    fs::remove_file(path(&cluster, "durable", 0, 0)).unwrap();

    let broken = cluster
        .clone()
        .with_checkpoints(FailAt(Boundary::BeforeCatalogCommit, "repair-plan"));
    let (bytes, report) = get(&broken, "durable", RangeRequest::Span { start: 0, end: 8 });
    assert_eq!(bytes, &data[..8]);
    assert!(report.repairs.is_empty());
    assert!(cluster.catalog().unwrap().repairs.is_empty());

    // A later read files the todo once persistence works again.
    let (bytes, report) = get(&cluster, "durable", RangeRequest::Span { start: 0, end: 8 });
    assert_eq!(bytes, &data[..8]);
    assert_eq!(report.repairs.len(), 1);
    assert_eq!(cluster.catalog().unwrap().repairs.len(), 1);
}
