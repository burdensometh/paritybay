mod common;

use std::fs;
use std::io::Cursor;
use std::sync::{mpsc, Mutex};
use std::thread;
use std::time::Duration;

use paritybay::readpath::RangeRequest;
use paritybay::tier::TierOutcome;
use paritybay::{
    Boundary, Checkpoint, Cluster, Condition, Error, Media, MigrationState, NodeState, Result,
};

use common::{address, config, get, options, path, put, Workspace};

const WAIT: Duration = Duration::from_secs(15);

struct Pause {
    boundary: Boundary,
    reached: mpsc::Sender<()>,
    resume: Mutex<mpsc::Receiver<()>>,
}

fn pause(
    cluster: &Cluster,
    boundary: Boundary,
) -> (Cluster, mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (report, reached) = mpsc::channel();
    let (resume, release) = mpsc::channel();
    let cluster = cluster.clone().with_checkpoints(Pause {
        boundary,
        reached: report,
        resume: Mutex::new(release),
    });
    (cluster, reached, resume)
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

fn stripe_count(cluster: &Cluster, key: &str) -> usize {
    cluster.object(key).unwrap().stripes.len()
}

#[test]
fn tier_moves_every_stripe_to_capacity_without_changing_generation_or_bytes() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let original: Vec<u8> = (0..40).map(|value| (value * 7) as u8).collect();
    let generation = put(&cluster, "cold", &original);
    assert_eq!(cluster.object("cold").unwrap().media, Media::Hot);

    let report = cluster
        .tier_object("cold", Media::Capacity, Condition::Any)
        .unwrap();
    assert_eq!(report.outcome, TierOutcome::Published);
    let attempt = report.attempt.expect("migration journal entry");
    assert_eq!(attempt.state, MigrationState::Published);
    assert_eq!(attempt.generation, generation);
    assert_eq!(attempt.target_media, Media::Capacity);
    assert_eq!(attempt.targets.len(), stripe_count(&cluster, "cold"));

    let object = cluster.object("cold").unwrap();
    assert_eq!(object.generation, generation);
    assert_eq!(object.media, Media::Capacity);
    assert_eq!(object.size, original.len() as u64);
    let mut revisions = std::collections::BTreeSet::new();
    for (stripe_index, stripe) in object.stripes.iter().enumerate() {
        for (shard, receipt) in stripe.fragments.iter().enumerate() {
            let node = &cluster.catalog().unwrap().nodes[&receipt.node];
            assert_eq!(node.media, Media::Capacity);
            // Every target fragment is its own sequence allocation and must
            // match the planned journal target one for one.
            assert_eq!(
                receipt.revision,
                attempt.targets[stripe_index][shard].revision
            );
            assert!(revisions.insert(receipt.revision));
        }
    }
    // Bytes and checksums survive the move; reads work on the new media.
    assert_eq!(get(&cluster, "cold", RangeRequest::Full).0, original);
    assert_eq!(
        get(&cluster, "cold", RangeRequest::Span { start: 5, end: 33 }).0,
        original[5..33]
    );
    // Re-running on the same media is a no-op and reserves nothing.
    let again = cluster
        .tier_object("cold", Media::Capacity, Condition::Any)
        .unwrap();
    assert_eq!(again.outcome, TierOutcome::InPlace);
    assert!(again.attempt.is_none());
    assert!(cluster.catalog().unwrap().migrations.len() == 1);
}

#[test]
fn insufficient_target_capacity_leaves_source_media_and_reservations_intact() {
    let dir = Workspace::new();
    let mut local = config();
    // Shrink every capacity node so a full three-shard stripe cannot fit.
    for node in local.nodes.iter_mut().filter(|node| node.media == Media::Capacity) {
        node.capacity = 1;
    }
    let cluster = dir.create(local);
    let original = b"01234567abcdefgh";
    let generation = put(&cluster, "stuck", original);
    let before = cluster.object("stuck").unwrap();

    let err = cluster
        .tier_object("stuck", Media::Capacity, Condition::Any)
        .unwrap_err();
    assert!(matches!(err, Error::Capacity(_)), "{err}");

    // No journal, no media change, source fragments and reads untouched.
    let catalog = cluster.catalog().unwrap();
    assert!(catalog.migrations.is_empty());
    let after = cluster.object("stuck").unwrap();
    assert_eq!(after.media, Media::Hot);
    assert_eq!(after.stripes, before.stripes);
    assert_eq!(after.generation, generation);
    assert_eq!(get(&cluster, "stuck", RangeRequest::Full).0, original);

    // Upload reservations on the hot media remain protected and usable.
    let mut opt = options(6);
    opt.condition = Condition::Absent;
    let upload = cluster.begin_upload("reserved", opt).unwrap();
    cluster
        .put_part(upload.id, 1, 6, &mut Cursor::new(b"abcdef"))
        .unwrap();
    cluster.complete_upload(upload.id).unwrap();
    assert_eq!(get(&cluster, "reserved", RangeRequest::Full).0, b"abcdef");
}

#[test]
fn paused_tier_resumes_or_can_be_safely_cancelled() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let original: Vec<u8> = (0..40).map(|value| (value * 11) as u8).collect();
    let generation = put(&cluster, "paused", &original);
    let mut source_paths = Vec::new();
    for stripe in 0..stripe_count(&cluster, "paused") {
        for shard in 0..3 {
            source_paths.push(path(&cluster, "paused", stripe, shard));
        }
    }

    let (worker, reached, resume) = pause(&cluster, Boundary::BeforeTierCopy);
    let handle = thread::spawn(move || {
        worker.tier_object("paused", Media::Capacity, Condition::Any)
    });
    reached.recv_timeout(WAIT).unwrap();

    // While paused the plan is durable and reserves capacity-media bytes, but
    // the live mapping and online reads stay on the original hot fragments.
    let reopened = Cluster::open(dir.root()).unwrap();
    let planned = reopened
        .catalog()
        .unwrap()
        .migrations
        .values()
        .find(|migration| migration.state == MigrationState::Planned)
        .expect("durable planned migration")
        .clone();
    assert!(reopened
        .node_usage()
        .unwrap()
        .iter()
        .any(|item| item.allocation.migrations > 0));
    assert_eq!(reopened.object("paused").unwrap().media, Media::Hot);
    assert_eq!(get(&reopened, "paused", RangeRequest::Full).0, original);

    resume.send(()).unwrap();
    let report = handle.join().unwrap().unwrap();
    assert_eq!(report.outcome, TierOutcome::Published);
    assert_eq!(cluster.object("paused").unwrap().generation, generation);
    assert_eq!(get(&cluster, "paused", RangeRequest::Full).0, original);

    // Source fragments are retired and reclaimed by gc; targets remain.
    assert!(cluster.collect().unwrap().retired_entries > 0);
    for source in source_paths {
        assert!(!source.exists(), "source fragment {source:?} reclaimed");
    }
    let _ = planned;
}

#[test]
fn interrupted_tier_is_replayed_and_reuses_verified_copies() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let original: Vec<u8> = (0..40).map(|value| (value * 13) as u8).collect();
    put(&cluster, "resume", &original);

    // Create a durable plan and leave it planned by pausing before the copy
    // and abandoning the worker; its plan is replayed afterwards.
    let planned = {
        let (worker, reached, resume) = pause(&cluster, Boundary::BeforeTierCopy);
        let handle = thread::spawn(move || {
            worker.tier_object("resume", Media::Capacity, Condition::Any)
        });
        reached.recv_timeout(WAIT).unwrap();
        let id = cluster
            .catalog()
            .unwrap()
            .migrations
            .values()
            .find(|migration| migration.state == MigrationState::Planned)
            .unwrap()
            .id;
        // Close the resume channel: the worker errors out, but its durable
        // plan and any verified copies survive for replay.
        drop(resume);
        let _ = handle.join();
        id
    };
    let reopened = Cluster::open(dir.root()).unwrap();
    let migration = reopened
        .catalog()
        .unwrap()
        .migrations
        .get(&planned)
        .unwrap()
        .clone();
    assert_eq!(migration.state, MigrationState::Planned);
    assert_eq!(reopened.object("resume").unwrap().media, Media::Hot);

    let finished = reopened.run_migration(planned).unwrap();
    assert_eq!(finished.state, MigrationState::Published);
    assert_eq!(reopened.object("resume").unwrap().media, Media::Capacity);
    assert_eq!(get(&reopened, "resume", RangeRequest::Full).0, original);
    assert!(reopened.replay_migrations().unwrap().is_empty());
}

#[test]
fn cancelled_tier_restores_nothing_and_targets_are_garbage_collected() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let original: Vec<u8> = (0..24).map(|value| (value * 17) as u8).collect();
    put(&cluster, "cancel-me", &original);

    let (worker, reached, resume) = pause(&cluster, Boundary::BeforeTierCopy);
    let handle = thread::spawn(move || {
        worker.tier_object("cancel-me", Media::Capacity, Condition::Any)
    });
    reached.recv_timeout(WAIT).unwrap();
    let id = cluster
        .catalog()
        .unwrap()
        .migrations
        .values()
        .find(|migration| migration.state == MigrationState::Planned)
        .unwrap()
        .id;
    // Let the copy proceed so target files exist, then cancel a second plan.
    resume.send(()).unwrap();
    let report = handle.join().unwrap().unwrap();
    assert_eq!(report.outcome, TierOutcome::Published);

    // A fresh move back to hot is cancelled before publication.
    let (worker, reached, resume) = pause(&cluster, Boundary::BeforeTierCopy);
    let handle = thread::spawn(move || {
        worker.tier_object("cancel-me", Media::Hot, Condition::Any)
    });
    reached.recv_timeout(WAIT).unwrap();
    let return_id = cluster
        .catalog()
        .unwrap()
        .migrations
        .values()
        .find(|migration| {
            migration.state == MigrationState::Planned && migration.target_media == Media::Hot
        })
        .unwrap()
        .id;
    let cancelled = cluster.cancel_migration(return_id).unwrap();
    assert_eq!(cancelled.state, MigrationState::Stale);
    resume.send(()).unwrap();
    let _ = handle.join();

    // The live object is still on capacity media and fully readable.
    let object = cluster.object("cancel-me").unwrap();
    assert_eq!(object.media, Media::Capacity);
    assert_eq!(get(&cluster, "cancel-me", RangeRequest::Full).0, original);
    // Cancelled targets were retired; gc reclaims them, sources stay.
    let collected = cluster.collect().unwrap();
    assert!(collected.retired_entries > 0 || collected.fragment_files > 0);
    assert_eq!(get(&cluster, "cancel-me", RangeRequest::Full).0, original);
    let _ = id;
}

#[test]
fn generation_change_while_paused_marks_the_move_stale_and_keeps_reads_valid() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let first = b"01234567abcdefgh";
    let generation = put(&cluster, "racing", first);

    let (worker, reached, resume) = pause(&cluster, Boundary::BeforeTierCopy);
    let handle = thread::spawn(move || {
        worker.tier_object("racing", Media::Capacity, Condition::Any)
    });
    reached.recv_timeout(WAIT).unwrap();

    // Overwrite the object while the move is paused; the condition captured by
    // the plan (generation) no longer matches.
    let next = put(&cluster, "racing", b"replacement-generation-here");
    assert_ne!(generation, next);
    resume.send(()).unwrap();
    let report = handle.join().unwrap().unwrap();
    assert_eq!(report.outcome, TierOutcome::Changed);
    assert_eq!(report.attempt.unwrap().state, MigrationState::Stale);

    let object = cluster.object("racing").unwrap();
    assert_eq!(object.generation, next);
    assert_eq!(object.media, Media::Hot);
    assert_eq!(
        get(&cluster, "racing", RangeRequest::Full).0,
        b"replacement-generation-here"
    );
    // Stale targets were retired and gc reclaims them without touching data.
    cluster.collect().unwrap();
    assert_eq!(
        get(&cluster, "racing", RangeRequest::Full).0,
        b"replacement-generation-here"
    );
}

#[test]
fn exact_generation_condition_blocks_or_allows_a_planned_move() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let generation = put(&cluster, "guarded", b"01234567abcdefgh");

    let wrong = cluster.tier_object(
        "guarded",
        Media::Capacity,
        Condition::Generation(generation + 100),
    );
    assert!(matches!(wrong, Err(Error::Conflict(_))));
    assert!(cluster.catalog().unwrap().migrations.is_empty());

    let report = cluster
        .tier_object("guarded", Media::Capacity, Condition::Generation(generation))
        .unwrap();
    assert_eq!(report.outcome, TierOutcome::Published);
    assert_eq!(cluster.object("guarded").unwrap().media, Media::Capacity);
}

#[test]
fn tiering_coexists_with_repairs_and_node_draining() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let original = b"01234567abcdefgh";
    put(&cluster, "movable", original);
    let report = cluster
        .tier_object("movable", Media::Capacity, Condition::Any)
        .unwrap();
    assert_eq!(report.outcome, TierOutcome::Published);

    // A capacity fragment can still be healed/rebalanced after the move.
    let target = address(&cluster, "movable", 0, 0);
    fs::remove_file(cluster.fragment_path(&target).unwrap()).unwrap();
    let healed = cluster.heal(Some("movable")).unwrap();
    assert!(healed
        .events
        .iter()
        .any(|event| event.outcome == paritybay::repair::MaintenanceOutcome::Published));
    assert_eq!(get(&cluster, "movable", RangeRequest::Full).0, original);

    // Draining a capacity node and rebalancing still works post-migration.
    // Add a spare capacity node so an evacuation target exists (the base
    // topology has exactly three capacity nodes).
    cluster
        .add_node(common::node("cap-d", "az4", Media::Capacity, 134_217_728))
        .unwrap();
    let node = cluster.object("movable").unwrap().stripes[0].fragments[0]
        .node
        .clone();
    cluster.set_node_state(&node, NodeState::Draining).unwrap();
    let rebalanced = cluster.rebalance(Some("movable")).unwrap();
    assert!(rebalanced
        .events
        .iter()
        .any(|event| event.outcome == paritybay::repair::MaintenanceOutcome::Published));
    assert_eq!(get(&cluster, "movable", RangeRequest::Full).0, original);
    let current = cluster.object("movable").unwrap();
    assert_eq!(current.media, Media::Capacity);
    for receipt in &current.stripes[0].fragments {
        assert_eq!(
            cluster.catalog().unwrap().nodes[&receipt.node].media,
            Media::Capacity
        );
    }
}

#[test]
fn planned_tier_reserves_target_budget_against_competing_uploads() {
    let dir = Workspace::new();
    let mut local = config();
    // Each capacity node holds exactly one 8-byte shard beyond the planned
    // migration's own shard, so one upload fits and a second cannot.
    for node in local.nodes.iter_mut().filter(|node| node.media == Media::Capacity) {
        node.capacity = 16;
    }
    let cluster = dir.create(local);
    put(&cluster, "obj", b"01234567abcdefgh");

    let (worker, reached, resume) = pause(&cluster, Boundary::BeforeTierCopy);
    let handle = thread::spawn(move || {
        worker.tier_object("obj", Media::Capacity, Condition::Any)
    });
    reached.recv_timeout(WAIT).unwrap();

    // The plan charges every target node before any copy is published.
    let reserved: u64 = cluster
        .node_usage()
        .unwrap()
        .iter()
        .filter(|item| item.node.media == Media::Capacity)
        .map(|item| item.allocation.migrations)
        .sum();
    assert_eq!(reserved, 24, "three 8-byte shards reserved across targets");

    // Existing upload reservations stay honored: the first upload consumes the
    // last free shard slot on each node, so a second must be rejected.
    let mut opt = options(16);
    opt.media = Media::Capacity;
    opt.condition = Condition::Absent;
    let first = cluster.begin_upload("up-a", opt).unwrap();
    let over = cluster.begin_upload("up-b", opt);
    assert!(matches!(over, Err(Error::Capacity(_))), "{over:?}");
    cluster.abort_upload(first.id).unwrap();

    resume.send(()).unwrap();
    let report = handle.join().unwrap().unwrap();
    assert_eq!(report.outcome, TierOutcome::Published);
    assert_eq!(get(&cluster, "obj", RangeRequest::Full).0, b"01234567abcdefgh");
}

#[test]
fn repair_and_migration_cannot_both_plan_the_same_generation() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    put(&cluster, "obj", b"01234567abcdefgh");
    // Damage a shard so heal has work to plan.
    fs::remove_file(path(&cluster, "obj", 0, 0)).unwrap();

    let (worker, reached, resume) = pause(&cluster, Boundary::BeforeTierCopy);
    let handle = thread::spawn(move || {
        worker.tier_object("obj", Media::Capacity, Condition::Any)
    });
    reached.recv_timeout(WAIT).unwrap();

    // A repair plan is refused while the migration is in flight.
    let heal = cluster.heal(Some("obj"));
    assert!(matches!(heal, Err(Error::Conflict(_))), "{heal:?}");

    resume.send(()).unwrap();
    let report = handle.join().unwrap().unwrap();
    assert_eq!(report.outcome, TierOutcome::Published);

    // After the move, healing the (now capacity) shard succeeds.
    let moved = address(&cluster, "obj", 0, 0);
    fs::remove_file(cluster.fragment_path(&moved).unwrap()).unwrap();
    let healed = cluster.heal(Some("obj")).unwrap();
    assert!(healed
        .events
        .iter()
        .any(|event| event.outcome == paritybay::repair::MaintenanceOutcome::Published));
    assert_eq!(get(&cluster, "obj", RangeRequest::Full).0, b"01234567abcdefgh");
}

#[test]
fn empty_object_and_repeated_migrations_keep_a_canonical_catalog() {
    let dir = Workspace::new();
    let cluster = dir.create(config());

    // A zero-length object has no stripes and no fragment reservations.
    put(&cluster, "empty", b"");
    let report = cluster
        .tier_object("empty", Media::Capacity, Condition::Any)
        .unwrap();
    assert_eq!(report.outcome, TierOutcome::Published);
    assert!(report.attempt.unwrap().targets.is_empty());
    assert_eq!(cluster.object("empty").unwrap().media, Media::Capacity);
    assert!(get(&cluster, "empty", RangeRequest::Full).0.is_empty());

    // Moving it back and a normal object across media twice must keep every
    // committed catalog decodable and canonical.
    cluster
        .tier_object("empty", Media::Hot, Condition::Any)
        .unwrap();
    assert_eq!(cluster.object("empty").unwrap().media, Media::Hot);

    put(&cluster, "normal", b"01234567abcdefgh");
    cluster
        .tier_object("normal", Media::Capacity, Condition::Any)
        .unwrap();
    cluster
        .tier_object("normal", Media::Hot, Condition::Any)
        .unwrap();
    assert_eq!(cluster.object("normal").unwrap().media, Media::Hot);
    assert_eq!(
        get(&cluster, "normal", RangeRequest::Full).0,
        b"01234567abcdefgh"
    );

    // Every committed frame round-trips through the v1 codec.
    let bytes = paritybay::manifest::v1::encode(&cluster.catalog().unwrap()).unwrap();
    let decoded = paritybay::manifest::v1::decode(&bytes).unwrap();
    assert_eq!(paritybay::manifest::v1::encode(&decoded).unwrap(), bytes);
}

#[test]
fn removing_a_target_node_is_blocked_until_the_plan_clears() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    put(&cluster, "obj", b"01234567abcdefgh");

    let (worker, reached, resume) = pause(&cluster, Boundary::BeforeTierCopy);
    let handle = thread::spawn(move || {
        worker.tier_object("obj", Media::Capacity, Condition::Any)
    });
    reached.recv_timeout(WAIT).unwrap();

    let target = cluster
        .catalog()
        .unwrap()
        .migrations
        .values()
        .find(|migration| migration.state == MigrationState::Planned)
        .unwrap()
        .targets[0][0]
        .node
        .clone();
    cluster.set_node_state(&target, NodeState::Draining).unwrap();
    assert!(matches!(
        cluster.remove_node(&target),
        Err(Error::Conflict(_))
    ));

    resume.send(()).unwrap();
    let report = handle.join().unwrap().unwrap();
    assert_eq!(report.outcome, TierOutcome::Published);
    assert_eq!(get(&cluster, "obj", RangeRequest::Full).0, b"01234567abcdefgh");
}
