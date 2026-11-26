mod common;

use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Mutex};
use std::thread;
use std::time::Duration;

use paritybay::readpath::{FaultKind, RangeRequest, StripeVerdict};
use paritybay::repair::MaintenanceOutcome;
use paritybay::{
    Boundary, Checkpoint, Cluster, Condition, Error, Media, NodeState, RepairState, Result,
};

use common::{address, config, get, options, path, put, Workspace};

const WAIT: Duration = Duration::from_secs(15);

struct FailAt(Boundary, &'static str);

impl Checkpoint for FailAt {
    fn reach(&self, boundary: Boundary, detail: &str) -> Result<()> {
        if boundary == self.0 && detail == self.1 {
            return Err(Error::Conflict(format!("forced failure at {detail}")));
        }
        Ok(())
    }
}

struct Pause {
    boundary: Boundary,
    reached: mpsc::Sender<()>,
    resume: Mutex<mpsc::Receiver<()>>,
}

fn pause(cluster: &Cluster, boundary: Boundary) -> (Cluster, mpsc::Receiver<()>, mpsc::Sender<()>) {
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

#[test]
fn fragment_sync_and_catalog_commit_failure_keep_old_mapping() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let generation = put(&cluster, "sync", b"original");
    for (boundary, detail) in [
        (Boundary::BeforeFileSync, "fragment"),
        (Boundary::BeforeCatalogCommit, "put"),
    ] {
        let broken = cluster.clone().with_checkpoints(FailAt(boundary, detail));
        assert!(matches!(
            broken.put("sync", options(4), &mut Cursor::new(b"next")),
            Err(Error::Conflict(_))
        ));
        let reopened = Cluster::open(dir.root()).unwrap();
        assert_eq!(reopened.object("sync").unwrap().generation, generation);
        assert_eq!(get(&reopened, "sync", RangeRequest::Full).0, b"original");
    }
    assert_eq!(cluster.collect().unwrap().fragment_files, 3);
    assert_eq!(cluster.collect().unwrap().fragment_files, 0);
}

#[test]
fn generation_swap_during_read_does_not_mix_stripes_or_collect_pinned_files() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let old = b"first generation crossing the stripe boundary";
    let current = b"replacement generation with different contents";
    let original_id = put(&cluster, "racy", old);
    let previous_path = path(&cluster, "racy", 0, 0);

    let (reader, reached, resume) = pause(&cluster, Boundary::ReadPinned);
    let running = thread::spawn(move || get(&reader, "racy", RangeRequest::Full));
    reached.recv_timeout(WAIT).unwrap();
    let current_id = put(&cluster, "racy", current);
    assert_ne!(original_id, current_id);
    assert!(cluster.collect().unwrap().deferred);
    assert!(previous_path.is_file());

    resume.send(()).unwrap();
    let (bytes, report) = running.join().unwrap();
    assert_eq!(bytes, old);
    assert_eq!(report.generation, original_id);
    assert_eq!(get(&cluster, "racy", RangeRequest::Full).0, current);
    assert!(cluster.collect().unwrap().retired_entries > 0);
    assert!(!previous_path.exists());
}

#[test]
fn repair_replay_reuses_verified_copy_after_interrupted_publication() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    put(&cluster, "retry", b"12345678ABCDEFGH");
    let original = address(&cluster, "retry", 0, 0);
    fs::write(cluster.fragment_path(&original).unwrap(), b"1234567x").unwrap();
    assert_eq!(
        cluster.inspect("retry").unwrap().stripes[0].faults[0].kind,
        FaultKind::InvalidChecksum
    );

    let interrupted = cluster
        .clone()
        .with_checkpoints(FailAt(Boundary::BeforeCatalogCommit, "repair-publish"));
    assert!(matches!(
        interrupted.heal(Some("retry")),
        Err(Error::Conflict(_))
    ));
    let reopened = Cluster::open(dir.root()).unwrap();
    let repair = reopened
        .catalog()
        .unwrap()
        .repairs
        .into_values()
        .next()
        .unwrap();
    assert_eq!(repair.state, RepairState::Planned);
    assert_eq!(address(&reopened, "retry", 0, 0), original);

    let target = paritybay::manifest::FragmentAddress::from_receipt(
        repair.generation,
        repair.stripe,
        &repair.to,
    );
    let target_path = reopened.fragment_path(&target).unwrap();
    assert_eq!(fs::read(&target_path).unwrap(), b"12345678");
    assert_eq!(reopened.collect().unwrap().fragment_files, 0);
    fs::remove_file(path(&reopened, "retry", 0, 2)).unwrap();
    let result = reopened.replay_repairs().unwrap();
    assert_eq!(result[0].state, RepairState::Published);
    assert_eq!(address(&reopened, "retry", 0, 0), target);
    assert!(reopened.replay_repairs().unwrap().is_empty());
    assert_eq!(reopened.run_repair(repair.id).unwrap(), result[0]);
    assert_eq!(
        get(&reopened, "retry", RangeRequest::Full).0,
        b"12345678ABCDEFGH"
    );
}

#[test]
fn repair_paused_before_copy_cannot_revive_deleted_generation() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let generation = put(&cluster, "obsolete", b"12345678ABCDEFGH");
    fs::remove_file(path(&cluster, "obsolete", 0, 0)).unwrap();
    let (worker, reached, resume) = pause(&cluster, Boundary::BeforeRepairCopy);
    let worker = thread::spawn(move || worker.heal(Some("obsolete")).unwrap());
    reached.recv_timeout(WAIT).unwrap();

    cluster
        .delete("obsolete", Condition::Generation(generation))
        .unwrap();
    assert!(cluster.collect().unwrap().deferred);
    let fresh = put(&cluster, "obsolete", b"new");
    assert_ne!(generation, fresh);
    resume.send(()).unwrap();

    let report = worker.join().unwrap();
    assert_eq!(report.events[0].outcome, MaintenanceOutcome::Changed);
    assert_eq!(
        cluster
            .repair_attempt(report.events[0].attempt.unwrap())
            .unwrap()
            .state,
        RepairState::Stale
    );
    assert_eq!(get(&cluster, "obsolete", RangeRequest::Full).0, b"new");
    assert!(cluster.collect().unwrap().retired_entries >= 4);
}

#[test]
fn rebalance_moves_draining_shard_with_readers_present_then_removes_node() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let original = b"01234567abcdefgh";
    put(&cluster, "drain", original);
    let address = address(&cluster, "drain", 0, 0);
    let old = cluster.fragment_path(&address).unwrap();
    let pin = cluster.pin_object("drain").unwrap();

    cluster
        .set_node_state(&address.node, NodeState::Draining)
        .unwrap();
    let report = cluster.rebalance(Some("drain")).unwrap();
    assert!(report
        .events
        .iter()
        .any(|event| event.outcome == MaintenanceOutcome::Published));
    assert!(report.collection.unwrap().deferred);
    assert!(old.is_file());
    assert!(matches!(
        cluster.remove_node(&address.node),
        Err(Error::Conflict(_))
    ));
    let mut output = Vec::new();
    pin.read(RangeRequest::Full, &mut output).unwrap();
    assert_eq!(output, original);
    drop(pin);

    cluster.collect().unwrap();
    assert!(!old.exists());
    cluster.remove_node(&address.node).unwrap();
    assert_eq!(
        cluster.catalog().unwrap().nodes[&address.node].state,
        NodeState::Removed
    );
    assert_eq!(
        cluster.inspect("drain").unwrap().stripes[0].verdict,
        StripeVerdict::Healthy
    );
}

#[test]
fn removal_rechecks_state_after_controlled_pause() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    cluster
        .set_node_state("hot-e", NodeState::Draining)
        .unwrap();
    let (remover, reached, resume) = pause(&cluster, Boundary::BeforeNodeRemoval);
    let running = thread::spawn(move || remover.remove_node("hot-e"));
    reached.recv_timeout(WAIT).unwrap();
    cluster.set_node_state("hot-e", NodeState::Online).unwrap();
    resume.send(()).unwrap();
    assert!(matches!(running.join().unwrap(), Err(Error::Conflict(_))));
    assert_eq!(cluster.catalog().unwrap().nodes["hot-e"].media, Media::Hot);
}

/// A PUT that fails before its catalog commit leaves fragment files behind that no
/// receipt names. Sweep every node directory so a later `removed` tombstone cannot
/// strand them, whichever order collection and removal ran in.
fn failed_put_leaves_shards(cluster: &Cluster, dir: &Workspace, key: &str) -> Vec<PathBuf> {
    let broken = cluster
        .clone()
        .with_checkpoints(FailAt(Boundary::BeforeCatalogCommit, "put"));
    assert!(matches!(
        broken.put(key, options(16), &mut Cursor::new(b"01234567abcdefgh")),
        Err(Error::Conflict(_))
    ));
    let reopened = Cluster::open(dir.root()).unwrap();
    assert!(matches!(reopened.object(key), Err(Error::NotFound(_))));
    catalog_fragment_files(dir.root())
}

fn catalog_fragment_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_fragment_files(&root.join("nodes"), &mut files);
    files.sort();
    files
}

fn collect_fragment_files(dir: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            collect_fragment_files(&entry.path(), files);
        } else if entry.file_name().to_string_lossy().ends_with(".pb") {
            files.push(entry.path());
        }
    }
}

fn node_of(path: &Path) -> String {
    path.parent()
        .unwrap()
        .parent()
        .unwrap()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned()
}

#[test]
fn collect_after_removal_reclaims_orphans_on_removed_node() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let orphans = failed_put_leaves_shards(&cluster, &dir, "failed");
    assert_eq!(orphans.len(), 3);
    let removed = node_of(&orphans[0]);
    cluster
        .set_node_state(&removed, NodeState::Draining)
        .unwrap();
    cluster.remove_node(&removed).unwrap();

    let report = cluster.collect().unwrap();
    assert!(!report.deferred);
    assert_eq!(report.fragment_files, 3);
    assert!(catalog_fragment_files(dir.root()).is_empty());

    // Reopening and collecting again must not charge anything twice or leave files.
    let reopened = Cluster::open(dir.root()).unwrap();
    let second = reopened.collect().unwrap();
    assert_eq!(second.fragment_files, 0);
    assert!(catalog_fragment_files(dir.root()).is_empty());
}

#[test]
fn remove_after_collection_keeps_reclaiming_across_reopen() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let orphans = failed_put_leaves_shards(&cluster, &dir, "failed");
    let removed = node_of(&orphans[0]);
    cluster
        .set_node_state(&removed, NodeState::Draining)
        .unwrap();

    // Collecting while the node still exists clears the live orphans there already.
    assert!(cluster.collect().unwrap().fragment_files >= 1);
    let reopened = Cluster::open(dir.root()).unwrap();
    reopened.remove_node(&removed).unwrap();
    assert_eq!(reopened.collect().unwrap().fragment_files, 0);
    assert!(catalog_fragment_files(dir.root()).is_empty());
}

#[test]
fn removed_node_sweep_preserves_active_pinned_and_unmanaged_files() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    put(&cluster, "keep", b"01234567abcdefgh");
    let pinned_address = address(&cluster, "keep", 0, 0);
    let pinned_path = cluster.fragment_path(&pinned_address).unwrap();
    let pin = cluster.pin_object("keep").unwrap();

    // An unmanaged file with a name the collector does not recognize.
    let unmanaged = cluster
        .root()
        .join("nodes")
        .join(&pinned_address.node)
        .join("fragments")
        .join("notes.txt");
    fs::write(&unmanaged, b"operator note").unwrap();

    // Orphan files that sit on a node which will be tombstoned. Pick a node that
    // does not host the live object so removal stays eligible.
    let keep = cluster.object("keep").unwrap();
    let used: Vec<String> = keep
        .stripes
        .iter()
        .flat_map(|stripe| {
            stripe
                .fragments
                .iter()
                .map(|fragment| fragment.node.clone())
        })
        .collect();
    let orphan_node = cluster
        .catalog()
        .unwrap()
        .nodes
        .keys()
        .find(|id| !used.contains(id))
        .cloned()
        .unwrap();
    let orphan_dir = cluster
        .root()
        .join("nodes")
        .join(&orphan_node)
        .join("fragments");
    fs::create_dir_all(&orphan_dir).unwrap();
    let orphan = orphan_dir.join("00000000000000ff-00000000-0-00000000000000ff.pb");
    fs::write(&orphan, b"orphan").unwrap();

    cluster
        .set_node_state(&orphan_node, NodeState::Draining)
        .unwrap();
    cluster.remove_node(&orphan_node).unwrap();

    // While the pinned read is in flight, collection must defer and touch nothing.
    assert!(cluster.collect().unwrap().deferred);
    assert!(orphan.exists());

    let mut output = Vec::new();
    pin.read(RangeRequest::Full, &mut output).unwrap();
    assert_eq!(output, b"01234567abcdefgh");
    drop(pin);

    let report = cluster.collect().unwrap();
    assert!(!report.deferred);
    assert!(!orphan.exists());
    assert!(pinned_path.is_file());
    assert!(unmanaged.is_file());

    let reopened = Cluster::open(dir.root()).unwrap();
    reopened.collect().unwrap();
    assert!(unmanaged.is_file());
    assert_eq!(
        get(&reopened, "keep", RangeRequest::Full).0,
        b"01234567abcdefgh"
    );
}
