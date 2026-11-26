mod common;

use std::fs;
use std::sync::{Arc, Mutex};

use paritybay::readpath::RangeRequest;
use paritybay::repair::MaintenanceOutcome;
use paritybay::{Boundary, Checkpoint, Cluster, Config, Error, RepairAttempt, RepairState, Result};

use common::{config, get, path, put, Workspace};

/// Build a config whose blocks force several 64 KiB repair windows per shard.
fn large_block_config() -> Config {
    let mut config = config();
    config.block_bytes = 512 * 1024;
    config
}

/// Deterministic varied payload so every stripe carries non-repeating bytes.
fn payload(size: usize) -> Vec<u8> {
    (0..size)
        .map(|index| {
            let byte = index as u8;
            byte.wrapping_mul(73)
                .wrapping_add((index >> 5) as u8)
                .wrapping_add(11)
        })
        .collect()
}

fn assert_round_trip(cluster: &Cluster, key: &str, expected: &[u8]) {
    assert_eq!(get(cluster, key, RangeRequest::Full).0, expected);
}

fn shard0_length(cluster: &Cluster, key: &str) -> usize {
    cluster.object(key).unwrap().stripes[0].data_bytes[0] as usize
}

#[test]
fn healing_each_shard_position_reproduces_original_bytes() {
    for lost in 0u8..3 {
        let dir = Workspace::new();
        let cluster = dir.create(large_block_config());
        let data = payload(3 * 512 * 1024 + 137);
        put(&cluster, "object", &data);

        let stripes = cluster.object("object").unwrap().stripes.len();
        for stripe in 0..stripes {
            fs::remove_file(path(&cluster, "object", stripe, lost as usize)).unwrap();
        }
        let report = cluster.heal(Some("object")).unwrap();
        assert!(
            report
                .events
                .iter()
                .all(|event| event.outcome == MaintenanceOutcome::Published),
            "shard {lost}: {report:?}"
        );
        assert_round_trip(&cluster, "object", &data);

        // A reopened cluster sees the published mapping and no pending work.
        let reopened = Cluster::open(dir.root()).unwrap();
        assert_eq!(get(&reopened, "object", RangeRequest::Full).0, data);
        assert!(reopened.replay_repairs().unwrap().is_empty());
    }
}

#[test]
fn tail_stripe_rebuild_handles_short_and_zero_length_second_shard() {
    let data = payload(51); // block_bytes = 8: final stripe geometry [3, 0].
    for lost in 0u8..3 {
        let dir = Workspace::new();
        let cluster = dir.create(config());
        put(&cluster, "tail", &data);
        let object = cluster.object("tail").unwrap();
        let last = object.stripes.len() - 1;
        assert_eq!(object.stripes[last].data_bytes, [3, 0]);
        assert_eq!(object.stripes[last].fragments[1].bytes, 0);
        drop(object);

        fs::remove_file(path(&cluster, "tail", last, lost as usize)).unwrap();
        cluster.heal(Some("tail")).unwrap();
        assert_round_trip(&cluster, "tail", &data);

        // A rebuilt zero-length shard stays a real, empty published fragment.
        let healed = cluster.object("tail").unwrap();
        let target = &healed.stripes[last].fragments[1];
        assert_eq!(target.bytes, 0);
        assert_eq!(target.crc32c, 0);
        let address = paritybay::manifest::FragmentAddress::from_receipt(
            healed.generation,
            last as u32,
            target,
        );
        assert_eq!(
            fs::metadata(cluster.fragment_path(&address).unwrap())
                .unwrap()
                .len(),
            0
        );
    }
}

#[test]
fn non_aligned_tail_sizes_match_original_across_window_boundaries() {
    let dir = Workspace::new();
    let cluster = dir.create(large_block_config());
    // 64 KiB window against 512 KiB blocks, with awkward tail remainders.
    for size in [
        1usize,
        64 * 1024 - 1,
        64 * 1024,
        64 * 1024 + 1,
        2 * 512 * 1024 + 64 * 1024 + 7,
        4 * 512 * 1024 + 3,
    ] {
        let key = format!("size-{size}");
        let data = payload(size);
        put(&cluster, &key, &data);
        for lost in 0u8..3 {
            let object = cluster.object(&key).unwrap();
            for stripe in 0..object.stripes.len() {
                let receipt = &object.stripes[stripe].fragments[lost as usize];
                let address = paritybay::manifest::FragmentAddress::from_receipt(
                    object.generation,
                    stripe as u32,
                    receipt,
                );
                fs::remove_file(cluster.fragment_path(&address).unwrap()).unwrap();
            }
            drop(object);
            cluster.heal(Some(&key)).unwrap();
            assert_round_trip(&cluster, &key, &data);
        }
    }
}

#[test]
fn rebalance_evacuation_streams_healthy_shards_unchanged() {
    let dir = Workspace::new();
    let cluster = dir.create(large_block_config());
    let data = payload(2 * 512 * 1024 + 99);
    put(&cluster, "move", &data);
    let evacuated = cluster.object("move").unwrap().stripes[0].fragments[0]
        .node
        .clone();
    cluster
        .set_node_state(&evacuated, paritybay::NodeState::Draining)
        .unwrap();

    let report = cluster.rebalance(Some("move")).unwrap();
    assert!(report
        .events
        .iter()
        .any(|event| event.outcome == MaintenanceOutcome::Published));
    assert_round_trip(&cluster, "move", &data);
    assert_ne!(
        cluster.object("move").unwrap().stripes[0].fragments[0].node,
        evacuated
    );
}

#[test]
fn two_faulty_sources_are_unrecoverable_without_publishing_a_mapping() {
    let dir = Workspace::new();
    let cluster = dir.create(large_block_config());
    let data = payload(512 * 1024 + 5);
    put(&cluster, "crc", &data);
    let before = cluster.object("crc").unwrap().stripes[0].clone();

    // Two faulty shards exceed the single-fault tolerance of a 2+1 stripe.
    let source_path = path(&cluster, "crc", 0, 0);
    let mut bytes = fs::read(&source_path).unwrap();
    bytes[200_000] ^= 0xff;
    fs::write(&source_path, bytes).unwrap();
    fs::remove_file(path(&cluster, "crc", 0, 2)).unwrap();

    let report = cluster.heal(Some("crc")).unwrap();
    assert_eq!(report.events[0].outcome, MaintenanceOutcome::Unrecoverable);
    assert_eq!(report.scan.unrecoverable, 1);

    // Nothing was planned or published; the original mapping is intact.
    assert!(cluster.catalog().unwrap().repairs.is_empty());
    let after = cluster.object("crc").unwrap().stripes[0].clone();
    assert_eq!(after, before);

    let reopened = Cluster::open(dir.root()).unwrap();
    assert!(reopened.replay_repairs().unwrap().is_empty());
}

#[test]
fn source_checksum_failure_falls_back_to_verified_rebuild_and_publishes() {
    let dir = Workspace::new();
    let cluster = dir.create(large_block_config());
    let data = payload(512 * 1024 + 5);
    put(&cluster, "crc-ok", &data);
    let shard0_len = shard0_length(&cluster, "crc-ok");

    // A single damaged source: the direct streaming copy fails its rolling
    // CRC, then the rebuild path verifies both peers and reconstructs it.
    let source_path = path(&cluster, "crc-ok", 0, 0);
    let mut bytes = fs::read(&source_path).unwrap();
    bytes[200_000] ^= 0xff;
    fs::write(&source_path, bytes).unwrap();

    let report = cluster.heal(Some("crc-ok")).unwrap();
    assert!(report
        .events
        .iter()
        .any(|event| event.outcome == MaintenanceOutcome::Published));
    assert_round_trip(&cluster, "crc-ok", &data);

    // The rebuilt fragment is byte-identical to the original shard.
    let healed = cluster.object("crc-ok").unwrap();
    let target = &healed.stripes[0].fragments[0];
    assert_eq!(target.bytes as usize, shard0_len);
    let address = paritybay::manifest::FragmentAddress::from_receipt(healed.generation, 0, target);
    let recovered = fs::read(cluster.fragment_path(&address).unwrap()).unwrap();
    assert_eq!(recovered, data[..shard0_len]);
}

/// Sabotages the target node directory of the first planned repair at the
/// `before-repair-copy` boundary, i.e. after the plan is durable but before
/// any streaming write begins.
#[derive(Clone)]
struct SabotageTarget {
    cluster: Cluster,
    swap: Arc<Mutex<Option<(std::path::PathBuf, std::path::PathBuf)>>>,
}

impl Checkpoint for SabotageTarget {
    fn reach(&self, boundary: Boundary, attempt_id: &str) -> Result<()> {
        if boundary != Boundary::BeforeRepairCopy {
            return Ok(());
        }
        let id: u64 = attempt_id.parse().unwrap();
        let attempt = self
            .cluster
            .catalog()
            .unwrap()
            .repairs
            .values()
            .find(|candidate| candidate.id == id)
            .cloned()
            .unwrap();
        let target = paritybay::manifest::FragmentAddress::from_receipt(
            attempt.generation,
            attempt.stripe,
            &attempt.to,
        );
        let target_dir = self
            .cluster
            .fragment_path(&target)
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        let placeholder = target_dir.with_extension("sabotaged");
        fs::rename(&target_dir, &placeholder).unwrap();
        fs::write(&target_dir, b"not a directory").unwrap();
        *self.swap.lock().unwrap() = Some((target_dir, placeholder));
        Ok(())
    }
}

#[test]
fn torn_target_draft_is_not_published_and_replay_completes() {
    let dir = Workspace::new();
    let cluster = dir.create(large_block_config());
    let data = payload(512 * 1024 + 11);
    put(&cluster, "torn", &data);
    fs::remove_file(path(&cluster, "torn", 0, 0)).unwrap();

    let swap = Arc::new(Mutex::new(None));
    let sabotaged = cluster.clone().with_checkpoints(SabotageTarget {
        cluster: cluster.clone(),
        swap: swap.clone(),
    });
    let error = sabotaged.heal(Some("torn")).unwrap_err();
    assert!(matches!(error, Error::Io { .. }), "{error:?}");

    // The attempt stays planned and the live mapping is untouched.
    let planned: RepairAttempt = cluster
        .catalog()
        .unwrap()
        .repairs
        .values()
        .find(|attempt| attempt.state == RepairState::Planned)
        .cloned()
        .expect("attempt stays planned for replay");
    assert_eq!(
        cluster
            .catalog()
            .unwrap()
            .objects
            .get("torn")
            .unwrap()
            .stripes[0]
            .fragments[0],
        planned.from
    );

    // Restore the sabotaged node directory; replay finishes the same plan.
    let (target_dir, placeholder) = swap.lock().unwrap().clone().expect("sabotage ran");
    fs::remove_file(&target_dir).unwrap();
    fs::rename(&placeholder, &target_dir).unwrap();

    let reopened = Cluster::open(dir.root()).unwrap();
    let finished = reopened.run_repair(planned.id).unwrap();
    assert_eq!(finished.state, RepairState::Published);
    assert_round_trip(&reopened, "torn", &data);
}

#[test]
fn replay_after_completed_copy_reuses_verified_file_without_rewriting() {
    let dir = Workspace::new();
    let cluster = dir.create(large_block_config());
    let data = payload(512 * 1024 + 13);
    put(&cluster, "reuse", &data);
    fs::remove_file(path(&cluster, "reuse", 0, 2)).unwrap();

    let report = cluster.heal(Some("reuse")).unwrap();
    let attempt_id = report.events[0].attempt.unwrap();
    let target = {
        let attempt = cluster
            .catalog()
            .unwrap()
            .repairs
            .values()
            .find(|candidate| candidate.id == attempt_id)
            .unwrap()
            .clone();
        paritybay::manifest::FragmentAddress::from_receipt(
            attempt.generation,
            attempt.stripe,
            &attempt.to,
        )
    };
    let target_path = cluster.fragment_path(&target).unwrap();
    let modified = fs::metadata(&target_path).unwrap().modified().unwrap();

    // Idempotent replay must not rewrite the already verified fragment.
    let again = cluster.run_repair(attempt_id).unwrap();
    assert_eq!(again.state, RepairState::Published);
    assert_eq!(
        fs::metadata(&target_path).unwrap().modified().unwrap(),
        modified
    );
    assert_round_trip(&cluster, "reuse", &data);
}
