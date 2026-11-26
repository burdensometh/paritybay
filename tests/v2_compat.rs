mod common;

use std::fs;
use std::io::Cursor;
use std::path::Path;

use paritybay::manifest::{v1, v2};
use paritybay::readpath::RangeRequest;
use paritybay::{Boundary, Checkpoint, Cluster, Condition, Error, Media, RepairState, Result};

use common::{config, copy_tree, get, node, options, put, Workspace};

fn sample() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("samples/cluster-v1")
}

/// Fails every reach of one boundary, simulating a stop at that point.
struct Veto(Boundary);

impl Checkpoint for Veto {
    fn reach(&self, boundary: Boundary, _: &str) -> Result<()> {
        if boundary == self.0 {
            Err(Error::Conflict(format!("stopped at {}", boundary.name())))
        } else {
            Ok(())
        }
    }
}

#[test]
fn migration_preserves_every_reference() {
    let dir = Workspace::new();
    copy_tree(&sample(), dir.root());
    let cluster = Cluster::open(dir.root()).unwrap();
    let before = cluster.catalog().unwrap();
    // The sample keeps two capacity nodes in one failure domain.
    assert_eq!(before.nodes["c1"].domain, before.nodes["c2"].domain);
    assert_eq!(before.nodes.len(), 6);

    let report = cluster.migrate_v2().unwrap();
    assert!(report.switched);
    assert_eq!(report.nodes, 6);
    assert_eq!(report.objects, 4);
    assert!(!dir.root().join("catalog.v1").exists());
    assert!(dir.root().join("catalog.v2").is_file());

    // Nodes, same-domain members, stripes, reservations, repairs and retired
    // entries map one-to-one onto the pre-migration catalog.
    let after = cluster.catalog().unwrap();
    assert_eq!(before, after);

    // Objects still read, including the degraded and shared-domain stripes.
    assert_eq!(
        get(&cluster, "tail", RangeRequest::Full).0,
        b"abcdefghijklmnopqrstuv"
    );
    assert_eq!(
        get(&cluster, "degraded", RangeRequest::Full).0,
        b"01234567abcdefgh"
    );
    assert_eq!(
        get(&cluster, "shared-domain", RangeRequest::Full).0,
        b"domain"
    );
    assert!(get(&cluster, "empty", RangeRequest::Full).0.is_empty());

    // The canonical v2 bytes round-trip through the decoder.
    let bytes = fs::read(dir.root().join("catalog.v2")).unwrap();
    assert_eq!(v2::encode(&v2::decode(&bytes).unwrap()).unwrap(), bytes);

    // Migration is idempotent.
    assert!(!cluster.migrate_v2().unwrap().switched);

    // Writes continue in v2 and never resurrect the v1 file.
    cluster
        .put("added", options(2), &mut Cursor::new(b"OK"))
        .unwrap();
    assert!(!dir.root().join("catalog.v1").exists());
    let reopened = Cluster::open(dir.root()).unwrap();
    assert_eq!(get(&reopened, "added", RangeRequest::Full).0, b"OK");
    let raw = fs::read(reopened.catalog_path()).unwrap();
    assert_eq!(v2::decode(&raw).unwrap().objects.len(), 5);
}

#[test]
fn v1_cluster_keeps_writing_v1_until_migration() {
    let dir = Workspace::new();
    copy_tree(&sample(), dir.root());
    let cluster = Cluster::open(dir.root()).unwrap();
    cluster
        .put("fresh", options(3), &mut Cursor::new(b"new"))
        .unwrap();
    cluster.delete("tail", Condition::Any).unwrap();
    assert!(!dir.root().join("catalog.v2").exists());

    // The catalog on disk is still decodable v1, retired entries included.
    let bytes = fs::read(dir.root().join("catalog.v1")).unwrap();
    let catalog = v1::decode(&bytes).unwrap();
    assert!(catalog.objects.contains_key("fresh"));
    assert_eq!(catalog.retired.len(), 6);
    assert_eq!(get(&cluster, "fresh", RangeRequest::Full).0, b"new");
}

#[test]
fn interrupted_migration_keeps_last_complete_catalog() {
    let dir = Workspace::new();
    copy_tree(&sample(), dir.root());
    let cluster = Cluster::open(dir.root())
        .unwrap()
        .with_checkpoints(Veto(Boundary::BeforeCatalogCommit));
    let error = cluster.migrate_v2().unwrap_err();
    assert!(matches!(error, Error::Conflict(_)));
    assert!(!dir.root().join("catalog.v2").exists());

    // The last complete catalog is still authoritative: reads, writes and
    // repairs continue in v1.
    let plain = Cluster::open(dir.root()).unwrap();
    assert_eq!(
        get(&plain, "tail", RangeRequest::Full).0,
        b"abcdefghijklmnopqrstuv"
    );
    plain
        .put("after-stop", options(2), &mut Cursor::new(b"up"))
        .unwrap();
    plain.delete("empty", Condition::Any).unwrap();
    assert_eq!(get(&plain, "after-stop", RangeRequest::Full).0, b"up");
    assert!(!dir.root().join("catalog.v2").exists());

    // A later migration completes the switch.
    assert!(plain.migrate_v2().unwrap().switched);
    assert!(dir.root().join("catalog.v2").is_file());
    assert!(!dir.root().join("catalog.v1").exists());
    assert_eq!(get(&plain, "after-stop", RangeRequest::Full).0, b"up");
}

#[test]
fn completed_switch_with_leftover_v1_still_opens_v2() {
    let dir = Workspace::new();
    copy_tree(&sample(), dir.root());
    let cluster = Cluster::open(dir.root()).unwrap();
    // Simulate the crash window: a complete catalog.v2 next to the old v1.
    let catalog = cluster.catalog().unwrap();
    fs::write(dir.root().join("catalog.v2"), v2::encode(&catalog).unwrap()).unwrap();

    // v2 is authoritative; the stale v1 does not roll the cluster back.
    cluster
        .put("v2-write", options(2), &mut Cursor::new(b"v2"))
        .unwrap();
    assert!(dir.root().join("catalog.v1").exists());
    let reopened = Cluster::open(dir.root()).unwrap();
    assert_eq!(get(&reopened, "v2-write", RangeRequest::Full).0, b"v2");

    // A repeated migration only finishes the interrupted cleanup.
    let report = cluster.migrate_v2().unwrap();
    assert!(!report.switched);
    assert!(!dir.root().join("catalog.v1").exists());
    assert_eq!(get(&reopened, "v2-write", RangeRequest::Full).0, b"v2");
}

#[test]
fn live_records_stay_consistent_across_migration() {
    let dir = Workspace::new();
    let cluster = dir.create(config());

    // Current objects plus retired fragments from a replacement.
    put(&cluster, "alpha", b"first-generation");
    put(&cluster, "alpha", b"second-generation!!");
    put(&cluster, "beta", b"beta-bytes");

    // An open upload reservation with a staged part.
    let upload = cluster.begin_upload("gamma", options(8)).unwrap();
    cluster
        .put_part(upload.id, 1, 8, &mut Cursor::new(b"aaaaaaaa"))
        .unwrap();

    // A planned repair that has not published yet.
    fs::remove_file(common::path(&cluster, "beta", 0, 0)).unwrap();
    let gated = cluster
        .clone()
        .with_checkpoints(Veto(Boundary::BeforeRepairCopy));
    assert!(gated.heal(Some("beta")).is_err());
    assert!(cluster
        .catalog()
        .unwrap()
        .repairs
        .values()
        .any(|attempt| attempt.state == RepairState::Planned));

    let before = cluster.catalog().unwrap();
    let report = cluster.migrate_v2().unwrap();
    assert!(report.switched);
    assert_eq!(report.uploads, 1);
    assert_eq!(report.repairs, 1);
    assert!(report.retired > 0);
    assert_eq!(cluster.catalog().unwrap(), before);

    // The reservation finishes on the migrated cluster.
    let receipt = cluster.complete_upload(upload.id).unwrap();
    assert_eq!(receipt.generation, upload.id);
    assert_eq!(get(&cluster, "gamma", RangeRequest::Full).0, b"aaaaaaaa");

    // The planned repair replays and publishes.
    let attempts = cluster.replay_repairs().unwrap();
    assert!(attempts
        .iter()
        .all(|attempt| attempt.state == RepairState::Published));
    assert_eq!(get(&cluster, "beta", RangeRequest::Full).0, b"beta-bytes");

    // Retired files are collected.
    let collection = cluster.collect().unwrap();
    assert!(collection.retired_entries > 0);
    assert_eq!(
        get(&cluster, "alpha", RangeRequest::Full).0,
        b"second-generation!!"
    );
}

#[test]
fn v2_is_compact_and_keeps_same_domain_nodes_distinct() {
    let dir = Workspace::new();
    // A third hot node joins failure domain az1 on purpose.
    let mut config = config();
    config
        .nodes
        .push(node("hot-f", "az1", Media::Hot, 67108864));
    let cluster = dir.create(config);

    let data: Vec<u8> = (0..4096_u32).map(|i| (i % 251) as u8).collect();
    cluster
        .put("wide", options(data.len()), &mut Cursor::new(&data))
        .unwrap();
    let before = cluster.catalog().unwrap();
    cluster.migrate_v2().unwrap();

    // Compact references shrink the catalog with many stripes.
    let v2_bytes = fs::read(dir.root().join("catalog.v2")).unwrap();
    let v1_bytes = v1::encode(&before).unwrap();
    assert!(v2_bytes.len() < v1_bytes.len());

    let decoded = v2::decode(&v2_bytes).unwrap();
    assert_eq!(decoded, before);
    // Same-domain members remain separate rows with separate references.
    assert_eq!(decoded.nodes.len(), 9);
    for id in ["hot-a", "hot-d", "hot-f"] {
        assert_eq!(decoded.nodes[id].domain, "az1");
    }
    assert_eq!(v2::encode(&decoded).unwrap(), v2_bytes);
    assert_eq!(get(&cluster, "wide", RangeRequest::Full).0, data);
}

#[test]
fn node_table_grows_after_migration() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    put(&cluster, "pinned", b"payload");
    cluster.migrate_v2().unwrap();
    cluster
        .add_node(node("hot-f", "az5", Media::Hot, 67108864))
        .unwrap();
    assert_eq!(cluster.catalog().unwrap().nodes.len(), 9);
    assert_eq!(get(&cluster, "pinned", RangeRequest::Full).0, b"payload");
}

#[test]
fn formats_reject_each_others_magic() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    put(&cluster, "key", b"payload");
    let v1_bytes = fs::read(dir.root().join("catalog.v1")).unwrap();
    assert!(matches!(v2::decode(&v1_bytes), Err(Error::Corrupt(_))));
    cluster.migrate_v2().unwrap();
    let v2_bytes = fs::read(dir.root().join("catalog.v2")).unwrap();
    assert!(matches!(v1::decode(&v2_bytes), Err(Error::Corrupt(_))));
}

#[test]
fn malformed_v2_frames_are_rejected() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    put(&cluster, "key", b"payload");
    cluster.migrate_v2().unwrap();
    let good = fs::read(dir.root().join("catalog.v2")).unwrap();
    for cut in [0, 7, 16, good.len() - 1] {
        assert!(
            matches!(v2::decode(&good[..cut]), Err(Error::Corrupt(_))),
            "cut={cut}"
        );
    }
    let mut bad = good.clone();
    bad[24] ^= 1;
    assert!(matches!(v2::decode(&bad), Err(Error::Corrupt(_))));
    let mut bad = good;
    bad.push(0);
    assert!(matches!(v2::decode(&bad), Err(Error::Corrupt(_))));
}

#[test]
fn node_reference_beyond_the_table_is_rejected() {
    // Minimal payload: three nodes, no uploads, objects or repairs, and one
    // retired entry whose node reference points past the node table.
    let mut payload = Vec::new();
    payload.extend_from_slice(&8_u32.to_le_bytes()); // block_bytes
    payload.extend_from_slice(&65536_u64.to_le_bytes()); // staging_limit
    payload.extend_from_slice(&1_u64.to_le_bytes()); // next_id
    payload.extend_from_slice(&3_u32.to_le_bytes()); // node count
    for (id, domain) in [("n1", "d1"), ("n2", "d2"), ("n3", "d3")] {
        for text in [id, domain] {
            payload.extend_from_slice(&(text.len() as u16).to_le_bytes());
            payload.extend_from_slice(text.as_bytes());
        }
        payload.push(0); // media hot
        payload.extend_from_slice(&65536_u64.to_le_bytes()); // capacity
        payload.push(0); // state online
    }
    payload.extend_from_slice(&0_u32.to_le_bytes()); // uploads
    payload.extend_from_slice(&0_u32.to_le_bytes()); // objects
    payload.extend_from_slice(&0_u32.to_le_bytes()); // repairs
    payload.extend_from_slice(&1_u32.to_le_bytes()); // retired count
    payload.extend_from_slice(&99_u16.to_le_bytes()); // node reference
    payload.extend_from_slice(&1_u64.to_le_bytes()); // generation
    payload.extend_from_slice(&0_u32.to_le_bytes()); // stripe
    payload.push(0); // shard
    payload.extend_from_slice(&1_u64.to_le_bytes()); // revision
    payload.extend_from_slice(&8_u32.to_le_bytes()); // bytes

    let mut frame = Vec::new();
    frame.extend_from_slice(&v2::MAGIC);
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&paritybay::codec::checksum(&payload).to_le_bytes());
    frame.extend_from_slice(&payload);
    let error = v2::decode(&frame).unwrap_err();
    assert!(matches!(error, Error::Corrupt(_)), "{error}");
}
