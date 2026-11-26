mod common;

use std::io::Cursor;

use paritybay::manifest::v1;
use paritybay::{Cluster, Error, Media, NodeState};

use common::{config, node_with_reserve, options, put, Workspace};

/// Three 8-byte nodes: one stripe (16 logical bytes) stores 8+0+8 bytes, so
/// each chosen node charges 8 bytes per stripe.
fn reserved_cluster(reserve: u64) -> (Workspace, Cluster) {
    let dir = Workspace::new();
    let nodes = vec![
        node_with_reserve("h0", "a", Media::Hot, 8, reserve),
        node_with_reserve("h1", "b", Media::Hot, 8, reserve),
        node_with_reserve("h2", "c", Media::Hot, 8, reserve),
    ];
    let mut cfg = config();
    cfg.nodes = nodes;
    let cluster = dir.create(cfg);
    (dir, cluster)
}

#[test]
fn foreground_writes_stop_at_the_maintenance_reserve() {
    let (_dir, cluster) = reserved_cluster(4);
    // Each node has 8 bytes of capacity and a 4-byte reserve, leaving only
    // 4 user-writable bytes; a full stripe needs 8 bytes on a node.
    let err = cluster.begin_upload("blocked", options(16)).unwrap_err();
    assert!(matches!(err, Error::Capacity(_)), "got {err:?}");

    // A direct PUT draws on the same budget and is refused the same way.
    let err = cluster
        .put("blocked", options(16), &mut Cursor::new(vec![0; 16]))
        .unwrap_err();
    assert!(matches!(err, Error::Capacity(_)), "got {err:?}");

    let usage = cluster.node_usage().unwrap();
    for item in &usage {
        assert_eq!(item.node.maintenance_reserve, 4);
        assert_eq!(item.user_free, 4);
        assert_eq!(item.maintenance_free, 8);
    }
}

#[test]
fn committed_reservations_keep_their_terms_when_reserve_grows() {
    let (_dir, cluster) = reserved_cluster(0);
    // The reservation consumes the whole 8-byte per-node budget.
    let upload = cluster.begin_upload("kept", options(16)).unwrap();
    assert_eq!(upload.placements.len(), 1);

    // Growing the reserve after the promise must not invalidate it, even
    // though only one user-writable byte remains under the new terms.
    for item in cluster.node_usage().unwrap() {
        cluster.set_node_reserve(&item.node.id, 7).unwrap();
    }
    let reopened = Cluster::open(cluster.root()).unwrap();
    let usage = reopened.node_usage().unwrap();
    // The existing reservation already charged all 8 bytes, so no
    // user-writable space remains even though the reserve itself is 7.
    assert!(usage.iter().all(|item| item.user_free == 0));
    assert!(usage
        .iter()
        .all(|item| item.node.maintenance_reserve == 7));
    // The maintenance budget still sees the unreserved headroom.
    assert!(usage
        .iter()
        .all(|item| item.maintenance_free == 0));

    // The promised multipart upload still finishes on its original nodes.
    reopened
        .put_part(upload.id, 1, 16, &mut Cursor::new(*b"01234567abcdefgh"))
        .unwrap();
    let receipt = reopened.complete_upload(upload.id).unwrap();
    assert_eq!(receipt.generation, upload.id);

    // A fresh reservation is now refused under the tightened terms.
    let err = reopened.begin_upload("new", options(16)).unwrap_err();
    assert!(matches!(err, Error::Capacity(_)), "got {err:?}");
}

#[test]
fn repair_lands_in_reserved_space_and_keeps_media_and_domain_rules() {
    // 16-byte capacity per node with an 8-byte reserve leaves exactly 8
    // user-writable bytes — enough for one stripe, with the reserve free
    // only for healing.
    let dir = Workspace::new();
    let nodes = vec![
        node_with_reserve("h0", "a", Media::Hot, 16, 8),
        node_with_reserve("h1", "b", Media::Hot, 16, 8),
        node_with_reserve("h2", "c", Media::Hot, 16, 8),
        node_with_reserve("h3", "d", Media::Hot, 16, 8),
    ];
    let mut cfg = config();
    cfg.nodes = nodes;
    let cluster = dir.create(cfg);

    put(&cluster, "obj", b"01234567abcdefgh");
    // Foreground space is now exhausted on every node, but maintenance space
    // remains. Drain the node holding shard 0 and force evacuation onto it.
    let object = cluster.object("obj").unwrap();
    let source = object.stripes[0].fragments[0].node.clone();
    cluster
        .set_node_state(&source, NodeState::Draining)
        .unwrap();

    let report = cluster.rebalance(Some("obj")).unwrap();
    assert!(
        report
            .events
            .iter()
            .any(|event| event.outcome == paritybay::repair::MaintenanceOutcome::Published),
        "rebalance should heal inside the maintenance reserve: {report:?}"
    );
    // Rebalance runs its own collection pass after publishing the move.
    let collection = report
        .collection
        .as_ref()
        .expect("rebalance reports its collection");
    assert!(collection.retired_entries > 0);

    // The replacement stays on hot media and preserves three distinct domains.
    let after = cluster.object("obj").unwrap();
    let mut domains = std::collections::BTreeSet::new();
    for fragment in &after.stripes[0].fragments {
        let node = &cluster.catalog().unwrap().nodes[&fragment.node];
        assert_eq!(node.media, Media::Hot);
        domains.insert(node.domain.clone());
    }
    assert_eq!(domains.len(), 3);
    assert!(!after.stripes[0]
        .fragments
        .iter()
        .any(|fragment| fragment.node == source));

    // The retired bytes were reclaimed by the rebalance collection and the
    // accounting stays consistent after reopening the catalog.
    let reopened = Cluster::open(cluster.root()).unwrap();
    assert!(reopened
        .node_usage()
        .unwrap()
        .iter()
        .all(|item| item.allocation.retired == 0));
    assert!(reopened
        .node_usage()
        .unwrap()
        .iter()
        .all(|item| item.maintenance_free >= 8));

    // A subsequent explicit GC finds nothing left to retire.
    let collected = reopened.collect().unwrap();
    assert_eq!(collected.retired_entries, 0);
}

#[test]
fn retired_occupancy_survives_reopen_until_gc_reclaims_it() {
    // Foreground writes fill the user budget; the reserve funds the heal.
    let dir = Workspace::new();
    let nodes = vec![
        node_with_reserve("h0", "a", Media::Hot, 16, 8),
        node_with_reserve("h1", "b", Media::Hot, 16, 8),
        node_with_reserve("h2", "c", Media::Hot, 16, 8),
        node_with_reserve("h3", "d", Media::Hot, 16, 8),
    ];
    let mut cfg = config();
    cfg.nodes = nodes;
    let cluster = dir.create(cfg);

    put(&cluster, "obj", b"01234567abcdefgh");
    let target = common::address(&cluster, "obj", 0, 0);
    std::fs::remove_file(cluster.fragment_path(&target).unwrap()).unwrap();

    // Heal does not collect, so the replaced fragment stays retired and charged.
    let report = cluster.heal(Some("obj")).unwrap();
    assert!(report.collection.is_none());
    let before: u64 = cluster
        .node_usage()
        .unwrap()
        .iter()
        .map(|item| item.allocation.retired)
        .sum();
    assert_eq!(before, 8);

    // Reopen: user-writable, maintenance-only and retired occupancy all show.
    let reopened = Cluster::open(cluster.root()).unwrap();
    let item = reopened
        .node_usage()
        .unwrap()
        .into_iter()
        .find(|item| item.allocation.retired > 0)
        .expect("a node still carries the retired fragment");
    assert_eq!(item.allocation.retired, 8);
    assert_eq!(item.node.maintenance_reserve, 8);

    // GC clears the retired charge and restores the space.
    let collected = reopened.collect().unwrap();
    assert_eq!(collected.retired_entries, 1);
    assert!(reopened
        .node_usage()
        .unwrap()
        .iter()
        .all(|item| item.allocation.retired == 0));
}

#[test]
fn reserve_must_leave_some_user_capacity() {
    let (_dir, cluster) = reserved_cluster(0);
    assert!(matches!(
        cluster.set_node_reserve("h0", 8),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        cluster.set_node_reserve("h0", 100),
        Err(Error::Invalid(_))
    ));
    cluster.set_node_reserve("h0", 7).unwrap();
    assert_eq!(
        cluster.node_usage().unwrap()[0].node.maintenance_reserve,
        7
    );
}

#[test]
fn shrinking_capacity_cannot_undercut_the_reserve() {
    let (_dir, cluster) = reserved_cluster(4);
    // reserve 4 means any capacity at or below 4 would leave no user budget.
    assert!(matches!(
        cluster.set_node_capacity("h0", 4),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        cluster.set_node_capacity("h0", 3),
        Err(Error::Invalid(_))
    ));
    // Capacity 5 still leaves one user-writable byte.
    cluster.set_node_capacity("h0", 5).unwrap();
    let item = cluster
        .node_usage()
        .unwrap()
        .into_iter()
        .find(|item| item.node.id == "h0")
        .unwrap();
    assert_eq!(item.node.capacity, 5);
    assert_eq!(item.user_free, 1);
}

#[test]
fn reserve_extension_round_trips_through_the_v1_catalog() {
    let (_dir, cluster) = reserved_cluster(6);
    cluster.set_node_reserve("h1", 0).unwrap(); // a zero-reserve node writes no extension

    let bytes = v1::encode(&cluster.catalog().unwrap()).unwrap();
    let decoded = v1::decode(&bytes).unwrap();
    // Re-encoding the decoded catalog must reproduce the exact bytes.
    assert_eq!(v1::encode(&decoded).unwrap(), bytes);

    let reserves: Vec<u64> = ["h0", "h1", "h2"]
        .iter()
        .map(|id| decoded.nodes[*id].maintenance_reserve)
        .collect();
    assert_eq!(reserves, vec![6, 0, 6]);

    // The value is durable across a process-level reopen.
    let reopened = Cluster::open(cluster.root()).unwrap();
    let item = reopened
        .node_usage()
        .unwrap()
        .into_iter()
        .find(|item| item.node.id == "h0")
        .unwrap();
    assert_eq!(item.node.maintenance_reserve, 6);
}

#[test]
fn config_accepts_cluster_default_and_per_node_reserve_overrides() {
    let source = "\
block_bytes = 8
maintenance_reserve = 12
node = h0, a, hot, 64, online
node = h1, b, hot, 64, online, 20
node = h2, c, hot, 64, online
";
    let parsed = paritybay::Config::parse(source).unwrap();
    assert_eq!(parsed.maintenance_reserve, 12);
    assert_eq!(parsed.nodes[0].maintenance_reserve, 12);
    assert_eq!(parsed.nodes[1].maintenance_reserve, 20);
    assert_eq!(parsed.nodes[2].maintenance_reserve, 12);

    let bad = "block_bytes = 8\nnode = h0, a, hot, 64, online, 64\n\
               node = h1, b, hot, 64, online\nnode = h2, c, hot, 64, online\n";
    assert!(matches!(
        paritybay::Config::parse(bad),
        Err(Error::Invalid(_))
    ));
}

