mod common;

use std::fs;
use std::io::Cursor;

use paritybay::readpath::RangeRequest;
use paritybay::{Cluster, Condition, Error, Media, NodeState};

use common::{address, config, get, node, options, put, Workspace};

#[test]
fn parts_survive_open_and_replace_atomically_with_ordered_commit() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let mut expected = options(21);
    expected.condition = Condition::Absent;
    let upload = cluster.begin_upload("multipart", expected).unwrap();

    let p2 = cluster
        .put_part(upload.id, 2, 13, &mut Cursor::new(b"xxxxxxxxxxxxx"))
        .unwrap();
    assert!(matches!(
        cluster.complete_upload(upload.id),
        Err(Error::Conflict(_))
    ));
    let reopened = Cluster::open(dir.root()).unwrap();
    let replacement = reopened
        .put_part(upload.id, 2, 13, &mut Cursor::new(b"abcdefghijklm"))
        .unwrap();
    assert_ne!(p2.revision, replacement.revision);
    reopened
        .put_part(upload.id, 1, 8, &mut Cursor::new(b"01234567"))
        .unwrap();

    let receipt = reopened.complete_upload(upload.id).unwrap();
    assert_eq!(receipt.generation, upload.id);
    assert_eq!(receipt.condition, Condition::Absent);
    assert_eq!(
        get(&reopened, "multipart", RangeRequest::Full).0,
        b"01234567abcdefghijklm"
    );
    assert!(!reopened.catalog().unwrap().uploads.contains_key(&upload.id));
    assert_eq!(reopened.collect().unwrap().staging_files, 3);
}

#[test]
fn gaps_and_checksum_damage_never_publish() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let upload = cluster.begin_upload("gap", options(6)).unwrap();
    let first = cluster
        .put_part(upload.id, 1, 3, &mut Cursor::new(b"123"))
        .unwrap();
    cluster
        .put_part(upload.id, 3, 3, &mut Cursor::new(b"456"))
        .unwrap();
    assert!(matches!(
        cluster.complete_upload(upload.id),
        Err(Error::Conflict(_))
    ));
    cluster.abort_upload(upload.id).unwrap();

    let other = cluster.begin_upload("corrupt", options(3)).unwrap();
    let part = cluster
        .put_part(other.id, 1, 3, &mut Cursor::new(b"xyz"))
        .unwrap();
    assert!(part.revision > first.revision);
    let part_path = dir.root().join(format!(
        "uploads/{:016x}/00000001-{:016x}.part",
        other.id, part.revision
    ));
    fs::write(part_path, b"bad").unwrap();
    assert!(matches!(
        cluster.complete_upload(other.id),
        Err(Error::Corrupt(_))
    ));
    assert!(matches!(cluster.object("corrupt"), Err(Error::NotFound(_))));
}

#[test]
fn generation_condition_is_checked_at_publication() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let first = put(&cluster, "conditional", b"old");
    let mut expected = options(3);
    expected.condition = Condition::Generation(first);
    let waiting = cluster.begin_upload("conditional", expected).unwrap();
    cluster
        .put_part(waiting.id, 1, 3, &mut Cursor::new(b"one"))
        .unwrap();
    let winner = cluster
        .put("conditional", expected, &mut Cursor::new(b"two"))
        .unwrap();
    assert_eq!(winner.replaced_generation, Some(first));
    assert!(matches!(
        cluster.complete_upload(waiting.id),
        Err(Error::Conflict(_))
    ));
    assert!(matches!(
        cluster.delete("conditional", expected.condition),
        Err(Error::Conflict(_))
    ));
    assert_eq!(get(&cluster, "conditional", RangeRequest::Full).0, b"two");
    cluster.abort_upload(waiting.id).unwrap();
}

#[test]
fn node_reservations_compete_and_draining_blocks_planned_targets() {
    let dir = Workspace::new();
    let mut config = config();
    config.nodes = vec![
        node("h0", "a", Media::Hot, 8),
        node("h1", "b", Media::Hot, 8),
        node("h2", "c", Media::Hot, 8),
    ];
    let cluster = dir.create(config);
    let upload = cluster.begin_upload("first", options(16)).unwrap();
    // All three nodes are fully reserved by the single stripe, so a second
    // concurrent upload has nowhere to go.
    assert!(matches!(
        cluster.begin_upload("second", options(16)),
        Err(Error::Capacity(_))
    ));
    let old_node = upload.placements[0].nodes[0].clone();
    // While the node stays online it still backs the planned placement, so it
    // cannot be removed.
    assert!(matches!(
        cluster.remove_node(&old_node),
        Err(Error::Conflict(_))
    ));
    // A draining node can never be a new target, so flipping a node that still
    // backs a planned placement is rejected rather than persisting an invalid
    // catalog; the session has to replan away from it first.
    assert!(matches!(
        cluster.set_node_state(&old_node, NodeState::Draining),
        Err(Error::Corrupt(_))
    ));
    // A fourth eligible node lets replan produce a fresh all-online layout.
    cluster.add_node(node("h3", "d", Media::Hot, 8)).unwrap();
    let replacement = cluster.replan_upload(upload.id).unwrap();
    assert_eq!(replacement.placements.len(), 1);
    for plan in &replacement.placements {
        for id in &plan.nodes {
            assert_eq!(
                cluster.catalog().unwrap().nodes[id].state,
                NodeState::Online
            );
        }
    }
    // Aborting releases every reserved byte back to the ledger.
    let reserved: u64 = cluster
        .node_usage()
        .unwrap()
        .iter()
        .map(|item| item.allocation.uploads)
        .sum();
    assert!(reserved > 0);
    cluster.abort_upload(upload.id).unwrap();
    let after: u64 = cluster
        .node_usage()
        .unwrap()
        .iter()
        .map(|item| item.allocation.uploads)
        .sum();
    assert_eq!(after, 0);
}

#[test]
fn draining_node_cannot_be_a_new_target_but_keeps_current_fragments() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    // A committed object gives every involved node a current fragment.
    put(&cluster, "keep", b"01234567abcdefgh");
    let target = address(&cluster, "keep", 0, 0).node.clone();
    cluster
        .set_node_state(&target, NodeState::Draining)
        .unwrap();
    // The catalog stays valid and the draining node's fragment remains
    // readable: recorded mappings tolerate a draining location.
    assert!(cluster.catalog().is_ok());
    assert_eq!(
        get(&cluster, "keep", RangeRequest::Full).0,
        b"01234567abcdefgh"
    );
    // Every new placement avoids the draining node even though it still holds
    // a readable current fragment.
    for _ in 0..3 {
        let key = format!("new-{target}");
        put(&cluster, &key, b"01234567abcdefgh");
        let object = cluster.object(&key).unwrap();
        for stripe in &object.stripes {
            for fragment in &stripe.fragments {
                assert_ne!(fragment.node, target);
            }
        }
    }
}

#[test]
fn replan_fails_when_fewer_than_three_targets_remain() {
    let dir = Workspace::new();
    let mut config = config();
    config.nodes = vec![
        node("h0", "a", Media::Hot, 8),
        node("h1", "b", Media::Hot, 8),
        node("h2", "c", Media::Hot, 8),
    ];
    let cluster = dir.create(config);
    put(&cluster, "anchor", b"01234567abcdefgh");
    assert!(matches!(
        cluster.begin_upload("blocked", options(6)),
        Err(Error::Capacity(_))
    ));
}

#[test]
fn wrong_input_length_cannot_replace_current_generation() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let before = put(&cluster, "once", b"original");
    assert!(matches!(
        cluster.put("once", options(8), &mut Cursor::new(b"abc")),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        cluster.put("once", options(1), &mut Cursor::new(b"abc")),
        Err(Error::Invalid(_))
    ));
    assert_eq!(cluster.object("once").unwrap().generation, before);
    assert_eq!(get(&cluster, "once", RangeRequest::Full).0, b"original");
}
