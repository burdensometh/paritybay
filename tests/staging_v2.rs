mod common;

use std::fs;
use std::io::Cursor;

use paritybay::manifest::v1;
use paritybay::readpath::RangeRequest;
use paritybay::staging::frame;
use paritybay::{Cluster, Error};

use common::{config, get, options, Workspace};

/// Count the loose `.part`, `.pack` and sidecar files in one upload directory.
struct LayoutCounts {
    loose: usize,
    packs: usize,
    index: bool,
}

fn layout(root: &std::path::Path, upload: u64) -> LayoutCounts {
    let dir = root.join("uploads").join(format!("{upload:016x}"));
    let mut counts = LayoutCounts {
        loose: 0,
        packs: 0,
        index: false,
    };
    let Ok(entries) = fs::read_dir(&dir) else {
        return counts;
    };
    for entry in entries {
        let name = entry.unwrap().file_name().to_string_lossy().into_owned();
        if name.ends_with(".part") {
            counts.loose += 1;
        } else if name.ends_with(".pack") {
            counts.packs += 1;
        } else if name == "staging.v2" {
            counts.index = true;
        }
    }
    counts
}

#[test]
fn legacy_sessions_keep_loose_parts_until_compaction() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let upload = cluster.begin_upload("legacy", options(12)).unwrap();
    cluster
        .put_part(upload.id, 1, 6, &mut Cursor::new(b"012345"))
        .unwrap();
    cluster
        .put_part(upload.id, 2, 6, &mut Cursor::new(b"6789ab"))
        .unwrap();

    let before = layout(dir.root(), upload.id);
    assert_eq!(before.loose, 2);
    assert_eq!(before.packs, 0);
    assert!(!before.index);

    // Compaction packs both parts into fewer files and removes the loose copies.
    let report = cluster.compact_staging().unwrap();
    assert_eq!(report.parts_packed, 2);
    assert_eq!(report.loose_removed, 2);
    assert!(report.packs_written >= 1);
    let after = layout(dir.root(), upload.id);
    assert_eq!(after.loose, 0);
    assert!(after.packs >= 1);
    assert!(after.index);

    // The object still assembles byte-for-byte after reopen once finished.
    let reopened = Cluster::open(dir.root()).unwrap();
    let receipt = reopened.complete_upload(upload.id).unwrap();
    assert_eq!(receipt.bytes, 12);
    assert_eq!(
        get(&reopened, "legacy", RangeRequest::Full).0,
        b"0123456789ab"
    );
}

#[test]
fn packed_parts_coexist_with_loose_parts_in_one_session() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let upload = cluster.begin_upload("mixed", options(18)).unwrap();

    // Part 1 starts life as a loose part.
    cluster
        .put_part(upload.id, 1, 6, &mut Cursor::new(b"012345"))
        .unwrap();
    cluster.compact_staging().unwrap();

    // Parts 2 and 3 now go straight into packs because the sidecar exists.
    cluster
        .put_part(upload.id, 2, 6, &mut Cursor::new(b"6789ab"))
        .unwrap();
    cluster
        .put_part(upload.id, 3, 6, &mut Cursor::new(b"cdefgh"))
        .unwrap();

    let counts = layout(dir.root(), upload.id);
    assert_eq!(counts.loose, 0, "part 1 was packed by compaction");
    assert!(counts.packs >= 1);
    assert!(counts.index);

    cluster.complete_upload(upload.id).unwrap();
    assert_eq!(
        get(&cluster, "mixed", RangeRequest::Full).0,
        b"0123456789abcdefgh"
    );
}

#[test]
fn replacing_a_packed_part_never_exposes_neighbour_or_old_revision() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let upload = cluster.begin_upload("replace", options(12)).unwrap();
    let first = cluster
        .put_part(upload.id, 1, 6, &mut Cursor::new(b"012345"))
        .unwrap();
    let second = cluster
        .put_part(upload.id, 2, 6, &mut Cursor::new(b"6789ab"))
        .unwrap();
    cluster.compact_staging().unwrap();

    // Replace part 2; the new revision gets a fresh extent, the old pack bytes
    // remain on disk but are no longer referenced by any receipt.
    let replacement = cluster
        .put_part(upload.id, 2, 6, &mut Cursor::new(b"ZZZZZZ"))
        .unwrap();
    assert_ne!(replacement.revision, second.revision);
    let part_one = cluster
        .put_part(upload.id, 1, 6, &mut Cursor::new(b"012345"))
        .unwrap();

    let index = cluster
        .load_index_for_test(upload.id)
        .expect("sidecar present");
    // Only current revisions are reachable; the superseded revision is absent.
    assert!(index.parts.contains_key(&replacement.revision));
    assert!(!index.parts.contains_key(&second.revision));
    // Every extent matches its own receipt length; no two extents overlap.
    let mut spans: Vec<(u64, u64, u64)> = index
        .parts
        .values()
        .map(|extent| (extent.pack, extent.offset, u64::from(extent.length)))
        .collect();
    spans.sort();
    for window in spans.windows(2) {
        if window[0].0 == window[1].0 {
            assert!(window[0].1 + window[0].2 <= window[1].1);
        }
    }

    cluster.complete_upload(upload.id).unwrap();
    assert_eq!(
        get(&cluster, "replace", RangeRequest::Full).0,
        b"012345ZZZZZZ"
    );
    // Re-uploading identical part 1 bytes still allocates a new revision, but
    // the assembled object follows the current receipts.
    assert_ne!(part_one.revision, first.revision);
}

#[test]
fn gc_keeps_referenced_packs_and_reclaims_orphans_and_loose() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let upload = cluster.begin_upload("gc", options(12)).unwrap();
    cluster
        .put_part(upload.id, 1, 6, &mut Cursor::new(b"012345"))
        .unwrap();
    cluster
        .put_part(upload.id, 2, 6, &mut Cursor::new("oldval".as_bytes()))
        .unwrap();
    cluster.compact_staging().unwrap();
    let old_p2 = cluster.catalog().unwrap().uploads[&upload.id].parts[&2].revision;
    cluster
        .put_part(upload.id, 2, 6, &mut Cursor::new(b"6789ab"))
        .unwrap();

    // The superseded revision is no longer reachable through the sidecar.
    let index = cluster.load_index_for_test(upload.id).unwrap();
    assert!(!index.parts.contains_key(&old_p2));
    let referenced: std::collections::BTreeSet<u64> =
        index.parts.values().map(|extent| extent.pack).collect();

    // Drop a genuinely orphan pack (id no sidecar references) and a draft.
    let upload_dir = dir
        .root()
        .join("uploads")
        .join(format!("{:016x}", upload.id));
    let orphan_pack = upload_dir.join("000000000000000f.pack");
    fs::write(&orphan_pack, b"stranded").unwrap();
    let draft = upload_dir.join(format!(".pb-tmp-{}-1", std::process::id()));
    fs::write(&draft, b"draft").unwrap();

    let report = cluster.collect().unwrap();
    assert!(!report.deferred);
    assert!(report.staging_files >= 1, "orphan pack reclaimed");
    assert!(report.temporary_files >= 1, "draft reclaimed");
    assert!(!orphan_pack.exists());
    // Every sidecar-referenced pack survives gc.
    for pack in &referenced {
        assert!(cluster
            .root()
            .join("uploads")
            .join(format!("{:016x}", upload.id))
            .join(format!("{pack:016x}.pack"))
            .is_file());
    }

    // Current parts remain readable and the session completes.
    cluster.complete_upload(upload.id).unwrap();
    assert_eq!(get(&cluster, "gc", RangeRequest::Full).0, b"0123456789ab");

    // After completion the whole upload directory is collectable.
    let cleanup = cluster.collect().unwrap();
    assert!(cleanup.staging_files >= 1);
    let gone = cluster.collect().unwrap();
    assert_eq!(gone.staging_files, 0);
    assert!(!upload_dir.exists());
}

#[test]
fn interrupted_compaction_leaves_either_layout_usable() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let upload = cluster.begin_upload("crash", options(12)).unwrap();
    cluster
        .put_part(upload.id, 1, 6, &mut Cursor::new(b"012345"))
        .unwrap();
    cluster
        .put_part(upload.id, 2, 6, &mut Cursor::new(b"6789ab"))
        .unwrap();

    // A real crash window: a pack file (or its draft) can reach disk without a
    // sidecar ever being published. Drop an orphan pack into the still-loose
    // upload directory. The loose layout must remain fully usable and gc must
    // reclaim the orphan without touching the loose parts.
    let orphan = dir
        .root()
        .join("uploads")
        .join(format!("{:016x}", upload.id))
        .join("0000000000000009.pack");
    fs::write(&orphan, b"orphan-bytes").unwrap();

    let reopened = Cluster::open(dir.root()).unwrap();
    let report = reopened.collect().unwrap();
    assert!(report.staging_files >= 1, "orphan pack reclaimed");
    assert!(!orphan.exists());

    let counts = layout(dir.root(), upload.id);
    assert_eq!(counts.loose, 2, "loose parts survive orphan cleanup");
    assert!(!counts.index);

    reopened.complete_upload(upload.id).unwrap();
    assert_eq!(
        get(&reopened, "crash", RangeRequest::Full).0,
        b"0123456789ab"
    );
}

#[test]
fn rerun_compaction_after_partial_progress_is_idempotent() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let upload = cluster.begin_upload("idem", options(12)).unwrap();
    cluster
        .put_part(upload.id, 1, 6, &mut Cursor::new(b"012345"))
        .unwrap();
    cluster
        .put_part(upload.id, 2, 6, &mut Cursor::new(b"6789ab"))
        .unwrap();

    let first = cluster.compact_staging().unwrap();
    assert_eq!(first.parts_packed, 2);
    // Everything is already packed; a second run has nothing to copy.
    let second = cluster.compact_staging().unwrap();
    assert_eq!(second.parts_packed, 0);
    assert_eq!(second.loose_removed, 0);

    cluster.complete_upload(upload.id).unwrap();
    assert_eq!(get(&cluster, "idem", RangeRequest::Full).0, b"0123456789ab");
}

#[test]
fn sidecar_frame_round_trips_and_rejects_damage() {
    let mut index = frame::StagingIndex::default();
    index.parts.insert(
        7,
        frame::StagedExtent {
            pack: 1,
            offset: 0,
            length: 6,
            crc32c: 0xdead_beef,
        },
    );
    index.parts.insert(
        9,
        frame::StagedExtent {
            pack: 1,
            offset: 6,
            length: 4,
            crc32c: 0x1234_5678,
        },
    );
    let bytes = frame::encode(&index).unwrap();
    assert_eq!(&bytes[..8], frame::MAGIC);
    assert_eq!(frame::decode(&bytes).unwrap(), index);

    for cut in [0, 7, 15, bytes.len() - 1] {
        assert!(matches!(
            frame::decode(&bytes[..cut]),
            Err(Error::Corrupt(_))
        ));
    }
    let mut damaged = bytes.clone();
    damaged[20] ^= 1;
    assert!(matches!(frame::decode(&damaged), Err(Error::Corrupt(_))));
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(matches!(frame::decode(&trailing), Err(Error::Corrupt(_))));
}

#[test]
fn catalog_stays_on_v1_wire_format_with_packed_sessions() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let session = cluster.begin_upload("wire", options(6)).unwrap();
    cluster
        .put_part(session.id, 1, 6, &mut Cursor::new(b"012345"))
        .unwrap();
    cluster.compact_staging().unwrap();

    let catalog_bytes = fs::read(cluster.catalog_path()).unwrap();
    assert_eq!(&catalog_bytes[..8], v1::MAGIC);
    let decoded = v1::decode(&catalog_bytes).unwrap();
    assert!(decoded.uploads.contains_key(&session.id));
    // Re-encoding the catalog is byte-stable; packs live only in the sidecar.
    assert_eq!(v1::encode(&decoded).unwrap(), catalog_bytes);
}

#[test]
fn gc_reconciles_sidecar_extents_unknown_to_the_catalog() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let upload = cluster.begin_upload("reconcile", options(6)).unwrap();
    cluster
        .put_part(upload.id, 1, 6, &mut Cursor::new(b"012345"))
        .unwrap();
    cluster.compact_staging().unwrap();

    let live: Vec<u64> = cluster
        .load_index_for_test(upload.id)
        .unwrap()
        .parts
        .keys()
        .copied()
        .collect();
    assert_eq!(live.len(), 1);

    // A stranded revision from a put_part that crashed after the sidecar write.
    let stranded = live[0] + 1000;
    let upload_dir = dir
        .root()
        .join("uploads")
        .join(format!("{:016x}", upload.id));
    fs::write(upload_dir.join("000000000000000a.pack"), b"stranded").unwrap();
    cluster
        .inject_orphan_extent_for_test(
            upload.id,
            stranded,
            paritybay::staging::StagedExtent {
                pack: 0xa,
                offset: 0,
                length: 8,
                crc32c: 0,
            },
        )
        .unwrap();
    assert!(cluster
        .load_index_for_test(upload.id)
        .unwrap()
        .parts
        .contains_key(&stranded));

    // gc removes the unknown extent and the pack only it referenced.
    let report = cluster.collect().unwrap();
    assert!(report.staging_files >= 1);
    let reconciled = cluster.load_index_for_test(upload.id).unwrap();
    assert!(!reconciled.parts.contains_key(&stranded));
    assert_eq!(reconciled.parts.len(), 1);
    assert!(!upload_dir.join("000000000000000a.pack").exists());

    // The live part still assembles correctly.
    cluster.complete_upload(upload.id).unwrap();
    assert_eq!(get(&cluster, "reconcile", RangeRequest::Full).0, b"012345");
}
