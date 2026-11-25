mod common;

use std::fs;
use std::io::Cursor;
use std::path::Path;

use paritybay::manifest::v1;
use paritybay::readpath::{FaultKind, RangeRequest, StripeVerdict};
use paritybay::{Cluster, Error};

use common::{get, options, Workspace};

fn sample() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("samples/cluster-v1")
}

fn copy_tree(source: &Path, target: &Path) {
    fs::create_dir_all(target).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let destination = target.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &destination);
        } else {
            fs::copy(entry.path(), destination).unwrap();
        }
    }
}

#[test]
fn read_and_extend_original_v1_cluster() {
    let dir = Workspace::new();
    copy_tree(&sample(), dir.root());
    let cluster = Cluster::open(dir.root()).unwrap();
    let bytes = fs::read(cluster.catalog_path()).unwrap();
    assert_eq!(v1::encode(&v1::decode(&bytes).unwrap()).unwrap(), bytes);

    assert!(get(&cluster, "empty", RangeRequest::Full).0.is_empty());
    assert_eq!(
        get(&cluster, "tail", RangeRequest::Full).0,
        b"abcdefghijklmnopqrstuv"
    );
    let inspection = cluster.inspect("shared-domain").unwrap();
    assert_eq!(
        inspection.stripes[0]
            .domains
            .surviving_shards_after_worst_domain_loss,
        1
    );
    assert_eq!(
        get(&cluster, "degraded", RangeRequest::Full).0,
        b"01234567abcdefgh"
    );
    let inspect = cluster.inspect("degraded").unwrap();
    assert_eq!(inspect.stripes[0].verdict, StripeVerdict::Recoverable);
    assert_eq!(inspect.stripes[0].faults[0].kind, FaultKind::Missing);

    cluster
        .put("added", options(2), &mut Cursor::new(b"OK"))
        .unwrap();
    let reopened = Cluster::open(dir.root()).unwrap();
    assert_eq!(get(&reopened, "added", RangeRequest::Full).0, b"OK");
    assert_eq!(reopened.catalog().unwrap().objects.len(), 5);
}

#[test]
fn malformed_frames_are_rejected() {
    let good = fs::read(sample().join("catalog.v1")).unwrap();
    for cut in [0, 7, 16, good.len() - 1] {
        assert!(
            matches!(v1::decode(&good[..cut]), Err(Error::Corrupt(_))),
            "cut={cut}"
        );
    }
    let mut bad = good.clone();
    bad[24] ^= 1;
    assert!(matches!(v1::decode(&bad), Err(Error::Corrupt(_))));
    let mut bad = good;
    bad.push(0);
    assert!(matches!(v1::decode(&bad), Err(Error::Corrupt(_))));
}
