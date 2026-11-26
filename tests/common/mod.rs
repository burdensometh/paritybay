#![allow(dead_code)]

use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use paritybay::manifest::FragmentAddress;
use paritybay::readpath::RangeRequest;
use paritybay::upload::UploadOptions;
use paritybay::{Cluster, Condition, Config, DomainPolicy, Media, NodeDomain, NodeState};

static ID: AtomicU64 = AtomicU64::new(0);

pub struct Workspace(PathBuf);

impl Workspace {
    pub fn new() -> Self {
        loop {
            let id = ID.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("paritybay-test-{}-{id}", std::process::id()));
            match fs::create_dir(&path) {
                Ok(()) => return Self(path),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => panic!("temporary cluster: {e}"),
            }
        }
    }

    pub fn root(&self) -> &Path {
        &self.0
    }

    pub fn create(&self, config: Config) -> Cluster {
        Cluster::create(self.root(), config).unwrap()
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

pub fn config() -> Config {
    Config::parse(include_str!("../../config/cluster.conf")).unwrap()
}

pub fn node(id: &str, domain: &str, media: Media, capacity: u64) -> NodeDomain {
    node_with_reserve(id, domain, media, capacity, 0)
}

pub fn node_with_reserve(
    id: &str,
    domain: &str,
    media: Media,
    capacity: u64,
    maintenance_reserve: u64,
) -> NodeDomain {
    NodeDomain {
        id: id.into(),
        domain: domain.into(),
        media,
        capacity,
        maintenance_reserve,
        state: NodeState::Online,
    }
}

pub fn options(bytes: usize) -> UploadOptions {
    UploadOptions {
        bytes: bytes as u64,
        media: Media::Hot,
        domains: DomainPolicy::Strict,
        condition: Condition::Any,
    }
}

pub fn put(cluster: &Cluster, key: &str, bytes: &[u8]) -> u64 {
    cluster
        .put(key, options(bytes.len()), &mut Cursor::new(bytes))
        .unwrap()
        .generation
}

pub fn get(
    cluster: &Cluster,
    key: &str,
    range: RangeRequest,
) -> (Vec<u8>, paritybay::readpath::ReadReport) {
    let mut bytes = Vec::new();
    let report = cluster.get(key, range, &mut bytes).unwrap();
    (bytes, report)
}

pub fn address(cluster: &Cluster, key: &str, stripe: usize, shard: usize) -> FragmentAddress {
    let object = cluster.object(key).unwrap();
    let stripe = &object.stripes[stripe];
    FragmentAddress::from_receipt(object.generation, stripe.index, &stripe.fragments[shard])
}

pub fn path(cluster: &Cluster, key: &str, stripe: usize, shard: usize) -> PathBuf {
    cluster
        .fragment_path(&address(cluster, key, stripe, shard))
        .unwrap()
}
