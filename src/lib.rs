#![forbid(unsafe_code)]

pub mod codec;
mod eligibility;
pub mod manifest;
pub mod placement;
pub mod readpath;
pub mod repair;
pub mod upload;

mod boundary;
mod config;
mod error;
mod files;

pub use boundary::{Boundary, Checkpoint, FileGates, NoCheckpoints};
pub use config::{parse_node, Config};
pub use error::{Error, Result};
pub use manifest::{
    Catalog, Condition, DomainPolicy, FragmentReceipt, Media, NodeDomain, NodeState,
    ObjectGeneration, RepairAttempt, RepairKind, RepairState, StripeManifest, UploadSession,
};

use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone)]
pub struct Cluster {
    pub(crate) root: PathBuf,
    pub(crate) checkpoints: Arc<dyn Checkpoint>,
}

impl Cluster {
    pub fn create(root: impl AsRef<Path>, config: Config) -> Result<Self> {
        Self::create_with_checkpoints(root, config, NoCheckpoints)
    }

    pub fn create_with_checkpoints(
        root: impl AsRef<Path>,
        config: Config,
        checkpoints: impl Checkpoint + 'static,
    ) -> Result<Self> {
        let cluster = Self::at(root).with_checkpoints(checkpoints);
        cluster.initialize(config)?;
        Ok(cluster)
    }

    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let cluster = Self::at(root);
        cluster.catalog()?;
        Ok(cluster)
    }

    pub fn with_checkpoints(mut self, checkpoints: impl Checkpoint + 'static) -> Self {
        self.checkpoints = Arc::new(checkpoints);
        self
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn at(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_owned(),
            checkpoints: Arc::new(NoCheckpoints),
        }
    }

    pub(crate) fn checkpoint(&self, boundary: Boundary, detail: impl AsRef<str>) -> Result<()> {
        self.checkpoints.reach(boundary, detail.as_ref())
    }
}
