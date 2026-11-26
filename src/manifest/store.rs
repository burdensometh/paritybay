use std::fs::File;
use std::path::{Path, PathBuf};

use crate::files::{self, DraftFile, LockMode};
use crate::{Boundary, Cluster, Config, Error, Result};

use super::{
    check_condition, checked_key, v1, Catalog, Condition, FragmentAddress, ObjectGeneration,
};

const CATALOG: &str = "catalog.v1";
const CATALOG_LOCK: &str = "catalog.lock";
const READER_LOCK: &str = "readers.lock";

pub(crate) struct Txn<'a> {
    cluster: &'a Cluster,
    _lock: File,
    pub(crate) catalog: Catalog,
}

impl Txn<'_> {
    pub(crate) fn commit(self, action: &str) -> Result<()> {
        self.cluster.commit_catalog(&self.catalog, action)
    }
}

impl Cluster {
    pub(crate) fn initialize(&self, config: Config) -> Result<()> {
        let nodes = config.as_node_map()?;
        files::make_dir(&self.root)?;
        let _lock = files::lock(&self.root.join(CATALOG_LOCK), LockMode::Exclusive)?
            .expect("blocking catalog lock");
        if self
            .catalog_path()
            .try_exists()
            .map_err(|e| Error::io("locate catalog", &self.catalog_path(), e))?
        {
            return Err(Error::Conflict(format!(
                "{} is already initialized",
                self.root.display()
            )));
        }

        files::make_dir(&self.root.join("nodes"))?;
        files::make_dir(&self.root.join("uploads"))?;
        for node in nodes.values() {
            self.initialize_node_dir(&node.id)?;
        }
        files::sync_dir(&self.root)?;
        let checksum_algorithm = config.resolved_checksum();
        let catalog = Catalog {
            block_bytes: config.block_bytes,
            staging_limit: config.staging_limit,
            next_id: 1,
            checksum_algorithm,
            fragment_checksum: checksum_algorithm,
            checksum: checksum_algorithm,
            nodes,
            uploads: Default::default(),
            objects: Default::default(),
            repairs: Default::default(),
            retired: Vec::new(),
        };
        self.commit_catalog(&catalog, "initialize")
    }

    pub fn catalog(&self) -> Result<Catalog> {
        let _lock = files::lock(&self.existing_catalog_lock()?, LockMode::Shared)?;
        self.load_catalog()
    }

    /// Algorithm used for fragments written after this call. Existing
    /// fragments are untouched and keep verifying with their own algorithm.
    pub fn set_checksum_algorithm(
        &self,
        algorithm: crate::codec::ChecksumAlgorithm,
    ) -> Result<crate::codec::ChecksumAlgorithm> {
        let mut txn = self.transaction()?;
        txn.catalog.set_checksum_algorithm(algorithm);
        txn.commit("checksum-policy")?;
        Ok(algorithm)
    }

    /// Persist SHA-256 as the checksum for subsequent writes.
    pub fn enable_strong_checksums(&self) -> Result<()> {
        self.set_checksum_algorithm(crate::codec::ChecksumAlgorithm::Sha256)?;
        Ok(())
    }

    /// Alias for [`Cluster::enable_strong_checksums`].
    pub fn enable_sha256(&self) -> Result<()> {
        self.enable_strong_checksums()
    }

    /// Current new-write checksum algorithm.
    pub fn checksum_algorithm(&self) -> Result<crate::codec::ChecksumAlgorithm> {
        Ok(self.catalog()?.checksum_algorithm)
    }

    /// Alias for [`Cluster::set_checksum_algorithm`].
    pub fn set_fragment_checksum(
        &self,
        algorithm: crate::codec::ChecksumAlgorithm,
    ) -> Result<crate::codec::ChecksumAlgorithm> {
        self.set_checksum_algorithm(algorithm)
    }

    /// Alias for [`Cluster::set_checksum_algorithm`].
    pub fn configure_checksum(
        &self,
        algorithm: crate::codec::ChecksumAlgorithm,
    ) -> Result<crate::codec::ChecksumAlgorithm> {
        self.set_checksum_algorithm(algorithm)
    }

    pub fn object(&self, key: &str) -> Result<ObjectGeneration> {
        checked_key(key)?;
        self.catalog()?
            .objects
            .remove(key)
            .ok_or_else(|| Error::NotFound(format!("object {key:?}")))
    }

    pub fn delete(&self, key: &str, condition: Condition) -> Result<u64> {
        checked_key(key)?;
        let mut txn = self.transaction()?;
        check_condition(key, condition, txn.catalog.objects.get(key))?;
        let object = txn
            .catalog
            .objects
            .remove(key)
            .ok_or_else(|| Error::NotFound(format!("object {key:?}")))?;
        txn.catalog.retire_object(&object);
        txn.commit("delete")?;
        Ok(object.generation)
    }

    pub(crate) fn transaction(&self) -> Result<Txn<'_>> {
        let lock = files::lock(&self.existing_catalog_lock()?, LockMode::Exclusive)?
            .expect("blocking catalog lock");
        let catalog = self.load_catalog()?;
        Ok(Txn {
            cluster: self,
            _lock: lock,
            catalog,
        })
    }

    fn existing_catalog_lock(&self) -> Result<PathBuf> {
        if !self.root.is_dir() {
            return Err(Error::NotFound(format!(
                "cluster directory {}",
                self.root.display()
            )));
        }
        Ok(self.root.join(CATALOG_LOCK))
    }

    fn load_catalog(&self) -> Result<Catalog> {
        let path = self.catalog_path();
        let bytes = files::read_limited(&path, v1::MAX_CATALOG_BYTES).map_err(|error| {
            if error.is_not_found_io() {
                Error::NotFound(format!("v1 catalog {}", path.display()))
            } else {
                error
            }
        })?;
        v1::decode(&bytes)
    }

    fn commit_catalog(&self, catalog: &Catalog, action: &str) -> Result<()> {
        let bytes = v1::encode(catalog)?;
        let path = self.catalog_path();
        let mut draft = DraftFile::create(&path)?;
        draft.write(&bytes)?;
        draft.sync(self, "catalog")?;
        self.checkpoint(Boundary::BeforeCatalogCommit, action)?;
        draft.publish(&path)
    }

    pub fn catalog_path(&self) -> PathBuf {
        self.root.join(CATALOG)
    }

    pub fn export_catalog(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let data = v1::encode(&self.catalog()?)?;
        let mut draft = DraftFile::create_output(self, path)?;
        draft.write(&data)?;
        draft.sync(self, "catalog-export")?;
        draft.publish(path)
    }

    pub(crate) fn pin_fragments(&self) -> Result<File> {
        files::lock(&self.root.join(READER_LOCK), LockMode::Shared)
            .map(|file| file.expect("blocking reader lock"))
    }

    pub(crate) fn try_pin_cleanup(&self) -> Result<Option<File>> {
        files::lock(&self.root.join(READER_LOCK), LockMode::TryExclusive)
    }

    pub(crate) fn node_dir(&self, node: &str) -> PathBuf {
        self.root.join("nodes").join(node).join("fragments")
    }

    pub(crate) fn initialize_node_dir(&self, node: &str) -> Result<()> {
        let fragments = self.node_dir(node);
        files::make_dir(&fragments)?;
        let node_dir = files::parent(&fragments)?;
        files::sync_dir(node_dir)?;
        files::sync_dir(files::parent(node_dir)?)
    }

    pub fn fragment_path(&self, address: &FragmentAddress) -> Result<PathBuf> {
        super::checked_id(&address.node)?;
        if address.shard > 2 || address.generation == 0 || address.revision == 0 {
            return Err(Error::Invalid("invalid fragment address".into()));
        }
        Ok(self.node_dir(&address.node).join(address.file_name()))
    }
}
