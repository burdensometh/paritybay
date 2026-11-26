use std::fs::File;
use std::path::{Path, PathBuf};

use crate::files::{self, DraftFile, LockMode};
use crate::{Boundary, Cluster, Config, Error, MigrationReport, Result};

use super::{
    check_condition, checked_key, v1, v2, Catalog, Condition, FragmentAddress, ObjectGeneration,
};

const CATALOG_V1: &str = "catalog.v1";
const CATALOG_V2: &str = "catalog.v2";
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
        let catalog = Catalog {
            block_bytes: config.block_bytes,
            staging_limit: config.staging_limit,
            next_id: 1,
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
        // A migrated cluster may still carry its superseded v1 snapshot; the
        // complete v2 catalog is always authoritative.
        let v2_path = self.root.join(CATALOG_V2);
        match files::read_limited(&v2_path, v2::MAX_CATALOG_BYTES) {
            Ok(bytes) => return v2::decode(&bytes),
            Err(error) if error.is_not_found_io() => {}
            Err(error) => return Err(error),
        }
        let v1_path = self.root.join(CATALOG_V1);
        let bytes = files::read_limited(&v1_path, v1::MAX_CATALOG_BYTES).map_err(|error| {
            if error.is_not_found_io() {
                Error::NotFound(format!("catalog in {}", self.root.display()))
            } else {
                error
            }
        })?;
        v1::decode(&bytes)
    }

    fn commit_catalog(&self, catalog: &Catalog, action: &str) -> Result<()> {
        // Commits preserve the catalog's current format; v2-migrate performs
        // the only format switch.
        let (path, compact) = self.catalog_location();
        let bytes = if compact {
            v2::encode(catalog)?
        } else {
            v1::encode(catalog)?
        };
        let mut draft = DraftFile::create(&path)?;
        draft.write(&bytes)?;
        draft.sync(self, "catalog")?;
        self.checkpoint(Boundary::BeforeCatalogCommit, action)?;
        draft.publish(&path)
    }

    /// Switches the cluster catalog to the compact v2 format. The v2 file
    /// appears by atomic rename, so an interruption leaves the last complete
    /// catalog in place; a repeated call finishes the cleanup.
    pub fn migrate_v2(&self) -> Result<MigrationReport> {
        let txn = self.transaction()?;
        let v1_path = self.root.join(CATALOG_V1);
        let v2_path = self.root.join(CATALOG_V2);
        let switched = if v2_path.is_file() {
            false
        } else {
            let bytes = v2::encode(&txn.catalog)?;
            let mut draft = DraftFile::create(&v2_path)?;
            draft.write(&bytes)?;
            draft.sync(self, "catalog-migrate")?;
            self.checkpoint(Boundary::BeforeCatalogCommit, "catalog-migrate")?;
            draft.publish(&v2_path)?;
            true
        };
        // The rename already switched the cluster; dropping the superseded
        // v1 snapshot is cleanup that also completes an interrupted switch.
        if files::remove_file(&v1_path)? {
            files::sync_dir(&self.root)?;
        }
        Ok(MigrationReport {
            switched,
            objects: txn.catalog.objects.len(),
            uploads: txn.catalog.uploads.len(),
            nodes: txn.catalog.nodes.len(),
            repairs: txn.catalog.repairs.len(),
            retired: txn.catalog.retired.len(),
        })
    }

    fn catalog_location(&self) -> (PathBuf, bool) {
        let v2_path = self.root.join(CATALOG_V2);
        if v2_path.is_file() {
            (v2_path, true)
        } else {
            (self.root.join(CATALOG_V1), false)
        }
    }

    pub fn catalog_path(&self) -> PathBuf {
        self.catalog_location().0
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
