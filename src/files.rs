use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{Boundary, Cluster, Error, Result};

static TEMP_ID: AtomicU64 = AtomicU64::new(0);

pub(crate) fn make_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path).map_err(|e| Error::io("create directory", path, e))
}

pub(crate) fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|e| Error::io("sync directory", path, e))
}

pub(crate) fn read_limited(path: &Path, limit: usize) -> Result<Vec<u8>> {
    let file = File::open(path).map_err(|e| Error::io("open", path, e))?;
    let mut buf = Vec::new();
    file.take((limit as u64) + 1)
        .read_to_end(&mut buf)
        .map_err(|e| Error::io("read", path, e))?;
    if buf.len() > limit {
        return Err(Error::Corrupt(format!(
            "{} exceeds {limit} bytes",
            path.display()
        )));
    }
    Ok(buf)
}

pub(crate) fn remove_file(path: &Path) -> Result<bool> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(Error::io("remove file", path, e)),
    }
}

pub(crate) fn entries(path: &Path) -> Result<Vec<fs::DirEntry>> {
    let mut entries = fs::read_dir(path)
        .map_err(|e| Error::io("read directory", path, e))?
        .collect::<io::Result<Vec<_>>>()
        .map_err(|e| Error::io("list directory", path, e))?;
    entries.sort_unstable_by_key(|entry| entry.file_name());
    Ok(entries)
}

pub(crate) fn open_lock(path: &Path) -> Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(|e| Error::io("open lock", path, e))
}

#[derive(Clone, Copy)]
pub(crate) enum LockMode {
    Shared,
    Exclusive,
    TryExclusive,
}

pub(crate) fn lock(path: &Path, mode: LockMode) -> Result<Option<File>> {
    let file = open_lock(path)?;
    match mode {
        LockMode::Shared => file
            .lock_shared()
            .map_err(|e| Error::io("lock shared", path, e))?,
        LockMode::Exclusive => file
            .lock()
            .map_err(|e| Error::io("lock exclusive", path, e))?,
        LockMode::TryExclusive => match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Ok(None),
            Err(TryLockError::Error(e)) => return Err(Error::io("try lock", path, e)),
        },
    }
    Ok(Some(file))
}

pub(crate) struct DraftFile {
    path: PathBuf,
    file: Option<File>,
}

impl DraftFile {
    pub(crate) fn create(final_path: &Path) -> Result<Self> {
        Self::with_prefix(final_path, ".pb-tmp")
    }

    pub(crate) fn create_output(cluster: &Cluster, final_path: &Path) -> Result<Self> {
        fn canonical(path: &Path) -> Result<PathBuf> {
            fs::canonicalize(path).map_err(|error| Error::io("resolve directory", path, error))
        }

        if canonical(parent(final_path)?)?.starts_with(canonical(cluster.root())?) {
            return Err(Error::Invalid(
                "output path must be outside the managed cluster directory".into(),
            ));
        }
        Self::with_prefix(final_path, ".pb-output")
    }

    fn with_prefix(final_path: &Path, prefix: &str) -> Result<Self> {
        let parent = parent(final_path)?;
        loop {
            let id = TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let path = parent.join(format!("{prefix}-{}-{id}", std::process::id()));
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(file) => {
                    return Ok(Self {
                        path,
                        file: Some(file),
                    })
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(Error::io("create temporary file", &path, e)),
            }
        }
    }

    pub(crate) fn write(&mut self, data: &[u8]) -> Result<()> {
        self.writer()
            .write_all(data)
            .map_err(|e| Error::io("write temporary file", &self.path, e))
    }

    pub(crate) fn writer(&mut self) -> &mut File {
        self.file
            .as_mut()
            .expect("draft file open before publication")
    }

    pub(crate) fn sync(&self, cluster: &Cluster, detail: &str) -> Result<()> {
        cluster.checkpoint(Boundary::BeforeFileSync, detail)?;
        self.file
            .as_ref()
            .expect("draft file open before publication")
            .sync_all()
            .map_err(|e| Error::io("sync file", &self.path, e))
    }

    pub(crate) fn publish(mut self, final_path: &Path) -> Result<()> {
        drop(self.file.take());
        fs::rename(&self.path, final_path)
            .map_err(|e| Error::io("rename temporary file", final_path, e))?;
        sync_dir(parent(final_path)?)
    }
}

impl Drop for DraftFile {
    fn drop(&mut self) {
        drop(self.file.take());
        let _ = fs::remove_file(&self.path);
    }
}

pub(crate) fn atomic_file(cluster: &Cluster, path: &Path, data: &[u8], detail: &str) -> Result<()> {
    let mut draft = DraftFile::create(path)?;
    draft.write(data)?;
    draft.sync(cluster, detail)?;
    draft.publish(path)
}

pub(crate) fn parent(path: &Path) -> Result<&Path> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::Invalid(format!("no parent for {}", path.display())))?;
    if parent.as_os_str().is_empty() {
        return Ok(Path::new("."));
    }
    Ok(parent)
}
