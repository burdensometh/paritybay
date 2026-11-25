use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::{Error, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Boundary {
    BeforeFileSync,
    BeforeCatalogCommit,
    ReadPinned,
    BeforeRepairCopy,
    BeforeNodeRemoval,
}

impl Boundary {
    pub fn name(self) -> &'static str {
        match self {
            Self::BeforeFileSync => "before-file-sync",
            Self::BeforeCatalogCommit => "before-catalog-commit",
            Self::ReadPinned => "read-pinned",
            Self::BeforeRepairCopy => "before-repair-copy",
            Self::BeforeNodeRemoval => "before-node-removal",
        }
    }
}

pub trait Checkpoint: Send + Sync {
    fn reach(&self, boundary: Boundary, detail: &str) -> Result<()>;
}

pub struct NoCheckpoints;

impl Checkpoint for NoCheckpoints {
    fn reach(&self, _: Boundary, _: &str) -> Result<()> {
        Ok(())
    }
}

static NEXT_GATE: AtomicU64 = AtomicU64::new(0);

pub struct FileGates {
    dir: PathBuf,
    timeout: Duration,
}

impl FileGates {
    pub fn new(dir: impl Into<PathBuf>, timeout: Duration) -> Self {
        Self {
            dir: dir.into(),
            timeout,
        }
    }

    fn claim(&self, boundary: Boundary, event: &Path) -> Result<bool> {
        let arm = self.dir.join(format!("{}.arm", boundary.name()));
        match fs::rename(&arm, event) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == ErrorKind::NotFound && self.dir.is_dir() => Ok(false),
            Err(e) => Err(Error::io("claim checkpoint", &arm, e)),
        }
    }
}

impl Checkpoint for FileGates {
    fn reach(&self, boundary: Boundary, detail: &str) -> Result<()> {
        let id = NEXT_GATE.fetch_add(1, Ordering::Relaxed);
        let stem = format!("{}.{}.{}", boundary.name(), std::process::id(), id);
        let claimed = self.dir.join(format!("{stem}.claimed"));
        if !self.claim(boundary, &claimed)? {
            return Ok(());
        }

        let ready = self.dir.join(format!("{stem}.ready"));
        let resume = self.dir.join(format!("{stem}.resume"));
        let fail = self.dir.join(format!("{stem}.fail"));
        let mut event = OpenOptions::new()
            .append(true)
            .open(&claimed)
            .map_err(|e| Error::io("open checkpoint event", &claimed, e))?;
        writeln!(event, "{}", detail.escape_default())
            .map_err(|e| Error::io("write checkpoint event", &claimed, e))?;
        drop(event);
        fs::rename(&claimed, &ready).map_err(|e| Error::io("publish checkpoint", &ready, e))?;

        let start = Instant::now();
        while start.elapsed() < self.timeout {
            if fail
                .try_exists()
                .map_err(|e| Error::io("check failure gate", &fail, e))?
            {
                return Err(Error::Conflict(format!(
                    "checkpoint {} failed",
                    boundary.name()
                )));
            }
            if resume
                .try_exists()
                .map_err(|e| Error::io("check resume gate", &resume, e))?
            {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Err(Error::Conflict(format!(
            "checkpoint {} timed out",
            boundary.name()
        )))
    }
}
