use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::files;
use crate::manifest::PartReceipt;
use crate::{Cluster, Error, Result};

use super::frame::{self, StagedExtent, StagingIndex};
use super::paths::PartLocation;
use super::PACK_TARGET;

/// In-memory view of one upload directory's v2 sidecar. The index is loaded
/// lazily and cached for the lifetime of the store; all writers go through
/// [`StagingStore::add_part`], which rewrites the sidecar atomically.
pub(crate) struct StagingStore<'a> {
    cluster: &'a Cluster,
    upload: u64,
    index: Option<StagingIndex>,
    loaded: bool,
}

impl<'a> StagingStore<'a> {
    pub(crate) fn open(cluster: &'a Cluster, upload: u64) -> Self {
        Self {
            cluster,
            upload,
            index: None,
            loaded: false,
        }
    }

    fn ensure_loaded(&mut self) -> Result<&StagingIndex> {
        if !self.loaded {
            self.index = self.cluster.load_index(self.upload)?;
            self.loaded = true;
        }
        self.index.as_ref().ok_or_else(|| {
            Error::Corrupt(format!(
                "staging index for upload {} is unavailable",
                self.upload
            ))
        })
    }

    /// Resolve a current part receipt. A loose part always wins when its file
    /// exists, which keeps pre-upgrade sessions on their original bytes; a
    /// packed part is only returned when the sidecar extent matches the
    /// current revision, length and checksum.
    pub(crate) fn locate(&mut self, receipt: &PartReceipt) -> Result<PartLocation> {
        let loose = self
            .cluster
            .loose_part_path(self.upload, receipt.number, receipt.revision);
        if loose
            .try_exists()
            .map_err(|e| Error::io("locate staged part", &loose, e))?
        {
            return Ok(PartLocation::Loose);
        }
        let index = self.ensure_loaded()?;
        let Some(extent) = index.get(receipt.revision) else {
            return Err(Error::Corrupt(format!(
                "staged part {} has no file for revision {}",
                receipt.number, receipt.revision
            )));
        };
        if !PartLocation::Packed(extent).matches_receipt(receipt) {
            return Err(Error::Corrupt(format!(
                "staging index extent for revision {} does not match part {} receipt",
                receipt.revision, receipt.number
            )));
        }
        Ok(PartLocation::Packed(extent))
    }

    /// Pick the (pack, offset) at which a part of `length` bytes can be
    /// appended. The offset is the maximum extent end inside the highest pack
    /// id, so pruning a superseded revision never reuses or collides with live
    /// bytes. Packs fill up to [`PACK_TARGET`]; ids never collide with a file
    /// an interrupted run left on disk even if the sidecar missed it.
    fn allocate(&self, index: &StagingIndex, length: u64) -> Result<(u64, u64)> {
        let highest = index.parts.values().map(|extent| extent.pack).max();
        let Some(highest) = highest else {
            return Ok((self.next_free_pack(1)?, 0));
        };
        let tail = index
            .parts
            .values()
            .filter(|extent| extent.pack == highest)
            .map(|extent| {
                extent
                    .offset
                    .checked_add(u64::from(extent.length))
                    .ok_or_else(|| Error::Corrupt("pack extent overflow".into()))
            })
            .try_fold(0_u64, |acc, end| end.map(|end| acc.max(end)))?;
        if tail > 0 && tail + length <= PACK_TARGET {
            Ok((highest, tail))
        } else {
            Ok((self.next_free_pack(highest + 1)?, 0))
        }
    }

    fn next_free_pack(&self, mut candidate: u64) -> Result<u64> {
        loop {
            if !self.cluster.pack_path(self.upload, candidate).exists() {
                return Ok(candidate);
            }
            candidate = candidate
                .checked_add(1)
                .ok_or_else(|| Error::Capacity("staging pack sequence exhausted".into()))?;
        }
    }

    /// Append one part's already-buffered bytes to a v2 pack and atomically
    /// publish the sidecar referencing its extent. Existing extents are never
    /// rewritten, so replacing a part cannot disturb another revision's bytes and
    /// a crash can only truncate the new part at its tail. When `superseded`
    /// names the revision this part replaces, its extent is dropped from the
    /// sidecar in the same publication; the bytes it occupied are reclaimed by
    /// gc once their whole pack is no longer referenced.
    pub(crate) fn append_part(
        &mut self,
        receipt: &PartReceipt,
        bytes: &[u8],
        superseded: Option<u64>,
    ) -> Result<StagedExtent> {
        debug_assert_eq!(bytes.len(), receipt.bytes as usize);
        let mut index = match self.cluster.load_index(self.upload)? {
            Some(index) => index,
            None => StagingIndex::default(),
        };
        if let Some(old_revision) = superseded {
            index.parts.remove(&old_revision);
        }
        // A replacement can leave a hole at the tail of the current pack, so it
        // always starts a fresh pack. Pure appends (superseded == None) pack
        // contiguously into the highest pack while it stays under target.
        let (pack, offset) = if superseded.is_some() {
            let highest = index
                .parts
                .values()
                .map(|extent| extent.pack)
                .max()
                .unwrap_or(0);
            (self.next_free_pack(highest + 1)?, 0)
        } else {
            self.allocate(&index, bytes.len() as u64)?
        };
        let path = self.cluster.pack_path(self.upload, pack);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)
            .map_err(|e| Error::io("open pack for append", &path, e))?;
        // The index is the source of truth for extents. Seek to the recorded
        // offset and verify the file currently ends exactly there, so a torn
        // append from an interrupted run cannot shift or overwrite a part.
        let file_len = file
            .metadata()
            .map_err(|e| Error::io("inspect pack", &path, e))?
            .len();
        if file_len != offset {
            return Err(Error::Corrupt(format!(
                "pack {pack} length {file_len} does not match index append offset {offset}"
            )));
        }
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| Error::io("seek pack append", &path, e))?;
        file.write_all(bytes)
            .map_err(|e| Error::io("append pack extent", &path, e))?;
        file.sync_all()
            .map_err(|e| Error::io("sync pack", &path, e))?;
        drop(file);
        files::sync_dir(files::parent(&path)?)?;

        let extent = StagedExtent {
            pack,
            offset,
            length: receipt.bytes,
            crc32c: receipt.crc32c,
        };
        if index.parts.insert(receipt.revision, extent).is_some() {
            return Err(Error::Corrupt(format!(
                "pack revision {} already staged for upload {}",
                receipt.revision, self.upload
            )));
        }
        self.cluster
            .save_index(self.upload, &index, "staging-index")?;
        self.index = Some(index);
        self.loaded = true;
        Ok(extent)
    }
}

impl Cluster {
    pub(crate) fn load_index(&self, upload: u64) -> Result<Option<StagingIndex>> {
        let path = self.index_path(upload);
        let bytes = match files::read_limited(&path, frame::MAX_INDEX_BYTES) {
            Ok(bytes) => bytes,
            Err(error) if error.is_not_found_io() => return Ok(None),
            Err(error) => return Err(error),
        };
        Ok(Some(frame::decode(&bytes)?))
    }

    pub(crate) fn save_index(&self, upload: u64, index: &StagingIndex, detail: &str) -> Result<()> {
        let path = self.index_path(upload);
        files::atomic_file(self, &path, &frame::encode(index)?, detail)
    }

    /// Read exactly the bytes of one packed extent, no further. The file length
    /// and extent boundary are checked so neighbouring or superseded parts in
    /// the same pack can never feed the object.
    pub(crate) fn read_packed(
        &self,
        upload: u64,
        receipt: &PartReceipt,
        extent: StagedExtent,
    ) -> Result<Vec<u8>> {
        if extent.length != receipt.bytes || extent.crc32c != receipt.crc32c {
            return Err(Error::Corrupt(format!(
                "packed extent does not match part {} receipt",
                receipt.number
            )));
        }
        let path = self.pack_path(upload, extent.pack);
        let mut file = File::open(&path).map_err(|e| Error::io("open pack", &path, e))?;
        let file_len = file
            .metadata()
            .map_err(|e| Error::io("inspect pack", &path, e))?
            .len();
        let end = extent
            .offset
            .checked_add(u64::from(extent.length))
            .filter(|end| *end <= file_len)
            .ok_or_else(|| {
                Error::Corrupt(format!(
                    "pack {} extent for part {} runs past the file",
                    extent.pack, receipt.number
                ))
            })?;
        file.seek(SeekFrom::Start(extent.offset))
            .map_err(|e| Error::io("seek pack", &path, e))?;
        let mut bytes = vec![0; extent.length as usize];
        file.read_exact(&mut bytes)
            .map_err(|e| Error::io("read pack extent", &path, e))?;
        if crate::codec::checksum(&bytes) != receipt.crc32c {
            return Err(Error::Corrupt(format!(
                "packed part {} checksum mismatch",
                receipt.number
            )));
        }
        // Confirm no bytes from a neighbouring extent were pulled in.
        debug_assert_eq!(bytes.len(), receipt.bytes as usize);
        let _ = end;
        Ok(bytes)
    }

    /// Every v2 pack id still referenced by an upload's sidecar.
    pub(crate) fn referenced_packs(&self, upload: u64) -> Result<BTreeMap<u64, ()>> {
        let mut packs = BTreeMap::new();
        if let Some(index) = self.load_index(upload)? {
            for extent in index.parts.values() {
                packs.insert(extent.pack, ());
            }
        }
        Ok(packs)
    }

    /// Drop sidecar extents whose revision is not a current part receipt. Such
    /// an extent can only come from a `put_part` that published its sidecar but
    /// crashed before the catalog commit; mirroring the v1 collector, staging
    /// that the catalog no longer references is reclaimed. Returns whether the
    /// sidecar was rewritten. Runs under the exclusive cleanup pin, so no
    /// completion or other collector can be touching the same directory.
    pub(crate) fn reconcile_index(
        &self,
        upload: u64,
        live_revisions: &BTreeSet<u64>,
    ) -> Result<bool> {
        let Some(mut index) = self.load_index(upload)? else {
            return Ok(false);
        };
        let before = index.parts.len();
        index
            .parts
            .retain(|revision, _| live_revisions.contains(revision));
        if index.parts.len() == before {
            return Ok(false);
        }
        self.save_index(upload, &index, "staging-reconcile")?;
        Ok(true)
    }

    /// Test support: read the decoded sidecar for an upload, which must exist.
    #[doc(hidden)]
    pub fn load_index_for_test(&self, upload: u64) -> Result<StagingIndex> {
        self.load_index(upload)?
            .ok_or_else(|| Error::NotFound(format!("staging index for upload {upload}")))
    }

    /// Test support: inject an extra extent that no catalog receipt references,
    /// modelling a put_part that died between sidecar and catalog commits.
    #[doc(hidden)]
    pub fn inject_orphan_extent_for_test(
        &self,
        upload: u64,
        revision: u64,
        extent: StagedExtent,
    ) -> Result<()> {
        let mut index = self.load_index(upload)?.unwrap_or_default();
        index.parts.insert(revision, extent);
        self.save_index(upload, &index, "staging-test-inject")
    }
}

/// Iterate upload directories, tolerating a missing root the way the v1
/// collector does.
pub(crate) fn upload_directories(root: &Path) -> Result<Vec<std::fs::DirEntry>> {
    match files::entries(&root.join("uploads")) {
        Err(error) if error.is_not_found_io() => Ok(Vec::new()),
        result => result,
    }
}
