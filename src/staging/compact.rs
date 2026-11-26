use std::collections::BTreeMap;

use crate::files;
use crate::manifest::PartReceipt;
use crate::{Cluster, Result};

use super::paths::PartLocation;
use super::store::StagingStore;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompactionReport {
    pub uploads: usize,
    pub parts_packed: usize,
    pub loose_removed: usize,
    pub packs_written: usize,
}

impl Cluster {
    /// Pack the loose parts of one active upload into v2 pack files while
    /// preserving any parts an earlier, interrupted run already packed.
    ///
    /// The catalog's writer lock is held for the whole run, so no part can be
    /// replaced, completed or aborted underneath the copy. Each part is
    /// verified against its receipt, appended to a pack and published in the
    /// sidecar before its loose file is unlinked. A crash therefore leaves
    /// either the old loose layout usable, or a durable packed copy; the next
    /// call skips parts that already resolve through the sidecar.
    pub(crate) fn compact_upload(
        &self,
        upload: u64,
        receipts: &BTreeMap<u32, PartReceipt>,
        report: &mut CompactionReport,
    ) -> Result<()> {
        if receipts.is_empty() {
            return Ok(());
        }
        let mut store = StagingStore::open(self, upload);
        let mut packed_this_call = 0_usize;
        let mut new_packs: BTreeMap<u64, ()> = BTreeMap::new();

        for receipt in receipts.values() {
            let loose = self.loose_part_path(upload, receipt.number, receipt.revision);
            if !loose
                .try_exists()
                .map_err(|e| crate::Error::io("locate loose part", &loose, e))?
            {
                // An interrupted run may already have packed this part.
                let PartLocation::Packed(_) = store.locate(receipt)? else {
                    return Err(crate::Error::Corrupt(format!(
                        "part {} of upload {} has neither loose nor packed bytes",
                        receipt.number, upload
                    )));
                };
                continue;
            }

            let bytes = files::read_limited(&loose, receipt.bytes as usize)?;
            if bytes.len() != receipt.bytes as usize
                || crate::codec::checksum(&bytes) != receipt.crc32c
            {
                return Err(crate::Error::Corrupt(format!(
                    "loose part {} of upload {} failed verification before packing",
                    receipt.number, upload
                )));
            }
            let extent = store.append_part(receipt, &bytes, None)?;
            new_packs.insert(extent.pack, ());
            packed_this_call += 1;

            // The sidecar entry is durable before the loose copy is removed.
            report.loose_removed += usize::from(files::remove_file(&loose)?);
        }

        if packed_this_call > 0 {
            files::sync_dir(&self.upload_dir(upload))?;
            report.uploads += 1;
            report.parts_packed += packed_this_call;
            report.packs_written += new_packs.len();
        }
        Ok(())
    }

    /// Compact every active upload that still has loose part files.
    pub fn compact_staging(&self) -> Result<CompactionReport> {
        let txn = self.transaction()?;
        let mut report = CompactionReport::default();
        let uploads: Vec<u64> = txn.catalog.uploads.keys().copied().collect();
        for id in uploads {
            let receipts = txn
                .catalog
                .uploads
                .get(&id)
                .map(|upload| upload.parts.clone())
                .expect("upload set cannot shrink under the writer lock");
            self.compact_upload(id, &receipts, &mut report)?;
        }
        Ok(report)
    }
}

/// Packs stay small enough that a single part (at most 64 MiB) and a modest
/// number of neighbours share one file without creating oversized packs.
pub(crate) const PACK_TARGET: u64 = 8 * 1024 * 1024;
