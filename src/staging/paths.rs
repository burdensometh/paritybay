use std::path::PathBuf;

use crate::manifest::PartReceipt;
use crate::Cluster;

pub const INDEX_NAME: &str = "staging.v2";
pub const PACK_SUFFIX: &str = ".pack";

impl Cluster {
    pub(crate) fn index_path(&self, upload: u64) -> PathBuf {
        self.upload_dir(upload).join(INDEX_NAME)
    }

    pub(crate) fn pack_path(&self, upload: u64, pack: u64) -> PathBuf {
        self.upload_dir(upload)
            .join(format!("{pack:016x}{PACK_SUFFIX}"))
    }

    /// The legacy v1 loose-part location. Existing sessions keep their old
    /// parts there until a compaction packs them.
    pub(crate) fn loose_part_path(&self, upload: u64, number: u32, revision: u64) -> PathBuf {
        self.part_path(upload, number, revision)
    }
}

/// Where the bytes for one current part receipt live. The v2 index is keyed by
/// revision, so a stale or neighbouring revision can never resolve here.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PartLocation {
    /// A legacy `NNNNNNNN-RRRRRRRRRRRRRRRR.part` file.
    Loose,
    /// An extent inside a v2 pack file.
    Packed(super::frame::StagedExtent),
}

impl PartLocation {
    pub(crate) fn matches_receipt(&self, receipt: &PartReceipt) -> bool {
        match self {
            PartLocation::Loose => true,
            PartLocation::Packed(extent) => {
                extent.length == receipt.bytes && extent.crc32c == receipt.crc32c
            }
        }
    }
}

pub(crate) fn is_index_name(name: &str) -> bool {
    name == INDEX_NAME
}

pub(crate) fn pack_id(name: &str) -> Option<u64> {
    let stem = name.strip_suffix(PACK_SUFFIX)?;
    let id = u64::from_str_radix(stem, 16).ok()?;
    (id > 0 && format!("{id:016x}") == stem).then_some(id)
}
