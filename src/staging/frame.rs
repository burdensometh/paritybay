//! v2 compact staging sidecar frames.
//!
//! A v2 upload directory keeps staged part bytes in numbered pack files and
//! records the placement of every packed part in a single sidecar index. The
//! frame shares the v1 framing conventions: an eight byte magic, a little
//! endian payload length, a CRC32C over the payload, then the payload itself.
//! The catalog stays on the v1 wire format; the sidecar only maps part
//! revisions to pack extents.

use std::collections::BTreeMap;

use crate::codec::checksum;
use crate::manifest::MAX_PARTS;
use crate::{Error, Result};

pub const MAGIC: [u8; 8] = *b"PBAYSIDX";
pub const HEADER_BYTES: usize = 16;
pub const MAX_INDEX_BYTES: usize = 64 * 1024 * 1024;
pub const INDEX_FILE: &str = "staging.v2";

/// One packed extent. `length` and the part receipt always carry the same
/// value; the extent is independently validated against the receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StagedExtent {
    pub pack: u64,
    pub offset: u64,
    pub length: u32,
    pub crc32c: u32,
}

/// Records keyed by part revision. A revision appears at most once, so an
/// index can never point a current receipt at a superseded part's bytes.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StagingIndex {
    pub parts: BTreeMap<u64, StagedExtent>,
}

impl StagingIndex {
    pub fn get(&self, revision: u64) -> Option<StagedExtent> {
        self.parts.get(&revision).copied()
    }
}

pub fn encode(index: &StagingIndex) -> Result<Vec<u8>> {
    let count = u32::try_from(index.parts.len())
        .map_err(|_| Error::Capacity("staging index too large".into()))?;
    if count as usize > MAX_PARTS {
        return Err(Error::Capacity("staging index too large".into()));
    }
    let mut out = Vec::with_capacity(HEADER_BYTES + 4 + index.parts.len() * 28);
    out.extend_from_slice(&[0; HEADER_BYTES]);
    out.extend_from_slice(&count.to_le_bytes());
    for (revision, extent) in &index.parts {
        out.extend_from_slice(&revision.to_le_bytes());
        out.extend_from_slice(&extent.pack.to_le_bytes());
        out.extend_from_slice(&extent.offset.to_le_bytes());
        out.extend_from_slice(&extent.length.to_le_bytes());
        out.extend_from_slice(&extent.crc32c.to_le_bytes());
    }
    if out.len() > MAX_INDEX_BYTES {
        return Err(Error::Capacity(format!(
            "staging index exceeds {MAX_INDEX_BYTES} bytes"
        )));
    }

    let payload_len = out.len() - HEADER_BYTES;
    let size = (payload_len as u32).to_le_bytes();
    let crc = checksum(&out[HEADER_BYTES..]).to_le_bytes();
    out[..8].copy_from_slice(&MAGIC);
    out[8..12].copy_from_slice(&size);
    out[12..16].copy_from_slice(&crc);
    Ok(out)
}

pub fn decode(bytes: &[u8]) -> Result<StagingIndex> {
    if bytes.len() > MAX_INDEX_BYTES {
        return Err(Error::Corrupt("oversized staging index".into()));
    }
    let mut header = Decoder::new(bytes);
    if header.take(MAGIC.len())? != MAGIC {
        return Err(Error::Corrupt("unknown staging index format".into()));
    }
    let payload_length = header.u32()? as usize;
    let payload_checksum = header.u32()?;
    let payload = header.take(payload_length)?;
    header.finish()?;
    if checksum(payload) != payload_checksum {
        return Err(Error::Corrupt("staging index checksum mismatch".into()));
    }

    let mut input = Decoder::new(payload);
    let count = input.u32()? as usize;
    if count > MAX_PARTS || count > input.remaining() / 28 {
        return Err(input.error("invalid staging index length"));
    }
    let mut parts = BTreeMap::new();
    let mut previous: Option<u64> = None;
    for _ in 0..count {
        let revision = input.u64()?;
        let pack = input.u64()?;
        let offset = input.u64()?;
        let length = input.u32()?;
        let crc = input.u32()?;
        if revision == 0 || pack == 0 || length == 0 {
            return Err(input.error("invalid staging extent"));
        }
        if let Some(previous) = previous {
            if revision <= previous {
                return Err(input.error("staging index entries must be unique and ordered"));
            }
        }
        previous = Some(revision);
        if parts
            .insert(
                revision,
                StagedExtent {
                    pack,
                    offset,
                    length,
                    crc32c: crc,
                },
            )
            .is_some()
        {
            return Err(input.error("duplicate staged extent"));
        }
    }
    input.finish()?;
    Ok(StagingIndex { parts })
}

struct Decoder<'a> {
    rest: &'a [u8],
    read: usize,
}

impl<'a> Decoder<'a> {
    fn new(rest: &'a [u8]) -> Self {
        Self { rest, read: 0 }
    }

    fn error(&self, message: &str) -> Error {
        Error::Corrupt(format!("staging index byte {}: {message}", self.read))
    }

    fn take(&mut self, bytes: usize) -> Result<&'a [u8]> {
        let (head, tail) = self
            .rest
            .split_at_checked(bytes)
            .ok_or_else(|| self.error("truncated field"))?;
        self.rest = tail;
        self.read += bytes;
        Ok(head)
    }

    fn finish(self) -> Result<()> {
        if self.rest.is_empty() {
            return Ok(());
        }
        Err(self.error("trailing bytes"))
    }

    fn remaining(&self) -> usize {
        self.rest.len()
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
}
