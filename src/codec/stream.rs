use std::fs::File;
use std::io::{ErrorKind, Read};
use std::path::Path;

use super::Crc32c;

/// Fixed size of every streaming verification/repair window. All live
/// buffers in the inspection and repair paths are bounded by this constant,
/// so their memory peak is independent of the configured block size.
pub const STREAM_WINDOW: usize = 64 * 1024;

/// Fault discovered while streaming a fragment whose receipt is known.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamFault {
    /// The fragment file does not exist.
    Missing,
    /// The fragment was shorter, longer or otherwise mis-sized versus its
    /// receipt.
    InvalidLength,
    /// The full fragment had the right length but a mismatched checksum.
    InvalidChecksum,
    /// An underlying filesystem read error.
    Io,
}

/// Streaming reader over one fragment file.
///
/// The fragment is consumed in fixed windows with a rolling CRC32C, so the
/// whole shard is never retained. Windows past the declared fragment length
/// read as zeros, which is exactly how the absent tail of a shorter shard 1
/// participates in XOR reconstruction.
pub struct FragmentStream {
    file: File,
    expected: u32,
    expected_crc: u32,
    remaining: u64,
    read: u64,
    crc: Crc32c,
}

impl FragmentStream {
    /// Open the fragment at `path` for streaming against its receipt.
    pub fn open(
        path: &Path,
        expected: u32,
        expected_crc: u32,
    ) -> std::result::Result<Self, StreamFault> {
        let file = File::open(path).map_err(|error| match error.kind() {
            ErrorKind::NotFound => StreamFault::Missing,
            _ => StreamFault::Io,
        })?;
        Ok(Self {
            file,
            expected,
            expected_crc,
            remaining: u64::from(expected),
            read: 0,
            crc: Crc32c::default(),
        })
    }

    /// Whether every receipt byte has been streamed.
    pub fn exhausted(&self) -> bool {
        self.remaining == 0
    }

    /// Number of receipt bytes not yet streamed.
    pub fn remaining(&self) -> u64 {
        self.remaining
    }

    /// Fill `buf` with the next window. Bytes beyond the declared fragment
    /// length are zero filled instead of read.
    pub fn read_window(&mut self, buf: &mut [u8]) -> std::result::Result<(), StreamFault> {
        debug_assert!(!buf.is_empty());
        if self.remaining == 0 {
            buf.fill(0);
            return Ok(());
        }
        let take = buf.len().min(self.remaining as usize);
        let (head, tail) = buf.split_at_mut(take);
        if let Err(error) = self.file.read_exact(head) {
            return Err(match error.kind() {
                ErrorKind::UnexpectedEof => StreamFault::InvalidLength,
                _ => StreamFault::Io,
            });
        }
        tail.fill(0);
        self.crc.update(head);
        self.read += take as u64;
        self.remaining -= take as u64;
        Ok(())
    }

    /// Confirm the full length was consumed, no trailing bytes exist and the
    /// rolling checksum matches the receipt.
    pub fn finish(mut self) -> std::result::Result<(), StreamFault> {
        if self.remaining != 0 || self.read != u64::from(self.expected) {
            return Err(StreamFault::InvalidLength);
        }
        let mut extra = [0u8; 1];
        loop {
            match self.file.read(&mut extra) {
                Ok(0) => break,
                Ok(_) => return Err(StreamFault::InvalidLength),
                Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                Err(_) => return Err(StreamFault::Io),
            }
        }
        if self.crc.finish() != self.expected_crc {
            return Err(StreamFault::InvalidChecksum);
        }
        Ok(())
    }
}
