mod crc32c;
mod stream;
pub use crc32c::{checksum, Crc32c};
pub use stream::{FragmentStream, StreamFault, STREAM_WINDOW};

use std::io::{self, Read};

use crate::{Error, Result};

pub const DATA_SHARDS: usize = 2;
pub const SHARDS: usize = 3;

pub fn stripe_lengths(remaining: u64, block: u32) -> [u32; SHARDS] {
    let first = remaining.min(u64::from(block)) as u32;
    let second = (remaining - u64::from(first)).min(u64::from(block)) as u32;
    [first, second, first]
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedStripe {
    pub shards: [Vec<u8>; SHARDS],
    pub checksums: [u32; SHARDS],
}

impl EncodedStripe {
    pub fn read(input: &mut impl Read, lengths: [u32; SHARDS]) -> io::Result<Self> {
        let [first_len, second_len, parity_len] = lengths;
        if parity_len != first_len || second_len > first_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid 2+1 stripe geometry",
            ));
        }
        let mut first = vec![0; first_len as usize];
        let mut second = vec![0; second_len as usize];
        input.read_exact(&mut first)?;
        input.read_exact(&mut second)?;
        let parity = xor_prefix(&first, &second, parity_len as usize);
        let checksums = [checksum(&first), checksum(&second), checksum(&parity)];
        Ok(Self {
            shards: [first, second, parity],
            checksums,
        })
    }
}

fn xor_prefix(first: &[u8], second: &[u8], output: usize) -> Vec<u8> {
    let mut bytes = vec![0; output];
    for (dst, src) in bytes.iter_mut().zip(first) {
        *dst = *src;
    }
    for (dst, src) in bytes.iter_mut().zip(second) {
        *dst ^= src;
    }
    bytes
}

/// XOR two equal-length windows in place: `window ^= other`.
///
/// Streaming reconstruction keeps only fixed-size windows in memory, so the
/// peak buffer stays independent of the shard length. Absent tail bytes of a
/// shorter shard 1 are represented by callers passing zero-filled windows.
pub fn xor_window(window: &mut [u8], other: &[u8]) {
    debug_assert_eq!(window.len(), other.len());
    for (dst, src) in window.iter_mut().zip(other) {
        *dst ^= *src;
    }
}

/// Reconstruct one logical shard from the other two already verified shards.
pub fn reconstruct(
    lengths: [u32; SHARDS],
    missing: u8,
    present: [Option<&[u8]>; SHARDS],
) -> Result<Vec<u8>> {
    let missing = usize::from(missing);
    if missing >= SHARDS || lengths[0] != lengths[2] || lengths[1] > lengths[0] {
        return Err(Error::Invalid("invalid reconstruction geometry".into()));
    }
    let mut sources = Vec::with_capacity(2);
    for (index, shard) in present.into_iter().enumerate() {
        if index == missing {
            if shard.is_some() {
                return Err(Error::Invalid(
                    "target shard was also supplied as a source".into(),
                ));
            }
            continue;
        }
        match shard {
            Some(bytes) if bytes.len() == lengths[index] as usize => sources.push(bytes),
            _ => {
                return Err(Error::Corrupt(format!(
                    "reconstruction source shard {index} missing or wrong length"
                )))
            }
        }
    }
    Ok(xor_prefix(
        sources[0],
        sources[1],
        lengths[missing] as usize,
    ))
}
