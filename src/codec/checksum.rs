//! Algorithm-agile fragment checksums.
//!
//! Legacy v1 receipts only carry a CRC-32C digest; such receipts decode as
//! [`ChecksumAlgorithm::Crc32c`] and keep encoding byte-for-byte as v1. New
//! fragments may instead use [`ChecksumAlgorithm::Sha256`]. Every receipt
//! records the algorithm it was actually written with, so one catalog can mix
//! CRC-32C and SHA-256 fragments and verify each with its own algorithm.

use std::fmt;
use std::str::FromStr;

use super::crc32c::checksum as crc32c_of;
use super::sha256::{hex_encode, sha256};
use crate::{Error, Result};

/// Selectable fragment checksum algorithms.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ChecksumAlgorithm {
    /// Castagnoli CRC-32 as used by every v1 fragment and catalog.
    Crc32c,
    /// FIPS 180-4 SHA-256, the stronger checksum offered for new writes.
    Sha256,
}

// Naming aliases used across callers and tests.
pub use ChecksumAlgorithm as Algorithm;
pub use ChecksumAlgorithm as ChecksumKind;
pub use ChecksumAlgorithm as ChecksumType;
pub use ChecksumAlgorithm as DigestAlgorithm;
pub use ChecksumAlgorithm as HashAlgorithm;

impl ChecksumAlgorithm {
    /// Canonical lowercase configuration / catalog token.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Crc32c => "crc32c",
            Self::Sha256 => "sha256",
        }
    }

    /// All accepted spellings of the algorithm, including CLI aliases.
    pub fn parse_token(token: &str) -> Result<Self> {
        let normalized = token.trim().to_ascii_lowercase().replace(['-', '_'], "");
        match normalized.as_str() {
            "crc32c" | "crc32" | "crc" => Ok(Self::Crc32c),
            "sha256" | "sha2" | "strong" => Ok(Self::Sha256),
            _ => Err(Error::Invalid(format!(
                "unknown checksum algorithm {token:?}; use crc32c or sha256"
            ))),
        }
    }

    /// Number of raw digest bytes (4 for CRC-32C, 32 for SHA-256).
    pub fn digest_len(self) -> usize {
        match self {
            Self::Crc32c => 4,
            Self::Sha256 => 32,
        }
    }

    /// Compute the digest of `bytes` with this algorithm.
    pub fn compute(self, bytes: &[u8]) -> Checksum {
        match self {
            Self::Crc32c => Checksum::Crc32c(crc32c_of(bytes)),
            Self::Sha256 => Checksum::Sha256(sha256(bytes)),
        }
    }

    /// Alias for [`ChecksumAlgorithm::compute`].
    pub fn digest(self, bytes: &[u8]) -> Checksum {
        self.compute(bytes)
    }

    /// Alias for [`ChecksumAlgorithm::compute`].
    pub fn hash(self, bytes: &[u8]) -> Checksum {
        self.compute(bytes)
    }

    /// Streaming hasher for this algorithm.
    pub fn hasher(self) -> ChecksumHasher {
        ChecksumHasher::new(self)
    }

    /// True when this is the stronger SHA-256 checksum.
    pub fn is_strong(self) -> bool {
        matches!(self, Self::Sha256)
    }
}

/// Alternative spellings for callers that name the variants differently.
#[allow(non_upper_case_globals)]
impl ChecksumAlgorithm {
    pub const CRC32C: ChecksumAlgorithm = ChecksumAlgorithm::Crc32c;
    pub const CRC32: ChecksumAlgorithm = ChecksumAlgorithm::Crc32c;
    pub const Crc32: ChecksumAlgorithm = ChecksumAlgorithm::Crc32c;
    pub const SHA256: ChecksumAlgorithm = ChecksumAlgorithm::Sha256;
    pub const SHA_256: ChecksumAlgorithm = ChecksumAlgorithm::Sha256;
    pub const Sha: ChecksumAlgorithm = ChecksumAlgorithm::Sha256;
    pub const Strong: ChecksumAlgorithm = ChecksumAlgorithm::Sha256;
}

/// Compute a tagged checksum for `bytes` using `algorithm`.
pub fn compute_checksum(algorithm: ChecksumAlgorithm, bytes: &[u8]) -> Checksum {
    algorithm.compute(bytes)
}

/// Strong-checksum helper returning the tagged SHA-256 digest.
pub fn strong_checksum(bytes: &[u8]) -> Checksum {
    Checksum::Sha256(sha256(bytes))
}

/// SHA-256 digest as a tagged [`Checksum`].
pub fn sha256_checksum(bytes: &[u8]) -> Checksum {
    Checksum::Sha256(sha256(bytes))
}

/// Alias some callers use for the SHA-256 routine.
pub fn checksum_sha256(bytes: &[u8]) -> [u8; 32] {
    sha256(bytes)
}

impl Default for ChecksumAlgorithm {
    /// v1 compatibility default.
    fn default() -> Self {
        Self::Crc32c
    }
}

impl fmt::Display for ChecksumAlgorithm {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(self.as_str())
    }
}

impl FromStr for ChecksumAlgorithm {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        Self::parse_token(value)
    }
}

/// A concrete digest value together with its algorithm.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Checksum {
    Crc32c(u32),
    Sha256([u8; 32]),
}

impl Default for Checksum {
    /// Zero CRC-32C is the neutral default; real receipts always set a value.
    fn default() -> Self {
        Self::Crc32c(0)
    }
}

/// Naming aliases for the concrete digest value.
pub use Checksum as ChecksumDigest;
pub use Checksum as Digest;
pub use Checksum as FragmentChecksum;

impl Checksum {
    /// Algorithm that produced this digest.
    pub fn algorithm(&self) -> ChecksumAlgorithm {
        match self {
            Self::Crc32c(_) => ChecksumAlgorithm::Crc32c,
            Self::Sha256(_) => ChecksumAlgorithm::Sha256,
        }
    }

    /// Verify candidate bytes against this digest.
    pub fn verify(&self, bytes: &[u8]) -> bool {
        &self.algorithm().compute(bytes) == self
    }

    /// Alias for [`Checksum::verify`].
    pub fn matches(&self, bytes: &[u8]) -> bool {
        self.verify(bytes)
    }

    /// Raw digest bytes in little-endian order for the CRC word.
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Crc32c(value) => value.to_le_bytes().to_vec(),
            Self::Sha256(value) => value.to_vec(),
        }
    }

    /// Borrow the raw digest bytes where possible.
    pub fn as_array32(&self) -> Option<[u8; 32]> {
        match self {
            Self::Sha256(value) => Some(*value),
            Self::Crc32c(_) => None,
        }
    }

    pub fn crc32c(&self) -> Option<u32> {
        match self {
            Self::Crc32c(value) => Some(*value),
            Self::Sha256(_) => None,
        }
    }

    pub fn sha256(&self) -> Option<[u8; 32]> {
        self.as_array32()
    }

    /// Lowercase hexadecimal digest.
    pub fn to_hex(&self) -> String {
        match self {
            Self::Crc32c(value) => format!("{value:08x}"),
            Self::Sha256(value) => hex_encode(value),
        }
    }

    pub fn hex(&self) -> String {
        self.to_hex()
    }
}

impl fmt::Display for Checksum {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(out, "{}:{}", self.algorithm(), self.to_hex())
    }
}

/// Streaming hasher that can finalize into an algorithm-tagged [`Checksum`].
#[derive(Clone)]
pub struct ChecksumHasher {
    algorithm: ChecksumAlgorithm,
    crc: super::crc32c::Crc32c,
    sha: super::sha256::Sha256,
}

impl ChecksumHasher {
    pub fn new(algorithm: ChecksumAlgorithm) -> Self {
        Self {
            algorithm,
            crc: super::crc32c::Crc32c::default(),
            sha: super::sha256::Sha256::new(),
        }
    }

    pub fn update(&mut self, bytes: &[u8]) {
        match self.algorithm {
            ChecksumAlgorithm::Crc32c => self.crc.update(bytes),
            ChecksumAlgorithm::Sha256 => self.sha.update(bytes),
        }
    }

    pub fn finish(self) -> Checksum {
        match self.algorithm {
            ChecksumAlgorithm::Crc32c => Checksum::Crc32c(self.crc.finish()),
            ChecksumAlgorithm::Sha256 => Checksum::Sha256(self.sha.finish()),
        }
    }
}
