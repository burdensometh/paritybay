use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use crate::codec::{Checksum, ChecksumAlgorithm};
use crate::{Error, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Media {
    Hot,
    Capacity,
}

impl fmt::Display for Media {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(match self {
            Self::Hot => "hot",
            Self::Capacity => "capacity",
        })
    }
}

impl FromStr for Media {
    type Err = Error;

    fn from_str(word: &str) -> Result<Self> {
        match word {
            "hot" => Ok(Self::Hot),
            "capacity" => Ok(Self::Capacity),
            _ => Err(Error::Invalid(format!("invalid media class {word:?}"))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DomainPolicy {
    Strict,
    Prefer,
}

impl fmt::Display for DomainPolicy {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(match self {
            Self::Strict => "strict",
            Self::Prefer => "prefer",
        })
    }
}

impl FromStr for DomainPolicy {
    type Err = Error;

    fn from_str(word: &str) -> Result<Self> {
        match word {
            "strict" => Ok(Self::Strict),
            "prefer" => Ok(Self::Prefer),
            _ => Err(Error::Invalid(format!("invalid domain policy {word:?}"))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeState {
    Online,
    Draining,
    Offline,
    Removed,
}

impl NodeState {
    pub fn can_read(self) -> bool {
        matches!(self, Self::Online | Self::Draining)
    }

    pub fn can_place(self) -> bool {
        self == Self::Online
    }
}

impl fmt::Display for NodeState {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(match self {
            Self::Online => "online",
            Self::Draining => "draining",
            Self::Offline => "offline",
            Self::Removed => "removed",
        })
    }
}

impl FromStr for NodeState {
    type Err = Error;

    fn from_str(word: &str) -> Result<Self> {
        match word {
            "online" => Ok(Self::Online),
            "draining" => Ok(Self::Draining),
            "offline" => Ok(Self::Offline),
            "removed" => Ok(Self::Removed),
            _ => Err(Error::Invalid(format!("invalid node state {word:?}"))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeDomain {
    pub id: String,
    pub domain: String,
    pub media: Media,
    pub capacity: u64,
    pub state: NodeState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Condition {
    Any,
    Absent,
    Generation(u64),
}

impl fmt::Display for Condition {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Any => out.write_str("any"),
            Self::Absent => out.write_str("absent"),
            Self::Generation(id) => write!(out, "{id}"),
        }
    }
}

impl FromStr for Condition {
    type Err = Error;

    fn from_str(word: &str) -> Result<Self> {
        match word {
            "any" => Ok(Self::Any),
            "absent" => Ok(Self::Absent),
            _ => word
                .parse::<u64>()
                .ok()
                .filter(|id| *id > 0)
                .map(Self::Generation)
                .ok_or_else(|| Error::Invalid(format!("invalid write condition {word:?}"))),
        }
    }
}

/// v1 fixes the shard numbers at 0 and 1 for data, and 2 for parity.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FragmentReceipt {
    pub shard: u8,
    pub node: String,
    pub revision: u64,
    pub bytes: u32,
    /// Castagnoli CRC-32 of the fragment bytes. Always recorded, including
    /// for strong-checksum fragments (the copied bytes are identical across a
    /// checksum upgrade), and the sole digest carried by legacy v1 catalogs.
    pub crc32c: u32,
    /// Algorithm used to authoritatively verify this fragment. Defaults to
    /// CRC-32C for every receipt decoded from a legacy catalog.
    pub algorithm: ChecksumAlgorithm,
    /// Alias kept in sync with [`FragmentReceipt::algorithm`].
    pub checksum_algorithm: ChecksumAlgorithm,
    /// Alias kept in sync with [`FragmentReceipt::algorithm`].
    pub kind: ChecksumAlgorithm,
    /// SHA-256 digest when the receipt records one.
    pub sha256: Option<[u8; 32]>,
    /// Algorithm-tagged authoritative digest.
    pub checksum: Checksum,
    /// Alias for [`FragmentReceipt::checksum`].
    pub digest: Checksum,
}

impl FragmentReceipt {
    /// Build a legacy CRC-32C receipt. All algorithm aliases default to CRC.
    pub fn crc32c(
        shard: u8,
        node: impl Into<String>,
        revision: u64,
        bytes: u32,
        crc32c: u32,
    ) -> Self {
        let checksum = Checksum::Crc32c(crc32c);
        Self {
            shard,
            node: node.into(),
            revision,
            bytes,
            crc32c,
            algorithm: ChecksumAlgorithm::Crc32c,
            checksum_algorithm: ChecksumAlgorithm::Crc32c,
            kind: ChecksumAlgorithm::Crc32c,
            sha256: None,
            digest: checksum.clone(),
            checksum,
        }
    }

    /// Build a receipt whose authoritative digest is `digest`. The CRC-32C
    /// field is supplied separately because it remains populated even when a
    /// stronger algorithm verifies the fragment.
    pub fn from_digest(
        shard: u8,
        node: impl Into<String>,
        revision: u64,
        bytes: u32,
        crc32c: u32,
        digest: Checksum,
    ) -> Self {
        let algorithm = digest.algorithm();
        let sha256 = digest.sha256();
        Self {
            shard,
            node: node.into(),
            revision,
            bytes,
            crc32c,
            algorithm,
            checksum_algorithm: algorithm,
            kind: algorithm,
            sha256,
            digest: digest.clone(),
            checksum: digest,
        }
    }

    /// Record a different authoritative digest, keeping every alias in sync.
    pub fn set_checksum(&mut self, digest: Checksum) {
        let algorithm = digest.algorithm();
        self.algorithm = algorithm;
        self.checksum_algorithm = algorithm;
        self.kind = algorithm;
        self.sha256 = digest.sha256();
        self.digest = digest.clone();
        self.checksum = digest;
        if let Checksum::Crc32c(value) = self.checksum {
            self.crc32c = value;
        }
    }

    /// Replace only the checksum algorithm/digest (used by checksum upgrades).
    pub fn with_checksum(mut self, digest: Checksum) -> Self {
        self.set_checksum(digest);
        self
    }

    /// True when `bytes` match the digest this receipt actually uses.
    pub fn verify(&self, bytes: &[u8]) -> bool {
        self.checksum.verify(bytes)
    }

    /// Length plus algorithm-specific verification of candidate bytes.
    pub fn verify_bytes(&self, bytes: &[u8]) -> std::result::Result<(), String> {
        if bytes.len() != self.bytes as usize {
            return Err(format!(
                "expected {} bytes, read {}",
                self.bytes,
                bytes.len()
            ));
        }
        if !self.verify(bytes) {
            return Err(format!(
                "{} mismatch; expected {}",
                self.algorithm,
                self.checksum.to_hex()
            ));
        }
        Ok(())
    }

    /// Alias for [`FragmentReceipt::verify`].
    pub fn matches_bytes(&self, bytes: &[u8]) -> bool {
        self.verify(bytes)
    }

    /// Hexadecimal form of the tagged expected checksum.
    pub fn checksum_hex(&self) -> String {
        self.checksum.to_hex()
    }

    /// Alias for [`FragmentReceipt::checksum_hex`].
    pub fn digest_hex(&self) -> String {
        self.checksum.to_hex()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StripeManifest {
    pub index: u32,
    pub offset: u64,
    pub data_bytes: [u32; 2],
    pub fragments: [FragmentReceipt; 3],
}

impl StripeManifest {
    pub fn end(&self) -> u64 {
        self.offset + u64::from(self.data_bytes[0]) + u64::from(self.data_bytes[1])
    }

    pub fn lengths(&self) -> [u32; 3] {
        [self.data_bytes[0], self.data_bytes[1], self.data_bytes[0]]
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectGeneration {
    pub key: String,
    pub generation: u64,
    pub size: u64,
    pub media: Media,
    pub domain_policy: DomainPolicy,
    pub stripes: Vec<StripeManifest>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartReceipt {
    pub number: u32,
    pub revision: u64,
    pub bytes: u32,
    pub crc32c: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementPlan {
    pub nodes: [String; 3],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UploadSession {
    pub id: u64,
    pub key: String,
    pub expected_bytes: u64,
    pub media: Media,
    pub domain_policy: DomainPolicy,
    pub condition: Condition,
    pub parts: BTreeMap<u32, PartReceipt>,
    pub placements: Vec<PlacementPlan>,
}

impl UploadSession {
    pub fn staged_bytes(&self) -> u64 {
        self.parts.values().map(|part| u64::from(part.bytes)).sum()
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct FragmentAddress {
    pub node: String,
    pub generation: u64,
    pub stripe: u32,
    pub shard: u8,
    pub revision: u64,
}

impl FragmentAddress {
    pub fn from_receipt(generation: u64, stripe: u32, receipt: &FragmentReceipt) -> Self {
        Self {
            node: receipt.node.clone(),
            generation,
            stripe,
            shard: receipt.shard,
            revision: receipt.revision,
        }
    }

    pub fn file_name(&self) -> String {
        format!(
            "{:016x}-{:08x}-{}-{:016x}.pb",
            self.generation, self.stripe, self.shard, self.revision
        )
    }

    pub(crate) fn from_filename(node: &str, name: &str) -> Option<Self> {
        let mut fields = name.strip_suffix(".pb")?.split('-');
        let address = Self {
            node: node.into(),
            generation: u64::from_str_radix(fields.next()?, 16).ok()?,
            stripe: u32::from_str_radix(fields.next()?, 16).ok()?,
            shard: fields.next()?.parse().ok()?,
            revision: u64::from_str_radix(fields.next()?, 16).ok()?,
        };
        if fields.next().is_some() || address.shard > 2 || address.file_name() != name {
            return None;
        }
        Some(address)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetiredFragment {
    pub address: FragmentAddress,
    pub bytes: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepairKind {
    Heal,
    Rebalance,
}

/// Naming aliases for checksum-upgrade journal entries. A checksum upgrade is
/// durably tracked as a heal attempt; these associated constants let callers
/// refer to it by the feature name they use.
#[allow(non_upper_case_globals)]
impl RepairKind {
    pub const Upgrade: RepairKind = RepairKind::Heal;
    pub const ChecksumUpgrade: RepairKind = RepairKind::Heal;
    pub const UpgradeChecksum: RepairKind = RepairKind::Heal;
    pub const UpgradeChecksums: RepairKind = RepairKind::Heal;
    pub const Rechecksum: RepairKind = RepairKind::Heal;
    pub const MigrateChecksum: RepairKind = RepairKind::Heal;
}

impl fmt::Display for RepairKind {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(match self {
            Self::Heal => "heal",
            Self::Rebalance => "rebalance",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepairState {
    Planned,
    Published,
    Stale,
    Unrecoverable,
}

impl fmt::Display for RepairState {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(match self {
            Self::Planned => "planned",
            Self::Published => "published",
            Self::Stale => "stale",
            Self::Unrecoverable => "unrecoverable",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairAttempt {
    pub id: u64,
    pub key: String,
    pub generation: u64,
    pub stripe: u32,
    pub from: FragmentReceipt,
    pub to: FragmentReceipt,
    pub kind: RepairKind,
    pub state: RepairState,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Catalog {
    pub block_bytes: u32,
    pub staging_limit: u64,
    pub next_id: u64,
    /// Checksum algorithm used for fragments written after this catalog was
    /// initialized (or after a policy upgrade). Legacy v1 catalogs decode as
    /// [`ChecksumAlgorithm::Crc32c`]. Existing fragments keep their own
    /// per-receipt algorithm regardless of this policy.
    pub checksum_algorithm: ChecksumAlgorithm,
    /// Alias kept in sync with [`Catalog::checksum_algorithm`].
    pub fragment_checksum: ChecksumAlgorithm,
    /// Alias kept in sync with [`Catalog::checksum_algorithm`].
    pub checksum: ChecksumAlgorithm,
    pub nodes: BTreeMap<String, NodeDomain>,
    pub uploads: BTreeMap<u64, UploadSession>,
    pub objects: BTreeMap<String, ObjectGeneration>,
    pub repairs: BTreeMap<u64, RepairAttempt>,
    pub retired: Vec<RetiredFragment>,
}

impl Catalog {
    /// Set the new-write checksum policy and all of its alias fields.
    pub fn set_checksum_algorithm(&mut self, algorithm: ChecksumAlgorithm) {
        self.checksum_algorithm = algorithm;
        self.fragment_checksum = algorithm;
        self.checksum = algorithm;
    }
}

impl Catalog {
    pub(crate) fn retire_object(&mut self, object: &ObjectGeneration) {
        for stripe in &object.stripes {
            for fragment in &stripe.fragments {
                self.retire_fragment(object.generation, stripe.index, fragment);
            }
        }
    }

    pub(crate) fn retire_fragment(
        &mut self,
        generation: u64,
        stripe: u32,
        fragment: &FragmentReceipt,
    ) {
        self.retired.push(RetiredFragment {
            address: FragmentAddress::from_receipt(generation, stripe, fragment),
            bytes: fragment.bytes,
        });
    }
}
