mod model;
mod store;
pub mod v1;
mod validate;

/// Algorithm-agile catalog codec. New strong-checksum records are carried by
/// the compatible v1 extension, so this is an alias of [`v1`].
pub mod v2 {
    pub use super::v1::*;
}

pub use model::*;
pub(crate) use validate::{
    check_condition, checked_id, checked_key, expected_stripe_count, sequence, validate_catalog,
    MAX_NODES, MAX_PARTS, MAX_PART_BYTES,
};

pub use crate::codec::{
    Checksum, ChecksumAlgorithm, ChecksumDigest, ChecksumKind, Digest, DigestAlgorithm,
    FragmentChecksum,
};
