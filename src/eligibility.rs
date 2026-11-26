//! Single home for node and domain eligibility.
//!
//! The same media and domain rules serve every stage that decides where a
//! fragment may live; only the node-state requirement changes between stages:
//!
//! * [`Eligibility::Target`] governs every *new* destination: initial
//!   placement, repair planning and repair publication, plus the placement
//!   plans stored by an upload session. A target must be online, hold the
//!   object's media class, and satisfy the object's domain policy. Draining,
//!   offline and removed nodes can never become a new target.
//! * [`Eligibility::Existing`] governs mappings already recorded in the
//!   catalog: current object fragments and historical v1 samples. A node that
//!   is merely draining (or offline) still keeps a historically legal mapping
//!   readable; only a removed node invalidates it.
//! * [`Eligibility::Read`] governs actually serving a fragment to a reader.
//!   It is the most permissive state rule: online and draining nodes keep
//!   serving fragments while they are evacuated, whereas offline and removed
//!   nodes report an unavailable-node fault.
//!
//! Capacity is deliberately not part of these rules. Capacity is an
//! allocation-time concern tracked by the ledger, whereas eligibility decides
//! which nodes a mapping is allowed to name at all.

use crate::codec::SHARDS;
use crate::{Catalog, DomainPolicy, Error, Media, NodeDomain, NodeState, Result};

/// Which lifecycle stage an eligibility question belongs to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Eligibility {
    /// A destination a fragment is about to be written or moved onto.
    Target,
    /// A mapping already recorded in a catalog, kept for reading or history.
    Existing,
    /// Serving an already-recorded fragment to a live reader.
    Read,
}

impl Eligibility {
    fn allows(self, state: NodeState) -> bool {
        match self {
            // Only online nodes can receive new fragments.
            Self::Target => state.can_place(),
            // Historical mappings survive drain and offline until evacuation
            // (or node removal) relocates them.
            Self::Existing => state != NodeState::Removed,
            // Reads keep flowing from draining nodes during evacuation.
            Self::Read => state.can_read(),
        }
    }

    fn stage(self) -> &'static str {
        match self {
            Self::Target => "target",
            Self::Existing => "recorded",
            Self::Read => "read",
        }
    }
}

/// Number of distinct failure domains represented by a three-node layout.
pub fn domain_count(nodes: [&NodeDomain; SHARDS]) -> usize {
    let [a, b, c] = nodes;
    1 + usize::from(b.domain != a.domain)
        + usize::from(c.domain != a.domain && c.domain != b.domain)
}

/// Whether a single node may carry a fragment of `media` at this stage.
pub fn node_eligible<'a>(
    catalog: &'a Catalog,
    node_id: &str,
    media: Media,
    stage: Eligibility,
) -> Result<&'a NodeDomain> {
    let node = catalog
        .nodes
        .get(node_id)
        .ok_or_else(|| Error::Corrupt(format!("unknown node reference {node_id}")))?;
    if !stage.allows(node.state) {
        return Err(Error::Corrupt(format!(
            "{} node {} is {}",
            stage.stage(),
            node_id,
            node.state
        )));
    }
    if node.media != media {
        return Err(Error::Corrupt(format!(
            "invalid {media} node reference {node_id}"
        )));
    }
    Ok(node)
}

/// Whether a node may receive a *new* fragment of `media`. Used by placement
/// search and repair planning, where an ineligible node is simply skipped
/// rather than treated as catalog corruption.
pub fn can_target(catalog: &Catalog, node_id: &str, media: Media) -> bool {
    node_eligible(catalog, node_id, media, Eligibility::Target).is_ok()
}

/// Whether a recorded fragment on `node_id` may be served to a reader. This is
/// purely a node-state question: reads do not re-check media, because the
/// recorded mapping was already validated against media when it was written.
/// Online and draining nodes serve; offline and removed nodes do not.
pub fn can_read_from(catalog: &Catalog, node_id: &str) -> bool {
    catalog
        .nodes
        .get(node_id)
        .is_some_and(|node| Eligibility::Read.allows(node.state))
}

/// Validate the full three-shard layout of one stripe against the shared
/// media, uniqueness and domain rules. `stage` selects only the node-state
/// requirement; every other rule is identical across stages.
pub fn check_stripe(
    catalog: &Catalog,
    node_ids: [&str; SHARDS],
    media: Media,
    policy: DomainPolicy,
    stage: Eligibility,
) -> Result<()> {
    let checked: [Result<&NodeDomain>; SHARDS] =
        std::array::from_fn(|index| node_eligible(catalog, node_ids[index], media, stage));
    // A `Result` holding the non-Clone `Error` cannot be moved out of an array
    // by index more than once; borrow each slot and copy only the reference on
    // success, re-wrapping the shared eligibility error as corruption.
    fn pick<'a>(slot: &'a Result<&'a NodeDomain>) -> Result<&'a NodeDomain> {
        match slot {
            Ok(node) => Ok(*node),
            Err(error) => Err(Error::Corrupt(error.to_string())),
        }
    }
    let nodes = [pick(&checked[0])?, pick(&checked[1])?, pick(&checked[2])?];

    if node_ids[0] == node_ids[1] || node_ids[0] == node_ids[2] || node_ids[1] == node_ids[2] {
        return Err(Error::Corrupt("stripe uses a node twice".into()));
    }
    if policy == DomainPolicy::Strict && domain_count(nodes) < SHARDS {
        return Err(Error::Corrupt("strict stripe repeats a domain".into()));
    }
    Ok(())
}

/// Validate a layout recorded in the catalog: draining and offline nodes stay
/// legal as long as the node has not been removed.
pub fn check_existing_stripe(
    catalog: &Catalog,
    node_ids: [&str; SHARDS],
    media: Media,
    policy: DomainPolicy,
) -> Result<()> {
    check_stripe(catalog, node_ids, media, policy, Eligibility::Existing)
}

/// Validate a layout that is about to receive new fragments: every named node
/// must currently be online and match the media and domain policy.
pub fn check_target_stripe(
    catalog: &Catalog,
    node_ids: [&str; SHARDS],
    media: Media,
    policy: DomainPolicy,
) -> Result<()> {
    check_stripe(catalog, node_ids, media, policy, Eligibility::Target)
}
