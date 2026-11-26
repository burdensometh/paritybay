use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};

use crate::codec::{stripe_lengths, SHARDS};
use crate::manifest::{expected_stripe_count, PlacementPlan};
use crate::{
    Catalog, DomainPolicy, Error, Media, NodeDomain, ObjectGeneration, Result, StripeManifest,
};

use super::Ledger;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DomainHealth {
    pub domains: BTreeMap<String, usize>,
    pub surviving_shards_after_worst_domain_loss: usize,
}

impl DomainHealth {
    pub fn for_stripe(catalog: &Catalog, stripe: &StripeManifest) -> Self {
        let nodes = stripe
            .fragments
            .each_ref()
            .map(|fragment| &catalog.nodes[&fragment.node]);
        let mut domains = BTreeMap::new();
        for node in nodes {
            *domains.entry(node.domain.clone()).or_insert(0) += 1;
        }
        let largest = domains.values().copied().max().unwrap_or(0);
        Self {
            domains,
            surviving_shards_after_worst_domain_loss: SHARDS - largest,
        }
    }

    pub fn survives_any_single_domain_loss(&self) -> bool {
        self.surviving_shards_after_worst_domain_loss >= 2
    }
}

pub struct PlacementPlanner<'a> {
    catalog: &'a Catalog,
    ledger: Ledger,
}

impl<'a> PlacementPlanner<'a> {
    pub fn new(catalog: &'a Catalog, omit_upload: Option<u64>) -> Result<Self> {
        Ok(Self {
            catalog,
            ledger: Ledger::from_catalog(catalog, omit_upload)?,
        })
    }

    pub fn plan(
        &mut self,
        key: &str,
        size: u64,
        media: Media,
        policy: DomainPolicy,
    ) -> Result<Vec<PlacementPlan>> {
        crate::manifest::checked_key(key)?;
        if self.catalog.block_bytes == 0 {
            return Err(Error::Invalid(
                "placement requires a positive block size".into(),
            ));
        }
        let count = expected_stripe_count(size, self.catalog.block_bytes)?;
        let mut plans = Vec::with_capacity(count);
        let mut remaining = size;
        // Reserve in a trial ledger: failed requests leave the caller's budget intact.
        let mut trial = self.ledger.clone();
        for stripe in 0..count {
            let lengths = stripe_lengths(remaining, self.catalog.block_bytes);
            let nodes = plan_stripe(
                self.catalog,
                &trial,
                key,
                stripe as u32,
                lengths,
                media,
                policy,
            )?;
            for (node, bytes) in nodes.iter().zip(lengths) {
                trial.reserve(node, bytes)?;
            }
            plans.push(PlacementPlan { nodes });
            remaining -= u64::from(lengths[0]) + u64::from(lengths[1]);
        }
        self.ledger = trial;
        Ok(plans)
    }
}

fn plan_stripe(
    catalog: &Catalog,
    ledger: &Ledger,
    key: &str,
    stripe: u32,
    lengths: [u32; SHARDS],
    media: Media,
    policy: DomainPolicy,
) -> Result<[String; SHARDS]> {
    let candidates = std::array::from_fn::<_, SHARDS, _>(|shard| {
        ranked(
            catalog,
            ledger,
            key,
            stripe,
            shard as u8,
            lengths[shard],
            media,
        )
    });
    let best_possible = candidates
        .iter()
        .flatten()
        .map(|node| &node.domain)
        .collect::<BTreeSet<_>>()
        .len()
        .min(SHARDS);
    let least = if policy == DomainPolicy::Strict {
        SHARDS
    } else {
        1
    };
    for domains in (least..=best_possible).rev() {
        if let Some(layout) = first_layout(&candidates, domains) {
            return Ok(layout.map(|node| node.id.clone()));
        }
    }
    Err(Error::Capacity(format!(
        "stripe {stripe} of {key:?} cannot fit three online {media} nodes with {policy} domains"
    )))
}

fn first_layout<'a>(
    candidates: &[Vec<&'a NodeDomain>; SHARDS],
    domains_needed: usize,
) -> Option<[&'a NodeDomain; SHARDS]> {
    for &first in &candidates[0] {
        for &second in &candidates[1] {
            if first.id == second.id || (domains_needed == 3 && first.domain == second.domain) {
                continue;
            }
            for &parity in &candidates[2] {
                if parity.id == first.id || parity.id == second.id {
                    continue;
                }
                if domain_count([first, second, parity]) >= domains_needed {
                    return Some([first, second, parity]);
                }
            }
        }
    }
    None
}

fn domain_count(nodes: [&NodeDomain; SHARDS]) -> usize {
    let [a, b, c] = nodes;
    1 + usize::from(b.domain != a.domain)
        + usize::from(c.domain != a.domain && c.domain != b.domain)
}

fn ranked<'a>(
    catalog: &'a Catalog,
    ledger: &Ledger,
    key: &str,
    stripe: u32,
    shard: u8,
    bytes: u32,
    media: Media,
) -> Vec<&'a NodeDomain> {
    let mut ranked_nodes: Vec<_> = catalog
        .nodes
        .values()
        .filter(|node| {
            node.state.can_place()
                && node.media == media
                && ledger.fits(&node.id, node.capacity, bytes)
        })
        .map(|node| (Reverse(rank(key, stripe, shard, &node.id)), node))
        .collect();
    ranked_nodes.sort_unstable_by(|(a_rank, a_node), (b_rank, b_node)| {
        a_rank.cmp(b_rank).then_with(|| a_node.id.cmp(&b_node.id))
    });
    ranked_nodes.into_iter().map(|(_, node)| node).collect()
}

/// Stable rendezvous ranking. The avalanche avoids correlated ranks for nearby stripe indices.
fn rank(key: &str, stripe: u32, shard: u8, node: &str) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x100_0000_01b3;
    let mut hash = OFFSET;
    for byte in key
        .bytes()
        .chain([0])
        .chain(stripe.to_le_bytes())
        .chain([shard])
        .chain(node.bytes())
    {
        hash = (hash ^ u64::from(byte)).wrapping_mul(PRIME);
    }
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    hash ^ (hash >> 33)
}

#[derive(Clone, Copy)]
pub(crate) enum ReplacementPolicy {
    Heal,
    Evacuate,
    ImproveDomains,
}

pub(crate) fn replacement(
    catalog: &Catalog,
    object: &ObjectGeneration,
    stripe: &StripeManifest,
    shard: u8,
    mode: ReplacementPolicy,
) -> Result<String> {
    let ledger = Ledger::from_catalog(catalog, None)?;
    replacement_with_ledger(catalog, &ledger, object, stripe, shard, mode)
}

/// Select a replacement against a caller-provided budget. Maintenance keeps
/// one ledger for a whole repair round and books each planned copy into it,
/// so later stripes decide against the reservations of earlier ones without
/// rebuilding the full-directory totals.
pub(crate) fn replacement_with_ledger(
    catalog: &Catalog,
    ledger: &Ledger,
    object: &ObjectGeneration,
    stripe: &StripeManifest,
    shard: u8,
    mode: ReplacementPolicy,
) -> Result<String> {
    let current = &stripe.fragments[usize::from(shard)];
    let candidates = ranked(
        catalog,
        ledger,
        &object.key,
        stripe.index,
        shard,
        current.bytes,
        object.media,
    );
    candidates
        .into_iter()
        .filter_map(|node| {
            let domains = replacement_score(catalog, object, stripe, shard, node, mode)?;
            // At equal domain protection, in-place healing avoids changing the node assignment.
            let keeps_node = node.id == current.node;
            Some((node, domains, keeps_node))
        })
        .fold(None, |best, candidate| match best {
            Some((_, domains, keeps_node))
                if (domains, keeps_node) >= (candidate.1, candidate.2) =>
            {
                best
            }
            _ => Some(candidate),
        })
        .map(|(node, _, _)| node.id.clone())
        .ok_or_else(|| {
            Error::Capacity(format!(
                "no replacement for stripe {}, shard {shard} on {}",
                stripe.index, current.node
            ))
        })
}

pub(crate) fn replacement_valid(
    catalog: &Catalog,
    object: &ObjectGeneration,
    stripe: &StripeManifest,
    shard: u8,
    target: &str,
) -> bool {
    let Some(node) = catalog.nodes.get(target) else {
        return false;
    };
    replacement_score(
        catalog,
        object,
        stripe,
        shard,
        node,
        ReplacementPolicy::Heal,
    )
    .is_some()
}

fn replacement_score(
    catalog: &Catalog,
    object: &ObjectGeneration,
    stripe: &StripeManifest,
    shard: u8,
    candidate: &NodeDomain,
    mode: ReplacementPolicy,
) -> Option<usize> {
    if !candidate.state.can_place() || candidate.media != object.media {
        return None;
    }
    let index = usize::from(shard);
    let prior = stripe
        .fragments
        .each_ref()
        .map(|fragment| &catalog.nodes[&fragment.node]);
    if prior
        .iter()
        .enumerate()
        .any(|(i, node)| i != index && node.id == candidate.id)
    {
        return None;
    }
    if !matches!(mode, ReplacementPolicy::Heal) && prior[index].id == candidate.id {
        return None;
    }
    let mut after = prior;
    after[index] = candidate;
    let score = domain_count(after);
    if object.domain_policy == DomainPolicy::Strict && score < SHARDS {
        return None;
    }
    if score < domain_count(prior)
        || (matches!(mode, ReplacementPolicy::ImproveDomains) && score == domain_count(prior))
    {
        return None;
    }
    Some(score)
}
