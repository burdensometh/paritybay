use std::collections::{BTreeMap, BTreeSet};

use crate::codec::stripe_lengths;
use crate::config::{check_limits, validate_node};
use crate::{Error, Result};

use super::{
    Catalog, Condition, DomainPolicy, FragmentAddress, FragmentReceipt, Media, NodeState,
    ObjectGeneration, RepairAttempt, RepairState, UploadSession,
};

pub(crate) const MAX_OBJECT_BYTES: u64 = 512 * 1024 * 1024;
pub(crate) const MAX_STRIPES: usize = 65_536;
pub(crate) const MAX_NODES: usize = 64;
pub(crate) const MAX_PART_BYTES: u32 = 64 * 1024 * 1024;
pub(crate) const MAX_PARTS: usize = 16_384;

pub(crate) fn checked_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 48
        || !id.as_bytes()[0].is_ascii_alphanumeric()
        || !id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'_'))
    {
        return Err(Error::Invalid(format!(
            "invalid node or domain identifier {id:?}"
        )));
    }
    Ok(())
}

pub(crate) fn checked_key(key: &str) -> Result<()> {
    if key.is_empty() || key.len() > 512 || key.chars().any(char::is_control) {
        return Err(Error::Invalid(
            "object keys require 1–512 UTF-8 bytes without control characters".into(),
        ));
    }
    Ok(())
}

pub(crate) fn expected_stripe_count(size: u64, block_bytes: u32) -> Result<usize> {
    if size > MAX_OBJECT_BYTES {
        return Err(Error::Invalid(format!(
            "object exceeds {MAX_OBJECT_BYTES} bytes"
        )));
    }
    let count = size.div_ceil(u64::from(block_bytes) * 2);
    if count > MAX_STRIPES as u64 {
        return Err(Error::Invalid(format!(
            "object exceeds {MAX_STRIPES} stripes"
        )));
    }
    Ok(count as usize)
}

pub(crate) fn sequence(catalog: &mut Catalog) -> Result<u64> {
    let id = catalog.next_id;
    catalog.next_id = id
        .checked_add(1)
        .ok_or_else(|| Error::Capacity("sequence exhausted".into()))?;
    Ok(id)
}

pub(crate) fn check_condition(
    key: &str,
    condition: Condition,
    object: Option<&ObjectGeneration>,
) -> Result<()> {
    let current = object.map(|entry| entry.generation);
    let matched = match condition {
        Condition::Any => true,
        Condition::Absent => current.is_none(),
        Condition::Generation(expected) => current == Some(expected),
    };
    if !matched {
        return Err(Error::Conflict(format!(
            "condition {condition} failed for {key:?} with current generation {}",
            current.map_or_else(|| "none".into(), |id| id.to_string()),
        )));
    }
    Ok(())
}

fn corrupt(message: impl Into<String>) -> Error {
    Error::Corrupt(message.into())
}

fn existing_id(catalog: &Catalog, id: u64, field: &str) -> Result<()> {
    if id == 0 || id >= catalog.next_id {
        return Err(corrupt(format!("invalid {field} sequence {id}")));
    }
    Ok(())
}

fn matching_node(catalog: &Catalog, id: &str, media: Media) -> Result<()> {
    match catalog.nodes.get(id) {
        Some(node) if node.media == media && node.state != NodeState::Removed => Ok(()),
        _ => Err(corrupt(format!("invalid {media} node reference {id}"))),
    }
}

fn layout_valid(
    catalog: &Catalog,
    nodes: [&str; 3],
    media: Media,
    policy: DomainPolicy,
) -> Result<()> {
    let mut domains = BTreeSet::new();
    for id in nodes {
        matching_node(catalog, id, media)?;
        domains.insert(catalog.nodes[id].domain.as_str());
    }
    if nodes[0] == nodes[1] || nodes[0] == nodes[2] || nodes[1] == nodes[2] {
        return Err(corrupt("stripe uses a node twice"));
    }
    if policy == DomainPolicy::Strict && domains.len() != 3 {
        return Err(corrupt("strict stripe repeats a domain"));
    }
    Ok(())
}

fn shape(catalog: &Catalog, size: u64, stripes: usize) -> Result<()> {
    let expected =
        expected_stripe_count(size, catalog.block_bytes).map_err(|e| corrupt(e.to_string()))?;
    if expected != stripes {
        return Err(corrupt(format!(
            "expected {expected} stripes but found {stripes}"
        )));
    }
    Ok(())
}

fn validate_object(catalog: &Catalog, object: &ObjectGeneration) -> Result<()> {
    checked_key(&object.key).map_err(|e| corrupt(e.to_string()))?;
    existing_id(catalog, object.generation, "object generation")?;
    shape(catalog, object.size, object.stripes.len())?;
    let mut offset = 0;
    for (index, stripe) in object.stripes.iter().enumerate() {
        let lengths = stripe_lengths(object.size - offset, catalog.block_bytes);
        if stripe.index != index as u32
            || stripe.offset != offset
            || stripe.data_bytes != lengths[..2]
        {
            return Err(corrupt(format!("invalid stripe geometry at index {index}")));
        }
        for (shard, fragment) in stripe.fragments.iter().enumerate() {
            if usize::from(fragment.shard) != shard || fragment.bytes != lengths[shard] {
                return Err(corrupt(format!(
                    "invalid fragment geometry at stripe {index}, shard {shard}"
                )));
            }
            existing_id(catalog, fragment.revision, "fragment revision")?;
            if fragment.revision < object.generation {
                return Err(corrupt("fragment revision precedes object generation"));
            }
        }
        let nodes = stripe
            .fragments
            .each_ref()
            .map(|fragment| fragment.node.as_str());
        layout_valid(catalog, nodes, object.media, object.domain_policy)?;
        offset = stripe.end();
    }
    if offset != object.size {
        return Err(corrupt("stripe coverage does not match object length"));
    }
    Ok(())
}

fn validate_upload(catalog: &Catalog, upload: &UploadSession) -> Result<()> {
    checked_key(&upload.key).map_err(|e| corrupt(e.to_string()))?;
    existing_id(catalog, upload.id, "upload")?;
    shape(catalog, upload.expected_bytes, upload.placements.len())?;
    if upload.parts.len() > MAX_PARTS || upload.staged_bytes() > upload.expected_bytes {
        return Err(corrupt("invalid upload staging size"));
    }
    for (number, part) in &upload.parts {
        if *number == 0
            || *number as usize > MAX_PARTS
            || part.number != *number
            || part.bytes == 0
            || part.bytes > MAX_PART_BYTES
        {
            return Err(corrupt("invalid upload part"));
        }
        existing_id(catalog, part.revision, "part revision")?;
        if part.revision <= upload.id {
            return Err(corrupt("part revision does not follow upload creation"));
        }
    }
    if upload.condition == Condition::Generation(0) {
        return Err(corrupt("invalid upload condition"));
    }
    for plan in &upload.placements {
        let nodes = plan.nodes.each_ref().map(String::as_str);
        layout_valid(catalog, nodes, upload.media, upload.domain_policy)?;
    }
    Ok(())
}

fn validate_repair_receipt(catalog: &Catalog, receipt: &FragmentReceipt) -> Result<()> {
    if receipt.shard > 2
        || receipt.bytes > catalog.block_bytes
        || !catalog.nodes.contains_key(&receipt.node)
    {
        return Err(corrupt("invalid repair fragment"));
    }
    existing_id(catalog, receipt.revision, "repair fragment revision")
}

fn validate_repairs(catalog: &Catalog, referenced: &mut BTreeSet<FragmentAddress>) -> Result<()> {
    let mut pending_stripes = BTreeSet::new();
    for (id, attempt) in &catalog.repairs {
        existing_id(catalog, *id, "repair attempt")?;
        existing_id(catalog, attempt.generation, "repair generation")?;
        checked_key(&attempt.key).map_err(|e| corrupt(e.to_string()))?;
        validate_repair_receipt(catalog, &attempt.from)?;
        validate_repair_receipt(catalog, &attempt.to)?;
        if *id != attempt.id
            || attempt.to.revision != *id
            || attempt.from.revision < attempt.generation
            || attempt.from.revision >= *id
            || attempt.from.shard != attempt.to.shard
            || attempt.from.bytes != attempt.to.bytes
            || attempt.from.crc32c != attempt.to.crc32c
            || attempt.stripe as usize >= MAX_STRIPES
            || attempt.detail.len() > 1024
        {
            return Err(corrupt("invalid repair journal entry"));
        }
        if attempt.state == RepairState::Planned {
            if !pending_stripes.insert((attempt.generation, attempt.stripe)) {
                return Err(corrupt("multiple planned attempts for a stripe"));
            }
            let target =
                FragmentAddress::from_receipt(attempt.generation, attempt.stripe, &attempt.to);
            if !referenced.insert(target) {
                return Err(corrupt("planned target aliases another fragment"));
            }
            validate_plan_mapping(catalog, attempt)?;
        }
    }
    Ok(())
}

fn validate_plan_mapping(catalog: &Catalog, attempt: &RepairAttempt) -> Result<()> {
    for node in [&attempt.from.node, &attempt.to.node] {
        if catalog.nodes[node].state == NodeState::Removed {
            return Err(corrupt("planned repair references removed node"));
        }
    }
    let Some(object) = catalog
        .objects
        .get(&attempt.key)
        .filter(|object| object.generation == attempt.generation)
    else {
        // Deletion or replacement may occur while a copy holds an old mapping.
        return Ok(());
    };
    let stripe = object
        .stripes
        .get(attempt.stripe as usize)
        .ok_or_else(|| corrupt("planned repair references missing stripe"))?;
    let index = usize::from(attempt.from.shard);
    if stripe.fragments[index] != attempt.from {
        return Err(corrupt(
            "planned repair source differs from current receipt",
        ));
    }
    let mut nodes = stripe
        .fragments
        .each_ref()
        .map(|fragment| fragment.node.as_str());
    nodes[index] = &attempt.to.node;
    layout_valid(catalog, nodes, object.media, object.domain_policy)
}

fn validate_completions(catalog: &Catalog) -> Result<()> {
    for (upload, entry) in &catalog.completions {
        checked_key(&entry.key).map_err(|e| corrupt(e.to_string()))?;
        if *upload != entry.upload || entry.generation != *upload {
            return Err(corrupt("completion upload and generation disagree"));
        }
        // IDs are monotonic and never reused, so the upload's own sequence is
        // enough; it need not still name a live object, since the receipt
        // outlives replacement or deletion of the generation it published.
        existing_id(catalog, entry.upload, "completion upload")?;
        shape(catalog, entry.bytes, entry.stripes)?;
        if entry.condition == Condition::Generation(0)
            || entry.replaced_generation.is_some_and(|id| id == entry.generation)
            || entry.weaker_stripes > entry.stripes
        {
            return Err(corrupt("invalid completion receipt"));
        }
        if let Some(replaced) = entry.replaced_generation {
            existing_id(catalog, replaced, "completion prior generation")?;
        }
    }
    Ok(())
}

fn validate_retired(catalog: &Catalog, referenced: &BTreeSet<FragmentAddress>) -> Result<()> {    let mut seen = BTreeSet::new();
    for entry in &catalog.retired {
        let address = &entry.address;
        existing_id(catalog, address.generation, "retired generation")?;
        existing_id(catalog, address.revision, "retired revision")?;
        if address.shard > 2
            || address.revision < address.generation
            || address.stripe as usize >= MAX_STRIPES
            || catalog
                .nodes
                .get(&address.node)
                .is_none_or(|node| node.state == NodeState::Removed)
            || entry.bytes > catalog.block_bytes
            || referenced.contains(address)
            || !seen.insert(address)
        {
            return Err(corrupt(format!(
                "invalid retired fragment {}",
                address.file_name()
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_catalog(catalog: &Catalog) -> Result<()> {
    check_limits(catalog.block_bytes, catalog.staging_limit).map_err(|e| corrupt(e.to_string()))?;
    if catalog.next_id == 0 || !(3..=MAX_NODES).contains(&catalog.nodes.len()) {
        return Err(corrupt("invalid sequence or node count"));
    }
    for (id, node) in &catalog.nodes {
        validate_node(node).map_err(|e| corrupt(e.to_string()))?;
        if id != &node.id {
            return Err(corrupt("node table key mismatch"));
        }
    }

    let mut ids = BTreeSet::new();
    let mut referenced = BTreeSet::new();
    for (key, object) in &catalog.objects {
        if key != &object.key || !ids.insert(object.generation) {
            return Err(corrupt("object key mismatch or duplicate generation"));
        }
        validate_object(catalog, object)?;
        for stripe in &object.stripes {
            for fragment in &stripe.fragments {
                referenced.insert(FragmentAddress::from_receipt(
                    object.generation,
                    stripe.index,
                    fragment,
                ));
            }
        }
    }

    let mut staged = 0_u64;
    for (id, upload) in &catalog.uploads {
        if *id != upload.id || !ids.insert(*id) {
            return Err(corrupt("duplicate or mismatched upload id"));
        }
        validate_upload(catalog, upload)?;
        for part in upload.parts.values() {
            if !ids.insert(part.revision) {
                return Err(corrupt("part revision collides with another allocation"));
            }
        }
        staged = staged
            .checked_add(upload.expected_bytes)
            .ok_or_else(|| corrupt("staging reservation overflow"))?;
    }
    if staged > catalog.staging_limit {
        return Err(corrupt("staging reservations exceed cluster limit"));
    }
    validate_repairs(catalog, &mut referenced)?;
    for attempt in catalog.repairs.values() {
        if !ids.insert(attempt.id) {
            return Err(corrupt(
                "repair attempt id collides with another allocation",
            ));
        }
    }
    validate_completions(catalog)?;
    validate_retired(catalog, &referenced)?;

    let ledger = crate::placement::Ledger::from_catalog(catalog, None)?;
    for (id, node) in &catalog.nodes {
        if ledger.allocated(id) > node.capacity {
            return Err(corrupt(format!(
                "reservations exceed capacity of node {id}"
            )));
        }
    }
    Ok(())
}

pub(super) fn map_insert<K: Ord, V>(
    table: &mut BTreeMap<K, V>,
    key: K,
    value: V,
    label: &str,
) -> Result<()> {
    if table.insert(key, value).is_some() {
        return Err(corrupt(format!("duplicate {label} in v1 catalog")));
    }
    Ok(())
}
