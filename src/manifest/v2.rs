//! Durable v2 catalog. The frame and scalar encodings match v1, but every
//! node string outside the node table becomes a `u16` row reference, so
//! fragments, upload placements, repair receipts and retired entries stop
//! repeating node identifiers. The node table keeps one row per node id;
//! nodes that share a failure domain stay distinct rows.

use std::collections::BTreeMap;

use crate::codec::checksum;
use crate::{Error, Result};

use super::validate::{map_insert, MAX_NODES, MAX_PARTS, MAX_STRIPES};
use super::{
    validate_catalog, Catalog, Condition, DomainPolicy, FragmentAddress, FragmentReceipt, Media,
    NodeDomain, NodeState, ObjectGeneration, PartReceipt, PlacementPlan, RepairAttempt, RepairKind,
    RepairState, RetiredFragment, StripeManifest, UploadSession,
};

pub const MAGIC: [u8; 8] = *b"PBAYV2\r\n";
pub const HEADER_BYTES: usize = 16;
pub const MAX_CATALOG_BYTES: usize = 64 * 1024 * 1024;

pub fn encode(catalog: &Catalog) -> Result<Vec<u8>> {
    validate_catalog(catalog)?;
    let mut out = Encoder::new(catalog);
    out.bytes.resize(HEADER_BYTES, 0);
    out.u32(catalog.block_bytes);
    out.u64(catalog.staging_limit);
    out.u64(catalog.next_id);
    out.count(catalog.nodes.len())?;
    for node in catalog.nodes.values() {
        out.node(node)?;
    }
    out.count(catalog.uploads.len())?;
    for upload in catalog.uploads.values() {
        out.upload(upload)?;
    }
    out.count(catalog.objects.len())?;
    for object in catalog.objects.values() {
        out.object(object)?;
    }
    out.count(catalog.repairs.len())?;
    for repair in catalog.repairs.values() {
        out.repair(repair)?;
    }
    out.count(catalog.retired.len())?;
    for entry in &catalog.retired {
        out.retired(entry)?;
    }
    if out.bytes.len() > MAX_CATALOG_BYTES {
        return Err(Error::Capacity(format!(
            "v2 catalog exceeds {MAX_CATALOG_BYTES} bytes"
        )));
    }

    let payload = &out.bytes[HEADER_BYTES..];
    let size = (payload.len() as u32).to_le_bytes();
    let crc = checksum(payload).to_le_bytes();
    out.bytes[..8].copy_from_slice(&MAGIC);
    out.bytes[8..12].copy_from_slice(&size);
    out.bytes[12..16].copy_from_slice(&crc);
    Ok(out.bytes)
}

pub fn decode(bytes: &[u8]) -> Result<Catalog> {
    if bytes.len() > MAX_CATALOG_BYTES {
        return Err(Error::Corrupt("oversized v2 catalog".into()));
    }
    let mut header = Decoder::new(bytes);
    if header.take(MAGIC.len())? != MAGIC {
        return Err(Error::Corrupt("unknown ParityBay catalog format".into()));
    }
    let payload_length = header.u32()? as usize;
    let payload_checksum = header.u32()?;
    let payload = header.take(payload_length)?;
    header.finish()?;
    if checksum(payload) != payload_checksum {
        return Err(Error::Corrupt(
            "v2 catalog payload checksum mismatch".into(),
        ));
    }

    let mut input = Decoder::new(payload);
    let block_bytes = input.u32()?;
    let staging_limit = input.u64()?;
    let next_id = input.u64()?;
    let mut nodes = BTreeMap::new();
    let mut node_ids = Vec::new();
    for _ in 0..input.count(MAX_NODES, 14)? {
        let node = input.node()?;
        node_ids.push(node.id.clone());
        map_insert(&mut nodes, node.id.clone(), node, "node")?;
    }
    input.node_ids = node_ids;
    let mut uploads = BTreeMap::new();
    for _ in 0..input.count(usize::MAX, 29)? {
        let upload = input.upload()?;
        map_insert(&mut uploads, upload.id, upload, "upload")?;
    }
    let mut objects = BTreeMap::new();
    for _ in 0..input.count(usize::MAX, 24)? {
        let object = input.object()?;
        map_insert(&mut objects, object.key.clone(), object, "object")?;
    }
    let mut repairs = BTreeMap::new();
    for _ in 0..input.count(usize::MAX, 64)? {
        let repair = input.repair()?;
        map_insert(&mut repairs, repair.id, repair, "repair")?;
    }
    let mut retired = Vec::new();
    for _ in 0..input.count(usize::MAX, 27)? {
        retired.push(input.retired()?);
    }
    input.finish()?;
    let catalog = Catalog {
        block_bytes,
        staging_limit,
        next_id,
        nodes,
        uploads,
        objects,
        repairs,
        retired,
    };
    validate_catalog(&catalog)?;
    Ok(catalog)
}

struct Encoder<'a> {
    bytes: Vec<u8>,
    references: BTreeMap<&'a str, u16>,
}

impl<'a> Encoder<'a> {
    fn new(catalog: &'a Catalog) -> Self {
        let references = catalog
            .nodes
            .keys()
            .enumerate()
            .map(|(index, id)| (id.as_str(), index as u16))
            .collect();
        Self {
            bytes: Vec::with_capacity(HEADER_BYTES),
            references,
        }
    }

    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn count(&mut self, size: usize) -> Result<()> {
        self.u32(
            u32::try_from(size).map_err(|_| Error::Capacity("v2 collection too large".into()))?,
        );
        Ok(())
    }

    fn string(&mut self, value: &str) -> Result<()> {
        let length =
            u16::try_from(value.len()).map_err(|_| Error::Invalid("v2 string too long".into()))?;
        self.u16(length);
        self.bytes.extend_from_slice(value.as_bytes());
        Ok(())
    }

    fn reference(&mut self, node: &str) -> Result<()> {
        let index = self
            .references
            .get(node)
            .ok_or_else(|| Error::Corrupt(format!("v2 reference to unknown node {node}")))?;
        self.u16(*index);
        Ok(())
    }

    fn media(&mut self, media: Media) {
        self.u8(match media {
            Media::Hot => 0,
            Media::Capacity => 1,
        });
    }

    fn policy(&mut self, policy: DomainPolicy) {
        self.u8(match policy {
            DomainPolicy::Strict => 0,
            DomainPolicy::Prefer => 1,
        });
    }

    fn condition(&mut self, condition: Condition) {
        match condition {
            Condition::Any => self.u8(0),
            Condition::Absent => self.u8(1),
            Condition::Generation(id) => {
                self.u8(2);
                self.u64(id);
            }
        }
    }

    fn node(&mut self, node: &NodeDomain) -> Result<()> {
        self.string(&node.id)?;
        self.string(&node.domain)?;
        self.media(node.media);
        self.u64(node.capacity);
        self.u8(match node.state {
            NodeState::Online => 0,
            NodeState::Draining => 1,
            NodeState::Offline => 2,
            NodeState::Removed => 3,
        });
        Ok(())
    }

    fn fragment(&mut self, fragment: &FragmentReceipt) -> Result<()> {
        self.u8(fragment.shard);
        self.reference(&fragment.node)?;
        self.u64(fragment.revision);
        self.u32(fragment.bytes);
        self.u32(fragment.crc32c);
        Ok(())
    }

    fn stripe(&mut self, stripe: &StripeManifest) -> Result<()> {
        self.u32(stripe.index);
        self.u64(stripe.offset);
        self.u32(stripe.data_bytes[0]);
        self.u32(stripe.data_bytes[1]);
        for fragment in &stripe.fragments {
            self.fragment(fragment)?;
        }
        Ok(())
    }

    fn object(&mut self, object: &ObjectGeneration) -> Result<()> {
        self.string(&object.key)?;
        self.u64(object.generation);
        self.u64(object.size);
        self.media(object.media);
        self.policy(object.domain_policy);
        self.count(object.stripes.len())?;
        for stripe in &object.stripes {
            self.stripe(stripe)?;
        }
        Ok(())
    }

    fn upload(&mut self, upload: &UploadSession) -> Result<()> {
        self.u64(upload.id);
        self.string(&upload.key)?;
        self.u64(upload.expected_bytes);
        self.media(upload.media);
        self.policy(upload.domain_policy);
        self.condition(upload.condition);
        self.count(upload.parts.len())?;
        for part in upload.parts.values() {
            self.u32(part.number);
            self.u64(part.revision);
            self.u32(part.bytes);
            self.u32(part.crc32c);
        }
        self.count(upload.placements.len())?;
        for plan in &upload.placements {
            for node in &plan.nodes {
                self.reference(node)?;
            }
        }
        Ok(())
    }

    fn repair(&mut self, repair: &RepairAttempt) -> Result<()> {
        self.u64(repair.id);
        self.string(&repair.key)?;
        self.u64(repair.generation);
        self.u32(repair.stripe);
        self.fragment(&repair.from)?;
        self.fragment(&repair.to)?;
        self.u8(match repair.kind {
            RepairKind::Heal => 0,
            RepairKind::Rebalance => 1,
        });
        self.u8(match repair.state {
            RepairState::Planned => 0,
            RepairState::Published => 1,
            RepairState::Stale => 2,
            RepairState::Unrecoverable => 3,
        });
        self.string(&repair.detail)
    }

    fn retired(&mut self, entry: &RetiredFragment) -> Result<()> {
        let address = &entry.address;
        self.reference(&address.node)?;
        self.u64(address.generation);
        self.u32(address.stripe);
        self.u8(address.shard);
        self.u64(address.revision);
        self.u32(entry.bytes);
        Ok(())
    }
}

struct Decoder<'a> {
    rest: &'a [u8],
    read: usize,
    node_ids: Vec<String>,
}

impl<'a> Decoder<'a> {
    fn new(rest: &'a [u8]) -> Self {
        Self {
            rest,
            read: 0,
            node_ids: Vec::new(),
        }
    }

    fn error(&self, message: &str) -> Error {
        Error::Corrupt(format!("v2 byte {}: {message}", self.read))
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

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn count(&mut self, limit: usize, element_minimum: usize) -> Result<usize> {
        let count = self.u32()? as usize;
        if count > limit || count > self.rest.len() / element_minimum {
            return Err(self.error("invalid collection length"));
        }
        Ok(count)
    }

    fn string(&mut self, limit: usize) -> Result<String> {
        let length = self.u16()? as usize;
        if length > limit {
            return Err(self.error("string too long"));
        }
        let bytes = self.take(length)?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| self.error("invalid UTF-8"))
    }

    fn reference(&mut self) -> Result<String> {
        let index = usize::from(self.u16()?);
        self.node_ids
            .get(index)
            .cloned()
            .ok_or_else(|| self.error("unknown node reference"))
    }

    fn media(&mut self) -> Result<Media> {
        match self.u8()? {
            0 => Ok(Media::Hot),
            1 => Ok(Media::Capacity),
            _ => Err(self.error("unknown media class")),
        }
    }

    fn policy(&mut self) -> Result<DomainPolicy> {
        match self.u8()? {
            0 => Ok(DomainPolicy::Strict),
            1 => Ok(DomainPolicy::Prefer),
            _ => Err(self.error("unknown domain policy")),
        }
    }

    fn condition(&mut self) -> Result<Condition> {
        match self.u8()? {
            0 => Ok(Condition::Any),
            1 => Ok(Condition::Absent),
            2 => Ok(Condition::Generation(self.u64()?)),
            _ => Err(self.error("unknown write condition")),
        }
    }

    fn node(&mut self) -> Result<NodeDomain> {
        let id = self.string(48)?;
        let domain = self.string(48)?;
        let media = self.media()?;
        let capacity = self.u64()?;
        let state = match self.u8()? {
            0 => NodeState::Online,
            1 => NodeState::Draining,
            2 => NodeState::Offline,
            3 => NodeState::Removed,
            _ => return Err(self.error("unknown node state")),
        };
        Ok(NodeDomain {
            id,
            domain,
            media,
            capacity,
            state,
        })
    }

    fn fragment(&mut self) -> Result<FragmentReceipt> {
        Ok(FragmentReceipt {
            shard: self.u8()?,
            node: self.reference()?,
            revision: self.u64()?,
            bytes: self.u32()?,
            crc32c: self.u32()?,
        })
    }

    fn stripe(&mut self) -> Result<StripeManifest> {
        Ok(StripeManifest {
            index: self.u32()?,
            offset: self.u64()?,
            data_bytes: [self.u32()?, self.u32()?],
            fragments: [self.fragment()?, self.fragment()?, self.fragment()?],
        })
    }

    fn object(&mut self) -> Result<ObjectGeneration> {
        let key = self.string(512)?;
        let generation = self.u64()?;
        let size = self.u64()?;
        let media = self.media()?;
        let domain_policy = self.policy()?;
        let mut stripes = Vec::new();
        for _ in 0..self.count(MAX_STRIPES, 77)? {
            stripes.push(self.stripe()?);
        }
        Ok(ObjectGeneration {
            key,
            generation,
            size,
            media,
            domain_policy,
            stripes,
        })
    }

    fn upload(&mut self) -> Result<UploadSession> {
        let id = self.u64()?;
        let key = self.string(512)?;
        let expected_bytes = self.u64()?;
        let media = self.media()?;
        let domain_policy = self.policy()?;
        let condition = self.condition()?;
        let mut parts = BTreeMap::new();
        for _ in 0..self.count(MAX_PARTS, 20)? {
            let part = PartReceipt {
                number: self.u32()?,
                revision: self.u64()?,
                bytes: self.u32()?,
                crc32c: self.u32()?,
            };
            map_insert(&mut parts, part.number, part, "part")?;
        }
        let mut placements = Vec::new();
        for _ in 0..self.count(MAX_STRIPES, 6)? {
            placements.push(PlacementPlan {
                nodes: [self.reference()?, self.reference()?, self.reference()?],
            });
        }
        Ok(UploadSession {
            id,
            key,
            expected_bytes,
            media,
            domain_policy,
            condition,
            parts,
            placements,
        })
    }

    fn repair(&mut self) -> Result<RepairAttempt> {
        let id = self.u64()?;
        let key = self.string(512)?;
        let generation = self.u64()?;
        let stripe = self.u32()?;
        let from = self.fragment()?;
        let to = self.fragment()?;
        let kind = match self.u8()? {
            0 => RepairKind::Heal,
            1 => RepairKind::Rebalance,
            _ => return Err(self.error("unknown repair kind")),
        };
        let state = match self.u8()? {
            0 => RepairState::Planned,
            1 => RepairState::Published,
            2 => RepairState::Stale,
            3 => RepairState::Unrecoverable,
            _ => return Err(self.error("unknown repair state")),
        };
        let detail = self.string(1024)?;
        Ok(RepairAttempt {
            id,
            key,
            generation,
            stripe,
            from,
            to,
            kind,
            state,
            detail,
        })
    }

    fn retired(&mut self) -> Result<RetiredFragment> {
        let address = FragmentAddress {
            node: self.reference()?,
            generation: self.u64()?,
            stripe: self.u32()?,
            shard: self.u8()?,
            revision: self.u64()?,
        };
        Ok(RetiredFragment {
            address,
            bytes: self.u32()?,
        })
    }
}
