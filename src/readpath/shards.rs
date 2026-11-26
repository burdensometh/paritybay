use std::fmt;

use crate::codec::{checksum, reconstruct, SHARDS};
use crate::files;
use crate::manifest::{FragmentAddress, FragmentReceipt};
use crate::{Catalog, Cluster, Error, Result, StripeManifest};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultKind {
    NodeUnavailable,
    Missing,
    InvalidLength,
    InvalidChecksum,
    Io,
}

impl fmt::Display for FaultKind {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(match self {
            Self::NodeUnavailable => "node-unavailable",
            Self::Missing => "missing",
            Self::InvalidLength => "invalid-length",
            Self::InvalidChecksum => "invalid-checksum",
            Self::Io => "io",
        })
    }
}

impl FaultKind {
    /// Confirmed fragment damage: the file is gone or fails validation, so a
    /// degraded read files a repair todo. Anything else may be transient and
    /// is only reported as temporarily inaccessible.
    pub(crate) fn confirmed_damage(self) -> bool {
        matches!(
            self,
            Self::Missing | Self::InvalidLength | Self::InvalidChecksum
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardFault {
    pub address: FragmentAddress,
    pub kind: FaultKind,
    pub detail: String,
}

enum Shard {
    Unchecked,
    Verified(Vec<u8>),
    Fault(ShardFault),
}

pub(crate) struct StripeReader<'a> {
    cluster: &'a Cluster,
    catalog: &'a Catalog,
    generation: u64,
    stripe: &'a StripeManifest,
    shards: [Shard; SHARDS],
}

impl<'a> StripeReader<'a> {
    pub(crate) fn new(
        cluster: &'a Cluster,
        catalog: &'a Catalog,
        generation: u64,
        stripe: &'a StripeManifest,
    ) -> Self {
        Self {
            cluster,
            catalog,
            generation,
            stripe,
            shards: std::array::from_fn(|_| Shard::Unchecked),
        }
    }

    pub(crate) fn verify_all(&mut self) {
        for index in 0..SHARDS {
            self.verify(index);
        }
    }

    fn verify(&mut self, index: usize) {
        if !matches!(self.shards[index], Shard::Unchecked) {
            return;
        }
        let receipt = &self.stripe.fragments[index];
        self.shards[index] = match read_shard(
            self.cluster,
            self.catalog,
            self.generation,
            self.stripe.index,
            receipt,
        ) {
            Ok(bytes) => Shard::Verified(bytes),
            Err(fault) => Shard::Fault(fault),
        };
    }

    pub(crate) fn direct(&mut self, shard: u8) -> Option<&[u8]> {
        let index = usize::from(shard);
        self.verify(index);
        match &self.shards[index] {
            Shard::Verified(bytes) => Some(bytes),
            _ => None,
        }
    }

    pub(crate) fn rebuild(&mut self, shard: u8) -> Result<Vec<u8>> {
        let index = usize::from(shard);
        for source in (0..SHARDS).filter(|candidate| *candidate != index) {
            self.verify(source);
        }
        let sources = std::array::from_fn(|source| match &self.shards[source] {
            Shard::Verified(bytes) if source != index => Some(bytes.as_slice()),
            _ => None,
        });
        if sources.iter().flatten().count() != 2 {
            return Err(self.unrecoverable(format!(
                "cannot reconstruct shard {shard}; {}",
                self.faults()
                    .iter()
                    .map(|fault| format!("shard {} {}", fault.address.shard, fault.kind))
                    .collect::<Vec<_>>()
                    .join(", "),
            )));
        }
        let rebuilt = reconstruct(self.stripe.lengths(), shard, sources)?;
        if checksum(&rebuilt) != self.stripe.fragments[index].crc32c {
            return Err(
                self.unrecoverable(format!("reconstructed checksum differs for shard {shard}"))
            );
        }
        Ok(rebuilt)
    }

    pub(crate) fn parity_matches(&mut self) -> bool {
        self.verify_all();
        let [Shard::Verified(first), Shard::Verified(second), Shard::Verified(parity)] =
            &self.shards
        else {
            return false;
        };
        first
            .iter()
            .zip(parity)
            .enumerate()
            .all(|(i, (&data, &p))| data ^ second.get(i).copied().unwrap_or(0) == p)
    }

    pub(crate) fn faults(&self) -> Vec<ShardFault> {
        self.shards
            .iter()
            .filter_map(|shard| match shard {
                Shard::Fault(fault) => Some(fault.clone()),
                _ => None,
            })
            .collect()
    }

    pub(crate) fn verified_bytes(&self) -> u64 {
        self.shards
            .iter()
            .filter_map(|shard| match shard {
                Shard::Verified(bytes) => Some(bytes.len() as u64),
                _ => None,
            })
            .sum()
    }

    fn unrecoverable(&self, detail: String) -> Error {
        Error::Unrecoverable {
            generation: self.generation,
            stripe: self.stripe.index,
            detail,
        }
    }
}

fn read_shard(
    cluster: &Cluster,
    catalog: &Catalog,
    generation: u64,
    stripe: u32,
    receipt: &FragmentReceipt,
) -> std::result::Result<Vec<u8>, ShardFault> {
    let address = FragmentAddress::from_receipt(generation, stripe, receipt);
    let fault = |kind, detail: String| ShardFault {
        address: address.clone(),
        kind,
        detail,
    };
    if !catalog.nodes[&receipt.node].state.can_read() {
        let state = catalog.nodes[&receipt.node].state;
        return Err(fault(
            FaultKind::NodeUnavailable,
            format!("node {} is {state}", receipt.node),
        ));
    }
    let path = cluster
        .fragment_path(&address)
        .map_err(|e| fault(FaultKind::Io, e.to_string()))?;
    let bytes = files::read_limited(&path, receipt.bytes as usize).map_err(|e| {
        let kind = match &e {
            Error::Corrupt(_) => FaultKind::InvalidLength,
            _ if e.is_not_found_io() => FaultKind::Missing,
            _ => FaultKind::Io,
        };
        fault(kind, e.to_string())
    })?;
    if bytes.len() != receipt.bytes as usize {
        return Err(fault(
            FaultKind::InvalidLength,
            format!("expected {}, read {}", receipt.bytes, bytes.len()),
        ));
    }
    let seen = checksum(&bytes);
    if seen != receipt.crc32c {
        return Err(fault(
            FaultKind::InvalidChecksum,
            format!("expected {:08x}, read {seen:08x}", receipt.crc32c),
        ));
    }
    Ok(bytes)
}
