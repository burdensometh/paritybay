use std::fmt;

use crate::codec::{
    checksum, reconstruct, xor_window, Crc32c, FragmentStream, StreamFault, SHARDS, STREAM_WINDOW,
};
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

    pub(crate) fn verify(&mut self, index: usize) {
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

/// Resolve a fragment's path and node readability, mapping failures onto the
/// read-path fault vocabulary.
pub(crate) fn open_fragment_stream(
    cluster: &Cluster,
    catalog: &Catalog,
    generation: u64,
    stripe: u32,
    receipt: &FragmentReceipt,
) -> std::result::Result<FragmentStream, ShardFault> {
    let address = FragmentAddress::from_receipt(generation, stripe, receipt);
    let make_fault = |kind: FaultKind, detail: String| ShardFault {
        address: address.clone(),
        kind,
        detail,
    };
    let node = &catalog.nodes[&receipt.node];
    if !node.state.can_read() {
        return Err(make_fault(
            FaultKind::NodeUnavailable,
            format!("node {} is {}", receipt.node, node.state),
        ));
    }
    let path = cluster
        .fragment_path(&address)
        .map_err(|error| make_fault(FaultKind::Io, error.to_string()))?;
    FragmentStream::open(&path, receipt.bytes, receipt.crc32c).map_err(|fault| {
        make_fault(
            match fault {
                StreamFault::Missing => FaultKind::Missing,
                StreamFault::InvalidLength => FaultKind::InvalidLength,
                StreamFault::InvalidChecksum => FaultKind::InvalidChecksum,
                StreamFault::Io => FaultKind::Io,
            },
            fault_label(fault, receipt),
        )
    })
}

fn fault_label(fault: StreamFault, receipt: &FragmentReceipt) -> String {
    match fault {
        StreamFault::Missing => "fragment file not found".into(),
        StreamFault::InvalidLength => {
            format!("length mismatch: expected {} bytes", receipt.bytes)
        }
        StreamFault::InvalidChecksum => format!(
            "checksum mismatch: expected {:08x} over {} bytes",
            receipt.crc32c, receipt.bytes
        ),
        StreamFault::Io => "fragment read error".into(),
    }
}

/// Verify one fragment through rolling windows without retaining its bytes.
pub(crate) fn verify_shard(
    cluster: &Cluster,
    catalog: &Catalog,
    generation: u64,
    stripe: u32,
    receipt: &FragmentReceipt,
) -> std::result::Result<(), ShardFault> {
    let mut stream = open_fragment_stream(cluster, catalog, generation, stripe, receipt)?;
    let mut buffer = vec![0u8; STREAM_WINDOW];
    while !stream.exhausted() {
        let take = (stream.remaining() as usize).min(STREAM_WINDOW);
        stream
            .read_window(&mut buffer[..take])
            .map_err(|fault| shard_fault(receipt, generation, stripe, fault))?;
    }
    stream
        .finish()
        .map_err(|fault| shard_fault(receipt, generation, stripe, fault))
}

fn shard_fault(
    receipt: &FragmentReceipt,
    generation: u64,
    stripe: u32,
    fault: StreamFault,
) -> ShardFault {
    let address = FragmentAddress::from_receipt(generation, stripe, receipt);
    ShardFault {
        address,
        kind: match fault {
            StreamFault::Missing => FaultKind::Missing,
            StreamFault::InvalidLength => FaultKind::InvalidLength,
            StreamFault::InvalidChecksum => FaultKind::InvalidChecksum,
            StreamFault::Io => FaultKind::Io,
        },
        detail: fault_label(fault, receipt),
    }
}

/// Check the XOR relation of three already checksum-verified shards using
/// fixed windows, without holding any whole shard in memory.
pub(crate) fn streaming_parity_matches(
    cluster: &Cluster,
    catalog: &Catalog,
    generation: u64,
    stripe: &StripeManifest,
) -> bool {
    let lengths = stripe.lengths();
    if lengths[0] != lengths[2] || lengths[1] > lengths[0] {
        return false;
    }
    let opened: std::result::Result<Vec<FragmentStream>, ShardFault> = stripe
        .fragments
        .iter()
        .map(|receipt| open_fragment_stream(cluster, catalog, generation, stripe.index, receipt))
        .collect();
    let mut streams = match opened {
        Ok(streams) => streams,
        Err(_) => return false,
    };
    let mut first = vec![0u8; STREAM_WINDOW];
    let mut second = vec![0u8; STREAM_WINDOW];
    let mut parity = vec![0u8; STREAM_WINDOW];
    let mut remaining = u64::from(lengths[0]);
    let ok = (|| -> std::result::Result<(), StreamFault> {
        while remaining > 0 {
            let take = (remaining as usize).min(STREAM_WINDOW);
            streams[0].read_window(&mut first[..take])?;
            streams[1].read_window(&mut second[..take])?;
            streams[2].read_window(&mut parity[..take])?;
            for offset in 0..take {
                if first[offset] ^ second[offset] != parity[offset] {
                    return Err(StreamFault::InvalidLength);
                }
            }
            remaining -= take as u64;
        }
        for stream in streams {
            stream.finish()?;
        }
        Ok(())
    })();
    ok.is_ok()
}

/// Determine whether a single faulty shard can be rebuilt from its two peers,
/// streaming the XOR and checking the reconstructed receipt checksum without
/// retaining whole shards.
pub(crate) fn streaming_rebuild_verifies(
    cluster: &Cluster,
    catalog: &Catalog,
    generation: u64,
    stripe: &StripeManifest,
    missing: u8,
) -> bool {
    let index = usize::from(missing);
    let lengths = stripe.lengths();
    if index >= SHARDS || lengths[0] != lengths[2] || lengths[1] > lengths[0] {
        return false;
    }
    let source_ids: Vec<usize> = (0..SHARDS)
        .filter(|candidate| *candidate != index)
        .collect();
    let opened: std::result::Result<Vec<FragmentStream>, ShardFault> = source_ids
        .iter()
        .map(|source| {
            open_fragment_stream(
                cluster,
                catalog,
                generation,
                stripe.index,
                &stripe.fragments[*source],
            )
        })
        .collect();
    let mut sources = match opened {
        Ok(streams) => streams,
        Err(_) => return false,
    };

    let target = &stripe.fragments[index];
    let mut left = vec![0u8; STREAM_WINDOW];
    let mut right = vec![0u8; STREAM_WINDOW];
    let mut output_crc = Crc32c::default();
    let expected = u64::from(target.bytes);
    let mut written = 0u64;
    let result = (|| -> std::result::Result<(), StreamFault> {
        while written < expected {
            let take = (expected - written).min(STREAM_WINDOW as u64) as usize;
            sources[0].read_window(&mut left[..take])?;
            sources[1].read_window(&mut right[..take])?;
            xor_window(&mut left[..take], &right[..take]);
            output_crc.update(&left[..take]);
            written += take as u64;
        }
        // Drain longer source tails (e.g. rebuilding a short shard 1) so each
        // source receipt verifies in full.
        let mut drain = vec![0u8; STREAM_WINDOW];
        for source in &mut sources {
            while !source.exhausted() {
                let take = (source.remaining() as usize).min(STREAM_WINDOW);
                source.read_window(&mut drain[..take])?;
            }
        }
        for source in sources {
            source.finish()?;
        }
        Ok(())
    })();
    result.is_ok() && output_crc.finish() == target.crc32c
}
