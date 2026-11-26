use std::fs::File;
use std::io::Write;
use std::ops::Range;
use std::path::Path;
use std::str::FromStr;

use crate::manifest::checked_key;
use crate::{Boundary, Catalog, Cluster, Error, ObjectGeneration, Result};

use super::{ShardFault, StripeReader};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RangeRequest {
    #[default]
    Full,
    From(u64),
    Span {
        start: u64,
        end: u64,
    },
    Suffix(u64),
}

impl RangeRequest {
    pub fn resolve(self, size: u64) -> Result<Range<u64>> {
        let (start, end) = match self {
            Self::Full => (0, size),
            Self::From(start) => (start, size),
            Self::Span { start, end } => (start, end),
            Self::Suffix(length) => (size.saturating_sub(length), size),
        };
        if start > end || end > size {
            return Err(Error::Invalid(format!(
                "range [{start},{end}) outside object of {size} bytes"
            )));
        }
        Ok(start..end)
    }
}

impl FromStr for RangeRequest {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        let invalid = || {
            Error::Invalid(format!(
                "invalid range {value:?}; use START:END, START:, or :SIZE"
            ))
        };
        let (start, end) = value.split_once(':').ok_or_else(invalid)?;
        match (start, end) {
            ("", "") => Ok(Self::Full),
            ("", end) => Ok(Self::Suffix(end.parse().map_err(|_| invalid())?)),
            (start, "") => Ok(Self::From(start.parse().map_err(|_| invalid())?)),
            (start, end) => Ok(Self::Span {
                start: start.parse().map_err(|_| invalid())?,
                end: end.parse().map_err(|_| invalid())?,
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StripeRead {
    pub stripe: u32,
    pub bytes_emitted: u64,
    pub bytes_verified: u64,
    pub reconstructed: Vec<u8>,
    pub faults: Vec<ShardFault>,
}

/// A durable repair todo for a fragment a degraded read reconstructed.
/// `attempt` is the catalog repair ID that `replay` or `heal` executes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairTodo {
    pub attempt: u64,
    pub stripe: u32,
    pub shard: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadReport {
    pub key: String,
    pub generation: u64,
    pub range: Range<u64>,
    pub stripes: Vec<StripeRead>,
    /// Repair todos for confirmed fragment damage, deduplicated per stripe
    /// across concurrent readers of the same generation.
    pub repairs: Vec<RepairTodo>,
    /// Faults with no todo filed because the node may only be temporarily
    /// inaccessible.
    pub inaccessible: Vec<ShardFault>,
}

pub struct PinnedObject {
    cluster: Cluster,
    _pin: File,
    catalog: Catalog,
    object: ObjectGeneration,
}

impl Cluster {
    pub fn pin_object(&self, key: &str) -> Result<PinnedObject> {
        checked_key(key)?;
        let pin = self.pin_fragments()?;
        let mut catalog = self.catalog()?;
        let object = catalog
            .objects
            .remove(key)
            .ok_or_else(|| Error::NotFound(format!("object {key:?}")))?;
        self.checkpoint(
            Boundary::ReadPinned,
            format!("{}@{}", key, object.generation),
        )?;
        Ok(PinnedObject {
            cluster: self.clone(),
            _pin: pin,
            catalog,
            object,
        })
    }

    pub fn get(
        &self,
        key: &str,
        range: RangeRequest,
        output: &mut impl Write,
    ) -> Result<ReadReport> {
        self.pin_object(key)?.read(range, output)
    }

    pub fn get_to_file(
        &self,
        key: &str,
        range: RangeRequest,
        path: impl AsRef<Path>,
    ) -> Result<ReadReport> {
        let path = path.as_ref();
        let mut draft = crate::files::DraftFile::create_output(self, path)?;
        let report = self.get(key, range, draft.writer())?;
        draft.sync(self, "read-output")?;
        draft.publish(path)?;
        Ok(report)
    }
}

impl PinnedObject {
    pub fn object(&self) -> &ObjectGeneration {
        &self.object
    }

    pub fn read(&self, request: RangeRequest, output: &mut impl Write) -> Result<ReadReport> {
        let range = request.resolve(self.object.size)?;
        let mut report = ReadReport {
            key: self.object.key.clone(),
            generation: self.object.generation,
            range: range.clone(),
            stripes: Vec::new(),
            repairs: Vec::new(),
            inaccessible: Vec::new(),
        };
        if range.is_empty() {
            return Ok(report);
        }
        let stripes = &self.object.stripes;
        let start = stripes.partition_point(|stripe| stripe.end() <= range.start);
        for stripe in stripes[start..]
            .iter()
            .take_while(|stripe| stripe.offset < range.end)
        {
            let mut reader =
                StripeReader::new(&self.cluster, &self.catalog, self.object.generation, stripe);
            let mut offset = stripe.offset;
            let mut reconstructed = Vec::new();
            let mut emitted = 0;
            for (shard, length) in stripe.data_bytes.into_iter().enumerate() {
                let end = offset + u64::from(length);
                let from = range.start.max(offset);
                let to = range.end.min(end);
                if from < to {
                    let selected = (from - offset) as usize..(to - offset) as usize;
                    let shard = shard as u8;
                    if let Some(bytes) = reader.direct(shard) {
                        write_bytes(output, &self.object.key, &bytes[selected])?;
                    } else {
                        let bytes = reader.rebuild(shard)?;
                        write_bytes(output, &self.object.key, &bytes[selected])?;
                        reconstructed.push(shard);
                    }
                    emitted += to - from;
                }
                offset = end;
            }
            let faults = reader.faults();
            for fault in &faults {
                if fault.kind.confirmed_damage() {
                    // Best effort: a todo that fails to persist never fails
                    // the read, and concurrent readers share one plan.
                    let attempt = self
                        .cluster
                        .queue_repair_todo(&self.object, stripe, fault.address.shard)
                        .ok()
                        .flatten();
                    if let Some(attempt) = attempt {
                        report.repairs.push(RepairTodo {
                            attempt,
                            stripe: stripe.index,
                            shard: fault.address.shard,
                        });
                    }
                } else {
                    report.inaccessible.push(fault.clone());
                }
            }
            report.stripes.push(StripeRead {
                stripe: stripe.index,
                bytes_emitted: emitted,
                bytes_verified: reader.verified_bytes(),
                reconstructed,
                faults,
            });
        }
        Ok(report)
    }
}

fn write_bytes(output: &mut impl Write, key: &str, bytes: &[u8]) -> Result<()> {
    output
        .write_all(bytes)
        .map_err(|e| Error::io("write object output", Path::new(key), e))
}
