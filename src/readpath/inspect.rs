use std::fmt;

use crate::manifest::checked_key;
use crate::placement::DomainHealth;
use crate::{Catalog, Cluster, Error, ObjectGeneration, Result, StripeManifest};

use super::shards::{
    streaming_parity_matches, streaming_rebuild_verifies, verify_shard, ShardFault,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StripeVerdict {
    Healthy,
    Recoverable,
    Unrecoverable,
    ParityMismatch,
}

impl fmt::Display for StripeVerdict {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(match self {
            Self::Healthy => "healthy",
            Self::Recoverable => "recoverable",
            Self::Unrecoverable => "unrecoverable",
            Self::ParityMismatch => "parity-mismatch",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StripeInspection {
    pub manifest: StripeManifest,
    pub domains: DomainHealth,
    pub verdict: StripeVerdict,
    pub faults: Vec<ShardFault>,
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Inspection {
    pub object: ObjectGeneration,
    pub stripes: Vec<StripeInspection>,
}

impl Cluster {
    pub fn inspect(&self, key: &str) -> Result<Inspection> {
        checked_key(key)?;
        let _pin = self.pin_fragments()?;
        let catalog = self.catalog()?;
        let object = catalog
            .objects
            .get(key)
            .ok_or_else(|| Error::NotFound(format!("object {key:?}")))?;
        Ok(self.inspect_object(&catalog, object))
    }

    pub(crate) fn inspect_object(
        &self,
        catalog: &Catalog,
        object: &ObjectGeneration,
    ) -> Inspection {
        let stripes = object
            .stripes
            .iter()
            .map(|stripe| self.inspect_stripe(catalog, object.generation, stripe))
            .collect();
        Inspection {
            object: object.clone(),
            stripes,
        }
    }

    /// Inspect a stripe with fixed-window streaming checks. No whole shard is
    /// ever buffered, so inspection memory is independent of block size.
    pub(crate) fn inspect_stripe(
        &self,
        catalog: &Catalog,
        generation: u64,
        stripe: &StripeManifest,
    ) -> StripeInspection {
        let faults: Vec<ShardFault> = stripe
            .fragments
            .iter()
            .filter_map(|receipt| {
                verify_shard(self, catalog, generation, stripe.index, receipt).err()
            })
            .collect();

        let (verdict, detail) = match faults.as_slice() {
            [] => {
                if streaming_parity_matches(self, catalog, generation, stripe) {
                    (StripeVerdict::Healthy, None)
                } else {
                    (
                        StripeVerdict::ParityMismatch,
                        Some("three checksums passed but the XOR relation failed".into()),
                    )
                }
            }
            [fault] => {
                if streaming_rebuild_verifies(
                    self,
                    catalog,
                    generation,
                    stripe,
                    fault.address.shard,
                ) {
                    (StripeVerdict::Recoverable, None)
                } else {
                    (
                        StripeVerdict::Unrecoverable,
                        Some(format!(
                            "cannot reconstruct shard {} from the surviving peers",
                            fault.address.shard
                        )),
                    )
                }
            }
            _ => (
                StripeVerdict::Unrecoverable,
                Some("fewer than two verified shards".into()),
            ),
        };
        StripeInspection {
            manifest: stripe.clone(),
            domains: DomainHealth::for_stripe(catalog, stripe),
            verdict,
            faults,
            detail,
        }
    }
}
