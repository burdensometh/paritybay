use std::collections::BTreeMap;

use crate::codec::stripe_lengths;
use crate::manifest::Catalog;
use crate::{Error, RepairState, Result};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Allocation {
    pub current: u64,
    pub uploads: u64,
    pub repairs: u64,
    pub retired: u64,
}

impl Allocation {
    pub fn total(self) -> u64 {
        self.current + self.uploads + self.repairs + self.retired
    }
}

#[derive(Clone, Copy)]
enum Charge {
    Current,
    Upload,
    Repair,
    Retired,
}

#[derive(Clone, Debug)]
pub struct Ledger {
    nodes: BTreeMap<String, Allocation>,
}

impl Ledger {
    pub fn from_catalog(catalog: &Catalog, omit_upload: Option<u64>) -> Result<Self> {
        let mut ledger = Self {
            nodes: catalog
                .nodes
                .keys()
                .map(|id| (id.clone(), Allocation::default()))
                .collect(),
        };

        for object in catalog.objects.values() {
            for stripe in &object.stripes {
                for receipt in &stripe.fragments {
                    ledger.charge(&receipt.node, receipt.bytes, Charge::Current)?;
                }
            }
        }
        for upload in catalog
            .uploads
            .values()
            .filter(|entry| Some(entry.id) != omit_upload)
        {
            let mut remaining = upload.expected_bytes;
            for placement in &upload.placements {
                let lengths = stripe_lengths(remaining, catalog.block_bytes);
                for (node, bytes) in placement.nodes.iter().zip(lengths) {
                    ledger.charge(node, bytes, Charge::Upload)?;
                }
                remaining -= u64::from(lengths[0]) + u64::from(lengths[1]);
            }
        }
        for attempt in catalog
            .repairs
            .values()
            .filter(|entry| entry.state == RepairState::Planned)
        {
            ledger.charge(&attempt.to.node, attempt.to.bytes, Charge::Repair)?;
        }
        for entry in &catalog.retired {
            ledger.charge(&entry.address.node, entry.bytes, Charge::Retired)?;
        }
        Ok(ledger)
    }

    pub fn for_node(&self, node: &str) -> Allocation {
        self.nodes.get(node).copied().unwrap_or_default()
    }

    pub fn allocated(&self, node: &str) -> u64 {
        self.for_node(node).total()
    }

    /// Copy of the per-node bucket totals, keyed by node id.
    pub fn snapshot(&self) -> BTreeMap<String, Allocation> {
        self.nodes.clone()
    }

    /// Whether `bytes` more bytes still fit inside the node's budget.
    pub fn fits(&self, node: &str, capacity: u64, bytes: u32) -> bool {
        capacity.saturating_sub(self.allocated(node)) >= u64::from(bytes)
    }

    pub(crate) fn reserve(&mut self, node: &str, bytes: u32) -> Result<()> {
        self.charge(node, bytes, Charge::Upload)
    }

    /// Charge a freshly planned repair target. The reservation stays until the
    /// copy publishes or the plan is discarded.
    pub(crate) fn book_repair(&mut self, node: &str, bytes: u32) -> Result<()> {
        self.charge(node, bytes, Charge::Repair)
    }

    /// Publish a reserved repair copy as a current fragment on its node.
    pub(crate) fn shift_repair_to_current(
        &mut self,
        node: &str,
        bytes: u32,
    ) -> Result<()> {
        let allocation = self.require(node)?;
        allocation.repairs = allocation
            .repairs
            .checked_sub(u64::from(bytes))
            .ok_or_else(|| Error::Corrupt(format!("allocation underflow for node {node}")))?;
        allocation.current = allocation
            .current
            .checked_add(u64::from(bytes))
            .ok_or_else(|| Error::Corrupt(format!("allocation overflow for node {node}")))?;
        Ok(())
    }

    /// Retire a current fragment without reclaiming its bytes; retired files
    /// keep charging the node until collection removes them.
    pub(crate) fn shift_current_to_retired(
        &mut self,
        node: &str,
        bytes: u32,
    ) -> Result<()> {
        let allocation = self.require(node)?;
        allocation.current = allocation
            .current
            .checked_sub(u64::from(bytes))
            .ok_or_else(|| Error::Corrupt(format!("allocation underflow for node {node}")))?;
        allocation.retired = allocation
            .retired
            .checked_add(u64::from(bytes))
            .ok_or_else(|| Error::Corrupt(format!("allocation overflow for node {node}")))?;
        Ok(())
    }

    /// Discard a reservation that never became a current fragment.
    pub(crate) fn release_repair(&mut self, node: &str, bytes: u32) -> Result<()> {
        let allocation = self.require(node)?;
        allocation.repairs = allocation
            .repairs
            .checked_sub(u64::from(bytes))
            .ok_or_else(|| Error::Corrupt(format!("allocation underflow for node {node}")))?;
        Ok(())
    }

    /// Retire the reserved target of a repair that never published.
    pub(crate) fn shift_repair_to_retired(
        &mut self,
        node: &str,
        bytes: u32,
    ) -> Result<()> {
        let allocation = self.require(node)?;
        allocation.repairs = allocation
            .repairs
            .checked_sub(u64::from(bytes))
            .ok_or_else(|| Error::Corrupt(format!("allocation underflow for node {node}")))?;
        allocation.retired = allocation
            .retired
            .checked_add(u64::from(bytes))
            .ok_or_else(|| Error::Corrupt(format!("allocation overflow for node {node}")))?;
        Ok(())
    }

    fn require(&mut self, node: &str) -> Result<&mut Allocation> {
        self.nodes
            .get_mut(node)
            .ok_or_else(|| Error::Corrupt(format!("allocation references unknown node {node}")))
    }

    fn charge(&mut self, node: &str, bytes: u32, charge: Charge) -> Result<()> {
        let allocation = self.require(node)?;
        let bytes = u64::from(bytes);
        allocation
            .total()
            .checked_add(bytes)
            .ok_or_else(|| Error::Corrupt(format!("allocation overflow for node {node}")))?;
        let counter = match charge {
            Charge::Current => &mut allocation.current,
            Charge::Upload => &mut allocation.uploads,
            Charge::Repair => &mut allocation.repairs,
            Charge::Retired => &mut allocation.retired,
        };
        *counter += bytes;
        Ok(())
    }
}
