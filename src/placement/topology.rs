use crate::config::validate_node;
use crate::manifest::MAX_NODES;
use crate::{
    Boundary, Catalog, Cluster, Error, Media, MigrationState, NodeDomain, NodeState, RepairState,
    Result,
};

use super::Ledger;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeUsage {
    pub node: NodeDomain,
    pub allocation: super::Allocation,
    pub free: u64,
}

impl Cluster {
    pub fn add_node(&self, node: NodeDomain) -> Result<()> {
        validate_node(&node)?;
        if node.state == NodeState::Removed {
            return Err(Error::Invalid("cannot add a removed node".into()));
        }
        let mut txn = self.transaction()?;
        if txn.catalog.nodes.contains_key(&node.id) {
            return Err(Error::Conflict(format!(
                "node id {} is already allocated",
                node.id
            )));
        }
        if txn.catalog.nodes.len() >= MAX_NODES {
            return Err(Error::Capacity(format!(
                "node table is limited to {MAX_NODES} entries"
            )));
        }
        self.initialize_node_dir(&node.id)?;
        txn.catalog.nodes.insert(node.id.clone(), node);
        txn.commit("node-add")
    }

    pub fn set_node_state(&self, id: &str, state: NodeState) -> Result<()> {
        if state == NodeState::Removed {
            return self.remove_node(id);
        }
        let mut txn = self.transaction()?;
        let node = mutable_node(&mut txn.catalog, id)?;
        if node.state == state {
            return Ok(());
        }
        node.state = state;
        txn.commit("node-state")
    }

    pub fn set_node_capacity(&self, id: &str, capacity: u64) -> Result<()> {
        let mut txn = self.transaction()?;
        let allocated = Ledger::from_catalog(&txn.catalog, None)?.allocated(id);
        if capacity == 0 || capacity < allocated {
            return Err(Error::Capacity(format!(
                "node {id} requires capacity for {allocated} bytes"
            )));
        }
        let node = mutable_node(&mut txn.catalog, id)?;
        if node.capacity == capacity {
            return Ok(());
        }
        node.capacity = capacity;
        txn.commit("node-capacity")
    }

    pub fn set_node_media(&self, id: &str, media: Media) -> Result<()> {
        let mut txn = self.transaction()?;
        let node = mutable_node(&mut txn.catalog, id)?;
        if node.media == media {
            return Ok(());
        }
        if node.state == NodeState::Online {
            return Err(Error::Conflict(format!(
                "drain node {id} before changing its media"
            )));
        }
        ensure_unreferenced(&txn.catalog, id)?;
        mutable_node(&mut txn.catalog, id)?.media = media;
        txn.commit("node-media")
    }

    pub fn remove_node(&self, id: &str) -> Result<()> {
        // Keep the pause outside the catalog lock; the eligibility check runs after resume.
        self.checkpoint(Boundary::BeforeNodeRemoval, id)?;
        let mut txn = self.transaction()?;
        let node = txn
            .catalog
            .nodes
            .get(id)
            .ok_or_else(|| Error::NotFound(format!("node {id}")))?;
        if node.state == NodeState::Removed {
            return Ok(());
        }
        if node.state == NodeState::Online {
            return Err(Error::Conflict(format!(
                "drain or offline node {id} before removing it"
            )));
        }
        ensure_unreferenced(&txn.catalog, id)?;
        mutable_node(&mut txn.catalog, id)?.state = NodeState::Removed;
        txn.commit("node-remove")
    }

    pub fn node_usage(&self) -> Result<Vec<NodeUsage>> {
        let catalog = self.catalog()?;
        let ledger = Ledger::from_catalog(&catalog, None)?;
        Ok(catalog
            .nodes
            .into_values()
            .map(|node| {
                let allocation = ledger.for_node(&node.id);
                NodeUsage {
                    free: node.capacity - allocation.total(),
                    node,
                    allocation,
                }
            })
            .collect())
    }
}

fn mutable_node<'a>(catalog: &'a mut Catalog, id: &str) -> Result<&'a mut NodeDomain> {
    match catalog.nodes.get_mut(id) {
        Some(node) if node.state != NodeState::Removed => Ok(node),
        Some(_) => Err(Error::Conflict(format!("node {id} was removed"))),
        None => Err(Error::NotFound(format!("node {id}"))),
    }
}

fn ensure_unreferenced(catalog: &Catalog, id: &str) -> Result<()> {
    if catalog.objects.values().any(|object| {
        object
            .stripes
            .iter()
            .any(|stripe| stripe.fragments.iter().any(|fragment| fragment.node == id))
    }) {
        return Err(Error::Conflict(format!(
            "node {id} still stores current fragments"
        )));
    }

    if catalog.uploads.values().any(|upload| {
        upload
            .placements
            .iter()
            .any(|plan| plan.nodes.iter().any(|candidate| candidate == id))
    }) {
        return Err(Error::Conflict(format!(
            "node {id} still holds upload reservations"
        )));
    }

    if catalog.repairs.values().any(|attempt| {
        attempt.state == RepairState::Planned && (attempt.from.node == id || attempt.to.node == id)
    }) {
        return Err(Error::Conflict(format!(
            "node {id} still has a planned repair"
        )));
    }
    if catalog.migrations.values().any(|migration| {
        migration.state == MigrationState::Planned
            && migration
                .targets
                .iter()
                .flatten()
                .any(|receipt| receipt.node == id)
    }) {
        return Err(Error::Conflict(format!(
            "node {id} still holds a planned migration target"
        )));
    }
    if catalog
        .retired
        .iter()
        .any(|fragment| fragment.address.node == id)
    {
        return Err(Error::Conflict(format!(
            "node {id} still holds retired fragments; run gc"
        )));
    }
    Ok(())
}
