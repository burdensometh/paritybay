use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::files;
use crate::manifest::{checked_id, MAX_NODES};
use crate::{Error, NodeDomain, NodeState, Result};

const CONFIG_BYTES: usize = 64 * 1024;
pub(crate) const MAX_BLOCK_BYTES: u32 = 4 * 1024 * 1024;
const DEFAULT_BLOCK_BYTES: u32 = 64 * 1024;
const DEFAULT_STAGING_LIMIT: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Config {
    pub block_bytes: u32,
    pub staging_limit: u64,
    pub nodes: Vec<NodeDomain>,
}

impl Config {
    pub fn read(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let bytes = files::read_limited(path, CONFIG_BYTES)?;
        let source = std::str::from_utf8(&bytes)
            .map_err(|_| Error::Invalid(format!("{} is not UTF-8", path.display())))?;
        Self::parse(source)
    }

    pub fn parse(source: &str) -> Result<Self> {
        let mut settings = BTreeSet::new();
        let mut config = Self {
            block_bytes: DEFAULT_BLOCK_BYTES,
            staging_limit: DEFAULT_STAGING_LIMIT,
            nodes: Vec::new(),
        };

        for (index, line) in source.lines().enumerate() {
            let line = line.split('#').next().unwrap_or_default().trim();
            if line.is_empty() {
                continue;
            }
            let parse_error =
                |problem| Error::Invalid(format!("configuration line {}: {problem}", index + 1));
            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| parse_error("expected key = value"))?;
            let key = key.trim();
            let value = value.trim();
            if key != "node" && !settings.insert(key) {
                return Err(parse_error("repeated setting"));
            }

            match key {
                "block_bytes" => {
                    config.block_bytes = value
                        .parse()
                        .map_err(|_| parse_error("invalid block_bytes"))?;
                }
                "staging_limit" => {
                    config.staging_limit = value
                        .parse()
                        .map_err(|_| parse_error("invalid staging_limit"))?;
                }
                "node" => config.nodes.push(parse_node(value)?),
                _ => return Err(parse_error("unknown setting")),
            }
        }
        config.as_node_map()?;
        Ok(config)
    }

    pub(crate) fn as_node_map(&self) -> Result<BTreeMap<String, NodeDomain>> {
        check_limits(self.block_bytes, self.staging_limit)?;
        if !(3..=MAX_NODES).contains(&self.nodes.len()) {
            return Err(Error::Invalid(format!(
                "configure between 3 and {MAX_NODES} distinct nodes"
            )));
        }

        let mut nodes = BTreeMap::new();
        for node in &self.nodes {
            validate_node(node)?;
            if node.state == NodeState::Removed {
                return Err(Error::Invalid(format!(
                    "initial node {} is removed",
                    node.id
                )));
            }
            if nodes.insert(node.id.clone(), node.clone()).is_some() {
                return Err(Error::Invalid(format!("duplicate node {}", node.id)));
            }
        }
        Ok(nodes)
    }
}

pub fn parse_node(value: &str) -> Result<NodeDomain> {
    let fields: Vec<_> = value.split(',').map(str::trim).collect();
    let [id, domain, media, capacity, state] = fields.as_slice() else {
        return Err(Error::Invalid(
            "node requires id, domain, media, capacity, state".into(),
        ));
    };
    let node = NodeDomain {
        id: (*id).into(),
        domain: (*domain).into(),
        media: media.parse()?,
        capacity: capacity
            .parse()
            .map_err(|_| Error::Invalid("invalid node capacity".into()))?,
        state: state.parse()?,
    };
    validate_node(&node)?;
    Ok(node)
}

pub(crate) fn check_limits(block_bytes: u32, staging_limit: u64) -> Result<()> {
    if !(1..=MAX_BLOCK_BYTES).contains(&block_bytes) {
        return Err(Error::Invalid(format!(
            "block_bytes must be between 1 and {MAX_BLOCK_BYTES}"
        )));
    }
    if staging_limit == 0 {
        return Err(Error::Invalid("staging_limit must be positive".into()));
    }
    Ok(())
}

pub(crate) fn validate_node(node: &NodeDomain) -> Result<()> {
    checked_id(&node.id)?;
    checked_id(&node.domain)?;
    if node.capacity == 0 {
        return Err(Error::Invalid(format!(
            "node {} has zero capacity",
            node.id
        )));
    }
    Ok(())
}
