use std::fs;
use std::io::{Read, Write};
use std::path::Path;

use paritybay::manifest::v1;
use paritybay::repair::MaintenanceOutcome;
use paritybay::{Cluster, Error, RepairState, Result};

use super::arguments::Arguments;
use super::{io_error, render};

pub(super) fn recognizes(command: &str) -> bool {
    matches!(
        command,
        "nodes"
            | "node-add"
            | "node-state"
            | "node-size"
            | "node-media"
            | "scan"
            | "heal"
            | "rebalance"
            | "checksum-upgrade"
            | "upgrade-checksums"
            | "rechecksum"
            | "checksum-policy"
            | "replay"
            | "repair-log"
            | "repair-prune"
            | "gc"
            | "v1-export"
    )
}

pub(super) fn run(
    cluster: &Cluster,
    command: &str,
    mut args: Arguments,
    out: &mut dyn Write,
) -> Result<u8> {
    match command {
        "nodes" => {
            args.end()?;
            for item in cluster.node_usage()? {
                let node = item.node;
                let allocations = item.allocation;
                render::line(out, format_args!(
                    "node={} domain={} media={} state={} capacity={} free={} current={} uploads={} repairs={} retired={}",
                    node.id, node.domain, node.media, node.state, node.capacity, item.free, allocations.current,
                    allocations.uploads, allocations.repairs, allocations.retired,
                ))?;
            }
        }
        "node-add" => {
            let node = paritybay::parse_node(&args.text("node specification")?)?;
            args.end()?;
            let id = node.id.clone();
            render::line_after(out, &id, cluster.add_node(node))?;
        }
        "node-state" | "node-size" | "node-media" => {
            let id = args.text("node")?;
            let value = args.text("node value")?;
            args.end()?;
            let result = match command {
                "node-state" => cluster.set_node_state(&id, value.parse()?),
                "node-media" => cluster.set_node_media(&id, value.parse()?),
                _ => cluster.set_node_capacity(
                    &id,
                    value
                        .parse()
                        .map_err(|_| Error::Invalid("invalid node capacity".into()))?,
                ),
            };
            render::line_after(out, &id, result)?;
        }
        "checksum-policy" => {
            args.end()?;
            render::line(
                out,
                format_args!("checksum={}", cluster.checksum_algorithm()?),
            )?;
        }
        "checksum-upgrade" | "upgrade-checksums" | "rechecksum" => {
            let key = args.maybe_text()?;
            args.end()?;
            let report = cluster.upgrade_checksums(key.as_deref())?;
            render::maintenance(out, &report)?;
        }
        "scan" | "heal" | "rebalance" => {
            let key = args.maybe_text()?;
            args.end()?;
            if command == "scan" {
                let totals = cluster.scan(key.as_deref())?;
                render::scan(out, &totals)?;
                return Ok(if totals.unrecoverable + totals.parity_mismatch > 0 {
                    6
                } else {
                    0
                });
            }
            let report = if command == "heal" {
                cluster.heal(key.as_deref())?
            } else {
                cluster.rebalance(key.as_deref())?
            };
            render::maintenance(out, &report)?;
            if report
                .events
                .iter()
                .any(|event| event.outcome == MaintenanceOutcome::Unrecoverable)
            {
                return Ok(6);
            }
            if report
                .events
                .iter()
                .any(|event| event.outcome == MaintenanceOutcome::NoTarget)
            {
                return Ok(5);
            }
            if report
                .events
                .iter()
                .any(|event| event.outcome == MaintenanceOutcome::Pending)
            {
                return Ok(4);
            }
        }
        "replay" | "repair-log" => {
            let id = args.maybe_number("attempt")?;
            args.end()?;
            let attempts = match (command, id) {
                ("replay", Some(id)) => vec![cluster.run_repair(id)?],
                ("replay", None) => cluster.replay_repairs()?,
                (_, Some(id)) => vec![cluster.repair_attempt(id)?],
                (_, None) => cluster.catalog()?.repairs.into_values().collect(),
            };
            for attempt in &attempts {
                render::repair(out, attempt)?;
            }
            if command == "replay"
                && attempts
                    .iter()
                    .any(|attempt| attempt.state == RepairState::Unrecoverable)
            {
                return Ok(6);
            }
        }
        "repair-prune" => {
            let through = args.number("terminal attempt upper bound")?;
            args.end()?;
            let removed = cluster.prune_repairs(through)?;
            render::line(out, format_args!("pruned={removed}"))?;
        }
        "gc" => {
            args.end()?;
            render::collection(out, &cluster.collect()?)?;
        }
        "v1-export" => {
            let path = args.path("output")?;
            args.end()?;
            if path == Path::new("-") {
                let bytes = v1::encode(&cluster.catalog()?)?;
                out.write_all(&bytes)
                    .map_err(|e| io_error("write catalog", &path, e))?;
            } else {
                cluster.export_catalog(path)?;
            }
        }
        _ => unreachable!("command table checked"),
    }
    Ok(0)
}

pub(super) fn check_v1(path: &Path, out: &mut dyn Write) -> Result<u8> {
    let file = fs::File::open(path).map_err(|e| io_error("open v1", path, e))?;
    let mut bytes = Vec::new();
    file.take(v1::MAX_CATALOG_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| io_error("read v1", path, e))?;
    let catalog = v1::decode(&bytes)?;
    let canonical = bytes == v1::encode(&catalog)?;
    render::line(
        out,
        format_args!(
            "v1=true canonical={canonical} objects={} uploads={} nodes={} repairs={} retired={}",
            catalog.objects.len(),
            catalog.uploads.len(),
            catalog.nodes.len(),
            catalog.repairs.len(),
            catalog.retired.len(),
        ),
    )?;
    Ok(0)
}
