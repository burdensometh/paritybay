use std::fmt;
use std::io::Write;
use std::path::Path;

use paritybay::readpath::{Inspection, ReadReport, ShardFault};
use paritybay::repair::{CollectionReport, MaintenanceReport, ScanTotals};
use paritybay::upload::WriteReceipt;
use paritybay::{ObjectGeneration, RepairAttempt, Result, UploadSession};

pub(super) fn line(out: &mut dyn Write, message: fmt::Arguments<'_>) -> Result<()> {
    writeln!(out, "{message}").map_err(|e| super::io_error("write report", Path::new("-"), e))
}

pub(super) fn line_after(out: &mut dyn Write, id: &str, result: Result<()>) -> Result<()> {
    result?;
    line(out, format_args!("updated_node={id}"))
}

pub(super) fn object(out: &mut dyn Write, object: &ObjectGeneration) -> Result<()> {
    line(
        out,
        format_args!(
            "key={:?} generation={} bytes={} stripes={} media={} domains={}",
            object.key,
            object.generation,
            object.size,
            object.stripes.len(),
            object.media,
            object.domain_policy,
        ),
    )
}

pub(super) fn write_receipt(out: &mut dyn Write, item: &WriteReceipt) -> Result<()> {
    let replaced = item
        .replaced_generation
        .map_or_else(|| "none".into(), |id| id.to_string());
    line(
        out,
        format_args!(
            "generation={} key={:?} bytes={} stripes={} condition={} replaced={} weaker_stripes={}",
            item.generation,
            item.key,
            item.bytes,
            item.stripes,
            item.condition,
            replaced,
            item.weaker_stripes,
        ),
    )
}

pub(super) fn upload(out: &mut dyn Write, item: &UploadSession) -> Result<()> {
    line(
        out,
        format_args!(
            "upload={} key={:?} expected={} staged={} parts={} media={} domains={} condition={}",
            item.id,
            item.key,
            item.expected_bytes,
            item.staged_bytes(),
            item.parts.len(),
            item.media,
            item.domain_policy,
            item.condition,
        ),
    )?;
    for part in item.parts.values() {
        line(
            out,
            format_args!(
                "part={} bytes={} revision={} crc32c={:08x}",
                part.number, part.bytes, part.revision, part.crc32c,
            ),
        )?;
    }
    for (index, plan) in item.placements.iter().enumerate() {
        line(
            out,
            format_args!("stripe={index} reserved_nodes={}", plan.nodes.join(",")),
        )?;
    }
    Ok(())
}

pub(super) fn inspection(out: &mut dyn Write, inspection: &Inspection) -> Result<()> {
    object(out, &inspection.object)?;
    for stripe in &inspection.stripes {
        line(
            out,
            format_args!(
                "stripe={} offset={} end={} verdict={} domain_survivors={} detail={:?}",
                stripe.manifest.index,
                stripe.manifest.offset,
                stripe.manifest.end(),
                stripe.verdict,
                stripe.domains.surviving_shards_after_worst_domain_loss,
                stripe.detail.as_deref().unwrap_or(""),
            ),
        )?;
        for fragment in &stripe.manifest.fragments {
            line(
                out,
                format_args!(
                    "  shard={} node={} revision={} bytes={} crc32c={:08x}",
                    fragment.shard,
                    fragment.node,
                    fragment.revision,
                    fragment.bytes,
                    fragment.crc32c,
                ),
            )?;
        }
        for fault in &stripe.faults {
            fault_line(out, fault)?;
        }
    }
    Ok(())
}

fn fault_line(out: &mut dyn Write, fault: &ShardFault) -> Result<()> {
    line(
        out,
        format_args!(
            "  fault={} stripe={} shard={} node={} detail={:?}",
            fault.kind, fault.address.stripe, fault.address.shard, fault.address.node, fault.detail,
        ),
    )
}

pub(super) fn read_report(out: &mut dyn Write, report: &ReadReport) -> Result<()> {
    line(
        out,
        format_args!(
            "key={:?} generation={} start={} end={} stripes={}",
            report.key,
            report.generation,
            report.range.start,
            report.range.end,
            report.stripes.len(),
        ),
    )?;
    for stripe in &report.stripes {
        line(
            out,
            format_args!(
                "stripe={} emitted={} verified={} reconstructed={:?}",
                stripe.stripe, stripe.bytes_emitted, stripe.bytes_verified, stripe.reconstructed,
            ),
        )?;
        for fault in &stripe.faults {
            fault_line(out, fault)?;
        }
    }
    Ok(())
}

pub(super) fn scan(out: &mut dyn Write, totals: &ScanTotals) -> Result<()> {
    line(
        out,
        format_args!(
            "objects={} healthy={} recoverable={} unrecoverable={} parity_mismatch={}",
            totals.objects,
            totals.healthy,
            totals.recoverable,
            totals.unrecoverable,
            totals.parity_mismatch,
        ),
    )
}

pub(super) fn repair(out: &mut dyn Write, item: &RepairAttempt) -> Result<()> {
    line(out, format_args!(
        "attempt={} kind={} state={} key={:?} generation={} stripe={} shard={} from={} to={} detail={:?}",
        item.id, item.kind, item.state, item.key, item.generation, item.stripe, item.from.shard,
        item.from.node, item.to.node, item.detail,
    ))
}

pub(super) fn collection(out: &mut dyn Write, report: &CollectionReport) -> Result<()> {
    line(
        out,
        format_args!(
            "deferred={} retired_entries={} fragment_files={} staging_files={} temporary_files={}",
            report.deferred,
            report.retired_entries,
            report.fragment_files,
            report.staging_files,
            report.temporary_files,
        ),
    )
}

pub(super) fn maintenance(out: &mut dyn Write, report: &MaintenanceReport) -> Result<()> {
    scan(out, &report.scan)?;
    for event in &report.events {
        line(out, format_args!(
            "kind={} outcome={} key={:?} generation={} stripe={} shard={:?} attempt={:?} detail={:?}",
            report.kind, event.outcome, event.key, event.generation, event.stripe, event.shard,
            event.attempt, event.detail,
        ))?;
    }
    line(out, format_args!(
        "budget ledger_builds={} ledger_refreshes={} publication_rechecks={}",
        report.budget.ledger_builds,
        report.budget.ledger_refreshes,
        report.budget.publication_rechecks,
    ))?;
    if let Some(removed) = &report.collection {
        collection(out, removed)?;
    }
    Ok(())
}
