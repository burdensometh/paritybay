use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

use paritybay::manifest::FragmentAddress;
use paritybay::readpath::RangeRequest;
use paritybay::upload::UploadOptions;
use paritybay::{Cluster, Condition, DomainPolicy, Error, Media, Result};

use super::arguments::{Arguments, Switches};
use super::{io_error, render};

pub(super) fn recognizes(command: &str) -> bool {
    matches!(
        command,
        "put"
            | "begin"
            | "part"
            | "upload"
            | "replan"
            | "finish"
            | "abort"
            | "get"
            | "stat"
            | "list"
            | "delete"
            | "inspect"
            | "fragment-path"
    )
}

pub(super) fn run(
    cluster: &Cluster,
    command: &str,
    mut args: Arguments,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> Result<u8> {
    match command {
        "put" => {
            let key = args.text("key")?;
            let path = args.path("input")?;
            let flags = args.switches(&["--size", "--media", "--domains", "--if"], &[])?;
            let (mut input, size) = input(&path, flags.value("--size")?)?;
            let receipt = cluster.put(&key, upload_options(size, &flags)?, &mut input)?;
            render::write_receipt(out, &receipt)?;
        }
        "begin" => {
            let key = args.text("key")?;
            let size = args.number("size")?;
            let flags = args.switches(&["--media", "--domains", "--if"], &[])?;
            let session = cluster.begin_upload(&key, upload_options(size, &flags)?)?;
            render::upload(out, &session)?;
        }
        "part" => {
            let id = args.number("upload")?;
            let part = args.number("part")?;
            let path = args.path("input")?;
            let flags = args.switches(&["--size"], &[])?;
            let (mut input, size) = input(&path, flags.value("--size")?)?;
            let size = u32::try_from(size).map_err(|_| Error::Invalid("part too large".into()))?;
            let receipt = cluster.put_part(id, part, size, &mut input)?;
            render::line(
                out,
                format_args!(
                    "upload={id} part={} bytes={} revision={} crc32c={:08x}",
                    receipt.number, receipt.bytes, receipt.revision, receipt.crc32c,
                ),
            )?;
        }
        "upload" | "replan" | "finish" | "abort" => {
            let id = args.number("upload")?;
            args.end()?;
            match command {
                "upload" => render::upload(out, &cluster.upload(id)?)?,
                "replan" => render::upload(out, &cluster.replan_upload(id)?)?,
                "finish" => render::write_receipt(out, &cluster.complete_upload(id)?)?,
                _ => {
                    cluster.abort_upload(id)?;
                    render::line(out, format_args!("aborted={id}"))?;
                }
            }
        }
        "get" => {
            let key = args.text("key")?;
            let path = args.path("output")?;
            let flags = args.switches(&["--range"], &["--report"])?;
            let range = flags.value::<RangeRequest>("--range")?.unwrap_or_default();
            let report = if path == Path::new("-") {
                let mut stream = out;
                cluster.get(&key, range, &mut stream)?
            } else {
                cluster.get_to_file(&key, range, path)?
            };
            if flags.flag("--report") {
                render::read_report(err, &report)?;
            }
        }
        "stat" => {
            let key = args.text("key")?;
            args.end()?;
            render::object(out, &cluster.object(&key)?)?;
        }
        "list" => {
            args.end()?;
            for object in cluster.catalog()?.objects.values() {
                render::object(out, object)?;
            }
        }
        "delete" => {
            let key = args.text("key")?;
            let flags = args.switches(&["--if"], &[])?;
            let condition = flags.value("--if")?.unwrap_or(Condition::Any);
            if condition == Condition::Absent {
                return Err(Error::Invalid(
                    "delete requires 'any' or a generation".into(),
                ));
            }
            let retired = cluster.delete(&key, condition)?;
            render::line(
                out,
                format_args!("deleted={key:?} generation={retired} condition={condition}"),
            )?;
        }
        "inspect" => {
            let key = args.text("key")?;
            args.end()?;
            let report = cluster.inspect(&key)?;
            render::inspection(out, &report)?;
            if report.stripes.iter().any(|stripe| {
                matches!(
                    stripe.verdict,
                    paritybay::readpath::StripeVerdict::Unrecoverable
                        | paritybay::readpath::StripeVerdict::ParityMismatch
                )
            }) {
                return Ok(6);
            }
        }
        "fragment-path" => {
            let key = args.text("key")?;
            let stripe: usize = args.number("stripe")?;
            let shard: usize = args.number("shard")?;
            args.end()?;
            let object = cluster.object(&key)?;
            let stripe = object
                .stripes
                .get(stripe)
                .ok_or_else(|| Error::NotFound(format!("stripe {stripe}")))?;
            let receipt = stripe
                .fragments
                .get(shard)
                .ok_or_else(|| Error::Invalid(format!("shard {shard}")))?;
            let address = FragmentAddress::from_receipt(object.generation, stripe.index, receipt);
            let path = cluster.fragment_path(&address)?;
            render::line(out, format_args!("{}", path.display()))?;
        }
        _ => unreachable!("command table checked"),
    }
    Ok(0)
}

fn input(path: &Path, bytes: Option<u64>) -> Result<(Box<dyn Read>, u64)> {
    if path == Path::new("-") {
        let bytes = bytes.ok_or_else(|| Error::Invalid("stdin requires --size".into()))?;
        return Ok((Box::new(io::stdin().lock()), bytes));
    }
    let file = File::open(path).map_err(|e| io_error("open input", path, e))?;
    let meta = file
        .metadata()
        .map_err(|e| io_error("inspect input", path, e))?;
    let bytes = match bytes {
        Some(bytes) => bytes,
        None if meta.is_file() => meta.len(),
        None => return Err(Error::Invalid("input stream requires --size".into())),
    };
    Ok((Box::new(file), bytes))
}

fn upload_options(bytes: u64, flags: &Switches) -> Result<UploadOptions> {
    Ok(UploadOptions {
        bytes,
        media: flags.value("--media")?.unwrap_or(Media::Hot),
        domains: flags.value("--domains")?.unwrap_or(DomainPolicy::Strict),
        condition: flags.value("--if")?.unwrap_or(Condition::Any),
    })
}
