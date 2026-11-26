mod arguments;
mod maintenance;
mod objects;
mod render;

use std::ffi::OsString;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use paritybay::{Cluster, Config, Error, FileGates, Result};

use self::arguments::Arguments;

const HELP: &str = "ParityBay
Usage: paritybay [--root DIR] [--gates DIR] [--gate-timeout-ms N] COMMAND

  init CONFIG
  put KEY INPUT [--size N] [--media hot|capacity] [--domains strict|prefer] [--if any|absent|GEN]
  begin KEY SIZE [--media hot|capacity] [--domains strict|prefer] [--if any|absent|GEN]
  part UPLOAD PART INPUT [--size N]
  upload UPLOAD   | replan UPLOAD | finish UPLOAD | abort UPLOAD
  get KEY OUTPUT [--range START:END|START:|:SIZE] [--report]
  stat KEY        | list          | delete KEY [--if any|GEN]
  inspect KEY     | scan [KEY]    | fragment-path KEY STRIPE SHARD
  nodes           | node-add 'ID,DOMAIN,MEDIA,CAPACITY,STATE'
  node-state NODE online|offline|draining|removed
  node-size NODE CAPACITY         | node-media NODE hot|capacity
  heal [KEY]      | rebalance [KEY] | replay [ATTEMPT]
  repair-log [ATTEMPT]              | repair-prune THROUGH_ID
  gc
  v1-check FILE   | v1-export OUTPUT
  v2-check FILE   | v2-migrate
  help            | version

INPUT or OUTPUT may be '-' for stdin or stdout. stdin requires --size.
";

pub(super) fn run<I>(input: I, out: &mut dyn Write, err: &mut dyn Write) -> Result<u8>
where
    I: Iterator<Item = OsString>,
{
    let mut args = Arguments::new(input.collect());
    let global = Global::parse(&mut args)?;
    let command = args.maybe_text()?.unwrap_or_else(|| "help".into());

    match command.as_str() {
        "help" | "-h" | "--help" => {
            args.end()?;
            render::line(out, format_args!("{HELP}"))?;
            return Ok(0);
        }
        "version" | "--version" => {
            args.end()?;
            render::line(out, format_args!("paritybay {}", env!("CARGO_PKG_VERSION")))?;
            return Ok(0);
        }
        "v1-check" => {
            let path = args.path("v1 catalog")?;
            args.end()?;
            return maintenance::check_v1(&path, out);
        }
        "v2-check" => {
            let path = args.path("v2 catalog")?;
            args.end()?;
            return maintenance::check_v2(&path, out);
        }
        "init" => {
            let path = args.path("configuration")?;
            args.end()?;
            let config = Config::read(path)?;
            let cluster = match &global.gates {
                Some(dir) => Cluster::create_with_checkpoints(
                    &global.root,
                    config,
                    FileGates::new(dir, global.gate_timeout),
                )?,
                None => Cluster::create(&global.root, config)?,
            };
            render::line(
                out,
                format_args!("initialized={}", cluster.catalog_path().display()),
            )?;
            return Ok(0);
        }
        _ => {}
    }
    if !objects::recognizes(&command) && !maintenance::recognizes(&command) {
        return Err(Error::Invalid(format!(
            "unknown command {command:?}; see paritybay help"
        )));
    }
    let cluster = global.connect()?;
    if objects::recognizes(&command) {
        return objects::run(&cluster, &command, args, out, err);
    }
    maintenance::run(&cluster, &command, args, out)
}

struct Global {
    root: PathBuf,
    gates: Option<PathBuf>,
    gate_timeout: Duration,
}

impl Global {
    fn parse(args: &mut Arguments) -> Result<Self> {
        let mut root = None;
        let mut gates = None;
        let mut timeout = None;
        while let Some(option) = args.peek_text() {
            match option {
                "--root" if root.is_none() => {
                    args.skip();
                    root = Some(args.path("cluster root")?);
                }
                "--gates" if gates.is_none() => {
                    args.skip();
                    gates = Some(args.path("gate directory")?);
                }
                "--gate-timeout-ms" if timeout.is_none() => {
                    args.skip();
                    timeout = Some(args.number("gate timeout milliseconds")?);
                }
                "--root" | "--gates" | "--gate-timeout-ms" => {
                    return Err(Error::Invalid(format!("repeated global option {option}")));
                }
                _ => break,
            }
        }
        if timeout.is_some() && gates.is_none() {
            return Err(Error::Invalid("gate timeout requires --gates".into()));
        }
        Ok(Self {
            root: root.unwrap_or_else(|| {
                std::env::var_os("PARITYBAY_ROOT")
                    .map_or_else(|| PathBuf::from("var/paritybay"), PathBuf::from)
            }),
            gates,
            gate_timeout: Duration::from_millis(timeout.unwrap_or(30_000)),
        })
    }

    fn connect(&self) -> Result<Cluster> {
        let cluster = Cluster::open(&self.root)?;
        Ok(match &self.gates {
            Some(dir) => cluster.with_checkpoints(FileGates::new(dir, self.gate_timeout)),
            None => cluster,
        })
    }
}

fn io_error(op: &'static str, path: &Path, source: std::io::Error) -> Error {
    Error::Io {
        op,
        path: path.to_owned(),
        source,
    }
}
