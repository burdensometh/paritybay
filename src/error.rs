use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io {
        op: &'static str,
        path: PathBuf,
        source: io::Error,
    },
    Invalid(String),
    Corrupt(String),
    NotFound(String),
    Conflict(String),
    Capacity(String),
    Unrecoverable {
        generation: u64,
        stripe: u32,
        detail: String,
    },
}

impl Error {
    pub(crate) fn io(op: &'static str, path: &Path, source: io::Error) -> Self {
        Self::Io {
            op,
            path: path.to_owned(),
            source,
        }
    }

    pub(crate) fn is_not_found_io(&self) -> bool {
        matches!(self, Self::Io { source, .. } if source.kind() == io::ErrorKind::NotFound)
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::Io { .. } => "io",
            Self::Invalid(_) => "invalid",
            Self::Corrupt(_) => "corrupt",
            Self::NotFound(_) => "missing",
            Self::Conflict(_) => "conflict",
            Self::Capacity(_) => "capacity",
            Self::Unrecoverable { .. } => "unrecoverable",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { op, path, source } => write!(out, "{op} {}: {source}", path.display()),
            Self::Invalid(message)
            | Self::Corrupt(message)
            | Self::NotFound(message)
            | Self::Conflict(message)
            | Self::Capacity(message) => out.write_str(message),
            Self::Unrecoverable {
                generation,
                stripe,
                detail,
            } => {
                write!(out, "generation {generation}, stripe {stripe}: {detail}")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}
