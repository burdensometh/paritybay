use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::str::FromStr;
use std::vec::IntoIter;

use paritybay::{Error, Result};

pub(super) struct Arguments(std::iter::Peekable<IntoIter<OsString>>);

impl Arguments {
    pub(super) fn new(values: Vec<OsString>) -> Self {
        Self(values.into_iter().peekable())
    }

    pub(super) fn skip(&mut self) {
        self.0.next();
    }

    pub(super) fn peek_text(&mut self) -> Option<&str> {
        self.0.peek().and_then(|value| value.to_str())
    }

    fn raw(&mut self, label: &str) -> Result<OsString> {
        self.0
            .next()
            .ok_or_else(|| Error::Invalid(format!("missing {label}")))
    }

    pub(super) fn path(&mut self, label: &str) -> Result<PathBuf> {
        let path = self.raw(label)?;
        if path.is_empty() {
            return Err(Error::Invalid(format!("empty {label}")));
        }
        Ok(path.into())
    }

    pub(super) fn text(&mut self, label: &str) -> Result<String> {
        self.raw(label)?
            .into_string()
            .map_err(|_| Error::Invalid(format!("{label} must be UTF-8")))
    }

    pub(super) fn maybe_text(&mut self) -> Result<Option<String>> {
        if self.0.peek().is_none() {
            return Ok(None);
        }
        self.text("argument").map(Some)
    }

    pub(super) fn number<T: FromStr>(&mut self, label: &str) -> Result<T> {
        parse(&self.text(label)?, label)
    }

    pub(super) fn maybe_number<T: FromStr>(&mut self, label: &str) -> Result<Option<T>> {
        self.maybe_text()?
            .map(|value| parse(&value, label))
            .transpose()
    }

    pub(super) fn end(&mut self) -> Result<()> {
        match self.0.next() {
            Some(value) => Err(Error::Invalid(format!("unexpected argument {value:?}"))),
            None => Ok(()),
        }
    }

    pub(super) fn switches(
        mut self,
        values: &[&'static str],
        flags: &[&'static str],
    ) -> Result<Switches> {
        let mut found = BTreeMap::new();
        while let Some(option) = self.maybe_text()? {
            let setting = if values.contains(&option.as_str()) {
                self.text(&format!("value for {option}"))?
            } else if flags.contains(&option.as_str()) {
                String::new()
            } else {
                return Err(Error::Invalid(format!("unexpected option {option:?}")));
            };
            if found.insert(option.clone(), setting).is_some() {
                return Err(Error::Invalid(format!("repeated option {option}")));
            }
        }
        Ok(Switches(found))
    }
}

pub(super) struct Switches(BTreeMap<String, String>);

impl Switches {
    pub(super) fn flag(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }

    pub(super) fn value<T>(&self, key: &str) -> Result<Option<T>>
    where
        T: FromStr,
        T::Err: std::fmt::Display,
    {
        self.0
            .get(key)
            .map(|value| {
                value
                    .parse()
                    .map_err(|reason| Error::Invalid(format!("{key}: {reason}")))
            })
            .transpose()
    }
}

fn parse<T: FromStr>(text: &str, label: &str) -> Result<T> {
    text.parse()
        .map_err(|_| Error::Invalid(format!("invalid {label} {text:?}")))
}
