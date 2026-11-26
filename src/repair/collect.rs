use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::path::Path;

use crate::files;
use crate::manifest::FragmentAddress;
use crate::{Catalog, Cluster, Error, MigrationState, NodeState, RepairState, Result};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CollectionReport {
    pub deferred: bool,
    pub retired_entries: usize,
    pub fragment_files: usize,
    pub staging_files: usize,
    pub temporary_files: usize,
}

impl Cluster {
    pub fn collect(&self) -> Result<CollectionReport> {
        let Some(_pin) = self.try_pin_cleanup()? else {
            return Ok(CollectionReport {
                deferred: true,
                ..Default::default()
            });
        };
        let mut txn = self.transaction()?;
        let mut report = CollectionReport::default();
        let active = active_fragments(&txn.catalog);

        for entry in &txn.catalog.retired {
            let path = self.fragment_path(&entry.address)?;
            report.fragment_files += usize::from(files::remove_file(&path)?);
            // A prior collector may have crashed after unlink but before a directory sync.
            let dir = files::parent(&path)?;
            if dir
                .try_exists()
                .map_err(|error| Error::io("locate fragment directory", dir, error))?
            {
                files::sync_dir(dir)?;
            }
        }
        report.retired_entries = txn.catalog.retired.len();
        txn.catalog.retired.clear();

        for node in txn
            .catalog
            .nodes
            .values()
            .filter(|node| node.state != NodeState::Removed)
        {
            self.collect_node(&node.id, &active, &mut report)?;
        }
        self.collect_staging(&txn.catalog, &mut report)?;
        self.collect_catalog_drafts(&mut report)?;

        if report.retired_entries > 0 {
            txn.commit("collect-retired")?;
        }
        Ok(report)
    }

    fn collect_node(
        &self,
        id: &str,
        active: &BTreeSet<FragmentAddress>,
        report: &mut CollectionReport,
    ) -> Result<()> {
        let dir = self.node_dir(id);
        let entries = match files::entries(&dir) {
            Err(error) if error.is_not_found_io() => return Ok(()),
            result => result?,
        };
        let before = report.fragment_files + report.temporary_files;
        for entry in entries {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if is_draft(name) {
                report.temporary_files += usize::from(files::remove_file(&entry.path())?);
            } else if let Some(address) = FragmentAddress::from_filename(id, name) {
                if !active.contains(&address) {
                    report.fragment_files += usize::from(files::remove_file(&entry.path())?);
                }
            }
        }
        if before != report.fragment_files + report.temporary_files {
            files::sync_dir(&dir)?;
        }
        Ok(())
    }

    fn collect_staging(&self, catalog: &Catalog, report: &mut CollectionReport) -> Result<()> {
        let root = self.root.join("uploads");
        let mut dir_removed = false;
        for entry in files::entries(&root)? {
            let name = entry.file_name();
            let Some(id) = name.to_str().and_then(upload_id) else {
                continue;
            };
            let dir = entry.path();
            if !entry
                .file_type()
                .map_err(|e| Error::io("check upload directory", &dir, e))?
                .is_dir()
            {
                continue;
            }
            let upload = catalog.uploads.get(&id);
            let active: BTreeSet<_> = upload
                .into_iter()
                .flat_map(|upload| upload.parts.values())
                .map(|part| self.part_path(id, part.number, part.revision))
                .collect();

            let before = report.staging_files + report.temporary_files;
            for staged in files::entries(&dir)? {
                let path = staged.path();
                let name = staged.file_name();
                let Some(name) = name.to_str() else {
                    continue;
                };
                if is_draft(name) {
                    report.temporary_files += usize::from(files::remove_file(&path)?);
                } else if is_part(name) && !active.contains(&path) {
                    report.staging_files += usize::from(files::remove_file(&path)?);
                }
            }
            let removed = upload.is_none() && remove_empty(&dir)?;
            if removed {
                dir_removed = true;
            } else if before != report.staging_files + report.temporary_files {
                files::sync_dir(&dir)?;
            }
        }
        if dir_removed {
            files::sync_dir(&root)?;
        }
        Ok(())
    }

    fn collect_catalog_drafts(&self, report: &mut CollectionReport) -> Result<()> {
        let before = report.temporary_files;
        for entry in files::entries(&self.root)? {
            if entry.file_name().to_str().is_some_and(is_draft) {
                report.temporary_files += usize::from(files::remove_file(&entry.path())?);
            }
        }
        if before != report.temporary_files {
            files::sync_dir(&self.root)?;
        }
        Ok(())
    }
}

fn active_fragments(catalog: &Catalog) -> BTreeSet<FragmentAddress> {
    let mut active = BTreeSet::new();
    for object in catalog.objects.values() {
        for stripe in &object.stripes {
            for receipt in &stripe.fragments {
                active.insert(FragmentAddress::from_receipt(
                    object.generation,
                    stripe.index,
                    receipt,
                ));
            }
        }
    }
    for attempt in catalog
        .repairs
        .values()
        .filter(|attempt| attempt.state == RepairState::Planned)
    {
        active.insert(FragmentAddress::from_receipt(
            attempt.generation,
            attempt.stripe,
            &attempt.to,
        ));
    }
    for migration in catalog
        .migrations
        .values()
        .filter(|migration| migration.state == MigrationState::Planned)
    {
        for (stripe, targets) in migration.targets.iter().enumerate() {
            for target in targets {
                active.insert(FragmentAddress::from_receipt(
                    migration.generation,
                    stripe as u32,
                    target,
                ));
            }
        }
    }
    active
}

fn upload_id(name: &str) -> Option<u64> {
    let id = u64::from_str_radix(name, 16).ok()?;
    (id > 0 && format!("{id:016x}") == name).then_some(id)
}

fn is_part(name: &str) -> bool {
    let Some((part, revision)) = name
        .strip_suffix(".part")
        .and_then(|stem| stem.split_once('-'))
    else {
        return false;
    };
    part.len() == 8 && is_hex(part) && revision.len() == 16 && is_hex(revision)
}

fn is_hex(word: &str) -> bool {
    word.bytes()
        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn is_draft(name: &str) -> bool {
    let Some((pid, counter)) = name
        .strip_prefix(".pb-tmp-")
        .and_then(|tail| tail.split_once('-'))
    else {
        return false;
    };
    pid.parse::<u32>().is_ok() && counter.parse::<u64>().is_ok()
}

fn remove_empty(path: &Path) -> Result<bool> {
    match fs::remove_dir(path) {
        Ok(()) => Ok(true),
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DirectoryNotEmpty | ErrorKind::NotFound
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(Error::io("remove staging directory", path, error)),
    }
}
