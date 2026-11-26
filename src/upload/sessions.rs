use std::io::Read;

use crate::manifest::{check_condition, checked_key, sequence};
use crate::placement::{DomainHealth, PlacementPlanner};
use crate::{
    Catalog, Cluster, Condition, DomainPolicy, Error, Media, ObjectGeneration, Result,
    UploadSession,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UploadOptions {
    pub bytes: u64,
    pub media: Media,
    pub domains: DomainPolicy,
    pub condition: Condition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteReceipt {
    pub key: String,
    pub generation: u64,
    pub bytes: u64,
    pub stripes: usize,
    pub condition: Condition,
    pub replaced_generation: Option<u64>,
    pub weaker_stripes: usize,
}

impl Cluster {
    pub fn begin_upload(&self, key: &str, options: UploadOptions) -> Result<UploadSession> {
        checked_key(key)?;
        let mut txn = self.transaction()?;
        check_condition(key, options.condition, txn.catalog.objects.get(key))?;
        check_staging(&txn.catalog, options.bytes)?;
        let placements = PlacementPlanner::new(&txn.catalog, None)?.plan(
            key,
            options.bytes,
            options.media,
            options.domains,
        )?;
        let id = sequence(&mut txn.catalog)?;
        let upload = UploadSession {
            id,
            key: key.to_owned(),
            expected_bytes: options.bytes,
            media: options.media,
            domain_policy: options.domains,
            condition: options.condition,
            parts: Default::default(),
            placements,
        };
        self.initialize_upload_dir(id)?;
        txn.catalog.uploads.insert(id, upload.clone());
        txn.commit("upload-begin")?;
        Ok(upload)
    }

    pub fn upload(&self, id: u64) -> Result<UploadSession> {
        self.catalog()?
            .uploads
            .remove(&id)
            .ok_or_else(|| missing_upload(id))
    }

    pub fn replan_upload(&self, id: u64) -> Result<UploadSession> {
        let mut txn = self.transaction()?;
        let upload = txn
            .catalog
            .uploads
            .get(&id)
            .ok_or_else(|| missing_upload(id))?;
        let plans = PlacementPlanner::new(&txn.catalog, Some(id))?.plan(
            &upload.key,
            upload.expected_bytes,
            upload.media,
            upload.domain_policy,
        )?;
        let upload = txn
            .catalog
            .uploads
            .get_mut(&id)
            .expect("upload observed under writer lock");
        upload.placements = plans;
        let result = upload.clone();
        txn.commit("upload-replan")?;
        Ok(result)
    }

    pub fn abort_upload(&self, id: u64) -> Result<()> {
        let mut txn = self.transaction()?;
        if txn.catalog.uploads.remove(&id).is_none() {
            return Err(missing_upload(id));
        }
        txn.commit("upload-abort")
    }

    pub fn put(
        &self,
        key: &str,
        options: UploadOptions,
        input: &mut impl Read,
    ) -> Result<WriteReceipt> {
        checked_key(key)?;
        let mut txn = self.transaction()?;
        check_condition(key, options.condition, txn.catalog.objects.get(key))?;
        let placements = PlacementPlanner::new(&txn.catalog, None)?.plan(
            key,
            options.bytes,
            options.media,
            options.domains,
        )?;
        let generation = sequence(&mut txn.catalog)?;
        let object =
            self.write_object(&txn.catalog, key, generation, options, &placements, input)?;
        let receipt = publish(&mut txn.catalog, object, options.condition);
        txn.commit("put")?;
        Ok(receipt)
    }

    pub fn complete_upload(&self, id: u64) -> Result<WriteReceipt> {
        // Hold the cleanup pin for the whole assembly so a concurrent gc cannot
        // unlink a loose part or pack while the object is being published.
        let _pin = self.pin_fragments()?;
        let mut txn = self.transaction()?;
        let upload = txn
            .catalog
            .uploads
            .get(&id)
            .cloned()
            .ok_or_else(|| missing_upload(id))?;
        check_condition(
            &upload.key,
            upload.condition,
            txn.catalog.objects.get(&upload.key),
        )?;
        if upload.staged_bytes() != upload.expected_bytes {
            return Err(Error::Conflict(format!(
                "upload {id} has {} staged bytes, expected {}",
                upload.staged_bytes(),
                upload.expected_bytes
            )));
        }
        for (offset, number) in upload.parts.keys().enumerate() {
            if *number as usize != offset + 1 {
                return Err(Error::Conflict(format!(
                    "upload {id} has a gap before part {number}"
                )));
            }
        }
        for plan in &upload.placements {
            for node in &plan.nodes {
                if !txn.catalog.nodes[node].state.can_place() {
                    return Err(Error::Conflict(format!(
                        "upload {id} needs replan; node {node} is unavailable"
                    )));
                }
            }
        }

        let mut stream = self.part_stream(&upload);
        let options = UploadOptions {
            bytes: upload.expected_bytes,
            media: upload.media,
            domains: upload.domain_policy,
            condition: upload.condition,
        };
        // The session number is already durable and reserved exclusively for this object.
        let object = self.write_object(
            &txn.catalog,
            &upload.key,
            id,
            options,
            &upload.placements,
            &mut stream,
        )?;
        txn.catalog.uploads.remove(&id);
        let receipt = publish(&mut txn.catalog, object, options.condition);
        txn.commit("upload-complete")?;
        Ok(receipt)
    }
}

pub(super) fn missing_upload(id: u64) -> Error {
    Error::NotFound(format!("upload {id}"))
}

fn check_staging(catalog: &Catalog, requested: u64) -> Result<()> {
    let reserved: u64 = catalog
        .uploads
        .values()
        .map(|upload| upload.expected_bytes)
        .sum();
    if requested > catalog.staging_limit - reserved {
        return Err(Error::Capacity(format!(
            "staging has {} unreserved bytes",
            catalog.staging_limit - reserved
        )));
    }
    Ok(())
}

fn publish(catalog: &mut Catalog, object: ObjectGeneration, condition: Condition) -> WriteReceipt {
    let prior = catalog.objects.remove(&object.key);
    if let Some(old) = &prior {
        catalog.retire_object(old);
    }
    let weaker_stripes = object
        .stripes
        .iter()
        .filter(|stripe| {
            !DomainHealth::for_stripe(catalog, stripe).survives_any_single_domain_loss()
        })
        .count();

    let receipt = WriteReceipt {
        key: object.key.clone(),
        generation: object.generation,
        bytes: object.size,
        stripes: object.stripes.len(),
        condition,
        replaced_generation: prior.map(|old| old.generation),
        weaker_stripes,
    };
    catalog.objects.insert(object.key.clone(), object);
    receipt
}
