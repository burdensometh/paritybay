use std::io::{self, Read};

use crate::codec::{stripe_lengths, EncodedStripe, SHARDS};
use crate::files;
use crate::manifest::{
    Catalog, FragmentAddress, FragmentReceipt, ObjectGeneration, PlacementPlan, StripeManifest,
};
use crate::{Cluster, Error, Result};

use super::UploadOptions;

impl Cluster {
    pub(super) fn write_object(
        &self,
        catalog: &Catalog,
        key: &str,
        generation: u64,
        options: UploadOptions,
        placements: &[PlacementPlan],
        input: &mut impl Read,
    ) -> Result<ObjectGeneration> {
        let mut offset = 0;
        let mut stripes = Vec::with_capacity(placements.len());
        for (index, placement) in placements.iter().enumerate() {
            let lengths = stripe_lengths(options.bytes - offset, catalog.block_bytes);
            let encoded =
                EncodedStripe::read(input, lengths).map_err(|error| input_error(key, error))?;
            let stripe = self.write_stripe(
                generation,
                index as u32,
                offset,
                lengths,
                placement,
                encoded,
            )?;
            offset = stripe.end();
            stripes.push(stripe);
        }
        require_eof(input).map_err(|error| input_error(key, error))?;

        Ok(ObjectGeneration {
            key: key.into(),
            generation,
            size: options.bytes,
            media: options.media,
            domain_policy: options.domains,
            stripes,
        })
    }

    fn write_stripe(
        &self,
        generation: u64,
        index: u32,
        offset: u64,
        lengths: [u32; SHARDS],
        plan: &PlacementPlan,
        encoded: EncodedStripe,
    ) -> Result<StripeManifest> {
        let fragments = std::array::from_fn(|shard| FragmentReceipt {
            shard: shard as u8,
            node: plan.nodes[shard].clone(),
            revision: generation,
            bytes: lengths[shard],
            crc32c: encoded.checksums[shard],
        });
        for (receipt, bytes) in fragments.iter().zip(&encoded.shards) {
            self.write_fragment(generation, index, receipt, bytes)?;
        }
        Ok(StripeManifest {
            index,
            offset,
            data_bytes: [lengths[0], lengths[1]],
            fragments,
        })
    }

    pub(crate) fn write_fragment(
        &self,
        generation: u64,
        stripe: u32,
        receipt: &FragmentReceipt,
        bytes: &[u8],
    ) -> Result<()> {
        let address = FragmentAddress::from_receipt(generation, stripe, receipt);
        let path = self.fragment_path(&address)?;
        files::atomic_file(self, &path, bytes, "fragment")
    }
}

pub(super) fn require_eof(input: &mut impl Read) -> io::Result<()> {
    let mut extra = [0];
    loop {
        match input.read(&mut extra) {
            Ok(0) => return Ok(()),
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "input exceeds declared bytes",
                ))
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

pub(super) fn input_error(key: &str, error: io::Error) -> Error {
    match error.kind() {
        io::ErrorKind::UnexpectedEof | io::ErrorKind::InvalidInput => {
            Error::Invalid(format!("input for {key:?}: {error}"))
        }
        io::ErrorKind::InvalidData => Error::Corrupt(format!("input for {key:?}: {error}")),
        _ => Error::io("read input", std::path::Path::new(key), error),
    }
}
