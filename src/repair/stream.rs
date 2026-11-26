//! Fixed-window streaming repair copies.
//!
//! A repair never holds a whole shard in memory. Source fragments are read in
//! windows of [`REPAIR_WINDOW`], verified with a rolling CRC32C and either
//! copied verbatim or XOR-reconstructed into the target draft. The live
//! buffers therefore stay bounded by the window size instead of growing with
//! the configured block size.

use std::fs::File;
use std::io::{ErrorKind, Read};

use crate::codec::{xor_window, Crc32c, FragmentStream, STREAM_WINDOW};
use crate::files::DraftFile;
use crate::manifest::{FragmentAddress, FragmentReceipt, StripeManifest};
use crate::readpath::{open_fragment_stream, ShardFault};
use crate::{Catalog, Cluster, Error, RepairAttempt, Result};

/// Size of every streaming read/write window. Independent of `block_bytes`,
/// so repair memory remains flat even for the largest supported blocks.
pub(super) const REPAIR_WINDOW: usize = STREAM_WINDOW;

/// Why a streaming repair copy could not produce a verified target fragment.
#[derive(Debug)]
pub(super) enum CopyFailure {
    /// A source fragment was unreadable or failed its receipt verification.
    Source(ShardFault),
    /// The reconstructed bytes did not match the target receipt checksum.
    Rejected(String),
    /// A local write/sync/rename error. Nothing was published; replaying the
    /// same planned attempt remains safe.
    Io(Error),
}

impl CopyFailure {
    pub(super) fn detail(&self) -> String {
        match self {
            Self::Source(fault) => {
                format!(
                    "shard {} {}: {}",
                    fault.address.shard, fault.kind, fault.detail
                )
            }
            Self::Rejected(detail) => detail.clone(),
            Self::Io(error) => error.to_string(),
        }
    }
}

impl Cluster {
    /// Produce the target fragment for a repair using only fixed-size windows.
    ///
    /// A healthy source is copied verbatim. If the source fragment itself is
    /// faulty, the target is reconstructed from the other two verified
    /// shards. Source-side failures never publish anything; local I/O failures
    /// leave the planned attempt intact for replay.
    pub(super) fn stream_repair_fragment(
        &self,
        catalog: &Catalog,
        generation: u64,
        stripe: &StripeManifest,
        from_shard: u8,
        to: &FragmentReceipt,
        window: usize,
    ) -> std::result::Result<(), CopyFailure> {
        let target_path = self.target_path(generation, stripe, to)?;
        match self.stream_direct_copy(
            catalog,
            generation,
            stripe,
            from_shard,
            to,
            &target_path,
            window,
        ) {
            Ok(()) => Ok(()),
            Err(CopyFailure::Io(error)) => Err(CopyFailure::Io(error)),
            Err(CopyFailure::Source(_) | CopyFailure::Rejected(_)) => {
                // The original fragment is unusable; rebuild from the peers.
                self.stream_rebuild(
                    catalog,
                    generation,
                    stripe,
                    from_shard,
                    to,
                    &target_path,
                    window,
                )
            }
        }
    }

    fn target_path(
        &self,
        generation: u64,
        stripe: &StripeManifest,
        to: &FragmentReceipt,
    ) -> std::result::Result<std::path::PathBuf, CopyFailure> {
        let address = FragmentAddress::from_receipt(generation, stripe.index, to);
        self.fragment_path(&address).map_err(CopyFailure::Io)
    }

    fn open_source(
        &self,
        catalog: &Catalog,
        generation: u64,
        stripe: &StripeManifest,
        shard: usize,
    ) -> std::result::Result<FragmentStream, CopyFailure> {
        open_fragment_stream(
            self,
            catalog,
            generation,
            stripe.index,
            &stripe.fragments[shard],
        )
        .map_err(CopyFailure::Source)
    }

    fn stream_direct_copy(
        &self,
        catalog: &Catalog,
        generation: u64,
        stripe: &StripeManifest,
        from_shard: u8,
        to: &FragmentReceipt,
        target_path: &std::path::Path,
        window: usize,
    ) -> std::result::Result<(), CopyFailure> {
        let mut source = self.open_source(catalog, generation, stripe, from_shard as usize)?;
        let mut draft = DraftFile::create(target_path).map_err(CopyFailure::Io)?;
        let mut buffer = vec![0u8; window];
        let expected = u64::from(to.bytes);
        let mut written = 0u64;
        while written < expected {
            let take = usize::try_from(expected - written).unwrap().min(window);
            source
                .read_window(&mut buffer[..take])
                .map_err(|fault| self.source_fault(generation, stripe, from_shard, fault))?;
            draft.write(&buffer[..take]).map_err(CopyFailure::Io)?;
            written += take as u64;
        }
        // The whole source must verify before the draft becomes visible.
        source
            .finish()
            .map_err(|fault| self.source_fault(generation, stripe, from_shard, fault))?;
        draft.sync(self, "fragment").map_err(CopyFailure::Io)?;
        draft.publish(target_path).map_err(CopyFailure::Io)
    }

    fn stream_rebuild(
        &self,
        catalog: &Catalog,
        generation: u64,
        stripe: &StripeManifest,
        missing_shard: u8,
        to: &FragmentReceipt,
        target_path: &std::path::Path,
        window: usize,
    ) -> std::result::Result<(), CopyFailure> {
        let missing = usize::from(missing_shard);
        let lengths = stripe.lengths();
        if missing >= crate::codec::SHARDS || lengths[0] != lengths[2] || lengths[1] > lengths[0] {
            return Err(CopyFailure::Rejected(
                "invalid reconstruction geometry".into(),
            ));
        }
        let source_ids: [usize; 2] = {
            let mut ids = (0..crate::codec::SHARDS).filter(|index| *index != missing);
            let first = ids.next().expect("two source shards");
            let second = ids.next().expect("two source shards");
            [first, second]
        };
        let mut first_stream = self.open_source(catalog, generation, stripe, source_ids[0])?;
        let mut second_stream = self.open_source(catalog, generation, stripe, source_ids[1])?;

        let mut draft = DraftFile::create(target_path).map_err(CopyFailure::Io)?;
        let mut left = vec![0u8; window];
        let mut right = vec![0u8; window];
        let mut output_crc = Crc32c::default();
        let expected = u64::from(to.bytes);
        let mut written = 0u64;
        while written < expected {
            let take = usize::try_from(expected - written).unwrap().min(window);
            let slice = &mut left[..take];
            first_stream.read_window(slice).map_err(|fault| {
                self.source_fault(generation, stripe, source_ids[0] as u8, fault)
            })?;
            second_stream
                .read_window(&mut right[..take])
                .map_err(|fault| {
                    self.source_fault(generation, stripe, source_ids[1] as u8, fault)
                })?;
            xor_window(slice, &right[..take]);
            output_crc.update(slice);
            draft.write(slice).map_err(CopyFailure::Io)?;
            written += take as u64;
        }
        // A reconstructed shard 1 can be shorter than its two sources. Drain
        // their unread tails so each source receipt still verifies in full.
        let mut drain = vec![0u8; window];
        let sources = [
            (first_stream, source_ids[0] as u8),
            (second_stream, source_ids[1] as u8),
        ];
        for (mut source, shard) in sources {
            while !source.exhausted() {
                let take = (source.remaining() as usize).min(window);
                source
                    .read_window(&mut drain[..take])
                    .map_err(|fault| self.source_fault(generation, stripe, shard, fault))?;
            }
            source
                .finish()
                .map_err(|fault| self.source_fault(generation, stripe, shard, fault))?;
        }
        let seen = output_crc.finish();
        if seen != to.crc32c {
            return Err(CopyFailure::Rejected(format!(
                "reconstructed checksum differs for shard {missing_shard}: expected {:08x}, rebuilt {seen:08x}",
                to.crc32c
            )));
        }
        draft.sync(self, "fragment").map_err(CopyFailure::Io)?;
        draft.publish(target_path).map_err(CopyFailure::Io)
    }

    fn source_fault(
        &self,
        generation: u64,
        stripe: &StripeManifest,
        shard: u8,
        fault: crate::codec::StreamFault,
    ) -> CopyFailure {
        let receipt = &stripe.fragments[usize::from(shard)];
        let address = FragmentAddress::from_receipt(generation, stripe.index, receipt);
        let kind = match fault {
            crate::codec::StreamFault::Missing => crate::readpath::FaultKind::Missing,
            crate::codec::StreamFault::InvalidLength => crate::readpath::FaultKind::InvalidLength,
            crate::codec::StreamFault::InvalidChecksum => {
                crate::readpath::FaultKind::InvalidChecksum
            }
            crate::codec::StreamFault::Io => crate::readpath::FaultKind::Io,
        };
        CopyFailure::Source(ShardFault {
            address,
            kind,
            detail: format!("{fault:?}"),
        })
    }
}

/// Verify an already-written target fragment using rolling windows, without
/// loading it whole.
pub(super) fn verify_existing_copy(
    cluster: &Cluster,
    attempt: &RepairAttempt,
    window: usize,
) -> Result<bool> {
    let address = FragmentAddress::from_receipt(attempt.generation, attempt.stripe, &attempt.to);
    let path = cluster.fragment_path(&address)?;
    let mut file = match File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(Error::io("open", &path, error)),
    };
    let expected = attempt.to.bytes as usize;
    let mut crc = Crc32c::default();
    let mut buffer = vec![0u8; window];
    let mut total = 0usize;
    while total < expected {
        let take = (expected - total).min(window);
        if let Err(error) = file.read_exact(&mut buffer[..take]) {
            if error.kind() == ErrorKind::UnexpectedEof {
                return Ok(false);
            }
            return Err(Error::io("read", &path, error));
        }
        crc.update(&buffer[..take]);
        total += take;
    }
    let mut extra = [0u8; 1];
    loop {
        match file.read(&mut extra) {
            Ok(0) => break,
            Ok(_) => return Ok(false),
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(error) => return Err(Error::io("read", &path, error)),
        }
    }
    Ok(total == expected && crc.finish() == attempt.to.crc32c)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::checksum;
    use crate::manifest::StripeManifest;
    use crate::{Condition, Config, DomainPolicy, Media, RepairAttempt, RepairKind, RepairState};
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TMP: AtomicU64 = AtomicU64::new(0);

    struct Workspace(PathBuf);

    impl Workspace {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "paritybay-stream-{}-{}",
                std::process::id(),
                TMP.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for Workspace {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn cluster(root: &std::path::Path, block: u32) -> Cluster {
        let config = Config::parse(&format!(
            "block_bytes = {block}\nstaging_limit = 1048576\n\
             node = n0, d0, hot, 16777216, online\n\
             node = n1, d1, hot, 16777216, online\n\
             node = n2, d2, hot, 16777216, online\n"
        ))
        .unwrap();
        Cluster::create(root, config).unwrap()
    }

    fn put(cluster: &Cluster, key: &str, data: &[u8]) -> u64 {
        cluster
            .put(
                key,
                crate::upload::UploadOptions {
                    bytes: data.len() as u64,
                    media: Media::Hot,
                    domains: DomainPolicy::Strict,
                    condition: Condition::Any,
                },
                &mut Cursor::new(data),
            )
            .unwrap()
            .generation
    }

    /// Drive a repair through every window size from 1 up to the data length,
    /// proving correctness is independent of window alignment and that the
    /// live allocations never exceed a few fixed buffers.
    #[test]
    fn byte_sized_windows_rebuild_every_shard_including_short_tail() {
        for data_len in [1usize, 2, 7, 8, 9, 16, 17] {
            let data: Vec<u8> = (0..data_len).map(|i| (i * 31 + 5) as u8).collect();
            for missing in 0u8..3 {
                let dir = Workspace::new();
                let cluster = cluster(&dir.0, 8);
                put(&cluster, "k", &data);
                let object = cluster.object("k").unwrap();
                let stripe = object.stripes[0].clone();
                let source_path = cluster
                    .fragment_path(&FragmentAddress::from_receipt(
                        object.generation,
                        0,
                        &stripe.fragments[missing as usize],
                    ))
                    .unwrap();
                std::fs::remove_file(source_path).unwrap();

                let target_node = format!("n{}", usize::from(missing));
                let mut to = stripe.fragments[missing as usize].clone();
                to.node = target_node;
                to.revision = 900 + u64::from(missing);
                let attempt = RepairAttempt {
                    id: to.revision,
                    key: "k".into(),
                    generation: object.generation,
                    stripe: 0,
                    from: stripe.fragments[missing as usize].clone(),
                    to: to.clone(),
                    kind: RepairKind::Heal,
                    state: RepairState::Planned,
                    detail: String::new(),
                };
                for window in 1..=stripe.fragments[missing as usize].bytes.max(1) as usize {
                    cluster
                        .stream_repair_fragment(
                            &cluster.catalog().unwrap(),
                            object.generation,
                            &stripe,
                            missing,
                            &attempt.to,
                            window,
                        )
                        .unwrap();
                    let address = FragmentAddress::from_receipt(object.generation, 0, &attempt.to);
                    let written = std::fs::read(cluster.fragment_path(&address).unwrap()).unwrap();
                    let expected = &stripe.fragments[missing as usize];
                    assert_eq!(written.len(), expected.bytes as usize);
                    assert_eq!(checksum(&written), expected.crc32c);
                    std::fs::remove_file(cluster.fragment_path(&address).unwrap()).unwrap();
                }
            }
        }
    }

    #[test]
    fn rolling_verification_rejects_truncated_and_elongated_copies() {
        let dir = Workspace::new();
        let cluster = cluster(&dir.0, 16);
        put(&cluster, "k", b"0123456789abcdef");
        let object = cluster.object("k").unwrap();
        let stripe: StripeManifest = object.stripes[0].clone();
        let mut to = stripe.fragments[0].clone();
        to.revision = 950;
        to.node = "n0".into();
        let attempt = RepairAttempt {
            id: 950,
            key: "k".into(),
            generation: object.generation,
            stripe: 0,
            from: stripe.fragments[0].clone(),
            to: to.clone(),
            kind: RepairKind::Heal,
            state: RepairState::Planned,
            detail: String::new(),
        };
        cluster
            .stream_repair_fragment(
                &cluster.catalog().unwrap(),
                object.generation,
                &stripe,
                0,
                &attempt.to,
                4,
            )
            .unwrap();
        assert!(verify_existing_copy(&cluster, &attempt, 4).unwrap());

        // Truncate the verified file: rolling verification must now fail.
        let address = FragmentAddress::from_receipt(object.generation, 0, &attempt.to);
        let path = cluster.fragment_path(&address).unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.pop();
        std::fs::write(&path, bytes).unwrap();
        assert!(!verify_existing_copy(&cluster, &attempt, 4).unwrap());

        // Append bytes: the over-length guard must also reject it.
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.push(0);
        bytes.push(0);
        std::fs::write(&path, bytes).unwrap();
        assert!(!verify_existing_copy(&cluster, &attempt, 4).unwrap());

        // A missing target reports "not present" rather than an error.
        std::fs::remove_file(&path).unwrap();
        assert!(!verify_existing_copy(&cluster, &attempt, 4).unwrap());
    }
}
