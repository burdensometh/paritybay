use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;

use crate::codec::Crc32c;
use crate::files::{self, DraftFile};
use crate::manifest::{sequence, PartReceipt, MAX_PARTS, MAX_PART_BYTES};
use crate::staging::{PartLocation, StagingStore};
use crate::{Cluster, Error, Result, UploadSession};

use super::publish::{input_error, require_eof};
use super::sessions::missing_upload;

const PART_BUFFER: usize = 64 * 1024;

impl Cluster {
    pub fn put_part(
        &self,
        id: u64,
        number: u32,
        bytes: u32,
        input: &mut impl Read,
    ) -> Result<PartReceipt> {
        if number == 0 || number as usize > MAX_PARTS || bytes == 0 || bytes > MAX_PART_BYTES {
            return Err(Error::Invalid(format!(
                "part number must be 1–{MAX_PARTS} and part size must be 1–{MAX_PART_BYTES} bytes"
            )));
        }
        let mut txn = self.transaction()?;
        let upload = txn
            .catalog
            .uploads
            .get(&id)
            .ok_or_else(|| missing_upload(id))?;
        let replaced = upload
            .parts
            .get(&number)
            .map_or(0, |part| u64::from(part.bytes));
        if upload.staged_bytes() - replaced + u64::from(bytes) > upload.expected_bytes {
            return Err(Error::Conflict(format!(
                "part exceeds declared object size for upload {id}"
            )));
        }

        let superseded = txn.catalog.uploads[&id]
            .parts
            .get(&number)
            .map(|part| part.revision);
        let key = upload.key.clone();
        let revision = sequence(&mut txn.catalog)?;
        let receipt = self.stage_part(&id, number, revision, bytes, input, &key, superseded)?;
        require_eof(input).map_err(|error| input_error(&key, error))?;

        let upload = txn
            .catalog
            .uploads
            .get_mut(&id)
            .expect("upload observed under writer lock");
        upload.parts.insert(number, receipt.clone());
        txn.commit("upload-part")?;
        Ok(receipt)
    }

    /// Stage one part's bytes. Uploads that already use v2 packs keep using
    /// them; a pre-upgrade upload with only loose parts stays loose until an
    /// explicit compaction, so existing sessions and their old parts behave
    /// exactly as before.
    fn stage_part(
        &self,
        id: &u64,
        number: u32,
        revision: u64,
        bytes: u32,
        input: &mut impl Read,
        key: &str,
        superseded: Option<u64>,
    ) -> Result<PartReceipt> {
        let packed = self.load_index(*id)?.is_some();
        if packed {
            self.stage_packed_part(*id, number, revision, bytes, input, key, superseded)
        } else {
            self.stage_loose_part(*id, number, revision, bytes, input, key)
        }
    }

    fn stage_loose_part(
        &self,
        id: u64,
        number: u32,
        revision: u64,
        bytes: u32,
        input: &mut impl Read,
        key: &str,
    ) -> Result<PartReceipt> {
        let path = self.part_path(id, number, revision);
        let mut file = DraftFile::create(&path)?;
        let mut crc = Crc32c::default();
        self.stream_part(bytes, input, key, &mut crc, |block| file.write(block))?;
        file.sync(self, "upload-part")?;
        file.publish(&path)?;
        Ok(PartReceipt {
            number,
            revision,
            bytes,
            crc32c: crc.finish(),
        })
    }

    fn stage_packed_part(
        &self,
        id: u64,
        number: u32,
        revision: u64,
        bytes: u32,
        input: &mut impl Read,
        key: &str,
        superseded: Option<u64>,
    ) -> Result<PartReceipt> {
        let mut store = StagingStore::open(self, id);
        let mut buffer = Vec::with_capacity(bytes as usize);
        let mut crc = Crc32c::default();
        self.stream_part(bytes, input, key, &mut crc, |block| {
            buffer.extend_from_slice(block);
            Ok(())
        })?;
        let receipt = PartReceipt {
            number,
            revision,
            bytes,
            crc32c: crc.finish(),
        };
        // The pack append and sidecar publication (including pruning the
        // superseded revision) are crash-safe inside the store.
        store.append_part(&receipt, &buffer, superseded)?;
        Ok(receipt)
    }

    fn stream_part(
        &self,
        bytes: u32,
        input: &mut impl Read,
        key: &str,
        crc: &mut Crc32c,
        mut write: impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<()> {
        let _ = self;
        let mut remaining = bytes as usize;
        let mut buf = vec![0; remaining.min(PART_BUFFER)];
        while remaining > 0 {
            let read = remaining.min(buf.len());
            let block = &mut buf[..read];
            input
                .read_exact(block)
                .map_err(|error| input_error(key, error))?;
            crc.update(block);
            write(block)?;
            remaining -= read;
        }
        Ok(())
    }

    pub(crate) fn upload_dir(&self, id: u64) -> PathBuf {
        self.root.join("uploads").join(format!("{id:016x}"))
    }

    pub(super) fn initialize_upload_dir(&self, id: u64) -> Result<()> {
        let dir = self.upload_dir(id);
        files::make_dir(&dir)?;
        files::sync_dir(files::parent(&dir)?)
    }

    pub(crate) fn part_path(&self, id: u64, number: u32, revision: u64) -> PathBuf {
        self.upload_dir(id)
            .join(format!("{number:08x}-{revision:016x}.part"))
    }

    pub(super) fn part_stream<'a>(&'a self, upload: &'a UploadSession) -> PartStream<'a> {
        PartStream {
            cluster: self,
            id: upload.id,
            next: upload.parts.values(),
            store: StagingStore::open(self, upload.id),
            current: None,
        }
    }
}

pub(super) struct PartStream<'a> {
    cluster: &'a Cluster,
    id: u64,
    next: std::collections::btree_map::Values<'a, u32, PartReceipt>,
    store: StagingStore<'a>,
    current: Option<PartReader<'a>>,
}

enum PartSource {
    Loose(File),
    Packed(std::io::Cursor<Vec<u8>>),
}

struct PartReader<'a> {
    receipt: &'a PartReceipt,
    source: PartSource,
    remaining: usize,
    crc: Crc32c,
}

impl Read for PartStream<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        if self.current.is_none() {
            let Some(part) = self.next.next() else {
                return Ok(0);
            };
            let location = self
                .store
                .locate(part)
                .map_err(|e| invalid_part(part.number, e))?;
            let source = match location {
                PartLocation::Loose => {
                    let path = self.cluster.part_path(self.id, part.number, part.revision);
                    let file = File::open(&path).map_err(|e| invalid_part(part.number, e))?;
                    let length = file
                        .metadata()
                        .map_err(|e| invalid_part(part.number, e))?
                        .len();
                    if length != u64::from(part.bytes) {
                        return Err(invalid_part(part.number, "length mismatch"));
                    }
                    PartSource::Loose(file)
                }
                PartLocation::Packed(extent) => {
                    let bytes = self
                        .cluster
                        .read_packed(self.id, part, extent)
                        .map_err(|e| invalid_part(part.number, e))?;
                    PartSource::Packed(std::io::Cursor::new(bytes))
                }
            };
            self.current = Some(PartReader {
                receipt: part,
                source,
                remaining: part.bytes as usize,
                crc: Crc32c::default(),
            });
        }
        let part = self.current.as_mut().expect("readable part");
        let count = out.len().min(part.remaining);
        let n = match &mut part.source {
            PartSource::Loose(file) => file.read(&mut out[..count]),
            PartSource::Packed(cursor) => cursor.read(&mut out[..count]),
        }?;
        if n == 0 {
            return Err(invalid_part(part.receipt.number, "unexpected end of part"));
        }
        part.remaining -= n;
        part.crc.update(&out[..n]);
        if part.remaining == 0 {
            if let PartSource::Loose(file) = &mut part.source {
                require_eof(file).map_err(|e| invalid_part(part.receipt.number, e))?;
            }
            if part.crc.finish() != part.receipt.crc32c {
                return Err(invalid_part(part.receipt.number, "checksum mismatch"));
            }
            self.current = None;
        }
        Ok(n)
    }
}

fn invalid_part(number: u32, problem: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("staged part {number}: {problem}"),
    )
}
