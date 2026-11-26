//! v2 compact staging.
//!
//! Legacy sessions keep one loose `.part` file per revision under
//! `uploads/ID/`. New and compacted sessions store part bytes contiguously in
//! numbered `.pack` files and map each current revision to a verified extent
//! in the `staging.v2` sidecar. Loose and packed parts can coexist inside one
//! upload; reads always follow the current part receipts and never cross an
//! extent boundary.

pub mod compact;
pub mod frame;
mod paths;
mod store;

pub use compact::CompactionReport;
pub(crate) use compact::PACK_TARGET;
pub use frame::{StagedExtent, StagingIndex};
pub(crate) use paths::{is_index_name, pack_id, PartLocation};
pub(crate) use store::{upload_directories, StagingStore};
