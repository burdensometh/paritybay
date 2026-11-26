//! Measures live heap growth during a streaming repair to prove the buffer
//! peak is bounded by the fixed window, not by the block size.
//!
//! The process-wide tracking allocator samples the high-water mark of the
//! *delta* in live heap bytes across the heal call, comparing a small block
//! against the maximum supported 4 MiB block:
//!   cargo test --test memory_peak -- --nocapture
#![allow(clippy::cast_precision_loss)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::fs;
use std::sync::atomic::{AtomicIsize, Ordering};

use paritybay::readpath::RangeRequest;
use paritybay::Config;

mod common;

use common::{config, get, path, put, Workspace};

static LIVE: AtomicIsize = AtomicIsize::new(0);
static BASELINE: AtomicIsize = AtomicIsize::new(0);
static PEAK_DELTA: AtomicIsize = AtomicIsize::new(0);
static MEASURING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

struct Tracking;

unsafe impl GlobalAlloc for Tracking {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = System.alloc(layout);
        if !pointer.is_null() {
            let now =
                LIVE.fetch_add(layout.size() as isize, Ordering::SeqCst) + layout.size() as isize;
            if MEASURING.load(Ordering::SeqCst) {
                let delta = now - BASELINE.load(Ordering::SeqCst);
                PEAK_DELTA.fetch_max(delta, Ordering::SeqCst);
            }
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as isize, Ordering::SeqCst);
        System.dealloc(pointer, layout);
    }
}

#[global_allocator]
static ALLOCATOR: Tracking = Tracking;

fn large_config(block: u32) -> Config {
    let mut config = config();
    config.block_bytes = block;
    config
}

/// Returns (peak live-byte delta during heal, block size).
fn repair_peak(block: u32) -> (usize, usize) {
    let dir = Workspace::new();
    let cluster = dir.create(large_config(block));
    let size = (2 * block as usize) + 37;
    let data: Vec<u8> = (0..size).map(|i| (i.wrapping_mul(73)) as u8).collect();
    put(&cluster, "big", &data);
    let stripes = cluster.object("big").unwrap().stripes.len();
    for stripe in 0..stripes {
        fs::remove_file(path(&cluster, "big", stripe, 0)).unwrap();
    }

    BASELINE.store(LIVE.load(Ordering::SeqCst), Ordering::SeqCst);
    PEAK_DELTA.store(0, Ordering::SeqCst);
    MEASURING.store(true, Ordering::SeqCst);
    cluster.heal(Some("big")).unwrap();
    MEASURING.store(false, Ordering::SeqCst);
    let peak = PEAK_DELTA.load(Ordering::SeqCst).max(0) as usize;

    assert_eq!(get(&cluster, "big", RangeRequest::Full).0, data);
    (peak, block as usize)
}

#[test]
fn repair_allocation_peak_stays_flat_as_block_grows() {
    // Warm up one-time catalog/lock/frame allocations.
    let _ = repair_peak(1024);

    let (small_peak, small_block) = repair_peak(16 * 1024);
    let (large_peak, large_block) = repair_peak(4 * 1024 * 1024);

    // The old whole-shard design held roughly four block-sized buffers at
    // once (two verified sources + rebuilt output + write copy), so its peak
    // scaled linearly with the block size.
    let old_large_peak_estimate = 4 * large_block;
    println!("block={small_block:>9}  live_peak_delta={small_peak:>9} bytes");
    println!(
        "block={large_block:>9}  live_peak_delta={large_peak:>9} bytes  \
         (whole-shard design would need ~{old_large_peak_estimate} bytes)"
    );

    // A 256x larger block must not produce a materially larger repair peak.
    // The slack covers catalog frames and per-call report structures, but
    // stays far below any linear-in-block growth.
    let slack = small_peak * 4 + 256 * 1024;
    assert!(
        large_peak <= slack,
        "peak grew with block size: {small_peak} -> {large_peak} bytes"
    );
    // And the streaming peak must be decisively below the whole-shard design.
    assert!(
        large_peak < old_large_peak_estimate / 8,
        "streaming peak {large_peak} is not well below whole-shard ~{old_large_peak_estimate}"
    );
}
