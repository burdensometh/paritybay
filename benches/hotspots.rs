use std::hint::black_box;
use std::io::{self, Cursor};
use std::time::{Duration, Instant};

use paritybay::manifest::FragmentAddress;
use paritybay::placement::PlacementPlanner;
use paritybay::readpath::RangeRequest;
use paritybay::upload::UploadOptions;
use paritybay::{Cluster, Condition, Config, DomainPolicy, Media, NodeDomain, NodeState};

fn timed(mut operation: impl FnMut(usize), runs: usize) -> Duration {
    let start = Instant::now();
    for i in 0..runs {
        operation(i);
    }
    start.elapsed()
}

fn print(name: &str, elapsed: Duration, runs: usize, per_run: usize, unit: &str) {
    let ns = elapsed.as_nanos() as f64;
    println!(
        "benchmark={name} samples={runs} per_sample={per_run} unit={unit} ns_per_sample={:.0} units_per_second={:.0}",
        ns / runs as f64, per_run as f64 * runs as f64 / elapsed.as_secs_f64(),
    );
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::temp_dir().join(format!("paritybay-hotspots-{}", std::process::id()));
    std::fs::create_dir(&root)?;
    let _clean = Workspace(root.clone());
    let block = 32 * 1024;
    let nodes = (0..24)
        .map(|i| NodeDomain {
            id: format!("h{i:02}"),
            domain: format!("d{}", i % 6),
            media: Media::Hot,
            capacity: 64 * 1024 * 1024,
            state: NodeState::Online,
        })
        .collect();
    let cluster = Cluster::create(
        root,
        Config {
            block_bytes: block,
            staging_limit: 16 * 1024 * 1024,
            checksum_algorithm: paritybay::ChecksumAlgorithm::Crc32c,
            fragment_checksum: paritybay::ChecksumAlgorithm::Crc32c,
            checksum: paritybay::ChecksumAlgorithm::Crc32c,
            checksum_kind: paritybay::ChecksumAlgorithm::Crc32c,
            checksum_type: paritybay::ChecksumAlgorithm::Crc32c,
            digest_algorithm: paritybay::ChecksumAlgorithm::Crc32c,
            default_checksum: paritybay::ChecksumAlgorithm::Crc32c,
            nodes,
        },
    )?;
    let input: Vec<u8> = (0..4 * 1024 * 1024_usize)
        .map(|i| i.wrapping_mul(31) as u8)
        .collect();
    let write = cluster.put(
        "bench",
        UploadOptions {
            bytes: input.len() as u64,
            media: Media::Hot,
            domains: DomainPolicy::Strict,
            condition: Condition::Absent,
        },
        &mut Cursor::new(&input),
    )?;

    let pin = cluster.pin_object("bench")?;
    let runs = 800;
    let elapsed = timed(
        |i| {
            let start = (i * 19_337 % (input.len() - 4096)) as u64;
            let report = pin
                .read(
                    RangeRequest::Span {
                        start,
                        end: start + 4096,
                    },
                    &mut io::sink(),
                )
                .unwrap();
            black_box(report);
        },
        runs,
    );
    print("pinned-4k-range", elapsed, runs, 4096, "bytes");
    drop(pin);

    let catalog = cluster.catalog()?;
    let runs = 60;
    let elapsed = timed(
        |i| {
            let key = format!("planned/{i}");
            let mut planner = PlacementPlanner::new(&catalog, None).unwrap();
            black_box(
                planner
                    .plan(&key, input.len() as u64, Media::Hot, DomainPolicy::Strict)
                    .unwrap(),
            );
        },
        runs,
    );
    print("24-node-placement", elapsed, runs, write.stripes, "stripes");

    let object = cluster.object("bench")?;
    for stripe in object.stripes.iter().step_by(4) {
        let address =
            FragmentAddress::from_receipt(object.generation, stripe.index, &stripe.fragments[0]);
        std::fs::remove_file(cluster.fragment_path(&address)?)?;
    }
    let runs = 35;
    let elapsed = timed(
        |_| {
            let totals = cluster.scan(Some("bench")).unwrap();
            assert_eq!(totals.recoverable, 16);
            black_box(totals);
        },
        runs,
    );
    print(
        "degraded-reconstruction-scan",
        elapsed,
        runs,
        write.stripes,
        "stripes",
    );
    Ok(())
}

struct Workspace(std::path::PathBuf);

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
