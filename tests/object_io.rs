mod common;

use std::fs;
use std::io::Cursor;

use paritybay::codec::{checksum, reconstruct, stripe_lengths, EncodedStripe};
use paritybay::readpath::{FaultKind, RangeRequest, StripeVerdict};
use paritybay::{Cluster, Condition, DomainPolicy, Error, Media};

use common::{address, config, get, options, path, put, Workspace};

#[test]
fn codec_matches_castagnoli_and_recovers_each_short_shard() {
    assert_eq!(checksum(b"123456789"), 0xe306_9283);
    assert_eq!(checksum(b""), 0);
    for length in 0..=16 {
        let data: Vec<_> = (0..length as u8)
            .map(|item| item.wrapping_mul(19))
            .collect();
        let lengths = stripe_lengths(length, 8);
        let stripe = EncodedStripe::read(&mut Cursor::new(&data), lengths).unwrap();
        for missing in 0..3 {
            let present = std::array::from_fn(|index| {
                (index != missing).then_some(stripe.shards[index].as_slice())
            });
            assert_eq!(
                reconstruct(lengths, missing as u8, present).unwrap(),
                stripe.shards[missing]
            );
        }
    }
}

#[test]
fn steady_state_ranges_cover_geometry_including_empty_endpoint() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let source: Vec<_> = (0..51).map(|value| (value * 31) as u8).collect();
    let generation = put(&cluster, "image/块", &source);
    let reopened = Cluster::open(dir.root()).unwrap();
    let pin = reopened.pin_object("image/块").unwrap();
    for start in 0..=source.len() {
        for end in start..=source.len() {
            let request = RangeRequest::Span {
                start: start as u64,
                end: end as u64,
            };
            let mut output = Vec::new();
            let report = pin.read(request, &mut output).unwrap();
            assert_eq!(output, source[start..end], "[{start},{end})");
            assert_eq!(report.generation, generation);
            assert_eq!(
                report
                    .stripes
                    .iter()
                    .map(|stripe| stripe.bytes_emitted)
                    .sum::<u64>(),
                (end - start) as u64
            );
        }
    }
    assert_eq!(
        get(&reopened, "image/块", RangeRequest::Suffix(200)).0,
        source
    );
    assert!(matches!(get_invalid(&reopened), Err(Error::Invalid(_))));
}

fn get_invalid(cluster: &Cluster) -> paritybay::Result<paritybay::readpath::ReadReport> {
    cluster.get("image/块", RangeRequest::From(52), &mut Vec::new())
}

#[test]
fn degraded_ranges_only_open_needed_shards() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    let original = b"01234567abcdefgh";
    put(&cluster, "ranges", original);
    let missing = path(&cluster, "ranges", 0, 0);
    fs::remove_file(missing).unwrap();

    let (output, report) = get(&cluster, "ranges", RangeRequest::From(8));
    assert_eq!(output, b"abcdefgh");
    assert!(report.stripes[0].faults.is_empty());
    assert_eq!(report.stripes[0].bytes_verified, 8);

    let (output, report) = get(&cluster, "ranges", RangeRequest::Span { start: 3, end: 12 });
    assert_eq!(output, &original[3..12]);
    assert_eq!(report.stripes[0].reconstructed, vec![0]);
    assert_eq!(report.stripes[0].faults[0].kind, FaultKind::Missing);
    assert_eq!(report.stripes[0].bytes_verified, 16);

    fs::remove_file(path(&cluster, "ranges", 0, 2)).unwrap();
    assert!(matches!(
        cluster.get("ranges", RangeRequest::Full, &mut Vec::new()),
        Err(Error::Unrecoverable { stripe: 0, .. })
    ));
    assert_eq!(
        cluster.inspect("ranges").unwrap().stripes[0].verdict,
        StripeVerdict::Unrecoverable
    );
}

#[test]
fn empty_object_and_short_tail_use_distinct_persistent_geometry() {
    let dir = Workspace::new();
    let cluster = dir.create(config());
    put(&cluster, "empty", b"");
    assert!(cluster.object("empty").unwrap().stripes.is_empty());
    assert!(get(&cluster, "empty", RangeRequest::Full).0.is_empty());

    put(&cluster, "short", b"abc");
    let geometry = &cluster.object("short").unwrap().stripes[0];
    assert_eq!(geometry.data_bytes, [3, 0]);
    assert_eq!(geometry.fragments[1].crc32c, 0);
    assert_eq!(
        fs::metadata(path(&cluster, "short", 0, 1)).unwrap().len(),
        0
    );
    fs::remove_file(path(&cluster, "short", 0, 0)).unwrap();
    assert_eq!(get(&cluster, "short", RangeRequest::Full).0, b"abc");
}

#[test]
fn strict_domain_check_and_capacity_media_are_visible() {
    let dir = Workspace::new();
    let mut local = config();
    for node in local
        .nodes
        .iter_mut()
        .filter(|node| node.media == Media::Hot)
    {
        node.domain = "shared".into();
    }
    let cluster = dir.create(local);
    let bytes = b"shards";
    let err = cluster
        .put("strict", options(bytes.len()), &mut Cursor::new(bytes))
        .unwrap_err();
    assert!(matches!(err, Error::Capacity(_)));

    let mut opt = options(bytes.len());
    opt.domains = DomainPolicy::Prefer;
    let receipt = cluster
        .put("fewer-domains", opt, &mut Cursor::new(bytes))
        .unwrap();
    assert_eq!(receipt.weaker_stripes, 1);
    let report = cluster.inspect("fewer-domains").unwrap();
    assert_eq!(
        report.stripes[0]
            .domains
            .surviving_shards_after_worst_domain_loss,
        0
    );

    opt.media = Media::Capacity;
    opt.domains = DomainPolicy::Strict;
    opt.condition = Condition::Absent;
    let receipt = cluster
        .put("archive", opt, &mut Cursor::new(bytes))
        .unwrap();
    assert_eq!(receipt.weaker_stripes, 0);
    let catalog = cluster.catalog().unwrap();
    for shard in 0..3 {
        assert_eq!(
            catalog.nodes[&address(&cluster, "archive", 0, shard).node].media,
            Media::Capacity
        );
    }
}
