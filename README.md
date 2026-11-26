# ParityBay

ParityBay is a Linux directory-backed cluster for named objects. It stores
two data shards and one XOR shard per stripe on three distinct nodes. A node
has a failure domain, one of two media classes, a byte budget and a lifecycle
state. The crate exposes a Rust library and a single-process CLI that shares
the same durable catalog across invocations.

## Quick start

Requires Linux, Rust 1.89 or newer, and a filesystem that supports advisory
file locks, atomic same-directory rename and directory fsync. Cargo needs no
external packages. The scripts use Bash and standard Linux utilities.

```sh
cargo build --locked --offline
PB=target/debug/paritybay
"$PB" --root var/local init config/cluster.conf
printf '01234567abcdefgh' | "$PB" --root var/local put example - --size 16 --if absent
"$PB" --root var/local get example - --range 6:11 --report
"$PB" --root var/local inspect example
```

Object bytes go to stdout for `get KEY -`. The read report goes to stderr.
The named-output form publishes the whole result using a synced temporary
file outside the managed cluster directory. If a streaming read fails late,
its caller may have received a prefix. The default root is `var/paritybay`,
overridden by `PARITYBAY_ROOT` or the global `--root` option.

## Storage workflows

- **Put** accepts a local file or exact-length input stream. `--if absent`
  creates a key only if it is absent. `--if GEN` compares the current
  generation. The receipt includes the new generation, prior generation,
  total bytes, stripe count and number of stripes with weaker domain
  coverage. The default condition is `any`.
- **Multipart** starts with `begin KEY SIZE`, reserves a layout and reports
  its upload ID. Write numbered parts with `part ID NUMBER INPUT`. A part
  can arrive out of order or replace an earlier part. `finish ID` requires
  the exact total and consecutive parts starting at 1, verifies staged CRCs
  and rechecks the condition. If the planned nodes became unavailable, use
  `replan ID` to replace the reservation before retrying. `abort ID`
  releases it. A successfully published upload uses its ID as its object
  generation, so `stat KEY` can resolve a lost finish response while that
  generation is current.
- **Get** pins an object generation and its receipt mapping. `--range
  START:END` is half-open, `START:` reads to EOF, and `:SIZE` reads a
  suffix of up to that size. Invalid endpoints are rejected. For each
  intersecting block, the reader verifies the selected whole shard against
  its CRC32C, then emits only the requested bytes. A missing, too short, too
  long, unavailable or CRC-invalid shard triggers reconstruction if the
  other two shards and the resulting checksum verify.
- **Heal** uses the same shard validation as the read path. `inspect KEY`
  shows expected receipts, read faults, domain coverage and per-stripe
  conclusions. `scan [KEY]` summarizes without writing. `heal [KEY]`
  writes a new fragment revision and verifies it before publishing.
  Inspection and repair stream every fragment through fixed 64 KiB windows
  with rolling CRC32C, so their live buffer peak is independent of
  `block_bytes` (no whole shard is ever buffered); a short or zero-length
  second shard is reconstructed by treating its absent tail as zeros. The
  repair attempt remains in `repair-log`. `replay [ATTEMPT]` can resume a
  planned copy after interruption and reuses an already verified copy when
  available.
- **Rebalance** moves shards off draining and offline nodes to viable
  same-media placements. It also improves shared-domain layouts when a
  stronger placement is available. The repair transaction publishes the
  new mapping before collection. Set a node to `removed` after active
  fragment, upload, repair and retired references have cleared. Use
  `nodes` to inspect live, reserved and retired allocations. `node-media`
  changes the class of a drained, unreferenced node. Node removal marks the
  catalog entry and leaves the node directory available for inspection.

`paritybay help` lists all commands. Exit codes 2–7 distinguish invalid
input, missing entries, conflicts or pending repairs, unavailable capacity,
unrecoverable stripes and corrupt state. Exit 1 indicates other I/O errors.

## Placement and persistence

A stripe uses only the object's requested `hot` or `capacity` media.
The `strict` policy requires three distinct domains. `prefer` maximizes
the domain count available to that stripe and reports the smallest number
of surviving shards after the loss of one domain. Node ranking is stable
for a key, stripe index and shard index. Each stripe search checks the
current per-node budgets, including earlier placements in the same plan.
Planning carries prior stripes forward in order.

The v1 implementation supports up to 64 node IDs, including removal
tombstones. An object can contain up to 512 MiB and 65,536 stripes. One
multipart part can contain up to 64 MiB. The active staging reservation
limit and per-node byte budgets are set in the cluster configuration.

Both upload paths write and sync fragment files before an atomic catalog
replacement. The catalog and node fsync operations use the underlying
filesystem's durability semantics. Cleanup is separate from publication.
A pinned reader or repair can continue using its old receipt mapping after
replacement, deletion or relocation. `gc` defers while it cannot acquire
an exclusive cleanup lock. It reclaims retired and unreferenced managed
files, including abandoned staging data and temporary files. Retired
fragments continue charging the owning node until cleanup.

The cluster root owns `catalog.v1`, lock files, `nodes/ID/fragments` and
`uploads/ID`. Node and upload file names carry the generation or session
and a revision. The catalog records their contents and refuses malformed
geometry or references. The executable accepts a dedicated directory
under operator control. Simulated domains model availability of local
directory nodes. They do not create physical failure isolation. The
baseline models one unavailable fragment per stripe, and CRC32C detects
accidental corruption. See [v1 format](FORMAT.md) for the byte layout and
[samples](samples/README.md) for the original-format cluster.

## Verification

```sh
cargo test --locked --offline
bash scripts/demo.sh
bash scripts/fault-boundaries.sh
cargo bench --locked --offline --bench hotspots
```

The demo performs a multipart commit, removes one fragment at a time on
each assigned node, compares the recovered bytes and SHA-256 digests, then
heals before the next loss. It also evacuates a draining node and verifies
a durable v1 export. The benchmark measures a 4 KiB range on a pinned
object, a 24-node placement search and a degraded reconstruction scan.
It prints the workload size and timing at each hotspot.

To pause an operation, make a gate directory and create
`BOUNDARY.arm` there. Run the command with `--gates DIR` before the
subcommand. Once `BOUNDARY.PID.N.ready` appears, create the same basename
with the extension `.resume` or `.fail`. An arm file is claimed by one
process. Gates have a 30-second timeout by default. Rust callers can
inject a `Checkpoint` directly.

| Boundary | Position |
|---|---|
| `before-file-sync` | Draft created and written, before fsync |
| `before-catalog-commit` | Catalog draft synced, before atomic rename |
| `read-pinned` | Generation captured under a reader pin, before shard I/O |
| `before-repair-copy` | Repair plan persisted and source mapping read, before copy |
| `before-node-removal` | Before writer lock acquisition and removal eligibility checks |

## Source layout

| Directory | Responsibility |
|---|---|
| `src/upload` | Reservations, part staging, conditional object publication |
| `src/manifest` | v1 frame, metadata invariants, catalog and process locks |
| `src/codec` | Stripe geometry, XOR reconstruction, CRC32C |
| `src/placement` | Node states, byte accounting, domain-aware placement |
| `src/readpath` | Pinned reads, range assembly, shard verification |
| `src/repair` | Scan, replay, node evacuation and retired-file cleanup |
