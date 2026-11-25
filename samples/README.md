# v1 cluster

`cluster-v1/` is a cluster directory produced by the normal v1 writer. Copy it
before using it as a writable cluster. Relative node paths are resolved under
that copy.

| Key | Contents | Observable condition |
|---|---|---|
| `empty` | 0 bytes | No stripes |
| `tail` | `abcdefghijklmnopqrstuv` | A full 8+8 stripe followed by a 6+0 stripe |
| `shared-domain` | `domain` | Capacity media, two of three placements share a domain |
| `degraded` | `01234567abcdefgh` | Shard 0 of stripe 0 physically absent, range or full read reconstructs it |

Regenerate into a new directory with
`bash scripts/make-v1-sample.sh NEW_DIRECTORY`. The script uses native
`init`, `put`, `v1-check` and `inspect`. The integration test opens the
checked-in sample, round-trips its raw catalog bytes and writes back using a
copied cluster. Its lock files are created on first use.
