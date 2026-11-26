#!/usr/bin/env bash
set -Eeuo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
bin=${PARITYBAY_BIN:-"$repo/target/debug/paritybay"}
test -x "$bin"
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT

root="$work/cluster"
pb() { "$bin" --root "$root" "$@"; }

printf '01234567abcdefgh' > "$work/original"
printf '01234567' > "$work/part-1"
printf 'abcdefgh' > "$work/part-2"
pb init "$repo/config/cluster.conf"

printf '\n== Conditional put and cross-shard range ==\n'
pb put normal "$work/original" --if absent
pb get normal "$work/range" --range 6:11 --report
printf '67abc' | cmp - "$work/range"
set +e
pb put normal "$work/original" --if absent
status=$?
set -e
test "$status" -eq 4

printf '\n== Multipart commit ==\n'
begin=$(pb begin example 16 --if absent)
id=$(printf '%s\n' "$begin" | sed -n '1s/^upload=\([0-9][0-9]*\) .*/\1/p')
test -n "$id"
pb part "$id" 2 "$work/part-2"
pb part "$id" 1 "$work/part-1"
pb finish "$id"
pb get example "$work/full"
cmp "$work/original" "$work/full"

printf '\n== Remove and recover one shard on each assigned node ==\n'
for shard in 0 1 2; do
    fragment=$(pb fragment-path example 0 "$shard")
    node=$(basename -- "$(dirname -- "$(dirname -- "$fragment")")")
    rm -- "$fragment"
    printf 'removed shard=%s node=%s\n' "$shard" "$node"
    pb inspect example
    pb get example "$work/recovered" --report
    cmp "$work/original" "$work/recovered"
    sha256sum "$work/original" "$work/recovered"
    pb heal example
    pb inspect example | grep 'verdict=healthy'
    pb gc
done

printf '\n== Rebalance away from a draining node ==\n'
fragment=$(pb fragment-path example 0 0)
node=$(basename -- "$(dirname -- "$(dirname -- "$fragment")")")
pb node-state "$node" draining
pb rebalance
pb node-state "$node" removed
pb get example "$work/drained" --report
cmp "$work/original" "$work/drained"

printf '\n== Durable v1 catalog ==\n'
pb v1-export "$work/exported.v1"
pb v1-check "$work/exported.v1"
cmp "$root/catalog.v1" "$work/exported.v1"

printf '\n== Migrate to the compact v2 catalog ==\n'
pb v2-migrate
test -f "$root/catalog.v2"
test ! -e "$root/catalog.v1"
pb v2-check "$root/catalog.v2"
pb get example "$work/migrated" --report
cmp "$work/original" "$work/migrated"
pb put after-migration "$work/original" --if absent
pb get after-migration "$work/after"
cmp "$work/original" "$work/after"
pb scan
printf 'demo=passed\n'
