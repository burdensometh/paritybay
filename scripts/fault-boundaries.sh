#!/usr/bin/env bash
set -Eeuo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
bin=${PARITYBAY_BIN:-"$repo/target/debug/paritybay"}
test -x "$bin"
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
root="$work/cluster"
gates="$work/gates"
mkdir -- "$gates"
pb() { "$bin" --root "$root" "$@"; }
gated() { "$bin" --root "$root" --gates "$gates" --gate-timeout-ms 15000 "$@"; }
arm() { : > "$gates/$1.arm"; }
await_gate() {
    local name=$1 deadline=$((SECONDS + 12)) value
    while (( SECONDS < deadline )); do
        for value in "$gates/$name".*.ready; do
            if [[ -f $value ]]; then
                printf '%s\n' "${value%.ready}"
                return 0
            fi
        done
        sleep .05
    done
    printf 'checkpoint %s did not open\n' "$name" >&2
    return 1
}
expect_conflict() {
    local child=$1 result=0
    wait "$child" || result=$?
    test "$result" -eq 4
}

printf 'first generation crossing stripe boundary' > "$work/first"
printf 'second generation with different bytes' > "$work/second"
pb init "$repo/config/cluster.conf"
pb put race "$work/first"

printf '\n== Fail before file fsync ==\n'
arm before-file-sync
gated put race "$work/second" &
child=$!
gate=$(await_gate before-file-sync)
cat "$gate.ready"
: > "$gate.fail"
expect_conflict "$child"
pb get race "$work/read"
cmp "$work/first" "$work/read"

printf '\n== Fail before catalog commit ==\n'
arm before-catalog-commit
gated put race "$work/second" &
child=$!
gate=$(await_gate before-catalog-commit)
cat "$gate.ready"
: > "$gate.fail"
expect_conflict "$child"
pb get race "$work/read"
cmp "$work/first" "$work/read"

printf '\n== Replace a generation while a reader is paused ==\n'
arm read-pinned
gated get race "$work/pinned" --report &
child=$!
gate=$(await_gate read-pinned)
cat "$gate.ready"
pb put race "$work/second"
pb gc | tee "$work/pinned.gc" | grep '^deferred=true'
: > "$gate.resume"
wait "$child"
cmp "$work/first" "$work/pinned"
pb get race "$work/read"
cmp "$work/second" "$work/read"
pb gc

printf '\n== Interrupt repair before copy and replay ==\n'
fragment=$(pb fragment-path race 0 0)
rm -- "$fragment"
arm before-repair-copy
gated heal race &
child=$!
gate=$(await_gate before-repair-copy)
cat "$gate.ready"
: > "$gate.fail"
expect_conflict "$child"
pb repair-log | grep 'state=planned'
pb replay | grep 'state=published'
pb get race "$work/repaired" --report
cmp "$work/second" "$work/repaired"

printf '\n== Recheck a paused node removal ==\n'
pb node-add 'z-spare, spare, hot, 67108864, draining'
arm before-node-removal
gated node-state z-spare removed &
child=$!
gate=$(await_gate before-node-removal)
cat "$gate.ready"
pb node-state z-spare online
: > "$gate.resume"
expect_conflict "$child"
pb nodes | grep '^node=z-spare .*state=online'

printf '\n== Interrupt catalog migration before commit ==\n'
rm -f -- "$gates/before-catalog-commit".*
arm before-catalog-commit
gated v2-migrate &
child=$!
gate=$(await_gate before-catalog-commit)
cat "$gate.ready"
: > "$gate.fail"
expect_conflict "$child"
test -f "$root/catalog.v1"
test ! -e "$root/catalog.v2"
pb get race "$work/read"
cmp "$work/second" "$work/read"
pb v2-migrate | grep '^v2=true migrated=true'
test -f "$root/catalog.v2"
test ! -e "$root/catalog.v1"
pb get race "$work/migrated"
cmp "$work/second" "$work/migrated"
pb heal race
pb scan
printf 'fault_boundaries=passed\n'
