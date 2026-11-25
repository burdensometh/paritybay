#!/usr/bin/env bash
set -Eeuo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
bin=${PARITYBAY_BIN:-"$repo/target/debug/paritybay"}
output=${1:?"Usage: make-v1-sample.sh NEW_DIRECTORY"}
test -x "$bin"
if [[ -e $output || -L $output ]]; then
    printf 'sample destination already exists: %s\n' "$output" >&2
    exit 2
fi
mkdir -- "$output"
pb() { "$bin" --root "$output" "$@"; }

pb init "$repo/config/v1-sample.conf"
printf '' | pb put empty - --size 0 --if absent
printf 'abcdefghijklmnopqrstuv' | pb put tail - --size 22 --if absent
printf 'domain' | pb put shared-domain - --size 6 --media capacity --domains prefer --if absent
printf '01234567abcdefgh' | pb put degraded - --size 16 --if absent
missing=$(pb fragment-path degraded 0 0)
rm -- "$missing"
pb v1-check "$output/catalog.v1"
pb inspect shared-domain
pb inspect degraded

# Empty directories must survive source snapshots.
for directory in "$output/uploads" "$output"/nodes/*/fragments; do
    touch -- "$directory/.keep"
done
rm -- "$output/catalog.lock"
if [[ -e $output/readers.lock ]]; then
    rm -- "$output/readers.lock"
fi
