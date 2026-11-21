#!/bin/sh
# Read changed paths on stdin, one per line (git diff --name-only), and print which formal-methods jobs they touch, as
# "<job>=true|false" lines for $GITHUB_OUTPUT. A job runs when its spec, its script or the code it checks changed.
# Kani harnesses sit next to their code under cfg(kani), so a changed Rust file under rust/crates that mentions kani::
# (read from VERIFY_ROOT, default the repository) runs kani. --all runs every job: the weekly and manual runs.
#
#   git diff --name-only BASE HEAD | scripts/verify-paths.sh [--all]
set -eu
root=${VERIFY_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}
jobs="tlc-ledger tlc-worktree tlc-relay fuzz-smoke kani"
on=""
add() { for j in "$@"; do case " $on " in *" $j "*) ;; *) on="$on $j" ;; esac; done; }

if [ "${1:-}" = --all ]; then
    add $jobs
else
    while IFS= read -r path; do
        case $path in
            .github/workflows/verify.yml | scripts/verify.sh | scripts/verify-paths.sh | scripts/verify-changes.sh)
                add $jobs ;;
            scripts/tla2tools.sh) add tlc-ledger tlc-worktree tlc-relay ;;
            specs/ledger/* | scripts/verify-tlc-ledger.sh | rust/crates/ledgerd/src/store.rs | \
                rust/crates/ledgerd/src/main.rs) add tlc-ledger ;;  # main.rs: the serve loop and ack order
            specs/worktree/* | scripts/verify-tlc-worktree.sh | rust/crates/core/src/worktree.rs) add tlc-worktree ;;
            # The relay model also covers the Bus's ack, take-back and generation check.
            specs/relay/* | scripts/verify-tlc-relay.sh | rust/crates/cli/src/relay.rs | \
                rust/crates/core/src/relay.rs | rust/crates/core/src/bus.rs | rust/crates/core/src/mqtt.rs)
                add tlc-relay ;;
            rust/fuzz/* | scripts/verify-fuzz.sh) add fuzz-smoke ;;
            scripts/verify-kani.sh) add kani ;;
            # The code the Kani proofs check, besides the files holding harnesses (found by the kani:: rule below).
            rust/crates/core/src/protocol.rs | rust/crates/core/src/guard/paths.rs | \
                rust/crates/core/src/validation.rs | rust/crates/core/src/identity.rs) add kani ;;
        esac
        case $path in
            # The guard is both a TLC model's subject and a fuzz target's.
            rust/crates/core/src/guard/*) add tlc-worktree fuzz-smoke ;;
            # The other parsers the fuzz targets reach: topics, envelopes and ledgerd's request decode.
            rust/crates/core/src/protocol.rs | rust/crates/core/src/json.rs | rust/crates/core/src/validation.rs | \
                rust/crates/ledgerd/src/request.rs) add fuzz-smoke ;;
        esac
        case $path in
            rust/crates/*.rs) [ -f "$root/$path" ] && grep -q 'kani::' "$root/$path" && add kani ;;
        esac
    done
fi
for j in $jobs; do
    case " $on " in *" $j "*) echo "$j=true" ;; *) echo "$j=false" ;; esac
done
