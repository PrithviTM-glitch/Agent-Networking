#!/bin/sh
# Runs agent-bridge's Kani proofs, the cfg(kani) harnesses of rust/crates/core (a normal build never compiles them).
#
# VERIFIED, run with no arguments (what CI runs):
#   decide_gives_admin_iff_the_variable_names_it  provision.rs: admin's credentials only through the variable (~5 s)
#   every_session_digest_makes_a_parsable_address  address.rs: a session's address is provisionable, every digest
# PENDING, run only when named or with --pending: they don't finish in 30 min until their code gets byte-level seams
# (std Path, serde_json, sha256 and float formatting are more than CBMC gets through):
#   lexical_without_dotdot_stays_in_its_base, topic_parsing_never_panics, a_message_id_is_always_16_hex  (proofs.rs)
#
# Usage: scripts/verify-kani.sh [--pending | HARNESS ...]
# CARGO_KANI names cargo-kani (default ~/.cargo/bin/cargo-kani, Kani 0.68.0), so CI can pin its own. KANI_TIMEOUT is
# each harness's limit in seconds (default 900), KANI_MEMORY its address-space cap (default 12G; K, M or G): one that
# runs past either fails and says which, instead of hanging the job or being killed by the runner.
set -eu
verified="decide_gives_admin_iff_the_variable_names_it every_session_digest_makes_a_parsable_address"
pending="lexical_without_dotdot_stays_in_its_base topic_parsing_never_panics a_message_id_is_always_16_hex"
root=$(cd "$(dirname "$0")/.." && pwd)
kani=${CARGO_KANI:-$HOME/.cargo/bin/cargo-kani}
limit=${KANI_TIMEOUT:-900}
memory=${KANI_MEMORY:-12G}
case "$memory" in
*K) memory_kb=${memory%K} ;;
*M) memory_kb=$((${memory%M} * 1024)) ;;
*G) memory_kb=$((${memory%G} * 1024 * 1024)) ;;
*) echo "verify-kani: KANI_MEMORY must be a number with K, M or G, got '$memory'" >&2; exit 2 ;;
esac
if [ ! -x "$kani" ]; then
    echo "verify-kani: $kani not found; install Kani 0.68.0 or set CARGO_KANI" >&2
    exit 2
fi
if [ $# -eq 0 ]; then
    set -- $verified
elif [ "$1" = --pending ]; then
    set -- $pending
fi
cd "$root/rust"
log=$(mktemp)
trap 'rm -f "$log" "$log.status"' EXIT
failed=""
for harness in "$@"; do
    start=$(date +%s)
    # the cap holds for kani and everything it starts (cbmc included); the output is kept to tell an exhausted
    # cap from a failed proof
    {
        status=0
        (ulimit -v "$memory_kb" && exec timeout "$limit" "$kani" kani -p agent-bridge-core --harness "$harness") 2>&1 ||
            status=$?
        echo "$status" > "$log.status"
    } | tee "$log"
    status=$(cat "$log.status")
    if [ "$status" -eq 0 ]; then
        echo "verify-kani: $harness verified in $(($(date +%s) - start)) s"
        continue
    fi
    if [ "$status" -eq 124 ]; then
        echo "verify-kani: $harness did not finish within ${limit} s (KANI_TIMEOUT)" >&2
    elif grep -qiE "out of memory|bad_alloc|memory allocation of|cannot allocate memory|failed to map segment" "$log"; then
        echo "verify-kani: $harness ran out of memory (KANI_MEMORY=$memory)" >&2
    else
        echo "verify-kani: $harness FAILED (exit $status)" >&2
    fi
    failed="$failed $harness"
done
if [ -n "$failed" ]; then
    echo "verify-kani: not verified:$failed" >&2
    exit 1
fi
