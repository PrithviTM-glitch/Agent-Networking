#!/usr/bin/env bash
# Model-check the worktree lock (specs/worktree/WorktreeLock.tla) with TLC. Each configuration has an expected
# outcome, and the run fails when one differs:
#   MC_today    HeldIsRefused violated     0.3.2's order: the offline gap on the main worktree
#   MC_addonly  HeldIsRefused violated     acquire records, but remember overwrites (the lost update)
#   MC_fixed    no error                   0.3.3: record on acquire, merging remember
#   MC_window   GrantedNotRecorded violated  reachability: ledgerd down between the grant and the record
#   MC_reach    HeldMainOffline violated   vacuity: a held main worktree with ledgerd down is reached
#
#   scripts/verify-tlc-worktree.sh
#
# TLA2TOOLS is the tla2tools.jar to use (default ~/.local/share/tla/v1.7.4/tla2tools.jar; CI pins v1.7.4,
# sha256 936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88). Needs Java 11 or later.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
spec="$root/specs/worktree"
jar="${TLA2TOOLS:-$HOME/.local/share/tla/v1.7.4/tla2tools.jar}"
[ -f "$jar" ] || { echo "verify-tlc-worktree: no tla2tools.jar at $jar (set TLA2TOOLS)" >&2; exit 2; }
meta="$(mktemp -d)"
trap 'rm -rf "$meta"' EXIT

fail=0
expected=("today HeldIsRefused" "addonly HeldIsRefused" "fixed -" "window GrantedNotRecorded" "reach HeldMainOffline")
for want in "${expected[@]}"; do
    read -r cfg inv <<<"$want"
    out="$(cd "$spec" && java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers auto -metadir "$meta/$cfg" \
        -config "MC_$cfg.cfg" MC.tla 2>&1)" || true
    got="-"
    if grep -q "^Error: Invariant .* is violated" <<<"$out"; then
        got="$(sed -n 's/^Error: Invariant \(.*\) is violated.*/\1/p' <<<"$out" | head -1)"
    elif ! grep -q "Model checking completed. No error has been found" <<<"$out"; then
        got="error"
    fi
    states="$(grep -o '[0-9,]* distinct states found' <<<"$out" | head -1)"
    if [ "$got" = "$inv" ]; then
        echo "verify-tlc-worktree: MC_$cfg as expected (${inv/-/no error}; $states)"
    else
        echo "verify-tlc-worktree: MC_$cfg expected ${inv/-/no error}, got ${got/-/no error}" >&2
        grep -E '^(Error|State [0-9]+:)|Exception' <<<"$out" | head -20 >&2
        fail=1
    fi
done
exit $fail
