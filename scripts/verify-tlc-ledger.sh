#!/bin/sh
# Model-check ledgerd's lease protocol (specs/ledger/Ledger.tla) with TLC, the same locally and in verify.yml.
#   - Ledger.cfg must hold every invariant.
#   - Each broken variant must fail exactly the invariant its first line names ("MUST FAIL <Invariant>"):
#     LedgerDoubleGrant (0.2.1's double grant), LedgerLeaseToNobody (0.2.1's lease to nobody), LedgerFenceReset.
# A variant that passes means the spec no longer catches that bug, so it fails the check too.
#
#   scripts/verify-tlc-ledger.sh            # TLC from $TLA2TOOLS_JAR or $TLA2TOOLS, else the local install
#   scripts/verify-tlc-ledger.sh --mirror   # no Java: specs/ledger/mirror.py, the spec's executable mirror
#
# Cost, measured on 2026-11-20: Ledger.cfg is 9,675,075 distinct states, 3 min 48 s on 8 workers (about 4 GB heap);
# each broken variant fails in under 5 s.
set -eu
root=$(cd "$(dirname "$0")/.." && pwd)
spec=$root/specs/ledger

if [ "${1:-}" = --mirror ]; then
    exec python3 -I "$spec/mirror.py"
fi

jar=$(TLA2TOOLS_JAR=${TLA2TOOLS_JAR:-${TLA2TOOLS:-$HOME/.local/share/tla/v1.7.4/tla2tools.jar}} \
    "$root/scripts/tla2tools.sh" "${TMPDIR:-/tmp}/tla")
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cp "$spec"/Ledger.tla "$spec"/*.cfg "$work/"  # TLC writes its state and trace files next to the spec: never into the tree

status=0
for cfg in "$work"/*.cfg; do
    name=$(basename "$cfg" .cfg)
    must=$(head -n 1 "$cfg" | sed -n 's/.*MUST FAIL \([A-Za-z]*\).*/\1/p')
    out=$work/$name.out
    (cd "$work" && java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers auto -deadlock -config "$name.cfg" \
        -metadir "$work/states-$name" Ledger.tla >"$out" 2>&1) && rc=0 || rc=$?
    violated=$(sed -n 's/^Error: Invariant \([A-Za-z]*\) is violated\.$/\1/p' "$out" | head -n 1)
    states=$(sed -n 's/.* \([0-9]*\) distinct states found.*/\1/p' "$out" | tail -n 1)
    if [ -z "$must" ]; then
        if [ "$rc" -eq 0 ] && grep -q "No error has been found" "$out"; then
            echo "ok   $name: every invariant holds (${states:-?} distinct states)"
        else
            echo "FAIL $name: must hold every invariant; TLC exit $rc, violated ${violated:-nothing named}"
            tail -n 40 "$out"
            status=1
        fi
    elif [ "$violated" = "$must" ]; then
        echo "ok   $name: fails $must, as it must"
    else
        echo "FAIL $name: must fail $must; TLC exit $rc, violated ${violated:-nothing}"
        tail -n 40 "$out"
        status=1
    fi
done
exit $status
