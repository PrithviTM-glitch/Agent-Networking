#!/bin/sh
# Check specs/relay/RelayDelivery.tla: MC.cfg must hold every invariant, and each cfg whose first line says
# "MUST FAIL <Invariant>" must violate exactly that one, so the spec is known to bite.
#
# Runs the standard-library mirror (specs/relay/mirror.py), then TLC, the checker of record: TLA2TOOLS names the
# tla2tools.jar (default: the pinned v1.7.4 install), and its sha256 must match TLA2TOOLS_SHA256 (default: v1.7.4's),
# so CI can point both at its own pinned download. It never downloads anything.
# Exit: 0 every model as expected; 1 a model not as expected; 2 TLC unavailable or not the pinned jar.
set -eu
root=$(cd "$(dirname "$0")/.." && pwd)
specs="$root/specs/relay"

python3 "$specs/mirror.py"

jar=${TLA2TOOLS:-$HOME/.local/share/tla/v1.7.4/tla2tools.jar}
pin=${TLA2TOOLS_SHA256:-936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88}
if [ ! -f "$jar" ]; then
    echo "verify-tlc-relay: no tla2tools.jar at $jar (set TLA2TOOLS), so TLC did not run (the mirror above did)" >&2
    exit 2
fi
got=$(sha256sum "$jar" | cut -d' ' -f1)
if [ "$got" != "$pin" ]; then
    echo "verify-tlc-relay: $jar has sha256 $got, not the pinned $pin" >&2
    exit 2
fi
command -v java >/dev/null || { echo "verify-tlc-relay: java is not installed" >&2; exit 2; }

meta=$(mktemp -d)
trap 'rm -rf "$meta"' EXIT
status=0
for cfg in "$specs"/*.cfg; do
    name=$(basename "$cfg")
    must=$(head -1 "$cfg" | sed -n 's/.*MUST FAIL \([A-Za-z]*\).*/\1/p')
    out="$meta/$name.out"
    # -deadlock: the model may end (the session over, the broker down at the generation bound); that is not an error.
    if (cd "$specs" && java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -config "$name" -workers auto -deadlock \
        -metadir "$meta/$name.states" RelayDelivery.tla) >"$out" 2>&1; then
        ok=yes
    else
        ok=no
    fi
    if [ -z "$must" ]; then
        if [ "$ok" = yes ]; then
            echo "$name: TLC: holds every invariant"
        else
            echo "$name: TLC: FAILED"; tail -30 "$out"; status=1
        fi
    elif grep -q "Invariant $must is violated" "$out"; then
        echo "$name: TLC: fails $must as expected"
    else
        echo "$name: TLC: did NOT fail $must: the spec does not bite"; tail -30 "$out"; status=1
    fi
done
exit $status
