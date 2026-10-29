#!/bin/sh
# The release gate: build the wheel, install it into a fresh virtualenv, run the full suite against the installed wheel
# GATE_RUNS times in a row (default 3), then the Rust tests including the ones that need Docker. Stops at the first
# failure. Everything it builds lives in a temp directory removed on exit; the tests never touch the live state.
set -eu

root=$(cd "$(dirname "$0")/.." && pwd)
runs=${GATE_RUNS:-3}
python=${GATE_PYTHON:-3.12}  # the floor in requires-python
work=$(mktemp -d "${TMPDIR:-/tmp}/agent-bridge-gate.XXXXXX")
trap 'rm -rf "$work"' EXIT
trap 'exit 130' INT  # exit, so the EXIT trap cleans up; a trap that only cleaned up would let the script carry on
trap 'exit 143' TERM
cd "$root"

say() { printf 'gate: %s\n' "$*"; }

say "building the wheel"
uvx --from maturin==1.15.0 maturin build --release --out "$work/dist"

say "installing it into a fresh Python $python virtualenv"
uv venv --quiet --python "$python" "$work/venv"
uv pip install --quiet --python "$work/venv/bin/python" "$work"/dist/*.whl pytest==9.1.1 paho-mqtt==2.1.0
# The suite has to import the wheel, not src/ (pytest adds no source path; this proves it).
"$work/venv/bin/python" -I -c '
import agent_bridge, agent_bridge._native, sys
where = agent_bridge.__file__
assert sys.argv[1] not in where, f"agent_bridge imported from the tree: {where}"
print("gate: agent_bridge", agent_bridge._native.__version__, "from", where)
' "$root/src"

i=1
while [ "$i" -le "$runs" ]; do
    say "suite run $i of $runs"
    AGENT_BRIDGE_BIN="$work/venv/bin" "$work/venv/bin/python" -m pytest -p no:cacheprovider -q \
        || { say "suite run $i of $runs failed"; exit 1; }
    i=$((i + 1))
done

say "Rust tests, including the ones that need Docker"
cargo test --manifest-path rust/Cargo.toml --workspace -- --include-ignored

say "passed: $runs suite runs against the installed wheel, and the Rust tests"
