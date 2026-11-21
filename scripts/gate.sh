#!/bin/sh
# The release gate: build the wheel with its native binaries, install it into a fresh virtualenv, run the full suite
# against the installed wheel GATE_RUNS times in a row (default 3), then the Rust tests including the ones that need
# Docker. Stops at the first failure. The wheel and its virtualenv live in a temp directory removed on exit (the
# binaries are built into src/agent_bridge.data/scripts, as for any wheel build); the tests never touch the live state.
set -eu

# Started as a shell background job ("( ... ) &" in a script), the gate inherits SIGINT ignored, and a non-interactive
# shell can't undo that: Ctrl-C wouldn't stop it, its INT trap below would never fire, and every test child would
# inherit it too (0.3.2's first gate run failed that way). SIGINT is signal 2, so bit 1 of the SigIgn mask.
ignored=$(sed -n 's/^SigIgn:[[:space:]]*//p' "/proc/$$/status" 2>/dev/null || true)
if [ -n "$ignored" ] && [ $(((0x$ignored >> 1) & 1)) -eq 1 ]; then
    echo "gate: started with SIGINT ignored, as a shell background job is: Ctrl-C couldn't stop the gate, and every" \
        "test it starts would inherit it. Run it in the foreground, or set GATE_ALLOW_BG=1 to run anyway." >&2
    [ "${GATE_ALLOW_BG:-}" = 1 ] || exit 2
fi
# GATE_CHECK_ONLY=1 checks the launch environment above and stops: what the harness tests use.
[ "${GATE_CHECK_ONLY:-}" = 1 ] && exit 0

root=$(cd "$(dirname "$0")/.." && pwd)
runs=${GATE_RUNS:-3}
python=${GATE_PYTHON:-3.12}  # the floor in requires-python
work=$(mktemp -d "${TMPDIR:-/tmp}/agent-bridge-gate.XXXXXX")
trap 'rm -rf "$work"' EXIT
trap 'exit 130' INT  # exit, so the EXIT trap cleans up; a trap that only cleaned up would let the script carry on
trap 'exit 143' TERM
cd "$root"

say() { printf 'gate: %s\n' "$*"; }

say "building the native binaries and the wheel"
scripts/build-bins.sh
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
for cmd in agent-bridge agent-bridge-ledgerd agent-bridge-historyd; do
    file -L "$work/venv/bin/$cmd" | grep -q ELF || { say "the wheel did not install a native $cmd"; exit 1; }
done

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
