#!/bin/sh
# Builds the Rust binaries the wheel ships (agent-bridge, agent-bridge-ledgerd, agent-bridge-historyd) into
# src/agent_bridge.data/scripts, where maturin packs them next to the shims. Run it before `uv sync` or a wheel build
# from a checkout; CI runs the same inside the manylinux container, so the binaries match the wheel's glibc, with
# CARGO_BUILD_TARGET set for a cross build (cargo builds for it and writes to target/<triple>/release).
set -eu
root=$(cd "$(dirname "$0")/.." && pwd)
out=${CARGO_TARGET_DIR:-$root/rust/target}/${CARGO_BUILD_TARGET:+$CARGO_BUILD_TARGET/}release
cargo build --release --locked --manifest-path "$root/rust/Cargo.toml" \
    -p agent-bridge -p agent-bridge-ledgerd -p agent-bridge-historyd
for bin in agent-bridge agent-bridge-ledgerd agent-bridge-historyd; do
    install -m 755 "$out/$bin" "$root/src/agent_bridge.data/scripts/$bin"
done
