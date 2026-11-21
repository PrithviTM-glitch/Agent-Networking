#!/bin/sh
# Run cargo test with the given arguments; if tests fail, rerun only the failed test names once, exactly. Green on the
# rerun passes, but names every rerun test in a FLAKE line ("FLAKE <test name> run <run id>") in $FLAKES_FILE (default
# flakes.txt) and, in GitHub Actions, the step summary. Red on the rerun fails.
#
# --no-fail-fast is added, so one failing test binary can't stop the others from running and hide their failures. A
# test binary that never reported a result (a crash, or a build error) has no test names to rerun, and rerunning the
# others would hide it, so that fails at once.
#
#   scripts/rerun-cargo.sh [CARGO TEST ARGS...] [-- TEST BINARY ARGS...]
set -u
flakes=${FLAKES_FILE:-flakes.txt}
run=${GITHUB_RUN_ID:-local}
out=$(mktemp "${TMPDIR:-/tmp}/rerun-cargo.XXXXXX")
trap 'rm -f "$out"' EXIT

# Split the arguments at "--": cargo's own before it, the test binaries' after it.
cargo_args="" bin_args="" seen=0
for a in "$@"; do
    if [ "$seen" = 0 ] && [ "$a" = "--" ]; then seen=1; continue; fi
    q=$(printf "%s" "$a" | sed "s/'/'\\\\''/g")
    if [ "$seen" = 0 ]; then cargo_args="$cargo_args '$q'"; else bin_args="$bin_args '$q'"; fi
done

status=0
eval "cargo test $cargo_args --no-fail-fast -- $bin_args" > "$out" 2>&1 || status=$?
cat "$out"
[ "$status" -eq 0 ] && exit 0

started=$(grep -cE '^ +(Running|Doc-tests) ' "$out")
finished=$(grep -c '^test result: ' "$out")
failed=$(sed -n 's/^test \(.*\) \.\.\. FAILED$/\1/p' "$out" | sort -u)
if [ -z "$failed" ] || [ "$started" -ne "$finished" ]; then
    echo "rerun-cargo: $started test binaries started and $finished reported a result, with failed tests:" \
        "${failed:-none}; a binary that never reported (a crash or a build error), or no failed test," \
        "so not rerunning" >&2
    exit 1
fi
echo "rerun-cargo: rerunning the failed tests once:"
printf '  %s\n' $failed
# One line of quoted names: a newline left in the eval'd text would run the next name as a command.
names=$(printf '%s\n' "$failed" | sed "s/'/'\\\\''/g; s/.*/'&'/" | tr '\n' ' ')
eval "cargo test $cargo_args --no-fail-fast -- $bin_args --exact $names" || exit 1
printf '%s\n' "$failed" | while IFS= read -r name; do
    line="FLAKE $name run $run"
    echo "$line"
    echo "$line" >> "$flakes"
    [ -z "${GITHUB_STEP_SUMMARY:-}" ] || echo "$line" >> "$GITHUB_STEP_SUMMARY"
done
exit 0
