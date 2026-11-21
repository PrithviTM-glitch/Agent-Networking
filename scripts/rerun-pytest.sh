#!/bin/sh
# Run pytest with the given arguments; if it fails, rerun only the failed tests once (pytest --lf). Green on the rerun
# passes, but names every rerun test in a FLAKE line ("FLAKE <test id> run <run id>") in $FLAKES_FILE (default
# flakes.txt) and, in GitHub Actions, the step summary, so a flake never turns main red and never goes unseen. Red on
# the rerun fails. A failure with no failed test recorded (a usage or collection error) fails at once.
#
#   PYTHON=.venv/bin/python scripts/rerun-pytest.sh [PYTEST ARGS...]
set -u
python=${PYTHON:-python3}
flakes=${FLAKES_FILE:-flakes.txt}
run=${GITHUB_RUN_ID:-local}
cache=$(mktemp -d "${TMPDIR:-/tmp}/rerun-pytest.XXXXXX")
trap 'rm -rf "$cache"' EXIT
# The cache lives in a temp dir, so the rerun works without writing .pytest_cache into the tree.
pytest() { "$python" -m pytest -p cacheprovider -o cache_dir="$cache" "$@"; }

pytest "$@" && exit 0
failed=$("$python" -c '
import json, sys
try:
    print("\n".join(json.load(open(sys.argv[1]))))
except OSError:
    pass
' "$cache/v/cache/lastfailed")
if [ -z "$failed" ]; then
    echo "rerun-pytest: pytest failed with no failed test recorded (a usage or collection error), so not rerunning" >&2
    exit 1
fi
echo "rerun-pytest: rerunning the failed tests once:"
printf '  %s\n' $failed
pytest --lf "$@" || exit 1
printf '%s\n' "$failed" | while IFS= read -r id; do
    line="FLAKE $id run $run"
    echo "$line"
    echo "$line" >> "$flakes"
    [ -z "${GITHUB_STEP_SUMMARY:-}" ] || echo "$line" >> "$GITHUB_STEP_SUMMARY"
done
exit 0
