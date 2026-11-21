#!/bin/sh
# Print which formal-methods jobs a range of commits touches, as scripts/verify-paths.sh does for a list of paths: the
# whole range BASE..HEAD, so a spec changed in any commit of a push or pull request runs its job, not only one changed
# in the last. A range that can't be diffed runs every job: an empty BASE, a new branch (BASE all zeros), a BASE the
# clone doesn't have (after a force push, or a shallow fetch) or a diff that fails.
#
#   scripts/verify-changes.sh BASE HEAD >> "$GITHUB_OUTPUT"
set -eu
here=$(cd "$(dirname "$0")" && pwd)
root=${VERIFY_ROOT:-$(cd "$here/.." && pwd)}
base=${1:-} head=${2:?usage: verify-changes.sh BASE HEAD}

everything() { echo "verify-changes: $1, so every job runs" >&2; exec "$here/verify-paths.sh" --all; }

case $base in
    "" | *[!0]*) ;;
    *) everything "a new branch (no previous commit)" ;;
esac
[ -n "$base" ] || everything "no base commit"
git -C "$root" cat-file -e "$base^{commit}" 2>/dev/null || everything "base $base isn't in this clone"
changed=$(git -C "$root" diff --name-only "$base" "$head") || everything "git diff $base $head failed"
printf '%s\n' "$changed" >&2
printf '%s\n' "$changed" | "$here/verify-paths.sh"
