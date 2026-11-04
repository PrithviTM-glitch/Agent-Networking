#!/bin/sh
# Check that a release's distribution directory (default dist) holds exactly what gets published: one wheel per
# architecture (x86_64 and aarch64) and one sdist, all of VERSION when given, and nothing else. The release workflow
# runs it before the GitHub release and before the PyPI upload, which would refuse any other file.
set -eu
dir=${1:-dist}
version=${2:-}
version=${version#v}
fail() { echo "check-dist: $*" >&2; exit 1; }
[ -d "$dir" ] || fail "$dir is not a directory"
case $version in *[!0-9A-Za-z.+-]*) fail "bad version $version" ;; esac
v=${version:-*}  # unquoted in the patterns below, so the default matches any version
x86=0 arm=0 sdists=0
for f in "$dir"/* "$dir"/.[!.]*; do
    [ -e "$f" ] || continue
    name=${f##*/}
    case $name in
        agent_bridge-$v-*manylinux*_x86_64.whl) x86=$((x86 + 1)) ;;
        agent_bridge-$v-*manylinux*_aarch64.whl) arm=$((arm + 1)) ;;
        agent_bridge-$v.tar.gz) sdists=$((sdists + 1)) ;;
        *) fail "$name is not a wheel or sdist of agent-bridge${version:+ $version}; only those may be published" ;;
    esac
done
[ "$x86" -eq 1 ] && [ "$arm" -eq 1 ] || fail "$dir has $x86 x86_64 and $arm aarch64 wheels, expected one of each"
[ "$sdists" -eq 1 ] || fail "$dir has $sdists sdists, expected 1"
echo "check-dist: $dir holds 2 wheels and 1 sdist${version:+ of $version}"
