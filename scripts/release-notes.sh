#!/bin/sh
# Print VERSION's section of the changelog (default CHANGELOG.md next to this script's repo), the release notes for its
# tag: the lines between its "## [VERSION]" heading and the next heading or the link definitions, then its compare
# link. Exits 1 with a message on stderr when the section is missing or empty, so a release cannot go out without one.
set -eu
version=${1:?usage: scripts/release-notes.sh VERSION [CHANGELOG]}
version=${version#v}
changelog=${2:-$(cd "$(dirname "$0")/.." && pwd)/CHANGELOG.md}
notes=$(awk -v head="## [$version]" '
    index($0, "## [") == 1 || /^\[[^]]+\]: / { if (found) exit }
    found { print }
    index($0, head) == 1 && (length($0) == length(head) || substr($0, length(head) + 1, 1) == " ") { found = 1 }
' "$changelog")
if ! printf '%s\n' "$notes" | grep -q '[^[:space:]]'; then
    echo "release-notes: $changelog has no section for $version (a \"## [$version] - DATE\" heading with entries)" >&2
    exit 1
fi
# Trim the blank lines around the section.
printf '%s\n' "$notes" | awk 'NF { for (; blank > 0; blank--) print ""; print; seen = 1; next } seen { blank++ }'
link=$(grep "^\[$version\]: " "$changelog" | head -1 | cut -d' ' -f2-) || true
if [ -n "$link" ]; then
    printf '\n**Full changes:** %s\n' "$link"
fi
