#!/bin/sh
# Print the path of the pinned tla2tools.jar (TLC), checked by sha256 before use. It is $TLA2TOOLS_JAR or $TLA2TOOLS
# when set, else DIR/tla2tools.jar, downloaded from the pinned release when missing (CI caches DIR). A jar whose hash
# isn't the pinned one is refused, wherever it came from.
#
#   jar=$(scripts/tla2tools.sh DIR)
set -eu
version=v1.7.4
sha256=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
url="https://github.com/tlaplus/tlaplus/releases/download/$version/tla2tools.jar"

dir=${1:?usage: tla2tools.sh DIR}
jar=${TLA2TOOLS_JAR:-${TLA2TOOLS:-$dir/tla2tools.jar}}
if [ ! -f "$jar" ]; then
    mkdir -p "$(dirname "$jar")"
    curl -fsSL --retry 3 -o "$jar.part" "$url"
    mv "$jar.part" "$jar"
fi
got=$(sha256sum "$jar" | cut -d' ' -f1)
if [ "$got" != "$sha256" ]; then
    echo "tla2tools: $jar has sha256 $got, not the pinned $version's $sha256; refusing it" >&2
    exit 1
fi
echo "$jar"
