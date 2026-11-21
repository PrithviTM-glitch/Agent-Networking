#!/usr/bin/env bash
# Fuzz the parsers (rust/fuzz, cargo-fuzz on nightly): the guard's shell lexer and decision, its quoting
# equivalence, guard paths, topics (sender-bound forms included), JSON envelopes and ledgerd's request decode.
#
#   scripts/verify-fuzz.sh [SECONDS] [TARGET...]
#
# SECONDS per target (default 60, the CI smoke); TARGETs default to all. Corpora are seeded from the golden files and
# the tests (rust/fuzz/corpus/<target>/seed-*), then grow with what libFuzzer finds. A crash, a timeout (10 s on one
# input) or a failed property stops the run with the reproducer under rust/fuzz/artifacts/<target>/; reproduce with
#   cargo +nightly fuzz run <target> rust/fuzz/artifacts/<target>/<file>
# Needs: rustup's nightly toolchain and cargo-fuzz (cargo install cargo-fuzz). Set CARGO_TARGET_DIR to build
# elsewhere than rust/fuzz/target.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
fuzz="$root/rust/fuzz"
seconds="${1:-60}"
shift || true
targets=("$@")
if [ ${#targets[@]} -eq 0 ]; then
    targets=(guard-shell guard-equivalence guard-paths topics envelope ledger-request)
fi
case "$seconds" in '' | *[!0-9]*) echo "verify-fuzz: SECONDS must be a whole number, got $seconds" >&2; exit 2 ;; esac

# Seeds: real inputs from the golden files and tests, rewritten as each target reads them.
python3 -I - "$root" <<'PY'
import json, pathlib, re, sys

root = pathlib.Path(sys.argv[1])
corpus = root / "rust/fuzz/corpus"

def put(target, items):
    d = corpus / target
    d.mkdir(parents=True, exist_ok=True)
    for old in d.glob("seed-*"):
        old.unlink()
    for i, data in enumerate(dict.fromkeys(items)):
        (d / f"seed-{i:04d}").write_bytes(data)

# shell commands: the guard's unit and end-to-end tests
text = (root / "rust/crates/core/src/guard/tests.rs").read_text() + (root / "tests/test_guard.py").read_text()
cmds = []
for s in re.findall(r'"((?:[^"\\]|\\.)*)"', text):
    s = s.replace("{l}", "/tmp/lane").replace("{lane}", "/tmp/lane").replace('\\"', '"').replace("\\n", "\n")
    if " " in s and not s.startswith(("should", "the ", "a ", "inside", "read ", "outside", "unresolvable")):
        cmds.append(s.encode())
put("guard-shell", cmds)

# messages: every JSON line of the CLI golden transcript, plus each on its sender-bound topic
golden = (root / "rust/crates/cli/tests/golden/cli.txt").read_text()
objs = []
for line in golden.splitlines():
    if line.startswith("{"):
        line = line.replace("<t>", "1793000000.5").replace('"<id>"', '"0123456789abcdef"')
        try:
            objs.append(json.loads(line))
        except ValueError:
            pass
topics = []
for o in objs:
    t, frm = o.get("topic"), o.get("from")
    if not isinstance(t, str):
        continue
    payload = {k: v for k, v in o.items() if k not in ("topic", "id", "sender", "for_me")}
    raw = json.dumps(payload).encode()
    topics.append(t.encode() + b"\n" + raw)
    if isinstance(frm, str) and t.count("/") == 1:
        topics.append(f"{t}/{frm}".encode() + b"\n" + raw)
        topics.append(f"{t}/{frm}x".encode() + b"\n" + raw)  # a spoof: the topic names someone else
put("topics", topics)

# envelopes: the same objects, historyd's golden requests and replies, ledgerd's golden replies
env = [json.dumps(o).encode() for o in objs]
for pair in json.loads((root / "tests/golden/historyd.json").read_text()):
    env += [pair[k].encode() for k in ("request", "reply") if isinstance(pair.get(k), str)]
for line in (root / "tests/golden/ledgerd.jsonl").read_text().splitlines():
    env.append(json.dumps(json.loads(line)["reply"]).encode())
put("envelope", env)

# ledger requests: the literal requests in the ledger tests (Python names stand in for addresses)
reqs = []
for f in sorted((root / "tests").glob("test_ledger*.py")):
    for m in re.findall(r'\{"op": "[a-z_]+"[^{}\n]*\}', f.read_text()):
        keep = ("true", "false", "null", "NaN", "Infinity")
        name = lambda x: x.group(0) if x.group(1) in keep else ': "al-1"'
        m = re.sub(r':\s*([A-Za-z_][A-Za-z_0-9]*)(?=[,}])', name, m)
        reqs.append(m.encode())
put("ledger-request", reqs)
print("verify-fuzz: seeded " + ", ".join(f"{p.name} {len(list(p.glob('seed-*')))}" for p in sorted(corpus.iterdir())))
PY

fail=0
for t in "${targets[@]}"; do
    mkdir -p "$fuzz/corpus/$t"
    log="$(mktemp)"
    echo "verify-fuzz: $t for ${seconds}s"
    if (cd "$fuzz" && cargo +nightly fuzz run "$t" "corpus/$t" -- \
        -max_total_time="$seconds" -timeout=10 -rss_limit_mb=2048 -print_final_stats=1) >"$log" 2>&1; then
        runs=$(grep -m1 '^stat::number_of_executed_units' "$log" | awk '{print $2}')
        echo "verify-fuzz: $t ok, ${runs:-?} execs ($(( ${runs:-0} / (seconds > 0 ? seconds : 1) ))/s)"
    else
        echo "verify-fuzz: $t FAILED" >&2
        grep -E 'panicked|ERROR|SUMMARY|Test unit written|assertion' "$log" | head -20 >&2
        fail=1
    fi
    rm -f "$log"
done
exit $fail
