"""No test asserts on time or on "has it happened yet" after a fixed sleep: every red CI run this project had was one
of those. Waits go through tests/waits.py (wait_until, holds_for, and their async forms) instead.

Two patterns are refused in tests/*.py:
- a sleep (time, anyio or asyncio) whose next line of code is an assert: wait for the condition, or use holds_for
  for an absence check;
- an upper bound on elapsed time (elapsed < N, monotonic() - t < N, ["ms"] < N, took < N) without a "# ceiling:"
  comment on that line or the one above, saying which failure the bound still catches. A lower bound is fine:
  slowness can only make it pass.
"""

import re
from pathlib import Path

TESTS = Path(__file__).resolve().parent
SKIP = {"waits.py", Path(__file__).name}
SLEEP = re.compile(r"\b(?:time|anyio|asyncio)\.sleep\(")
UPPER = re.compile(
    r"(?:monotonic|perf_counter|time)\(\)\s*-\s*[\w.\[\]\"']+\s*<"
    r"|\belapsed\b\s*<"
    r"|\[[\"'](?:ms|took|secs|elapsed)[\"']\]\s*<"
    r"|\btook\b\s*<"
)


def offences(text):
    """(line number, what) for each refused pattern in text."""
    lines = text.splitlines()
    found = []
    for i, line in enumerate(lines):
        code = line.split("#", 1)[0].strip()
        if not code:
            continue
        if SLEEP.search(code):
            j = i + 1
            while j < len(lines) and (not lines[j].strip() or lines[j].strip().startswith("#")):
                j += 1
            if j < len(lines) and lines[j].strip().startswith("assert"):
                found.append((i + 1, "a sleep then an assert: wait for the condition (waits.wait_until/holds_for)"))
        if code.startswith("assert") and UPPER.search(code):
            marked = "# ceiling:" in line or (i > 0 and "# ceiling:" in lines[i - 1])
            if not marked:
                found.append((i + 1, 'an upper bound on time without a "# ceiling:" saying what it still catches'))
    return found


def test_the_checker_bites():
    """The patterns it must refuse, and the forms it must let through."""
    assert offences("time.sleep(1)\nassert x == 1\n"), "sleep then assert"
    assert offences("    await anyio.sleep(0.5)  # wait\n\n    assert ok\n"), "async sleep, a blank line, then assert"
    assert offences("assert time.monotonic() - t0 < 2\n"), "an unmarked upper bound"
    assert offences('assert elapsed < 1, "slow"\n') and offences('assert r["ms"] < 200\n')
    assert offences('assert answer["took"] < 15\n')
    assert not offences("# ceiling: the 30 s wait would fail it\nassert time.monotonic() - t0 < 10\n")
    assert not offences("assert time.monotonic() - t0 >= 0.4\n"), "a lower bound is fine"
    assert not offences("time.sleep(0.05)\nx += 1\n"), "a sleep in a polling loop"
    assert not offences("# time.sleep(1)\nassert x\n"), "a comment is not code"


def test_no_timing_asserts_in_tests():
    bad = [
        f"{p.name}:{n}: {what}"
        for p in sorted(TESTS.glob("*.py"))
        if p.name not in SKIP
        for n, what in offences(p.read_text())
    ]
    assert not bad, "\n".join(bad)
