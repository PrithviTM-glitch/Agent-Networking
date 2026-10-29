"""Regenerate ledgerd.jsonl from the Python ledgerd: python tests/golden/make_ledgerd.py. Run it only when the reference
behaviour changes on purpose, then review the diff; test_ledger_golden.py checks any ledgerd against the file."""

import os
import subprocess
import sys
from pathlib import Path

tests = Path(__file__).resolve().parent.parent
env = {k: v for k, v in os.environ.items() if k != "AGENT_BRIDGE_LEDGERD"}
env["LEDGER_GOLDEN_WRITE"] = "1"
sys.exit(subprocess.call([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "test_ledger_golden.py"],
                         cwd=tests, env=env))  # fmt: skip
