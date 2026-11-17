"""The relay's wake filters from Python (_native.relay_wake_modes and relay_set_wake), as the MCP server uses them.
No broker needed: the filters are a file in the state directory, which each case points at a temp dir."""

import json
import os
import subprocess
import sys

SESSION = "6e5d4c3b-2a19-4807-b6f5-e4d3c2b1a098"

SCRIPT = """
import json, sys
from agent_bridge import _native
session = sys.argv[1]
out = {"modes": list(_native.RELAY_WAKE_MODES), "empty": _native.relay_wake_modes(session)}
out["set"] = _native.relay_set_wake(session, "mrs", "mentions")
out["more"] = _native.relay_set_wake(session, "noisy", "admin")
out["cleared"] = _native.relay_set_wake(session, "mrs", "all")
out["read"] = _native.relay_wake_modes(session)
try:
    _native.relay_set_wake(session, "mrs", "loud")
except Exception as e:
    out["error"] = [type(e).__name__, str(e)]
print(json.dumps(out))
"""


def test_wake_filters_through_native(tmp_path):
    env = {k: v for k, v in os.environ.items() if not k.startswith("CLAUDE_CODE_") and k != "AGENT_BRIDGE_LOG"}
    env["XDG_STATE_HOME"] = str(tmp_path)
    done = subprocess.run([sys.executable, "-c", SCRIPT, SESSION], env=env, capture_output=True, text=True, timeout=30)
    assert done.returncode == 0, done.stderr
    out = json.loads(done.stdout)
    assert out["modes"] == ["all", "mentions", "admin"]
    assert out["empty"] == {}
    assert out["set"] == {"mrs": "mentions"}
    assert out["more"] == {"mrs": "mentions", "noisy": "admin"}
    assert out["cleared"] == {"noisy": "admin"}
    assert out["read"] == {"noisy": "admin"}
    assert out["error"] == ["BridgeError", 'wake mode: one of all, mentions, admin, got "loud"']
    # the file the relay reads, private to the session's state
    stored = json.loads((tmp_path / "agent-bridge" / "relay" / f"{SESSION}.wake").read_text())
    assert stored == {"rooms": {"noisy": "admin"}}
