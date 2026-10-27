"""Settings read from the environment at import, and where the bridge keeps its files."""

import math
import os


def env_number(name, default, lo, hi):
    """A numeric environment variable within [lo, hi]; anything else exits with a one-line message."""
    raw = os.environ.get(name, default)
    try:
        value = float(raw)
    except ValueError:
        value = math.nan
    if not lo <= value <= hi:
        raise SystemExit(f"{name} must be a number from {lo} to {hi}, got {raw!r}")
    return value


def _xdg(variable, fallback):
    return os.path.join(os.environ.get(variable) or os.path.expanduser(fallback), "agent-bridge")


HOST = os.environ.get("AGENT_BRIDGE_HOST", "127.0.0.1")
PORT = int(env_number("AGENT_BRIDGE_PORT", "1883", 1, 65535))
STALE_DAYS = env_number("AGENT_BRIDGE_STALE_DAYS", "3", 0, 36500)
STALE_S = max(1, int(STALE_DAYS * 86400))
STATE_DIR = _xdg("XDG_STATE_HOME", "~/.local/state")
CONFIG_DIR = _xdg("XDG_CONFIG_HOME", "~/.config")
# The bridge's other host files live next to this log: cli.lock, relay/, hook-state/ and admin-audit.jsonl.
LOG = os.environ.get("AGENT_BRIDGE_LOG") or os.path.join(STATE_DIR, "history.jsonl")


def data_path(*parts):
    """A path in the directory of the history log, which may not exist yet."""
    return os.path.join(os.path.dirname(LOG) or ".", *parts)


def data_dir():
    """The directory of the history log, created if missing."""
    os.makedirs(path := data_path(), exist_ok=True)
    return path
