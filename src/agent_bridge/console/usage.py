"""Claude Code usage per session from Prometheus, and the settings of the console's monitoring integrations.

Both optional. AGENT_BRIDGE_PROMETHEUS_URL (for example http://127.0.0.1:9090) turns on the usage numbers, read from
the claude_code_* metrics Claude Code exports with OpenTelemetry. AGENT_BRIDGE_GRAFANA_URL adds "Open in Grafana"
links to the agent-bridge dashboard. The console's own metrics are not here: Prometheus scrapes them from /metrics.

Usage is read only when a page asks, cached for a minute, with short timeouts; any failure leaves the numbers out
and says why. A session's totals are the sum over its series of each series' highest value in the last 30 days, so a
counter that restarts within one series (a resumed session) is undercounted."""

import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

PROMETHEUS = (os.environ.get("AGENT_BRIDGE_PROMETHEUS_URL") or "").rstrip("/") or None
GRAFANA = (os.environ.get("AGENT_BRIDGE_GRAFANA_URL") or "").rstrip("/") or None
TIMEOUT, CACHE_S, RANGE = 3, 60, "30d"
HINT = "Set AGENT_BRIDGE_PROMETHEUS_URL to the Prometheus that receives Claude Code's OpenTelemetry metrics."
# field -> (metric, split by its type label)
METRICS = {
    "cost": ("claude_code_cost_usage_USD_total", False),
    "tokens": ("claude_code_token_usage_tokens_total", True),
    "active": ("claude_code_active_time_seconds_total", True),
    "lines": ("claude_code_lines_of_code_count_total", True),
    "commits": ("claude_code_commit_count_total", False),
    "prs": ("claude_code_pull_request_count_total", False),
}
lock = threading.Lock()
state = {"at": -CACHE_S, "snap": None}


def _query(expr):
    url = f"{PROMETHEUS}/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    with urllib.request.urlopen(url, timeout=TIMEOUT) as r:
        d = json.load(r)
    if d.get("status") != "success" or d["data"]["resultType"] != "vector":
        raise ValueError(d.get("error") or "unexpected reply")
    return [(row["metric"], float(row["value"][1])) for row in d["data"]["result"]]


def _fetch():
    per = {}
    for field, (metric, typed) in METRICS.items():
        by = "session_id, type" if typed else "session_id"
        for labels, value in _query(f"sum by ({by}) (max_over_time({metric}[{RANGE}]))"):
            sid = labels.get("session_id", "").lower()
            if not sid:
                continue
            u = per.setdefault(sid, {"cost": 0.0, "tokens": {}, "active": {}, "lines": {}, "commits": 0, "prs": 0})
            if typed:
                u[field][labels.get("type", "other")] = value
            else:
                u[field] = value
    return per


def snapshot():
    """{"enabled", "hint", "grafana", "error", "ts", "sessions": {session id: usage}}, at most a minute old."""
    base = {"enabled": PROMETHEUS is not None, "hint": None if PROMETHEUS else HINT, "grafana": GRAFANA}
    if not PROMETHEUS:
        return {**base, "error": None, "ts": None, "sessions": {}}
    with lock:
        if state["snap"] and time.monotonic() - state["at"] < CACHE_S:
            return state["snap"]
        try:
            snap = {**base, "error": None, "ts": time.time(), "sessions": _fetch()}
        except (OSError, ValueError, KeyError, TypeError, IndexError) as e:
            old = state["snap"]["sessions"] if state["snap"] else {}
            snap = {**base, "error": f"Prometheus at {PROMETHEUS}: {e}", "ts": time.time(), "sessions": old}
        state["at"], state["snap"] = time.monotonic(), snap
        return snap
