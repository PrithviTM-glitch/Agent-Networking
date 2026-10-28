"""Claude Code's session registry: CLAUDE_CONFIG_DIR (or ~/.claude)/sessions/<pid>.json, one file per running
session, written by Claude Code itself. Internal and undocumented (this was built against 2.1.299), so every field is
optional, checked for type and range, and a malformed file is skipped. The <pid>.<hash>.key files next to them are
never opened."""

import json
import os
import re

from ..validation import UUID
from .transcripts import CLAUDE_DIR, CTRL, clean

REGISTRY = os.path.join(CLAUDE_DIR, "sessions")
REGISTRY_MAX = 64 * 1024
PID_MAX, TICKS_MAX = 4194304, 2**53
REGISTRY_FILE = re.compile(r"[0-9]{1,7}\.json")


def _count(v, cap):
    if isinstance(v, str) and v.isdigit() and len(v) < 20:
        v = int(v)
    return v if isinstance(v, int) and not isinstance(v, bool) and 0 < v <= cap else None


def _text(v, cap=200):
    return clean(v)[:cap] or None if isinstance(v, str) else None


def _ms(v):
    return v / 1000 if isinstance(v, (int, float)) and not isinstance(v, bool) and 0 < v < 1e13 else None


def _registry_entry(path):
    try:
        if os.path.getsize(path) > REGISTRY_MAX:
            return None
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
    except (OSError, ValueError, RecursionError):
        return None
    if not isinstance(d, dict) or not isinstance(d.get("sessionId"), str) or not UUID.fullmatch(d["sessionId"]):
        return None
    cwd = d.get("cwd")
    cwd = cwd if isinstance(cwd, str) and os.path.isabs(cwd) and len(cwd) < 4096 and not CTRL.search(cwd) else None
    pid, start = _count(d.get("pid"), PID_MAX), _count(d.get("procStart"), TICKS_MAX)
    return {
        "session": d["sessionId"].lower(),
        "pid": pid if start else None,
        "pid_start": start if pid else None,
        "cwd": cwd,
        "entrypoint": _text(d.get("entrypoint")),
        "mode": _text(d.get("kind")),
        "status": _text(d.get("status")),
        "version": _text(d.get("version")),
        "name": _text(d.get("name")),
        "name_source": _text(d.get("nameSource")),
        "desktop_id": _text(d.get("hostSessionId")),
        "started": _ms(d.get("startedAt")),
        "updated": _ms(d.get("updatedAt")),
    }


def read(alive):
    """session id -> registry entry; of several files for one session, the live one, else the newest. alive(pid,
    start) says whether a recorded process still runs."""
    out = {}
    try:
        names = os.listdir(REGISTRY)
    except OSError:
        names = []
    for name in names:
        # Only <pid>.json: the <pid>.<hash>.key files next to them are never opened.
        if not REGISTRY_FILE.fullmatch(name) or not (e := _registry_entry(os.path.join(REGISTRY, name))):
            continue
        e["alive"] = bool(e["pid"] and alive(e["pid"], e["pid_start"]))
        old = out.get(e["session"])
        if not old or (e["alive"], e["started"] or 0) > (old["alive"], old["started"] or 0):
            out[e["session"]] = e
    return out
