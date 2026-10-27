"""Who an agent is: the Claude Code session and process it runs under, and the addresses derived from them."""

import functools
import hashlib
import os
import re

from .validation import ADMIN, UUID

ANCESTRY_MAX = 64
ANCESTOR = None  # where origin() starts walking up; the relay sets it, being detached from its claude process


def read_proc(pid):
    """comm, ppid and start time in clock ticks of a live process, or None."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            stat = f.read()
        # comm is parenthesised and may itself hold spaces or parens: only the last ")" ends it.
        fields = stat[stat.rfind(")") + 2 :].split()
        return {"comm": stat[stat.find("(") + 1 : stat.rfind(")")], "ppid": int(fields[1]), "start": int(fields[19])}
    except (OSError, ValueError, IndexError):
        return None


@functools.cache
def origin():
    """The Claude Code session and process this agent runs under, published in presence for the admin console."""
    out = {"session": sid} if UUID.fullmatch(sid := os.environ.get("CLAUDE_CODE_SESSION_ID", "")) else {}
    pid = ANCESTOR or os.getppid()
    for _ in range(ANCESTRY_MAX):
        if pid < 1 or not (p := read_proc(pid)):
            break
        if p["comm"] == "claude":
            return {**out, "pid": pid, "pid_start": p["start"]}
        pid = p["ppid"]
    return out


def project_name(path=None):
    """A Claude Code project's default agent name: its folder name as an identifier."""
    base = os.path.basename(path or os.environ.get("CLAUDE_PROJECT_DIR") or os.getcwd())
    name = re.sub(r"[^A-Za-z0-9_-]+", "-", base).strip("-")[:40]
    return name if name and name.lower() != ADMIN else "agent"


def short_hash(text):
    return hashlib.sha256(text.encode()).hexdigest()[:8]


def session_address(session):
    """The fixed bus address of a Claude Code session, shared by its MCP server and its relay."""
    return f"{project_name()}-{short_hash(session)}"
