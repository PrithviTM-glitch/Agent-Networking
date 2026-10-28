"""Who an agent is: the session and process of the agent framework it runs under (see host.py), and the addresses
derived from them."""

import functools
import hashlib
import os
import re

from . import host
from .validation import ADMIN

ANCESTOR = None  # where origin() starts walking up; the relay sets it, being detached from its session's process


def read_proc(pid):
    """comm, ppid and start time in clock ticks of a live process, or None."""
    p = host.stat(pid)
    return p and {"comm": p["comm"], "ppid": p["ppid"], "start": p["start"]}


@functools.cache
def origin():
    """The session and process this agent runs under, published in presence for the admin console."""
    h = host.current()
    out = {"session": sid} if (sid := h.session()) else {}
    if p := h.process(ANCESTOR):
        out.update(pid=p[0], pid_start=p[1])
    return out


def project_name(path=None):
    """A session's default agent name: its project folder's name as an identifier."""
    project = next((d for h in host.installed() if (d := h.project_dir())), None)
    base = os.path.basename(path or project or os.getcwd())
    name = re.sub(r"[^A-Za-z0-9_-]+", "-", base).strip("-")[:40]
    return name if name and name.lower() != ADMIN else "agent"


def short_hash(text):
    return hashlib.sha256(text.encode()).hexdigest()[:8]


def session_address(session):
    """The fixed bus address of a session, shared by its MCP server and its relay."""
    return f"{project_name()}-{short_hash(session)}"
