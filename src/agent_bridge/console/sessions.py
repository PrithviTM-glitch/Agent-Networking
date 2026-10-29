"""Every agent-framework session and bus agent, joined by session id: names, liveness, kind and activity.

Sources: each installed framework's session list and transcripts (agent_bridge.host; for Claude Code, its session
registry and transcript files), bus presence, and each working directory's .git/HEAD."""

import json
import os
import re
import sys
import threading
import time

from .. import _native, config, host
from ..protocol import ORIGIN
from ..validation import ADMIN, UUID, check_ident, is_ident
from . import events
from .processes import Refused, verified

CHECK_S = 3
CTRL = re.compile(r"[\x00-\x1f\x7f-\x9f]")

lock = threading.Lock()
presence = {}
transcripts = {}
hosts = {}  # session -> the framework whose transcript it is
frameworks = {}  # agent or session key -> the framework last seen for it
kinds = {}
state = {"registry": None, "view": None}


def transcript(session):
    """The session's transcript reader from whichever framework keeps one, created once and cached."""
    with lock:
        if session not in transcripts:
            found = next(((h, t) for h in host.installed() if (t := h.transcript(session)) is not None), (None, None))
            hosts[session], transcripts[session] = found
        return transcripts[session]


def activity(session):
    t = transcripts.get(session)
    return list(t.recent) if t else []


def read_registry():
    """session id -> session record from every installed framework, each tagged with its framework's name."""
    out = {}
    for h in host.installed():
        for sid, e in h.sessions().items():
            out.setdefault(sid, {**e, "host": h.name})
    state["registry"] = out
    return out


def registry():
    return state["registry"] if state["registry"] is not None else read_registry()


# other per-session facts


def git_branch(cwd):
    """The branch checked out in cwd's repository from .git/HEAD, or None (no repo, detached HEAD)."""
    d = cwd
    try:
        for _ in range(64):
            git = os.path.join(d, ".git")
            if os.path.isfile(git):
                with open(git) as f:
                    first = f.readline(4096).strip()
                if not first.startswith("gitdir: "):
                    return None
                git = os.path.join(d, first[8:])
            if os.path.isdir(git):
                with open(os.path.join(git, "HEAD")) as f:
                    head = f.readline(4096).strip()
                if not head.startswith("ref: refs/heads/"):
                    return None
                return " ".join(CTRL.sub(" ", head[16:]).split())[:200] or None
            if (parent := os.path.dirname(d)) == d:
                return None
            d = parent
    except OSError:
        return None
    return None


# presence


def set_presence(addr, rec):
    if rec and addr == ADMIN:
        # The console is no claude session, even when started from one's shell: never offer to signal that.
        rec = {k: v for k, v in rec.items() if k not in ORIGIN}
    with lock:
        if rec and is_ident(addr):
            presence[addr] = rec
        else:
            presence.pop(addr, None)


def clear_presence():
    with lock:
        presence.clear()


def live_presence():
    cutoff = time.time() - config.STALE_S
    with lock:
        return {a: r for a, r in presence.items() if r["keep"] or r["seen"] >= cutoff}


# views


def framework(key, reg, proc, session):
    """The framework a session belongs to: its registry's, else its process's, else its transcript's, else the one
    last seen for it, else the only installed one."""
    named = reg and next((h for h in host.installed() if h.name == reg.get("host")), None)
    if h := named or (proc and host.by_process(proc[0])) or hosts.get(session):
        frameworks[key] = h
        return h
    only = host.installed()
    return frameworks.get(key) or (only[0] if len(only) == 1 else None)


def view(addr, rec, reg):
    """One session or agent as the page shows it. key is the address when on the bus, else the session id."""
    session = (reg and reg["session"]) or (rec and rec.get("session"))
    t = transcripts.get(session) if session else None
    pid, start = (
        (reg["pid"], reg["pid_start"])
        if reg and reg["pid"]
        else (rec.get("pid"), rec.get("pid_start"))
        if rec
        else (None, None)
    )
    p = verified(pid, start) if pid else None
    key = addr or session
    h = framework(key, reg, p, session)
    if p and h:
        kinds[key] = h.kind(argv0=p[3])
    entry = (reg and reg["entrypoint"]) or (t and t.entrypoint)
    kind = h.kind(entrypoint=entry) if entry and h else kinds.get(key)
    cwd = (reg and reg["cwd"]) or (t and t.cwd)
    branch = (cwd and git_branch(cwd)) or (t and t.branch if t and t.branch != "HEAD" else None)
    named = reg and reg["name_source"] != "derived" and reg["name"]
    task = named or (t and (t.title or (t.prompt and " ".join(t.prompt.split()[:6])[:48])))
    short = branch and branch.rsplit("/", 1)[-1][:24]
    fallback = addr or (reg and reg["name"]) or f"session {session[:8]}"
    seen = rec["seen"] if rec else (reg["updated"] or reg["started"] or 0)
    return {
        "key": key,
        "addr": addr,
        "session": session,
        "on_bus": rec is not None,
        "framework": h.name if h else None,
        "parent": rec and rec.get("parent"),
        "subagent": bool(rec and (rec.get("kind") == "subagent" or "parent" in rec)),
        "friendly": " · ".join(x for x in (short, task) if x) or fallback,
        "name": rec and rec["name"],
        "caps": rec["caps"] if rec else [],
        "keep": bool(rec and rec["keep"]),
        "rooms": [x[5:] for x in rec["topics"] if x.startswith("room/")] if rec else [],
        "topics": [x for x in rec["topics"] if not x.startswith("room/")] if rec else [],
        "seen": seen,
        "expires": None if not rec or rec["keep"] else rec["seen"] + config.STALE_S,
        "pid": pid,
        "pid_start": start,
        "alive": (p is not None) if pid else None,
        "kind": kind,
        "cwd": cwd,
        "branch": branch,
        "title": named or (t and t.title),
        "prompt": t and t.prompt and t.prompt[:300],
        "status": reg and reg["status"],
        "mode": reg and reg["mode"],
        "version": reg and reg["version"],
        "desktop_id": reg and reg["desktop_id"],
        "started": reg and reg["started"],
        "in_registry": reg is not None,
        "relay": _native.relay_live(session) if session else None,
    }


def entities():
    pres, regs = live_presence(), registry()
    out, joined = [], set()
    for addr, rec in pres.items():
        reg = regs.get(rec["session"].lower()) if rec.get("session") else None
        joined.add(reg and reg["session"])
        out.append(view(addr, rec, reg))
    cutoff = time.time() - config.STALE_S
    for session, reg in regs.items():
        if session not in joined and (reg["alive"] or (reg["updated"] or reg["started"] or 0) >= cutoff):
            out.append(view(None, None, reg))
    return sorted(out, key=lambda v: -v["seen"])


def find(body):
    """The entity a request names, by {"addr": ...} or {"session": ...}."""
    if "session" in body:
        s = body["session"]
        if not isinstance(s, str) or not UUID.fullmatch(s):
            raise Refused("session must be a session id")
        found = next((v for v in entities() if v["session"] == s.lower()), None)
    else:
        addr = check_ident(body.get("addr"), "address")
        found = next((v for v in entities() if v["addr"] == addr), None)
    if not found:
        raise Refused("no such agent or session")
    return found


def push_agents():
    v = entities()
    with lock:
        same = state["view"] == (j := json.dumps(v))
        state["view"] = j
    if not same:
        events.push({"kind": "agents", "agents": v})


def watch():
    """Death detection, registry and transcript refresh every few seconds while a page watches, so a dying session
    greys out live. With no page open it does nothing: every read computes what it needs."""
    while True:
        if not events.watching():
            time.sleep(CHECK_S)
            continue
        try:
            read_registry()
            for v in entities():
                if v["session"]:
                    transcript(v["session"]).refresh()
            push_agents()
        except Exception as e:  # noqa: BLE001  the watcher must outlive any single bad record
            print(f"watch: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
        time.sleep(CHECK_S)
