"""The room kill switch: stop every agent in a room, a whole Claude session or only a subagent's own bus processes.

A subagent's presence carries its parent session's claude pid (the nearest claude ancestor of the command that
registered it), so that pid is never signalled for a subagent: only the subagent's own agent-bridge commands are.
A whole session is killed only through its primary agent, and only when the admin asks for whole sessions."""

import os
import signal

from .. import relay
from ..identity import short_hash
from ..presence import wipe
from ..validation import ADMIN, BridgeError, check_ident
from . import processes, sessions
from .processes import Refused

SIGNALS = {"terminate": signal.SIGTERM, "kill": signal.SIGKILL}


def acts_as(argv, addr):
    """True if argv is an agent-bridge command acting as addr: `agent-bridge <command> <addr> ...`."""
    return any(os.path.basename(a) == "agent-bridge" and argv[i + 2 : i + 3] == [addr] for i, a in enumerate(argv))


def bus_processes(claude_pid, addr, procs):
    """The subagent's own processes: below its claude process, ours, and running agent-bridge as addr."""
    uid, names = os.getuid(), processes.session_names()
    return [
        {"pid": pid, "start": procs[pid]["start"], "cmd": " ".join(procs[pid]["argv"])[:200]}
        for pid in processes.descendants(claude_pid, procs)
        if procs[pid]["uid"] == uid and procs[pid]["comm"] not in names and acts_as(procs[pid]["argv"], addr)
    ]


def primary(session, group):
    """The session's own agent among group (the live agents carrying session): the one its relay holds, else the
    session-derived address, else the only one. None when that is ambiguous."""
    if (held := relay.held(session)) in group:
        return held
    derived = [a for a in group if a.endswith(f"-{short_hash(session)}")]
    if len(derived) == 1:
        return derived[0]
    return group[0] if len(group) == 1 else None


def _target(addr, rec, ctx, sig, kill_sessions):
    """One room member: its class and the exact action the kill will take."""
    out = {
        "addr": addr,
        "friendly": sessions.view(addr, rec, ctx["regs"].get((rec.get("session") or "").lower()))["friendly"],
        "session": rec.get("session") or rec.get("parent"),
        "pid": rec.get("pid"),
        "signal_claude": False,
        "procs": [],
        "wipe": False,
    }
    name = f"SIG{sig.name[3:]}"
    pid, start, session = rec.get("pid"), rec.get("pid_start"), rec.get("session")
    if addr == ADMIN:
        return {**out, "class": "refused", "reason": "the console", "action": "skip: the console's own agent"}
    group = ctx["groups"].get(session, [])
    main = session and primary(session, group)
    if rec.get("kind") == "subagent" or "parent" in rec:
        reason = "marked as a subagent"
    elif not session:
        reason = "no session"
    elif main != addr:
        reason = f"shares session {session} with " + (f"its own agent {main}" if main else "other agents, none primary")
    else:
        reason = None
    subagent = reason is not None
    reason = reason or f"the primary agent of session {session}"
    out["reason"] = reason
    why = None
    if not pid or not processes.verified(pid, start):
        why = "no claude process recorded" if not pid else f"pid {pid} is not the claude process that registered it"
    elif not subagent and not (
        (reg := ctx["regs"].get(session.lower())) and (reg["pid"], reg["pid_start"]) == (pid, start)
    ):
        why = f"session {session} is not in Claude Code's session registry with pid {pid}"
    if why and not subagent and not kill_sessions:
        return {**out, "class": "unverifiable", "action": f"skip: claims a whole session ({why})"}
    if why:
        return {**out, "class": "unverifiable", "wipe": True, "action": f"wipe the agent only ({why})"}
    if not subagent:
        others = sorted(
            a
            for a, r in ctx["presence"].items()
            if a != addr and (session in (r.get("session"), r.get("parent")) or r.get("pid") == pid)
        )
        also = f"; this also ends {', '.join(others)}" if others else ""
        if not kill_sessions:
            return {
                **out,
                "class": "session",
                "others": others,
                "action": f"skip: would end the WHOLE session {session}{also} (tick kill whole sessions)",
            }
        if pid in ctx["mine"]:
            return {**out, "class": "refused", "action": f"skip: claude pid {pid} runs the console"}
        return {
            **out,
            "class": "session",
            "others": others,
            "signal_claude": True,
            "pid_start": start,
            "action": f"{name} claude pid {pid}, ending the WHOLE session {session}{also}",
        }
    found = [p for p in bus_processes(pid, addr, ctx["procs"]) if p["pid"] not in ctx["mine"]]
    which = ", ".join(str(p["pid"]) for p in found)
    action = (
        f"{name} its bus processes {which}, then wipe the agent"
        if found
        else "no bus processes running, wipe the agent"
    )
    return {
        **out,
        "class": "subagent",
        "procs": found,
        "wipe": True,
        "action": f"{action}; parent claude pid {pid} keeps running",
    }


def plan(room=None, signal_name="terminate", kill_sessions=False, addr=None):
    """Every member of room (or just the agent addr) with its class and action, without doing anything."""
    if addr is None:
        room = check_ident(room, "room")
    else:
        addr = check_ident(addr, "address")
    if signal_name not in SIGNALS:
        raise Refused(f"signal must be one of {', '.join(SIGNALS)}")
    live = sessions.live_presence()
    groups = {}
    for a, r in sorted(live.items()):
        if r.get("session") and r.get("kind") != "subagent" and "parent" not in r:
            groups.setdefault(r["session"], []).append(a)
    ctx = {
        "presence": live,
        "groups": groups,
        "regs": sessions.read_registry(),
        "procs": processes.table(),
        "mine": processes.lineage(),
    }
    if addr is not None and addr not in live:
        raise Refused(f"unknown agent {addr}")
    return [
        _target(a, r, ctx, SIGNALS[signal_name], kill_sessions)
        for a, r in sorted(live.items())
        if (a == addr if addr is not None else f"room/{room}" in r["topics"])
    ]


def execute(t, signal_name):
    """Carry out one planned target; returns what happened, one line."""
    sig, done = SIGNALS[signal_name], []
    if t["signal_claude"]:
        start = t["pid_start"]
        processes.pidfd_signal(t["pid"], lambda p: processes.verified(p, start), sig, "that claude session")
        return f"session killed: sent SIG{sig.name[3:]} to claude pid {t['pid']}"
    for p in t["procs"]:

        def same(pid, p=p):
            i = processes.info(pid)
            return i and i["start"] == p["start"] and i["uid"] == os.getuid() and acts_as(i["argv"], t["addr"])

        try:
            processes.pidfd_signal(p["pid"], same, sig, "that agent-bridge command")
            done.append(f"signalled {p['pid']}")
        except Refused as e:
            done.append(str(e))
    if t["wipe"]:
        try:
            wipe(t["addr"])
            done.append("wiped the agent")
        except BridgeError as e:
            done.append(f"wipe failed: {e}")
    prefix = "subagent stopped" if t["class"] == "subagent" else t["class"]
    return f"{prefix}: {'; '.join(done) or t['action']}"


def public(t):
    """A target as the page shows it."""
    return {k: t.get(k) for k in ("addr", "friendly", "session", "pid", "class", "reason", "action")} | {
        "others": t.get("others", []),
        "procs": [{"pid": p["pid"], "cmd": p["cmd"]} for p in t["procs"]],
    }
