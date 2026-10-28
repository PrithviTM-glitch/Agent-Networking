"""Admin actions, each audited: messages, broadcasts, signals, restarts and ledger interventions."""

import collections
import fcntl
import json
import os
import re
import time

from .. import config
from ..validation import check_text, is_count, is_ident
from . import admin, events, kill, leaks, processes, sessions
from .processes import Refused

AUDIT = os.path.join(os.path.dirname(os.path.abspath(config.LOG)), "admin-audit.jsonl")
AUDIT_SHOWN = 300
ID16 = re.compile(r"[0-9a-f]{16}")


def _id16(body, key):
    if not isinstance(v := body.get(key), str) or not ID16.fullmatch(v):
        raise Refused(f"{key} must be 16 hex characters")
    return v


def _name(v):
    return v["addr"] or v["session"]


def send(b, _peer=None):
    console, kind, target = admin.need_bus(), b.get("kind"), b.get("target")
    if kind not in ("room", "dm", "topic"):
        raise Refused("kind must be room, dm or topic")
    {"room": console.say, "dm": console.dm, "topic": console.pub}[kind](target, b.get("text"))
    return f"{kind}:{target}", "sent"


def broadcast(b, _peer=None):
    admin.need_bus().broadcast(b.get("text"))
    return "broadcast", "sent"


def signal(b, _peer=None):
    v = sessions.find(b)
    if not v["alive"]:
        raise Refused(f"{_name(v)} is {'dead' if v['alive'] is False else 'not a claude session'}")
    return _name(v), processes.send_signal(v["pid"], v["pid_start"], b.get("signal"))


def _restartable(b):
    v = sessions.find(b)
    if v["alive"] is not False:
        raise Refused(f"{_name(v)} is {'alive' if v['alive'] else 'not a claude session'}, only dead sessions restart")
    if v["kind"] == "desktop":
        raise Refused("a Claude Desktop session: reopen it in the Claude Desktop app")
    cwd = v["cwd"]
    if v["kind"] != "cli" or not v["session"] or not cwd or not os.path.isabs(cwd) or not os.path.isdir(cwd):
        raise Refused("its session, kind or working directory is unknown")
    return v


def terminal(b, _peer=None):
    v = _restartable(b)
    return _name(v), processes.open_terminal(v["session"], v["cwd"])


def resume(b, _peer=None):
    v = _restartable(b)
    text = check_text(b.get("text"))
    if not text.strip():
        raise Refused("the message is empty")
    return _name(v), processes.resume(v["key"], v["session"], v["cwd"], text)


def release(b, _peer=None):
    lid = _id16(b, "lease_id")
    d = admin.dump()
    lease = next((x for x in d["leases"] if x["lease_id"] == lid), None)
    owner = lease and next((r["owner"] for r in d["resources"] if r["id"] == lease["resource"]), None)
    if not owner:
        raise Refused(f"no such lease: {lid}")
    admin.ledger_call("release", owner, lease_id=lid)
    return f"{lease['resource']} lease {lid} of {lease['holder']}", f"released as {owner}"


def revoke(b, _peer=None):
    rid = _id16(b, "request_id")
    row = next((r for r in admin.dump()["requests"] if r["id"] == rid), None)
    if not row:
        raise Refused(f"no such request: {rid}")
    admin.ledger_call("deny", row["owner"], request_id=rid)
    return f"{row['resource']} {row['level']} for {row['holder']}", f"denied as {row['owner']} (was {row['status']})"


def _kill_options(b):
    if not isinstance(kill_sessions := b.get("kill_sessions", False), bool):
        raise Refused("kill_sessions must be true or false")
    return b.get("signal", "terminate"), kill_sessions


def _kill(targets, signal_name, peer, room=None):
    """Execute planned targets, subagents first (a session killed before them would take their processes down
    unreported). A room kill audits each target as well as its summary."""
    results = []
    for t in sorted(targets, key=lambda t: t["class"] != "subagent"):
        try:
            result = kill.execute(t, signal_name)
        except Refused as e:
            result = f"refused: {e}"
        if room:
            audit("admin", peer, "room/kill", f"{room}:{t['addr']}", result)
        results.append({**kill.public(t), "result": result})
    return results


def kill_room(b, peer=None):
    """Kill the agents of a room as the preview showed; the confirmation must repeat the room name. Whole sessions
    are spared unless kill_sessions is true; `only`, a list of addresses, narrows it to those members."""
    room = b.get("room")
    if not isinstance(room, str) or b.get("confirm") != room:
        raise Refused("type the room name to confirm")
    signal_name, kill_sessions = _kill_options(b)
    targets = kill.plan(room, signal_name, kill_sessions)
    if (only := b.get("only")) is not None:
        if not isinstance(only, list) or not only or not all(isinstance(a, str) and is_ident(a) for a in only):
            raise Refused("only must be a non-empty list of addresses")
        if missing := set(only) - {t["addr"] for t in targets}:
            raise Refused(f"not members of #{room}: {', '.join(sorted(missing))}")
        targets = [t for t in targets if t["addr"] in only]
    results = _kill(targets, signal_name, peer, room)
    counts = collections.Counter(t["class"] for t in targets)
    summary = ", ".join(f"{n} {c}" for c, n in sorted(counts.items())) or "no members"
    mode = f"{signal_name}, whole sessions too" if kill_sessions else signal_name
    if only is not None:
        mode += f", {len(targets)} selected"
    return f"room:{room}", f"{mode}: {summary}", {"targets": results}


def stop_agent(b, peer=None):
    """Stop one agent with the room kill's rules: a subagent loses only its own processes, a session needs
    kill_sessions. The confirmation must repeat the address."""
    addr = b.get("addr")
    if not isinstance(addr, str) or b.get("confirm") != addr:
        raise Refused("type the agent's address to confirm")
    signal_name, kill_sessions = _kill_options(b)
    [result] = _kill(kill.plan(None, signal_name, kill_sessions, addr=addr), signal_name, peer)
    return addr, result["result"], {"targets": [result]}


def room_settings(b, _peer=None):
    room, mode, rules = b.get("room"), b.get("mode"), b.get("rules")
    info = admin.need_bus().room_set(room, mode=mode, rules=rules)
    return f"room:{room}", f"mode {info['mode']}, rules {len(info['rules'])} chars"


def room_moderator(b, _peer=None):
    room, action, addr = b.get("room"), b.get("action"), b.get("addr")
    admin.need_bus().mod(room, action, addr)
    return f"{room}:{addr}", f"moderator {action}"


MODERATION = {"mute": "muted", "unmute": "unmuted", "ban": "banned", "unban": "unbanned", "kick": "kicked"}


def room_member(b, _peer=None):
    room, action, addr = b.get("room"), b.get("action"), b.get("addr")
    if action not in MODERATION:
        raise Refused(f"action must be one of {', '.join(MODERATION)}")
    getattr(admin.need_bus(), action)(room, addr)
    return f"{room}:{addr}", MODERATION[action]


def leak_container(b, _peer=None):
    name = b.get("name")
    return name if isinstance(name, str) else None, leaks.remove_container(name)


def leak_process(b, _peer=None):
    pid, start = b.get("pid"), b.get("start")
    if not is_count(pid, 4194304) or not is_count(start, 2**53):
        raise Refused("pid and start must be positive integers")
    return f"pid {pid}", leaks.stop_process(pid, start)


ROUTES = {
    "/api/send": send,
    "/api/broadcast": broadcast,
    "/api/signal": signal,
    "/api/restart/terminal": terminal,
    "/api/restart/resume": resume,
    "/api/ledger/release": release,
    "/api/ledger/revoke": revoke,
    "/api/room/kill": kill_room,
    "/api/agent/stop": stop_agent,
    "/api/leaks/container": leak_container,
    "/api/leaks/process": leak_process,
    "/api/room/settings": room_settings,
    "/api/room/moderator": room_moderator,
    "/api/room/member": room_member,
}


ACTION_NAMES = {path.removeprefix("/api/") for path in ROUTES}


def audit(who, peer, action, target, result):
    entry = {"ts": time.time(), "who": who, "peer": peer, "action": action, "target": target, "result": result}
    os.makedirs(os.path.dirname(AUDIT), exist_ok=True)
    with open(AUDIT, "a", encoding="utf-8") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.write(json.dumps(entry) + "\n")
    events.push({"kind": "audit", "entry": entry})


def audit_query(action=None, target=None, since=None, until=None, limit=AUDIT_SHOWN):
    """The newest audit entries matching every given filter: action exactly, target as a substring, ts in range."""
    out = collections.deque(maxlen=limit)
    try:
        with open(AUDIT, encoding="utf-8", errors="replace") as f:
            for line in f:
                try:
                    e = json.loads(line)
                    ts, t = float(e["ts"]), e.get("target") or ""
                except (ValueError, KeyError, TypeError):
                    continue
                if (action and e.get("action") != action) or (target and target not in str(t)):
                    continue
                if (since is not None and ts < since) or (until is not None and ts > until):
                    continue
                out.append(e)
    except FileNotFoundError:
        pass
    return {"entries": list(out), "actions": sorted(ACTION_NAMES)}


def audit_counts():
    """action -> (entries, of which refused or failed), over the whole audit log."""
    counts = collections.defaultdict(lambda: [0, 0])
    try:
        with open(AUDIT, encoding="utf-8", errors="replace") as f:
            for line in f:
                try:
                    e = json.loads(line)
                    c = counts[str(e["action"])[:64]]
                except (ValueError, KeyError, TypeError):
                    continue
                c[0] += 1
                c[1] += str(e.get("result", "")).startswith(("refused", "error"))
    except FileNotFoundError:
        pass
    return dict(counts)


def audit_tail():
    try:
        with open(AUDIT, encoding="utf-8", errors="replace") as f:
            lines = collections.deque(f, AUDIT_SHOWN)
    except FileNotFoundError:
        return []
    out = []
    for line in lines:
        try:
            out.append(json.loads(line))
        except ValueError:
            pass
    return out
