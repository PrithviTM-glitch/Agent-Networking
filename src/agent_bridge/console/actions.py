"""Admin actions, each audited: messages, broadcasts, signals, restarts and ledger interventions."""

import collections
import fcntl
import json
import os
import re
import time

from .. import config
from ..validation import check_text
from . import admin, events, kill, processes, sessions
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


def kill_room(b, peer=None):
    """Kill every agent in a room as the preview showed; the confirmation must repeat the room name. Whole sessions
    are spared unless kill_sessions is true."""
    room = b.get("room")
    if not isinstance(room, str) or b.get("confirm") != room:
        raise Refused("type the room name to confirm")
    if not isinstance(kill_sessions := b.get("kill_sessions", False), bool):
        raise Refused("kill_sessions must be true or false")
    signal_name = b.get("signal", "terminate")
    # Subagents first: a session killed before them would take their processes down unreported.
    targets = sorted(kill.plan(room, signal_name, kill_sessions), key=lambda t: t["class"] != "subagent")
    results = []
    for t in targets:
        try:
            result = kill.execute(t, signal_name)
        except Refused as e:
            result = f"refused: {e}"
        audit("admin", peer, "room/kill", f"{room}:{t['addr']}", result)
        results.append({**kill.public(t), "result": result})
    counts = collections.Counter(t["class"] for t in targets)
    summary = ", ".join(f"{n} {c}" for c, n in sorted(counts.items())) or "no members"
    mode = f"{signal_name}, whole sessions too" if kill_sessions else signal_name
    return f"room:{room}", f"{mode}: {summary}", {"targets": results}


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


ROUTES = {
    "/api/send": send,
    "/api/broadcast": broadcast,
    "/api/signal": signal,
    "/api/restart/terminal": terminal,
    "/api/restart/resume": resume,
    "/api/ledger/release": release,
    "/api/ledger/revoke": revoke,
    "/api/room/kill": kill_room,
    "/api/room/settings": room_settings,
    "/api/room/moderator": room_moderator,
    "/api/room/member": room_member,
}


def audit(who, peer, action, target, result):
    entry = {"ts": time.time(), "who": who, "peer": peer, "action": action, "target": target, "result": result}
    os.makedirs(os.path.dirname(AUDIT), exist_ok=True)
    with open(AUDIT, "a", encoding="utf-8") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.write(json.dumps(entry) + "\n")
    events.push({"kind": "audit", "entry": entry})


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
