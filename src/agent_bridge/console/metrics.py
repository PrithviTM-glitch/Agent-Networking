"""One metrics registry for everything the console knows, served as Prometheus text on GET /metrics.

Contents: sessions and agents (as info gauges for joins), rooms, relays, per-session CPU and memory, the ledger, the
audit log, leftovers, the console process, the machine, and the broker's $SYS statistics. Claude cost and tokens are
left out on purpose: Claude Code exports them itself, and re-exporting them into the same Prometheus would count them
twice. Labels hold names and ids only, never message text, so the number of series follows the number of agents.
A source that fails is skipped and flagged in agent_bridge_scrape_error{source}; the rest still renders."""

import os
import re
import sys
import threading
import time

from .. import relay
from . import actions, admin, capacity, events, leaks, sessions

CLK = os.sysconf("SC_CLK_TCK")
SLOW_S = 30
STARTED = time.time()
LEVEL = re.compile(r"\$SYS/broker/load/(.+)/(1min|5min|15min)")
SYS = {  # $SYS topic -> (metric, labels); mosquitto resets these when it restarts, so all are gauges
    "clients/connected": ("agent_bridge_broker_clients", {"state": "connected"}),
    "clients/disconnected": ("agent_bridge_broker_clients", {"state": "disconnected"}),
    "clients/active": ("agent_bridge_broker_clients", {"state": "active"}),
    "clients/inactive": ("agent_bridge_broker_clients", {"state": "inactive"}),
    "clients/expired": ("agent_bridge_broker_clients", {"state": "expired"}),
    "clients/total": ("agent_bridge_broker_clients", {"state": "total"}),
    "clients/maximum": ("agent_bridge_broker_clients", {"state": "maximum"}),
    "messages/received": ("agent_bridge_broker_messages", {"direction": "received"}),
    "messages/sent": ("agent_bridge_broker_messages", {"direction": "sent"}),
    "messages/stored": ("agent_bridge_broker_messages_stored", {}),
    "bytes/received": ("agent_bridge_broker_bytes", {"direction": "received"}),
    "bytes/sent": ("agent_bridge_broker_bytes", {"direction": "sent"}),
    "publish/messages/received": ("agent_bridge_broker_publish_messages", {"kind": "received"}),
    "publish/messages/sent": ("agent_bridge_broker_publish_messages", {"kind": "sent"}),
    "publish/messages/dropped": ("agent_bridge_broker_publish_messages", {"kind": "dropped"}),
    "publish/bytes/received": ("agent_bridge_broker_publish_bytes", {"direction": "received"}),
    "publish/bytes/sent": ("agent_bridge_broker_publish_bytes", {"direction": "sent"}),
    "retained": ("agent_bridge_broker_retained_messages", {}),
    "subscriptions/count": ("agent_bridge_broker_subscriptions", {}),
    "shared_subscriptions/count": ("agent_bridge_broker_shared_subscriptions", {}),
    "store/messages/count": ("agent_bridge_broker_store_messages", {}),
    "store/messages/bytes": ("agent_bridge_broker_store_bytes", {}),
    "heap/current": ("agent_bridge_broker_heap_bytes", {"kind": "current"}),
    "heap/maximum": ("agent_bridge_broker_heap_bytes", {"kind": "maximum"}),
    "uptime": ("agent_bridge_broker_uptime_seconds", {}),
}
HELP = {
    "agent_bridge_session": "A Claude session's name, one series per session, for joins on session_id",
    "agent_bridge_agent": "A bus agent and its session (a subagent carries its parent's)",
    "agent_bridge_membership": "A bus agent's membership of a room",
    "agent_bridge_agent_alive": "1 while the agent's claude process lives, 0 once it died",
    "agent_bridge_agents": "Listed agents and sessions by state",
    "agent_bridge_room_members": "Agents in the room",
    "agent_bridge_room_moderators": "Moderators of the room",
    "agent_bridge_room_announce": "1 if only moderators may post",
    "agent_bridge_room_muted": "Muted agents in the room's record",
    "agent_bridge_room_banned": "Banned agents in the room's record",
    "agent_bridge_session_relay": "1 while the session's relay holds its lock",
    "agent_bridge_relays_live": "Sessions with a live relay",
    "agent_bridge_session_cpu_percent": "CPU of the session's claude process and everything below it, % of one core",
    "agent_bridge_session_memory_bytes": "Resident memory of the session's process tree, shared pages per process",
    "agent_bridge_session_processes": "Processes in the session's tree",
    "agent_bridge_ledgers": "Ledgers in ledgerd",
    "agent_bridge_ledger_resources": "Resources by visibility",
    "agent_bridge_ledger_requests": "Access requests by status",
    "agent_bridge_ledger_leases": "Leases by level",
    "agent_bridge_ledger_waiters": "Acquires waiting in queue",
    "agent_bridge_audit_entries": "Audit log entries by action and outcome",
    "agent_bridge_leftover_containers": "Test broker containers left behind",
    "agent_bridge_leftover_processes": "Orphaned agent-bridge processes by kind",
    "agent_bridge_leftover_cleanup_paused": "1 while a test run blocks test cleanup",
    "agent_bridge_console_cpu_seconds_total": "CPU time of the console process",
    "agent_bridge_console_memory_bytes": "Resident memory of the console process",
    "agent_bridge_console_threads": "Threads of the console process",
    "agent_bridge_console_open_fds": "Open file descriptors of the console process",
    "agent_bridge_console_uptime_seconds": "Seconds since the console started",
    "agent_bridge_console_stream_clients": "Pages holding the live event stream",
    "agent_bridge_console_broker_connected": "1 while the console holds the admin identity on the broker",
    "agent_bridge_machine_load": "Load average",
    "agent_bridge_machine_cpus": "CPU cores",
    "agent_bridge_machine_memory_bytes": "Machine memory by kind",
    "agent_bridge_broker_info": "The broker's version",
    "agent_bridge_broker_load": "The broker's moving averages per minute, from $SYS/broker/load",
    "agent_bridge_scrape_error": "1 for each source that failed to read in this collection",
}
lock = threading.Lock()
slow = {"at": -SLOW_S, "values": None}


class Registry:
    def __init__(self):
        self.families = {}

    def add(self, metric, value, /, _type="gauge", **labels):
        """One sample; labels with an empty value are left out. _type is "gauge" or "counter". A second sample with
        the same labels replaces the first: Prometheus rejects a scrape that repeats a series."""
        fam = self.families.setdefault(metric, {"type": _type, "samples": {}})
        labels = {k: str(v) for k, v in labels.items() if v not in (None, "")}
        fam["samples"][tuple(sorted(labels.items()))] = float(value)


def _sessions(r):
    """Agents and sessions; returns room -> member count for _rooms."""
    best, states, members, relays, relayed = {}, {}, {}, 0, set()
    # Most recently seen first, so among equals the freshest names the session.
    for v in sessions.entities():
        sid = v["session"] or v["parent"]
        kind = "subagent" if v["subagent"] else v["kind"]
        state = {True: "alive", False: "dead", None: "unknown"}[v["alive"]]
        states[(state, v["on_bus"])] = states.get((state, v["on_bus"]), 0) + 1
        if sid:
            # One per session for name joins: the agent its relay holds, else a bus agent that is no subagent.
            rank = (bool(v["addr"]) and v["addr"] == relay.held(sid), v["on_bus"] and not v["subagent"])
            if sid not in best or rank > best[sid][0]:
                best[sid] = (rank, {"session_id": sid, "name": v["friendly"], "addr": v["addr"], "kind": kind})
        if v["session"] and v["relay"] is not None and v["session"] not in relayed:
            relayed.add(v["session"])
            r.add("agent_bridge_session_relay", int(v["relay"]), session_id=v["session"])
            relays += v["relay"]
        if v["addr"]:
            r.add("agent_bridge_agent", 1, session_id=sid, addr=v["addr"], name=v["friendly"], kind=kind)
            if v["alive"] is not None:
                r.add("agent_bridge_agent_alive", int(v["alive"]), addr=v["addr"], session_id=sid)
            for room in v["rooms"]:
                r.add("agent_bridge_membership", 1, session_id=sid, addr=v["addr"], name=v["friendly"], room=room)
                members[room] = members.get(room, 0) + 1
    for _, labels in best.values():
        r.add("agent_bridge_session", 1, **labels)
    for (state, on_bus), n in sorted(states.items()):
        r.add("agent_bridge_agents", n, state=state, on_bus=str(on_bus).lower())
    r.add("agent_bridge_relays_live", relays)
    return members


def _rooms(r, members):
    for room in sorted(set(members) | set(admin.settings)):
        rec = admin.settings.get(room, {})
        r.add("agent_bridge_room_members", members.get(room, 0), room=room)
        r.add("agent_bridge_room_moderators", len(rec.get("moderators", [])), room=room)
        r.add("agent_bridge_room_announce", int(rec.get("mode") == "announce"), room=room)
        r.add("agent_bridge_room_muted", len(rec.get("muted", [])), room=room)
        r.add("agent_bridge_room_banned", len(rec.get("banned", [])), room=room)


def _capacity(r):
    c = capacity.snapshot()
    for row in c["sessions"]:
        if row["session"]:
            r.add("agent_bridge_session_cpu_percent", row["cpu"], session_id=row["session"])
            r.add("agent_bridge_session_memory_bytes", row["rss"], session_id=row["session"])
            r.add("agent_bridge_session_processes", row["procs"], session_id=row["session"])
    m = c["machine"]
    for window, value in zip(("1m", "5m", "15m"), m["load"] or (), strict=False):
        r.add("agent_bridge_machine_load", value, window=window)
    r.add("agent_bridge_machine_cpus", m["cpus"])
    for kind in ("total", "available"):
        if m[f"mem_{kind}"] is not None:
            r.add("agent_bridge_machine_memory_bytes", m[f"mem_{kind}"], kind=kind)


def _leftovers():
    """Leftovers cost a docker call and a /proc scan, so they are read at most every SLOW_S seconds."""
    with lock:
        if time.monotonic() - slow["at"] >= SLOW_S:
            slow["at"], slow["values"] = time.monotonic(), leaks.report()
        return slow["values"]


def _ledger(r):
    d = admin.dump()
    r.add("agent_bridge_ledgers", len(d["ledgers"]))
    for key, field, metric in (
        ("resources", "visibility", "agent_bridge_ledger_resources"),
        ("requests", "status", "agent_bridge_ledger_requests"),
        ("leases", "level", "agent_bridge_ledger_leases"),
    ):
        counts = {}
        for row in d[key]:
            counts[row[field]] = counts.get(row[field], 0) + 1
        for value, n in sorted(counts.items()):
            r.add(metric, n, **{field: value})
    r.add("agent_bridge_ledger_waiters", len(d["waiters"]))


def _audit(r):
    for action, (n, bad) in sorted(actions.audit_counts().items()):
        r.add("agent_bridge_audit_entries", n - bad, action=action, outcome="done")
        r.add("agent_bridge_audit_entries", bad, action=action, outcome="refused")


def _leaks(r):
    lf = _leftovers()
    r.add("agent_bridge_leftover_containers", len(lf["containers"]))
    for kind in ("session", "test"):
        r.add("agent_bridge_leftover_processes", sum(1 for p in lf["processes"] if p["kind"] == kind), kind=kind)
    r.add("agent_bridge_leftover_cleanup_paused", int(bool(lf["busy"])))


def _console(r):
    with open("/proc/self/stat", "rb") as f:
        fields = f.read().rpartition(b")")[2].split()
    r.add("agent_bridge_console_cpu_seconds_total", (int(fields[11]) + int(fields[12])) / CLK, _type="counter")
    r.add("agent_bridge_console_memory_bytes", int(fields[21]) * os.sysconf("SC_PAGE_SIZE"))
    r.add("agent_bridge_console_threads", int(fields[17]))
    r.add("agent_bridge_console_open_fds", len(os.listdir("/proc/self/fd")))
    r.add("agent_bridge_console_uptime_seconds", time.time() - STARTED)
    with events.lock:
        r.add("agent_bridge_console_stream_clients", len(events.streams))
    r.add("agent_bridge_console_broker_connected", int(admin.held["bus"] is not None))


def _broker(r):
    for topic, text in sorted(admin.broker.items()):
        if topic == "$SYS/broker/version":
            r.add("agent_bridge_broker_info", 1, version=text.removeprefix("mosquitto version ")[:40])
            continue
        try:
            value = float(text.split()[0])
        except (ValueError, IndexError):
            continue
        if m := LEVEL.fullmatch(topic):
            r.add("agent_bridge_broker_load", value, kind=m[1].replace("/", "_"), window=m[2])
        elif (spec := SYS.get(topic.removeprefix("$SYS/broker/"))) is not None:
            r.add(spec[0], value, **spec[1])


def _safe(r, name, fn):
    try:
        return fn(r)
    except Exception as e:  # noqa: BLE001  one failing source must not hide the others
        print(f"metrics: {name}: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
        r.add("agent_bridge_scrape_error", 1, source=name)
        return None


def collect():
    r = Registry()
    members = _safe(r, "sessions", _sessions) or {}
    for name, fn in (
        ("capacity", _capacity),
        ("ledger", _ledger),
        ("audit", _audit),
        ("leftovers", _leaks),
        ("console", _console),
        ("broker", _broker),
    ):
        _safe(r, name, fn)
    _safe(r, "rooms", lambda reg: _rooms(reg, members))
    return r


# rendering


def _escape(v):
    return v.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _number(v):
    return str(int(v)) if v.is_integer() and abs(v) < 2**53 else repr(v)


def exposition(r):
    """Prometheus text format 0.0.4."""
    out = []
    for name, fam in sorted(r.families.items()):
        out.append(f"# HELP {name} {HELP.get(name, name)}")
        out.append(f"# TYPE {name} {fam['type']}")
        for labels, value in fam["samples"].items():
            inner = ",".join(f'{k}="{_escape(v)}"' for k, v in labels)
            out.append(f"{name}{{{inner}}} {_number(value)}" if inner else f"{name} {_number(value)}")
    return "\n".join(out) + "\n"
