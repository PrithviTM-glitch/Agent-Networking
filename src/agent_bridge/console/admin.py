"""The console on the bus: a read-only observer of every topic, and the reserved admin identity for writes, both
from the native core (agent_bridge._native). Ledger requests go over the admin Bus's connection while it is held,
history requests over connections of their own."""

import sys
import threading
import time

from .. import _native
from ..errors import HistoryError, LedgerError
from ..validation import ADMIN
from . import events, sessions
from .processes import ERRORS, Refused

BACKLOG = 2000
MUTATING = ("release", "deny")  # the ledger ops the console's actions apply
SYS_MAX = 500  # $SYS topics kept: mosquitto publishes about 60, so this only bounds a hostile flood
# Publishes mosquitto dropped since it started, at its inflight and queue limits: QoS 0 past the queue, and QoS 1 to a
# connected agent that doesn't drain, while the sender still saw success. The page and status show it.
DROPPED = "$SYS/broker/publish/messages/dropped"
held = {"bus": None}
settings = {}  # room -> its moderation record as rooms_view shows it, from the retained rooms/<room> topics
broker = {}  # $SYS topic -> its latest text, the broker's own statistics (mosquitto publishes every sys_interval)


def _dispatch(kind, key, value):
    if kind == "connected":
        sessions.clear_presence()
        broker.clear()
    elif kind == "sys":
        if key in broker or len(broker) < SYS_MAX:
            old, broker[key] = broker.get(key), value
            if key == DROPPED and value != old:
                events.push({"kind": "broker", "dropped": dropped()})
    elif kind == "presence":
        sessions.set_presence(key, value)
        sessions.push_agents()
    elif kind == "room":
        settings[key] = _native.rooms_view(key, value)
        events.push({"kind": "room", "info": settings[key]})
    elif kind == "message":
        events.push({"kind": "msg", "msg": {"ts": key, **value}})


def _observe():
    """Feed the console from the native Observer: # and $SYS/#, service RPC traffic already filtered out."""
    while True:
        try:
            obs = _native.Observer()
            while True:
                if event := obs.next(1.0):
                    _dispatch(*event)
        except ERRORS as e:
            print(f"observer: {e}", file=sys.stderr, flush=True)
            time.sleep(3)


def observe():
    threading.Thread(target=_observe, daemon=True).start()


def hold_admin():
    """Hold the reserved admin identity and drain what reaches it; the observer shows those messages."""
    while True:
        try:
            bus = _native.Bus.register_admin()
        except ERRORS as e:
            print(f"admin: {e}", file=sys.stderr, flush=True)
            time.sleep(3)
            continue
        held["bus"] = bus
        try:
            while True:
                bus.recv(wait=60)
        except ERRORS as e:
            print(f"admin: {e}", file=sys.stderr, flush=True)
            held["bus"] = None
            bus.close()


def dropped():
    """mosquitto's dropped-publish count since it started, or None before its first $SYS report."""
    try:
        return int(broker[DROPPED])
    except (KeyError, ValueError):
        return None


def need_bus():
    if not (bus := held["bus"]):
        raise Refused("the console is not connected to the broker")
    return bus


def backlog():
    """The last messages from historyd, for a newly opened console; none while historyd is down."""
    try:
        return _native.history_call("recent", as_=ADMIN, last=BACKLOG)["messages"]
    except HistoryError as e:
        print(f"admin: no backlog: {e}", file=sys.stderr, flush=True)
        return []


def ledger_call(op, as_, **fields):
    """Over the admin Bus's connection while it is held, so a ledger view opens no connection; else over one of its
    own, so the ledger stays readable while another console holds the admin identity."""
    try:
        if bus := held["bus"]:
            return bus.ledger(op, as_, **fields)
        return _native.ledger_call(op, as_, **fields)
    except ERRORS as e:
        why = " ".join(str(e).split())
        if op in MUTATING and isinstance(e, LedgerError) and why.startswith("no reply"):
            # The request may have reached ledgerd and only its reply been lost, e.g. to another console that took
            # the admin identity mid-call.
            why += "; it may still have been applied, check the ledger view"
        raise Refused(f"ledger: {why}") from None


def dump():
    d = ledger_call("dump", ADMIN)
    d.pop("ok", None)
    return d


def ledger_of(addr, d):
    """One agent's slice of a dump: what it owns, what it was granted, its leases and waits."""
    mine = {r["id"] for r in d["resources"] if r["owner"] == addr}
    return {
        "resources": [r for r in d["resources"] if r["owner"] == addr],
        "given": [r for r in d["requests"] if r["owner"] == addr],
        "held": [r for r in d["requests"] if r["holder"] == addr],
        "leases": [x for x in d["leases"] if x["holder"] == addr or x["resource"] in mine],
        "waiters": [w for w in d["waiters"] if w["holder"] == addr or w["resource"] in mine],
        "truncated": d.get("truncated", False),
    }
