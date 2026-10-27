"""The console on the bus: a read-only observer of every topic, and the reserved admin identity for writes."""

import collections
import sys
import time
import uuid

import paho.mqtt.client as mqtt

from .. import config
from ..bus import Bus
from ..history import parse_entry
from ..ledger import client as ledger
from ..protocol import parse_message, parse_presence
from ..validation import ADMIN, BridgeError
from . import events, sessions
from .processes import Refused

BACKLOG = 2000
held = {"bus": None}


def _on_connect(c, *_):
    sessions.clear_presence()
    c.subscribe("#", qos=0)


def _on_message(_c, _u, m):
    if m.topic.startswith("agents/"):
        sessions.set_presence(m.topic[7:], m.payload and parse_presence(m.payload))
        sessions.push_agents()
    elif not m.topic.startswith("ledger/") and (msg := parse_message(m.topic, m.payload)):
        events.push({"kind": "msg", "msg": {"ts": time.time(), **msg}})


def observe():
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"monitor-{uuid.uuid4().hex[:8]}", protocol=mqtt.MQTTv5)
    c.on_connect = _on_connect
    c.on_message = _on_message
    c.connect_async(config.HOST, config.PORT)
    c.loop_start()


def hold_admin():
    """Hold the reserved admin identity and drain what reaches it; the observer shows those messages."""
    while True:
        try:
            bus = Bus.register(ADMIN, addr=ADMIN, keep=True, admin=True)
        except BridgeError as e:
            print(f"admin: {e}", file=sys.stderr, flush=True)
            time.sleep(3)
            continue
        held["bus"] = bus
        try:
            while True:
                bus.recv(wait=60)
        except BridgeError as e:
            print(f"admin: {e}", file=sys.stderr, flush=True)
            held["bus"] = None
            bus.close()


def need_bus():
    if not (bus := held["bus"]):
        raise Refused("the console is not connected to the broker")
    return bus


def backlog():
    try:
        with open(config.LOG, encoding="utf-8", errors="replace") as f:
            return [e for e in map(parse_entry, collections.deque(f, BACKLOG)) if e]
    except FileNotFoundError:
        return []


def ledger_call(op, as_, **fields):
    try:
        return ledger.call(op, as_=as_, client=need_bus()._c, **fields)
    except ledger.LedgerError as e:
        raise Refused(f"ledger: {' '.join(str(e).split())}") from None


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
