"""The directory of agents: retained presence records under agents/<addr>, read through short-lived clients."""

import threading
import time

import paho.mqtt.client as mqtt

from . import broker, config
from .protocol import NEVER, ORIGIN, parse_presence
from .validation import BridgeError, check_ident, is_ident

# A fixed wait for retained messages; use a marker publish instead if a slow broker ever misses some.
WINDOW = 0.3


def presences(pattern="agents/+", first=False):
    """Address -> presence record of every well-formed record matching pattern."""
    found = {}
    got = threading.Event()

    def on_message(_c, _u, m):
        addr = m.topic.removeprefix("agents/")
        if is_ident(addr) and (rec := parse_presence(m.payload)):
            found[addr] = rec
            got.set()

    c = broker.one_shot(on_message=on_message)
    c.subscribe(pattern, qos=1)
    got.wait(WINDOW) if first else time.sleep(WINDOW)
    broker.close(c)
    return found


def find(addr):
    return presences(f"agents/{check_ident(addr, 'address')}", first=True).get(addr)


def need(addr):
    if (rec := find(addr)) is None:
        raise BridgeError(f"unknown address: {addr}")
    return rec


def wipe(addr):
    """End addr's broker session and clear its presence."""
    c = broker.one_shot(addr)  # clean start with expiry 0: the session ends with this connection
    try:
        broker.publish(c, f"agents/{addr}", b"", retain=True)
    finally:
        broker.close(c)


def agents(room=None):
    """Every live agent, most recently seen first; with room, only its members."""
    now = time.time()
    room = room is not None and f"room/{check_ident(room, 'room')}"
    return [
        {
            "addr": addr,
            "name": rec["name"],
            "caps": rec["caps"],
            "topics": rec["topics"],
            "keep": rec["keep"],
            "idle_s": round(now - rec["seen"]),
            **{k: rec[k] for k in ORIGIN if k in rec},
        }
        for addr, rec in sorted(presences().items(), key=lambda kv: -kv[1]["seen"])
        if not room or any(mqtt.topic_matches_sub(t, room) for t in rec["topics"])
    ]


def rooms():
    """Every chat room with its members."""
    members = {}
    for addr, rec in presences().items():
        for t in rec["topics"]:
            if t.startswith("room/") and is_ident(t[5:]):
                members.setdefault(t[5:], []).append(addr)
    return [{"room": name, "members": len(who), "who": sorted(who)} for name, who in sorted(members.items())]


def expiry(rec):
    """The session expiry for an agent's record: forever with keep, else the stale period."""
    return NEVER if rec["keep"] else config.STALE_S
