"""The directory of agents: retained presence records under agents/<addr>, read through short-lived clients."""

import time

import paho.mqtt.client as mqtt

from . import broker, config
from .protocol import LINEAGE, NEVER, ORIGIN, parse_presence
from .validation import BridgeError, check_ident, is_ident


def parse_all(retained):
    """Address -> presence record of every well-formed record among retained topic -> payload."""
    return {
        t[7:]: rec
        for t, payload in retained.items()
        if t.startswith("agents/") and is_ident(t[7:]) and (rec := parse_presence(payload))
    }


def presences(pattern="agents/+", first=False):
    """Address -> presence record of every well-formed record matching pattern."""
    return parse_all(broker.retained([pattern], first))


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
            **{k: rec[k] for k in (*ORIGIN, *LINEAGE) if k in rec},
        }
        for addr, rec in sorted(presences().items(), key=lambda kv: -kv[1]["seen"])
        if not room or any(mqtt.topic_matches_sub(t, room) for t in rec["topics"])
    ]


def expiry(rec):
    """The session expiry for an agent's record: forever with keep, else the stale period."""
    return NEVER if rec["keep"] else config.STALE_S
