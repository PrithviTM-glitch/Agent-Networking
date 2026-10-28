"""Client for historyd. Every call raises HistoryError, one line, when historyd refuses, is down or the broker is.

Pass client= to reuse an already connected paho MQTT 5 client with its network loop running (a Bus's, even under
manual ack); otherwise each call opens and closes its own connection.
"""

import json
import math
import threading
import uuid
import weakref

from .. import broker
from ..validation import ADMIN, BridgeError, check_ident, check_last, check_topic, is_ident

_channels = weakref.WeakKeyDictionary()
_lock = threading.Lock()


class HistoryError(BridgeError):
    """An ok:false reply, a timeout, a malformed reply or no broker; .reply holds the reply dict."""

    def __init__(self, reply):
        super().__init__("history: " + " ".join(str(reply.get("error", "error")).split()))
        self.reply = reply


def _channel(c):
    """One reply topic per client, named after its client id so a persistent session never piles up subscriptions."""
    with _lock:
        if c in _channels:
            return _channels[c]
        cid = c._client_id.decode(errors="replace")  # paho exposes no public getter
        topic = f"history/resp/{cid if is_ident(cid, 128) else uuid.uuid4().hex}"
        pending = {}

        def on_reply(cl, _u, m):
            cl.ack(m.mid, m.qos)  # a no-op unless the client runs manual_ack, as a Bus does
            slot = pending.get(getattr(m.properties, "CorrelationData", None))
            if slot:
                slot[1].append(m.payload)
                slot[0].set()

        c.message_callback_add(topic, on_reply)
        _channels[c] = topic, pending
        return topic, pending


def call(op, as_=None, client=None, timeout=5.0, **fields):
    """One historyd request; the reply dict, ok:true."""
    from paho.mqtt.packettypes import PacketTypes
    from paho.mqtt.properties import Properties

    req = {"op": op, **{k: v for k, v in fields.items() if v is not None}}
    if as_ is not None:
        req["as"] = as_
    try:
        c = client or broker.one_shot()
    except BridgeError as e:
        raise HistoryError({"ok": False, "error": str(e)}) from None
    try:
        topic, pending = _channel(c)
        c.subscribe(topic, qos=1)  # every call: the broker may have lost the session since
        corr = uuid.uuid4().bytes
        pending[corr] = slot = (threading.Event(), [])
        try:
            props = Properties(PacketTypes.PUBLISH)
            props.ResponseTopic = topic
            props.CorrelationData = corr
            props.MessageExpiryInterval = math.ceil(timeout)  # unanswered requests do not pile up for historyd
            req_topic = f"history/req/{as_}" if is_ident(as_) else "history/req"
            c.publish(req_topic, json.dumps(req), qos=1, properties=props)
            if not slot[0].wait(timeout):
                error = f"no reply from historyd within {timeout:g}s, is it running?"
                raise HistoryError({"ok": False, "error": error})
        finally:
            pending.pop(corr, None)
    finally:
        if client is None:
            broker.close(c)
    try:
        reply = json.loads(slot[1][0])
    except ValueError:
        reply = None
    if not isinstance(reply, dict):
        raise HistoryError({"ok": False, "error": "malformed reply from historyd"})
    if reply.get("ok") is not True:
        raise HistoryError(reply)
    return reply


def room(room, last=20, before=None, client=None):
    """A room's last messages, oldest first; before (a ts) pages back."""
    check_ident(room, "room")
    check_last(last)
    return call("room", room=room, last=last, before=before, client=client)["messages"]


def dms(addr, peer=None, last=20, client=None):
    """addr's direct messages in both directions, oldest first; with peer, only those with peer."""
    check_ident(addr, "address")
    if peer is not None:
        check_ident(peer, "address")
    check_last(last)
    return call("dms", as_=addr, peer=peer, last=last, client=client)["messages"]


def topic(topic, last=20, client=None):
    """A raw topic's last messages, oldest first."""
    check_topic(topic)
    check_last(last)
    return call("topic", topic=topic, last=last, client=client)["messages"]


def broadcasts(since=0.0, last=20, timeout=1.0):
    """The admin's broadcasts newer than since (a ts), oldest first. Short timeout: hooks call it on every prompt."""
    return call("broadcasts", since=since, last=last, timeout=timeout)["messages"]


def recent(last=100, client=None):
    """The last messages of every topic, oldest first; for the admin console."""
    return call("recent", as_=ADMIN, last=last, client=client)["messages"]


def drop(addr, client=None, timeout=3.0):
    """Delete addr's received DMs (leave does this); the number deleted."""
    return call("drop", as_=addr, client=client, timeout=timeout)["dropped"]
