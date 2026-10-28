"""Client for ledgerd. call(op, as_=ADDR, **fields) returns the reply dict or raises LedgerError.

Pass client= to reuse an already connected paho MQTT 5 client with its network loop running; otherwise each call
opens and closes its own connection. CLI: agent-bridge-ledger OP --as ADDR [--field value ...]
"""

import argparse
import json
import math
import re
import socket
import sys
import threading
import uuid
import weakref

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from .. import config
from ..errors import LedgerError

_channels = weakref.WeakKeyDictionary()
_lock = threading.Lock()


def nodelay(_c, _u, sock):
    # Without it Nagle holds each small packet for the peer's 40 ms delayed ACK, twice per request/response.
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


def connect(client_id=""):
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, protocol=mqtt.MQTTv5)
    ready = threading.Event()
    c.on_connect = lambda *_: ready.set()
    c.on_socket_open = nodelay
    try:
        c.connect(config.HOST, config.PORT)
    except OSError as e:
        raise LedgerError({"ok": False, "error": f"broker unreachable at {config.HOST}:{config.PORT} ({e})"}) from None
    c.loop_start()
    if not ready.wait(5):
        c.loop_stop()
        raise LedgerError({"ok": False, "error": "broker did not accept the connection"})
    return c


def _channel(c):
    """One reply topic per client, named after the client id so a persistent session never piles up subscriptions
    across restarts."""
    with _lock:
        if c in _channels:
            return _channels[c]
        cid = c._client_id.decode(errors="replace")  # paho exposes no public getter
        topic = f"ledger/resp/{cid if re.fullmatch(r'[A-Za-z0-9_-]{1,128}', cid) else uuid.uuid4().hex}"
        pending = {}

        def on_reply(_c, _u, m):
            # A no-op unless the client runs manual_ack (the Bus does), where unacked replies would exhaust its
            # receive window and stall all delivery to that agent.
            _c.ack(m.mid, m.qos)
            slot = pending.get(getattr(m.properties, "CorrelationData", None))
            if slot:
                slot[1].append(m.payload)
                slot[0].set()

        c.message_callback_add(topic, on_reply)
        _channels[c] = topic, pending
        return topic, pending


def call(op, as_=None, client=None, timeout=None, **fields):
    req = {"op": op, **fields}
    if as_ is not None:
        req["as"] = as_
    wait = fields.get("wait", 0)
    extra = min(max(wait, 0), 300) if isinstance(wait, (int, float)) and math.isfinite(wait) else 0
    timeout = timeout or 10 + extra
    # math.ceil and Event.wait reject infinite, NaN or huge timeouts; a bad wait is left for ledgerd to refuse.
    if not 0 < timeout <= 3600:
        timeout = 3600 if timeout > 3600 else 10 + extra
    c = client or connect()
    try:
        topic, pending = _channel(c)
        # Resubscribed every call: the broker may have lost the session since; it handles this before the request.
        c.subscribe(topic, qos=1)
        corr = uuid.uuid4().bytes
        pending[corr] = slot = (threading.Event(), [])
        try:
            props = Properties(PacketTypes.PUBLISH)
            props.ResponseTopic = topic
            props.CorrelationData = corr
            # The broker discards the request if ledgerd cannot pick it up before we stop listening.
            props.MessageExpiryInterval = math.ceil(timeout)
            # Per-agent topic, so broker ACLs can later vouch for the address; ledgerd checks it matches "as".
            valid = isinstance(as_, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,64}", as_)
            c.publish(f"ledger/req/{as_}" if valid else "ledger/req", json.dumps(req), qos=1, properties=props)
            if not slot[0].wait(timeout):
                raise LedgerError({"ok": False, "error": f"no reply from ledgerd within {timeout:g}s, is it running?"})
        finally:
            pending.pop(corr, None)
    finally:
        if client is None:
            c.disconnect()
            c.loop_stop()
    try:
        reply = json.loads(slot[1][0])
    except ValueError:
        reply = None
    if not isinstance(reply, dict):
        raise LedgerError({"ok": False, "error": "malformed reply"})
    if reply.get("ok") is not True:
        raise LedgerError(reply)
    return reply


def main():
    p = argparse.ArgumentParser(
        prog="agent-bridge-ledger", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("op")
    p.add_argument("--as", dest="as_", metavar="ADDR")
    p.add_argument("--timeout", type=float)
    a, rest = p.parse_known_args()
    if len(rest) % 2 or not all(k.startswith("--") for k in rest[::2]):
        p.error("fields go as --name value pairs")
    try:
        fields = {
            k[2:].replace("-", "_"): float(v) if k in ("--ttl", "--wait") else v
            for k, v in zip(rest[::2], rest[1::2], strict=True)
        }
        print(json.dumps(call(a.op, a.as_, timeout=a.timeout, **fields)))
    except LedgerError as e:
        print(json.dumps(e.reply))
        sys.exit(1)
    except ValueError as e:
        p.error(str(e))


if __name__ == "__main__":
    main()
