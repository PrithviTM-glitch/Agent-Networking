"""ledgerd: serves the resource ledger over MQTT 5 request/response.

Requests on ledger/req/<addr> (or plain ledger/req, where the body's "as" names the agent), JSON
{"op": ..., "as": "<addr>", ...} with ResponseTopic (under ledger/resp/) and CorrelationData. Every request, lease
expiry and presence update is handled in order by the main thread, one at a time, each in its own SQLite transaction.
"""

import argparse
import os
import queue
import re
import sys
import time
import traceback

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from .. import config
from ..protocol import NEVER
from .client import LedgerError, call, nodelay
from .store import ADDR, Ledger, log

DB = os.environ.get("LEDGER_DB") or os.path.join(config.STATE_DIR, "ledger.db")
INFLIGHT = 32  # unacked QoS 1 deliveries the broker may push at once, bounds the in-memory queue
REPLY_EXPIRY = 300
HEALTH_TIMEOUT = 5


def main(argv=None):
    p = argparse.ArgumentParser(
        prog="agent-bridge-ledgerd",
        allow_abbrev=False,
        description="Serves the agent-bridge resource ledger over MQTT 5 request/response on ledger/req. With no "
        "arguments it runs the service (AGENT_BRIDGE_HOST, AGENT_BRIDGE_PORT, LEDGER_DB).",
    )
    p.add_argument(
        "--health",
        action="store_true",
        help=f"ask the running ledgerd for a dump as health-0; exit 0 on an ok reply within {HEALTH_TIMEOUT} s, else 1",
    )
    # Only no arguments runs the service, so a stray flag can never start a second ledgerd that takes the live one's
    # session over; anything unknown is refused (exit 2) before a connection or the database is opened.
    if p.parse_args(argv).health:
        sys.exit(health())
    serve()


def health():
    """A round trip through the broker, so it fails when ledgerd is disconnected or wedged, not just when it exits."""
    try:
        call("dump", "health-0", timeout=HEALTH_TIMEOUT)
    except LedgerError as e:
        print(f"agent-bridge-ledgerd: unhealthy: {e}", file=sys.stderr)
        return 1
    return 0


def serve():
    inbox = queue.Queue()

    def on_connect(c, _u, _f, rc, _p):
        if rc.is_failure:
            return log("connect refused", rc)
        c.subscribe([("ledger/req", 1), ("ledger/req/+", 1), ("agents/+", 1)])
        log(f"connected to {config.HOST}:{config.PORT}, db {DB}")

    def on_disconnect(_c, _u, flags, rc, _p):
        log("disconnected", rc)
        # Step aside on any DISCONNECT the broker sends, so another ledgerd on this client id is the single writer.
        # mosquitto's takeover is reason 142 in one byte, which paho-mqtt 2.1 drops (client.py _handle_disconnect
        # reads a reason only when remaining_length > 2) and reports as 0; a broker shutdown sends no DISCONNECT.
        if flags.is_disconnect_packet_from_server:
            inbox.put(None)

    def on_message(_c, _u, m):
        props = m.properties
        item = (
            m.topic,
            m.payload,
            m.qos,
            m.mid,
            getattr(props, "ResponseTopic", None),
            getattr(props, "CorrelationData", None),
        )
        if m.qos or inbox.qsize() < 1000:
            inbox.put(item)

    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="ledgerd", protocol=mqtt.MQTTv5, manual_ack=True)
    c.on_connect, c.on_disconnect, c.on_message = on_connect, on_disconnect, on_message
    c.on_socket_open = nodelay
    c.reconnect_delay_set(1, 10)

    def publish(topic, payload, corr):
        props = Properties(PacketTypes.PUBLISH)
        if corr is not None:
            props.CorrelationData = corr
            props.MessageExpiryInterval = REPLY_EXPIRY
        c.publish(topic, payload, qos=1, properties=props)

    props = Properties(PacketTypes.CONNECT)
    props.SessionExpiryInterval = NEVER
    props.ReceiveMaximum = INFLIGHT
    ledger = Ledger(DB, publish)
    c.connect_async(config.HOST, config.PORT, clean_start=False, properties=props)
    c.loop_start()
    while True:
        try:
            item = inbox.get(timeout=min(60.0, max(0.0, ledger.next_deadline() - time.time())))
        except queue.Empty:
            item = ()
        if item is None:
            sys.exit("the broker disconnected this ledgerd (another took its session over), exiting")
        try:
            ledger.run(ledger.tick)
        except Exception:
            log("tick failed", traceback.format_exc())
        if not item:
            continue
        topic, payload, qos, mid, resp, corr = item
        try:
            if topic == "ledger/req" or topic.startswith("ledger/req/"):
                ledger.handle(payload, resp, corr, topic[len("ledger/req/") :] or None)
            elif topic.startswith("agents/") and re.fullmatch(ADDR, topic[7:]):
                ledger.run(ledger.presence, topic[7:], payload)
        except Exception:
            log("failed on", topic[:80], traceback.format_exc())
        if qos:
            # Acked only after the transaction committed, so a crash before this point means redelivery, not loss.
            c.ack(mid, qos)


if __name__ == "__main__":
    main()
