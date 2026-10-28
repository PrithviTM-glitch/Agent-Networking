"""ledgerd: serves the resource ledger over MQTT 5 request/response.

Requests on ledger/req/<addr> (or plain ledger/req, where the body's "as" names the agent), JSON
{"op": ..., "as": "<addr>", ...} with ResponseTopic (under ledger/resp/) and CorrelationData. Every request, lease
expiry and presence update is handled in order by the main thread, one at a time, each in its own SQLite transaction.
"""

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
from .client import nodelay
from .store import ADDR, Ledger, log

DB = os.environ.get("LEDGER_DB") or os.path.join(config.STATE_DIR, "ledger.db")
INFLIGHT = 32  # unacked QoS 1 deliveries the broker may push at once, bounds the in-memory queue
REPLY_EXPIRY = 300
SESSION_TAKEN_OVER = 142


def main():
    inbox = queue.Queue()

    def on_connect(c, _u, _f, rc, _p):
        if rc.is_failure:
            return log("connect refused", rc)
        c.subscribe([("ledger/req", 1), ("ledger/req/+", 1), ("agents/+", 1)])
        log(f"connected to {config.HOST}:{config.PORT}, db {DB}")

    def on_disconnect(_c, _u, _f, rc, _p):
        log("disconnected", rc)
        if rc == SESSION_TAKEN_OVER:  # another ledgerd connected: stepping aside keeps a single writer
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
            sys.exit("another client connected as ledgerd, exiting")
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
