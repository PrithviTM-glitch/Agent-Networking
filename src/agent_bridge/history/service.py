"""historyd: records the bus's messages and serves them over MQTT 5 request/response.

It subscribes to # and keeps rooms, inboxes, broadcasts and raw topics (store.stored). Requests on history/req/<addr>
(or plain history/req for ops that name nobody), JSON {"op": ..., "as": "<addr>", ...} with ResponseTopic (under
history/resp/) and CorrelationData. Messages, requests and pruning are handled in order by the main thread, each in its
own SQLite transaction, and a message is acked only after its row committed. On its first start it imports the legacy
history log (HISTORY_IMPORT, default the bridge's history.jsonl) once and leaves the file in place.

agent-bridge-historyd --health: one ping through the broker, exit status 0 if historyd answered.
"""

import os
import queue
import sys
import threading
import traceback

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.subscribeoptions import SubscribeOptions

from .. import config
from ..ledger.client import nodelay
from ..protocol import NEVER
from . import client
from .store import History, log

DB = os.environ.get("HISTORY_DB") or os.path.join(config.STATE_DIR, "history.db")
IMPORT = os.environ.get("HISTORY_IMPORT") or config.LOG
INFLIGHT = 32  # unacked QoS 1 deliveries the broker may push at once, bounds the in-memory queue
REPLY_EXPIRY = 300
SESSION_TAKEN_OVER = 142
# No Local: never its own replies. Retain handling 2: a (re)subscribe must not replay retained records as history.
EVERYTHING = SubscribeOptions(qos=1, noLocal=True, retainHandling=2)


def main():
    if sys.argv[1:] == ["--health"]:
        try:
            client.call("ping", timeout=5)
        except client.HistoryError as e:
            sys.exit(f"historyd: {e}")
        return
    inbox, subscribed = queue.Queue(), threading.Event()

    def on_connect(c, _u, _f, rc, _p):
        if rc.is_failure:
            return log("connect refused", rc)
        c.subscribe("#", options=EVERYTHING)
        log(f"connected to {config.HOST}:{config.PORT}, db {DB}")

    def on_disconnect(_c, _u, _f, rc, _p):
        log("disconnected", rc)
        if rc == SESSION_TAKEN_OVER:  # another historyd connected: stepping aside keeps a single writer
            inbox.put(None)

    def on_message(_c, _u, m):
        props = m.properties
        try:
            topic = m.topic
        except ValueError:
            return _c.ack(m.mid, m.qos)
        item = (topic, m.payload, m.qos, m.mid, getattr(props, "ResponseTopic", None),
                getattr(props, "CorrelationData", None))  # fmt: skip
        if m.qos or inbox.qsize() < 1000:
            inbox.put(item)

    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="historyd", protocol=mqtt.MQTTv5, manual_ack=True)
    c.on_connect, c.on_disconnect, c.on_message = on_connect, on_disconnect, on_message
    c.on_subscribe = lambda *_: subscribed.set()
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
    history = History(DB, publish)
    c.connect_async(config.HOST, config.PORT, clean_start=False, properties=props)
    c.loop_start()
    # After subscribing, so nothing sent meanwhile is missed (digests skip what arrives both ways); without a broker
    # within 30 s it imports anyway.
    subscribed.wait(30)
    history.migrate(IMPORT)
    while True:
        try:
            item = inbox.get(timeout=60)
        except queue.Empty:
            item = ()
        if item is None:
            sys.exit("another client connected as historyd, exiting")
        try:
            history.prune()
        except Exception:
            log("prune failed", traceback.format_exc())
        if not item:
            continue
        topic, payload, qos, mid, resp, corr = item
        try:
            if topic == "history/req" or topic.startswith("history/req/"):
                history.handle(payload, resp, corr, topic[len("history/req/") :] or None)
            else:
                history.record(topic, payload)
        except Exception:
            log("failed on", topic[:80], traceback.format_exc())
        if qos:
            # Acked only after the row committed, so a crash before this point means redelivery, not loss.
            c.ack(mid, qos)


if __name__ == "__main__":
    main()
