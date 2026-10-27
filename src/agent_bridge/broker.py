"""paho-mqtt helpers: MQTT 5 clients with TCP_NODELAY, connecting with a timeout, and confirmed QoS 1 publishes."""

import socket
import threading

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.subscribeoptions import SubscribeOptions

from . import config
from .validation import BridgeError

TIMEOUT = 5
RECEIVE_MAX = 100
SUB = SubscribeOptions(qos=1, noLocal=True)


def new_client(client_id, on_message=None, manual_ack=False):
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, protocol=mqtt.MQTTv5, manual_ack=manual_ack)
    c.suppress_exceptions = True
    c.on_message = on_message
    # Without it every request/response round trip waits on Nagle plus delayed ACK (~40 ms each way).
    c.on_socket_open = lambda _c, _u, sock: sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return c


def connect(c, clean, expiry, up):
    """Connect c and start its network loop; up is the event its on_connect sets."""
    props = Properties(PacketTypes.CONNECT)
    props.SessionExpiryInterval = expiry
    props.ReceiveMaximum = RECEIVE_MAX
    try:
        c.connect(config.HOST, config.PORT, clean_start=clean, properties=props)
    except (OSError, ValueError) as e:
        raise BridgeError(
            f"broker unreachable at {config.HOST}:{config.PORT} ({e}), start it with: docker compose up -d"
        ) from None
    c.loop_start()
    if not up.wait(TIMEOUT):
        c.loop_stop()
        raise BridgeError("broker did not accept the connection")


def one_shot(client_id="", clean=True, on_message=None):
    """A connected client for a single lookup or publish; its session ends when it disconnects."""
    c = new_client(client_id, on_message)
    up = threading.Event()
    c.on_connect = lambda _c, _u, _f, rc, _p: rc.is_failure or up.set()
    connect(c, clean, 0, up)
    return c


def close(c):
    c.disconnect()
    c.loop_stop()


def publish(c, topic, payload, retain=False, expire=None):
    """Publish at QoS 1 and wait for the broker to confirm it."""
    props = None
    if expire:
        props = Properties(PacketTypes.PUBLISH)
        props.MessageExpiryInterval = expire
    info = c.publish(topic, payload, qos=1, retain=retain, properties=props)
    try:
        info.wait_for_publish(TIMEOUT)
        ok = info.is_published()
    except (RuntimeError, ValueError):
        ok = False
    if not ok:
        raise BridgeError(f"broker did not confirm the publish to {topic}")
