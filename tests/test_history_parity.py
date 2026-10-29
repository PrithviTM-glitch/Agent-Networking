"""historyd handover with the rollback: the frozen Python historyd image (agent-bridge-historyd:1, built from v0.1.0;
AGENT_BRIDGE_ROLLBACK_HISTORYD names another) and the historyd under test (AGENT_BRIDGE_HISTORYD, the bundled Rust one
by default) take turns on one database. Messages recorded by one are not stored again by the other, and every request
gets the same reply, byte for byte. This is what the README's rollback promise rests on, so it lives exactly as long
as that image is offered as a rollback (until the first history schema change). Skipped when the image is not there;
CI's rollback job builds it from the v0.1.0 tag.
"""

import json
import os
import sqlite3
import subprocess
import threading
import time
import uuid

import paho.mqtt.client as mqtt
import pytest
from conftest import HISTORYD, wait_for
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from agent_bridge import config

ROLLBACK = os.environ.get("AGENT_BRIDGE_ROLLBACK_HISTORYD") or "agent-bridge-historyd:1"
RUN = uuid.uuid4().hex[:6]
CONTAINER = f"agent-bridge-parity-historyd-{RUN}"
pytestmark = pytest.mark.skipif(
    subprocess.run(["docker", "image", "inspect", ROLLBACK], capture_output=True).returncode != 0,
    reason=f"no rollback image {ROLLBACK}",
)

ROOM, AL, BO = f"par{RUN}", f"al{RUN}", f"bo{RUN}"


def msg(sender, body, ts, **extra):
    return json.dumps({"from": sender, "body": body, "ts": ts, **extra})


MESSAGES = [
    (f"room/{ROOM}", msg(AL, "hello", 1793000000.5)),
    (f"room/{ROOM}", msg(BO, f"@{AL} hi ✓  ", 1793000001.123456789, mentions=[AL])),
    (f"room/{ROOM}", msg(AL, "int ts", 1793000002)),
    (f"room/{ROOM}", msg(AL, "tiny ts", 1e-05)),
    (f"inbox/{BO}", msg(AL, "to bo", 1793000003.25)),
    (f"inbox/{AL}", msg(BO, "to al", 1793000004.0)),
    ("broadcast", msg("admin", "everyone", 1793000005.75)),
    ("broadcast", msg(AL, "forged broadcast", 1793000006.0)),
    (f"raw{RUN}/x", msg(AL, "json on raw", 1793000007.0)),
    (f"agents/{AL}", "never stored"),
    (f"room/{ROOM}big", msg(AL, "x" * 60_000, 1793000008.0)),
    *[(f"room/{ROOM}big", msg(AL, "y" * 60_000 + str(i), 1793000009.0 + i)) for i in range(5)],
]
REQUESTS = [
    ({"op": "ping"}, None),
    ({"op": "room", "room": ROOM}, None),
    ({"op": "room", "room": ROOM, "last": 2}, AL),
    ({"op": "room", "room": ROOM, "before": 1793000001.5}, None),
    ({"op": "room", "room": ROOM, "before": None, "last": None}, None),
    ({"op": "room", "room": f"{ROOM}big", "last": 1000}, None),
    ({"op": "dms"}, AL),
    ({"op": "dms", "peer": BO, "last": 1}, AL),
    ({"op": "topic", "topic": f"raw{RUN}/x"}, None),
    ({"op": "broadcasts", "since": 1793000005}, None),
    ({"op": "recent", "last": 3}, "admin"),
    ({"op": "drop", "addr": f"nobody{RUN}"}, f"nobody{RUN}"),
    (b"\xff\xfe", None),
    (b"[" * 20_000, None),
    (b"[1]", None),
    ({"op": ["room"]}, None),
    ({"op": "room"}, None),
    ({"op": "room", "room": ["r", 1, None, True, 2.5, {"k": "v"}]}, None),
    ({"op": "room", "room": "r", "last": 5.0}, None),
    ({"op": "room", "room": "r", "last": 10**30}, None),
    ({"op": "room", "room": "r", "before": 1e10}, None),
    ({"op": "room", "room": "r", "extra": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "g" * 40: 7}, None),
    ({"op": "dms", "as": "al-1"}, None),
    ({"op": "topic", "topic": "inbox/al-1"}, None),
    ({"op": "topic", "topic": "a/#"}, None),
    ({"op": "topic", "topic": 5}, None),
    ({"op": "topic", "topic": "a\u0001b"}, None),
    ({"op": "room", "room": "r", "as": 5}, None),
    ({"op": "room", "room": "r", "as": None}, AL),
    ({"op": "ping", "as": BO}, AL),
    ({"op": "ping"}, "a b"),
    ({"op": "dms", "peer": "x/y"}, AL),
    ({"op": "recent"}, AL),
    ({"op": "recent", "last": 9999}, "admin"),
    ({"op": "drop", "addr": BO}, AL),
    ({"op": "broadcasts", "since": float("inf")}, None),
]


class Asker:
    """Raw requests on history/resp/parity, so the payload, CorrelationData and expiry are exactly what was sent."""

    def __init__(self):
        self.replies, self.cv, self.n = {}, threading.Condition(), 0
        self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"parity{RUN}", protocol=mqtt.MQTTv5)
        self.c.on_message = self.on_message
        self.c.connect(config.HOST, config.PORT)
        self.c.loop_start()
        self.c.subscribe("history/resp/parity", qos=1)

    def on_message(self, _c, _u, m):
        with self.cv:
            corr = getattr(m.properties, "CorrelationData", None)
            self.replies[corr] = (m.payload, getattr(m.properties, "MessageExpiryInterval", None))
            self.cv.notify_all()

    def publish(self, topic, payload):
        self.c.publish(topic, payload, qos=1).wait_for_publish(5)

    def ask(self, req, via=None, corr="auto", resp="history/resp/parity", wait=3.0):
        self.n += 1
        props = Properties(PacketTypes.PUBLISH)
        if resp is not None:
            props.ResponseTopic = resp
        key = f"k{self.n}".encode() if corr == "auto" else corr
        if key is not None:
            props.CorrelationData = key
        payload = req if isinstance(req, bytes) else json.dumps(req).encode()
        topic = "history/req" + (f"/{via}" if via else "")
        self.c.publish(topic, payload, qos=1, properties=props).wait_for_publish(5)
        end = time.monotonic() + wait
        with self.cv:
            while key not in self.replies and time.monotonic() < end:
                self.cv.wait(0.1)
            return self.replies.pop(key, None)

    def all(self):
        replies = [self.ask(req, via) for req, via in REQUESTS]
        for kw in ({"corr": None}, {"corr": b""}, {"corr": b"x" * 257}, {"corr": b"y" * 256}):
            replies.append(self.ask({"op": "ping"}, **kw))
        for resp in (None, "elsewhere/resp/x"):
            replies.append(self.ask({"op": "ping"}, resp=resp, wait=1.0))
        return replies


def rows(db):
    with sqlite3.connect(db) as conn:
        return conn.execute("SELECT count(*) FROM messages").fetchone()[0]


def rollback(state_dir):
    """The rollback image as a foreground process on the test broker, writing state_dir's parity.db as this user (so
    the host-run historyd can open the files it leaves); --init so SIGTERM stops it."""
    return [
        "docker", "run", "--rm", "--init", "--name", CONTAINER, "--network", "host",
        "--user", f"{os.getuid()}:{os.getgid()}", "-v", f"{state_dir}:/data",
        "-e", "HISTORY_DB=/data/parity.db", "-e", "HISTORY_IMPORT=/data/none.jsonl",
        "-e", f"AGENT_BRIDGE_HOST={config.HOST}", "-e", f"AGENT_BRIDGE_PORT={os.environ['AGENT_BRIDGE_PORT']}",
        ROLLBACK, "agent-bridge-historyd",
    ]  # fmt: skip


def test_two_implementations_share_one_database(fresh_broker, state_dir):
    db, log = os.path.join(state_dir, "parity.db"), os.path.join(state_dir, "parity.log")
    asker = Asker()

    def start(cmd):
        proc = subprocess.Popen(cmd, stderr=open(log, "a"), env={**os.environ, "HISTORY_DB": db})
        assert wait_for(lambda: asker.ask({"op": "ping"}, wait=0.5), 60), open(log).read()
        return proc

    def stop(proc):
        # one writer at a time: the next one starts only once this one is gone and nothing answers
        proc.terminate()
        assert proc.wait(20) is not None and asker.ask({"op": "ping"}, wait=1.0) is None

    try:
        handover(asker, start, stop, rollback(state_dir), db)
    finally:
        subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True)


def handover(asker, start, stop, reference, db):
    """The rollback records, the one under test takes over the same db and answers alike, then back again."""

    def send(messages):
        for topic, payload in messages:
            asker.publish(topic, payload)
        before = asker.ask({"op": "ping"})  # a request after them: historyd handles in order
        assert before is not None

    ref = start(reference)
    send(MESSAGES)
    stored, expected = rows(db), asker.all()
    stop(ref)
    under_test = start(HISTORYD)
    send(MESSAGES)
    assert rows(db) == stored, "what the reference stored is not stored again"
    replies = asker.all()
    for (req, via), want, got in zip(REQUESTS, expected, replies, strict=False):
        assert got == want, (str(req)[:80], via, want and want[0][:200], got and got[0][:200])
    assert replies[len(REQUESTS) :] == expected[len(REQUESTS) :], "CorrelationData and ResponseTopic handling"
    new = (f"room/{ROOM}", msg(AL, "recorded by the one under test", 1793000099.03125))
    send([new])
    stop(under_test)
    ref = start(reference)
    send([new])
    assert rows(db) == stored + 1, "and what it stored, the reference does not store again"
    stop(ref)
