"""historyd golden replies: a fixed corpus of messages and requests, replayed against whichever historyd
AGENT_BRIDGE_HISTORYD names, must get the replies the Python reference gave, byte for byte (tests/golden/historyd.json).
It keeps the historyd held to the reference now that the Python one is gone. The documented divergences (a NaN echoed
in an error, JSON nested past 128 levels, an integer over 4300 digits) carry the Rust reply as an override, so they
stay pinned too.

The file was generated at cp8 (02db236) by replaying this corpus against the Python reference and the Rust historyd;
it refused to write where they differed anywhere else. With the reference deleted it cannot be regenerated, so the
corpus and the file are frozen together: changing one fails test_golden_file_is_the_corpus.
"""

import json
import os
import subprocess
import threading
import time
from pathlib import Path

import paho.mqtt.client as mqtt
from conftest import HISTORYD, wait_for
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from agent_bridge import config

GOLDEN = Path(__file__).parent / "golden" / "historyd.json"
ROOM, AL, BO = "golden", "al-g", "bo-g"


def msg(sender, body, ts, **extra):
    return json.dumps({"from": sender, "body": body, **({} if ts is None else {"ts": ts}), **extra})


MESSAGES = [
    (f"room/{ROOM}", msg(AL, "hello", 1793000000.5)),
    (f"room/{ROOM}", msg(BO, f"@{AL} hi ✓  ", 1793000001.123456789, mentions=[AL])),
    (f"room/{ROOM}", msg(AL, "int ts", 1793000002)),
    (f"room/{ROOM}", msg(AL, "tiny ts", 1e-05)),
    (f"room/{ROOM}", msg(AL, "no ts, so the receive time", None)),
    (f"inbox/{BO}", msg(AL, "to bo", 1793000003.25)),
    (f"inbox/{AL}", msg(BO, "to al", 1793000004.0)),
    ("broadcast", msg("admin", "everyone", 1793000005.75)),
    ("broadcast", msg(AL, "forged broadcast", 1793000006.0)),
    ("raw-golden/x", msg(AL, "json on raw", 1793000007.0)),
    ("raw-golden/plain", "plain text on raw: stored with the receive time, so never queried"),
    (f"agents/{AL}", "never stored"),
    ("ledger/req", msg(AL, "never stored", 1.0)),
    (f"room/{ROOM}", "not json, dropped"),
    (f"room/{ROOM}big", msg(AL, "x" * 60_000, 1793000008.0)),
    *[(f"room/{ROOM}big", msg(AL, "y" * 60_000 + str(i), 1793000009.0 + i)) for i in range(5)],
]
# "no ts" stores the receive time, which differs per run: requests that would return it filter it out (before, since)
# or are counted only. Each entry: (request, the address of history/req/<addr> or None, divergent).
REQUESTS = [
    ({"op": "ping"}, None, False),
    ({"op": "room", "room": ROOM, "before": 1793000003}, None, False),
    ({"op": "room", "room": ROOM, "last": 2, "before": 1793000002.5}, AL, False),
    ({"op": "room", "room": ROOM, "before": 1793000001.5}, None, False),
    ({"op": "room", "room": f"{ROOM}big", "last": 1000}, None, False),
    ({"op": "room", "room": "nothing-here"}, None, False),
    ({"op": "dms"}, AL, False),
    ({"op": "dms", "peer": BO, "last": 1}, AL, False),
    ({"op": "dms"}, BO, False),
    ({"op": "topic", "topic": "raw-golden/x"}, None, False),
    ({"op": "broadcasts", "since": 1793000005}, None, False),
    ({"op": "broadcasts"}, None, False),
    ({"op": "drop", "addr": "nobody-g"}, "nobody-g", False),
    (b"\xff\xfe", None, False),
    (b"[" * 20_000, None, False),
    (b"[1]", None, False),
    (b"null", None, False),
    ({"op": ["room"]}, None, False),
    ({"op": "nope"}, None, False),
    ({"op": "room"}, None, False),
    ({"op": "room", "room": ["r", 1, None, True, 2.5, {"k": "v"}]}, None, False),
    ({"op": "room", "room": "r", "last": 5.0}, None, False),
    ({"op": "room", "room": "r", "last": 0}, None, False),
    ({"op": "room", "room": "r", "last": 10**30}, None, False),
    ({"op": "room", "room": "r", "before": 1e10}, None, False),
    ({"op": "room", "room": "r", "before": True}, None, False),
    ({"op": "room", "room": "r", "extra": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "g" * 40: 7}, None, False),
    ({"op": "dms", "as": "al-1"}, None, False),
    ({"op": "topic", "topic": "inbox/al-1"}, None, False),
    ({"op": "topic", "topic": "a/#"}, None, False),
    ({"op": "topic", "topic": 5}, None, False),
    ({"op": "topic", "topic": "a\u0001b"}, None, False),
    ({"op": "room", "room": "r", "as": 5}, None, False),
    ({"op": "room", "room": "r", "as": None}, AL, False),
    ({"op": "ping", "as": BO}, AL, False),
    ({"op": "ping"}, "a b", False),
    ({"op": "dms", "peer": "x/y"}, AL, False),
    ({"op": "recent"}, AL, False),
    ({"op": "recent", "last": 9999}, "admin", False),
    ({"op": "recent", "last": 0}, "admin", False),
    ({"op": "drop", "addr": BO}, AL, False),
    ({"op": "broadcasts", "since": float("inf")}, None, False),
    (b'{"op": "broadcasts", "since": NaN}', None, False),
    (b"x" * 16_385, None, False),
    # the documented divergences: the error echoes NaN as {} in Rust; Rust refuses nesting past 128 at parse time;
    # Python refuses an integer over 4300 digits at parse time, where Rust parses it and checks the field
    (b'{"op": "room", "room": "r", "last": ' + b"9" * 4301 + b"}", None, True),
    (b'{"op": "ping", "x": ' + b"9" * 4301 + b"}", None, True),
    (b'{"op": "room", "room": NaN}', None, True),
    (b'{"op": "room", "room": ' + b"[" * 200 + b"]" * 200 + b"}", None, True),
]
CORR = [None, b"", b"x" * 257, b"y" * 256]  # CorrelationData cases, each on a ping
RESP = [None, "elsewhere/resp/x", "history/resp/" + "z" * 129]  # ResponseTopic cases with no reply at all


class Asker:
    """Raw requests on history/resp/golden, so the payload, CorrelationData and expiry are exactly what was sent."""

    def __init__(self):
        self.replies, self.cv, self.n = {}, threading.Condition(), 0
        self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="golden-asker", protocol=mqtt.MQTTv5)
        self.c.on_message = self.on_message
        self.c.connect(config.HOST, config.PORT)
        self.c.loop_start()
        self.c.subscribe("history/resp/golden", qos=1)

    def close(self):
        self.c.disconnect()
        self.c.loop_stop()

    def on_message(self, _c, _u, m):
        with self.cv:
            corr = getattr(m.properties, "CorrelationData", None)
            self.replies[corr] = (m.payload.decode(), getattr(m.properties, "MessageExpiryInterval", None))
            self.cv.notify_all()

    def publish(self, topic, payload):
        self.c.publish(topic, payload, qos=1).wait_for_publish(5)

    def ask(self, req, via=None, corr="auto", resp="history/resp/golden", wait=3.0):
        self.n += 1
        props = Properties(PacketTypes.PUBLISH)
        if resp is not None:
            props.ResponseTopic = resp
        key = f"k{self.n}".encode() if corr == "auto" else corr
        if key is not None:
            props.CorrelationData = key
        if key is not None and not 0 < len(key) <= 256:
            key = None  # refused CorrelationData: the error reply carries none
        with self.cv:
            self.replies.pop(key, None)  # nothing stale under the key this one is matched by
        payload = req if isinstance(req, bytes) else json.dumps(req).encode()
        self.c.publish("history/req" + (f"/{via}" if via else ""), payload, qos=1, properties=props).wait_for_publish(5)
        end = time.monotonic() + wait
        with self.cv:
            while key not in self.replies and time.monotonic() < end:
                self.cv.wait(0.1)
            return self.replies.pop(key, None)


def describe(req, via):
    text = req.decode(errors="replace") if isinstance(req, bytes) else json.dumps(req)
    return (text if len(text) <= 80 else f"{text[:60]}... ({len(text)} bytes)") + (
        f" on history/req/{via}" if via else ""
    )


def replay(cmd, db, log):
    """Start cmd on a fresh db, send the corpus, and return every reply as {"request", "reply", "expiry"}."""
    asker = Asker()
    proc = subprocess.Popen(cmd, stderr=open(log, "a"), env={**os.environ, "HISTORY_DB": db})
    try:
        assert wait_for(lambda: asker.ask({"op": "ping"}, wait=0.5), 30), open(log).read()
        for topic, payload in MESSAGES:
            asker.publish(topic, payload)
        assert asker.ask({"op": "ping"}), "a request after the messages: historyd handles in order"
        out = []
        for req, via, _ in REQUESTS:
            out.append((describe(req, via), asker.ask(req, via)))
        for corr in CORR:
            out.append((f"ping with CorrelationData {corr!r:.20}", asker.ask({"op": "ping"}, corr=corr)))
        for resp in RESP:
            out.append((f"ping with ResponseTopic {resp!r:.30}", asker.ask({"op": "ping"}, resp=resp, wait=1.0)))
        return [{"request": d, "reply": r and r[0], "expiry": r and r[1]} for d, r in out]
    finally:
        proc.terminate()
        proc.wait(10)
        asker.close()


def test_replies_match_golden(fresh_broker, state_dir, tmp_path):
    log = os.path.join(state_dir, "golden.log")
    got = replay(HISTORYD, str(tmp_path / "under-test.db"), log)
    golden = json.loads(GOLDEN.read_text())
    assert [g["request"] for g in golden] == [g["request"] for g in got], "the corpus and the golden file differ"
    for want, have in zip(golden, got, strict=True):
        reply = want.get("rust", want["reply"])
        assert have["reply"] == reply, (want["request"], reply and reply[:300], have["reply"] and have["reply"][:300])
        if reply is not None:
            assert want["expiry"] - 2 <= have["expiry"] <= want["expiry"], (want["request"], have["expiry"])


def test_golden_file_is_the_corpus():
    """Every corpus entry has its recorded reply, and only the documented divergences carry a Rust override."""
    golden = json.loads(GOLDEN.read_text())
    assert len(golden) == len(REQUESTS) + len(CORR) + len(RESP)
    overrides = [g["request"] for g in golden if "rust" in g]
    assert overrides == [describe(req, via) for req, via, div in REQUESTS if div]
