"""Golden replies: a fixed script of raw ledger requests whose normalised replies must equal what the Python ledgerd
answered, recorded in golden/ledgerd.jsonl. The file was generated from the Python ledgerd before it was removed (v0.1.0
is the last release that has it) and is now the reference: a deliberate change to a reply is a reviewed edit of that
file. It runs against whichever ledgerd AGENT_BRIDGE_LEDGERD names, the bundled Rust one by default.

Normalised the same way on both sides: request and lease ids become R1, L1, ... in order of first appearance, every
timestamp becomes "<t>", fencing tokens count from the first one seen, and dump's "ledgerd" field is dropped. Error
texts, keys and their order, list order and "truncated" are compared exactly. A reply over 4 KB is compared by its
size and the sha256 of its normalised JSON.
"""

import hashlib
import json
import os
import re
import subprocess
import threading
from pathlib import Path

import paho.mqtt.client as mqtt
from conftest import LEDGERD, _service_env, wait_for
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from agent_bridge import config

HERE = Path(__file__).resolve().parent
BROKER_CONF = HERE / "mosquitto-minimal.conf"  # no packet cap, so oversized requests reach ledgerd
GOLDEN = HERE / "golden" / "ledgerd.jsonl"
TIMES = {"ts", "since", "last_seen", "created", "updated", "expires_at", "queued_at", "deadline"}
ID = re.compile(r"[0-9a-f]{16}")
SUMMARY_OVER = 4096
# The Rust ledgerd's accepted divergences (README, Known differences), by step: what it answers instead.
RUST = {
    "junk depth 200": {"ok": False, "error": "payload must be UTF-8 JSON"},
    "junk huge int": {"ok": False, "error": "resource: required"},
}


class Wire:
    """One raw MQTT 5 client; every reply topic under ledger/resp/ is collected."""

    def __init__(self):
        self.got, self.cv = {}, threading.Condition()
        self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
        up, subscribed = threading.Event(), threading.Event()
        self.c.on_connect = lambda *_: up.set()
        self.c.on_subscribe = lambda *_: subscribed.set()
        self.c.on_message = self.arrived
        self.c.connect(config.HOST, config.PORT)
        self.c.loop_start()
        assert up.wait(5)
        self.c.subscribe("ledger/resp/#", qos=1)
        assert subscribed.wait(5)

    def arrived(self, _c, _u, m):
        with self.cv:
            self.got.setdefault(m.topic, []).append(m.payload)
            self.cv.notify_all()

    def send(self, payload, resp, corr, topic):
        props = Properties(PacketTypes.PUBLISH)
        props.ResponseTopic = resp
        if corr is not None:
            props.CorrelationData = corr
        self.c.publish(topic, payload, qos=1, properties=props).wait_for_publish(5)

    def reply(self, resp, n=1):
        with self.cv:
            assert self.cv.wait_for(lambda: len(self.got.get(resp, [])) >= n, 15), f"no reply on {resp}"
            return json.loads(self.got[resp][n - 1])

    def close(self):
        self.c.disconnect()
        self.c.loop_stop()


class Script:
    def __init__(self, wire):
        self.wire, self.steps, self.ids, self.first_token, self.n = wire, [], {}, None, 0
        self.rust = False

    def collect(self, v, key=None):
        """Give each new request id (key "id") and lease id (key "lease_id") its placeholder, in order of appearance."""
        if isinstance(v, dict):
            for k, x in v.items():
                self.collect(x, k)
        elif isinstance(v, list):
            for x in v:
                self.collect(x)
        elif key in ("id", "lease_id") and isinstance(v, str) and ID.fullmatch(v) and v not in self.ids:
            prefix = "R" if key == "id" else "L"
            self.ids[v] = f"{prefix}{sum(p.startswith(prefix) for p in self.ids.values()) + 1}"

    def norm(self, v, key=None):
        if isinstance(v, dict):
            return {k: self.norm(x, k) for k, x in v.items() if k != "ledgerd"}
        if isinstance(v, list):
            return [self.norm(x) for x in v]
        if key in TIMES and isinstance(v, (int, float)) and not isinstance(v, bool):
            return "<t>"
        if key == "token" and isinstance(v, int) and not isinstance(v, bool):
            self.first_token = self.first_token if self.first_token is not None else v
            return v - self.first_token + 1
        if isinstance(v, str):
            for real, placeholder in self.ids.items():
                v = v.replace(real, placeholder)
        return v

    def record(self, name, reply):
        if reply.get("ledgerd", {}).get("impl") == "rust":
            self.rust = True
        self.collect(reply)
        shown = self.norm(reply)
        text = json.dumps(shown)
        if len(text) > SUMMARY_OVER:
            shown = {
                "bytes": len(text),
                "sha256": hashlib.sha256(text.encode()).hexdigest(),
                "truncated": "truncated" in reply,
            }
        self.steps.append({"step": name, "reply": shown})
        return reply

    def send(self, body, topic="ledger/req", corr=b"golden", resp=None):
        self.n += 1
        resp = resp or f"ledger/resp/g{self.n}"
        payload = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.wire.send(payload, resp, corr, topic)
        return resp

    def step(self, name, body, topic="ledger/req", corr=b"golden", resp=None, n=1, record=True):
        reply = self.wire.reply(self.send(body, topic, corr, resp), n)
        return self.record(name, reply) if record else reply


def run(s):
    o, r, w, h = "gold-o1", "gold-r1", "gold-w1", "gold-h1"
    pub, priv = f"{o}/pub", f"{o}/priv"

    # ledgers and resources
    for who in (o, r, w, h):
        s.step(f"init {who}", {"op": "init", "as": who})
    s.step("init again", {"op": "init", "as": o})
    s.step("ledger before init", {"op": "ledger", "as": "gold-none0"})
    s.step("add pub", {"op": "add", "as": o, "name": "pub", "description": "shared", "visibility": "public",
                       "max_level": "write"})  # fmt: skip
    s.step("add priv", {"op": "add", "as": o, "name": "priv", "description": "secret"})
    s.step("add again", {"op": "add", "as": o, "name": "pub"})
    s.step("ledger own", {"op": "ledger", "as": o})
    s.step("ledger other", {"op": "ledger", "as": r, "owner": o})
    s.step("public", {"op": "public", "as": r})
    s.step("update visibility", {"op": "update", "as": o, "name": "priv", "visibility": "public"})
    s.step("update nothing", {"op": "update", "as": o, "name": "priv"})
    s.step("update not owner", {"op": "update", "as": r, "name": "priv", "visibility": "private"})
    s.step("update back", {"op": "update", "as": o, "name": "priv", "visibility": "private", "description": "still"})
    s.step("add tmp", {"op": "add", "as": o, "name": "tmp"})
    s.step("remove tmp", {"op": "remove", "as": o, "name": "tmp"})
    s.step("remove again", {"op": "remove", "as": o, "name": "tmp"})

    # requests and grants
    s.step("request auto", {"op": "request", "as": r, "resource": pub, "level": "read"})
    s.step("request auto again", {"op": "request", "as": r, "resource": pub, "level": "read"})
    pending = s.step("request pending", {"op": "request", "as": r, "resource": priv, "level": "read"})["request"]
    s.step("request pending again", {"op": "request", "as": r, "resource": priv, "level": "read"})
    s.step("grant not owner", {"op": "grant", "as": r, "request_id": pending["id"]})
    s.step("grant", {"op": "grant", "as": o, "request_id": pending["id"]})
    denied = s.step("request to deny", {"op": "request", "as": w, "resource": priv, "level": "write"})["request"]
    s.step("deny", {"op": "deny", "as": o, "request_id": denied["id"]})
    s.step("request own", {"op": "request", "as": o, "resource": pub, "level": "read"})
    s.step("grants holder", {"op": "grants", "as": r})
    s.step("grants owner", {"op": "grants", "as": o})

    # leases: shared read, exclusive write, FIFO, fencing, renew and release
    s.step("acquire without grant", {"op": "acquire", "as": w, "resource": priv, "level": "read"})
    read = s.step("acquire read", {"op": "acquire", "as": r, "resource": pub, "level": "read", "ttl": 60})["lease"]
    s.step("request write", {"op": "request", "as": w, "resource": pub, "level": "write"})
    s.step("acquire busy", {"op": "acquire", "as": w, "resource": pub, "level": "write"})
    queued = s.send({"op": "acquire", "as": w, "resource": pub, "level": "write", "ttl": 30, "wait": 20})
    s.step("dump with waiter", {"op": "dump", "as": "admin-0"})
    s.step("renew", {"op": "renew", "as": r, "lease_id": read["lease_id"], "ttl": 120})
    s.step("renew not holder", {"op": "renew", "as": w, "lease_id": read["lease_id"]})
    s.step("release not holder", {"op": "release", "as": w, "lease_id": read["lease_id"]})
    s.step("release read", {"op": "release", "as": r, "lease_id": read["lease_id"]})
    write = s.record("waiter served", s.wire.reply(queued))["lease"]
    s.step("acquire behind writer", {"op": "acquire", "as": o, "resource": pub, "level": "write"})
    s.step("owner breaks lease", {"op": "release", "as": o, "lease_id": write["lease_id"]})
    own = s.step("acquire owner write", {"op": "acquire", "as": o, "resource": pub, "level": "write", "ttl": 30})
    s.step("release owner write", {"op": "release", "as": o, "lease_id": own["lease"]["lease_id"]})
    s.step("renew unknown", {"op": "renew", "as": o, "lease_id": "0123456789abcdef"})

    # a redelivered copy is answered from the stored reply, not run again
    again = {"op": "acquire", "as": o, "resource": pub, "level": "write", "ttl": 30}
    first = s.step("replay first", again, corr=b"replay-1", resp="ledger/resp/replay")
    s.step("replay copy", again, corr=b"replay-1", resp="ledger/resp/replay", n=2)
    s.step("release replayed", {"op": "release", "as": o, "lease_id": first["lease"]["lease_id"]})

    # request topics and properties
    s.step("topic ok", {"op": "grants"}, topic=f"ledger/req/{o}")
    s.step("topic as mismatch", {"op": "grants", "as": r}, topic=f"ledger/req/{o}")
    s.step("topic bad address", {"op": "grants"}, topic="ledger/req/bad.addr")
    s.step("plain without as", {"op": "grants"})
    s.step("no correlation", {"op": "init", "as": o}, corr=None)
    s.step("correlation too long", {"op": "init", "as": o}, corr=b"z" * 1000)

    # hostile input
    good = {"op": "acquire", "as": h, "resource": pub, "level": "read"}
    raw_ttl = b'{"op": "acquire", "as": "gold-h1", "resource": "gold-o1/pub", "level": "read", "ttl": '
    junk = [
        ("not json", b"not json"),
        ("not utf-8", b"\xff\xfe\xfd"),
        ("empty", b""),
        ("array", b"[]"),
        ("string", b'"x"'),
        ("null", b"null"),
        ("number", b"42"),
        ("deep arrays", b"[" * 16000),
        ("deep objects", b"{" * 5000),
        ("one megabyte", b"x" * 1_000_000),
        ("huge int", b'{"op": "acquire", "as": "gold-h1", "ttl": ' + b"9" * 5000 + b"}"),
        ("nan", raw_ttl + b"NaN}"),
        ("infinity", raw_ttl + b"Infinity}"),
        ("overflow", raw_ttl + b"1e400}"),
        ("depth 200", b'{"op": "init", "as": "gold-h1", "x": ' + b"[" * 200 + b"]" * 200 + b"}"),
        ("op int", {"op": 5, "as": h}),
        ("op null", {"op": None, "as": h}),
        ("as missing", {"op": "init"}),
        ("as slash", {"op": "init", "as": "bad/addr"}),
        ("as empty", {"op": "init", "as": ""}),
        ("as long", {"op": "init", "as": "a" * 65}),
        ("as list", {"op": "init", "as": ["x"]}),
        ("as unicode", {"op": "init", "as": "ünï"}),
        ("as plus", {"op": "init", "as": "a+"}),
        ("extra field", {"op": "init", "as": h, "extra": 1}),
        ("dunder op", {"op": "__init__", "as": h}),
        ("long op", {"op": "init" * 1000, "as": h}),
        ("name slash", {"op": "add", "as": h, "name": "x/y"}),
        ("name hash", {"op": "add", "as": h, "name": "#"}),
        ("name empty", {"op": "add", "as": h, "name": ""}),
        ("name dot", {"op": "add", "as": h, "name": ".hidden"}),
        ("name int", {"op": "add", "as": h, "name": 123}),
        ("name dict", {"op": "add", "as": h, "name": {"a": 1}}),
        ("description bell", {"op": "add", "as": h, "name": "n", "description": "bell\x07"}),
        ("description surrogate", {"op": "add", "as": h, "name": "n", "description": "\ud800"}),
        ("description long", {"op": "add", "as": h, "name": "n", "description": "x" * 2000}),
        ("visibility bad", {"op": "add", "as": h, "name": "n", "visibility": "secret"}),
        ("level bad", {"op": "add", "as": h, "name": "n", "max_level": "admin"}),
        ("level list", {"op": "add", "as": h, "name": "n", "max_level": ["write"]}),
        ("ttl string", {**good, "ttl": "10"}),
        ("ttl bool", {**good, "ttl": True}),
        ("ttl negative", {**good, "ttl": -1}),
        ("ttl zero", {**good, "ttl": 0}),
        ("ttl huge", {**good, "ttl": 1e9}),
        ("wait huge", {**good, "wait": 10_000}),
        ("acquire level bad", {**good, "level": "admin"}),
        ("resource dots", {**good, "resource": "gold-o1/../x"}),
        ("resource bare", {**good, "resource": "gold-o1"}),
        ("lease id path", {"op": "renew", "as": h, "lease_id": "../../etc/passwd"}),
        ("lease id int", {"op": "renew", "as": h, "lease_id": 12}),
        ("request id bad", {"op": "grant", "as": h, "request_id": "ZZZZZZZZZZZZZZZZ"}),
        ("request id missing", {"op": "grant", "as": h}),
        ("owner long", {"op": "ledger", "as": h, "owner": "x" * 100}),
        ("null field", {"op": "init", "as": "not-inited-0", "x": None}),
        ("not inited", {"op": "public", "as": "never-inited-0"}),
    ]
    for name, body in junk:
        s.step(f"junk {name}", body)

    # a reply too big to send: dump truncates its lists, other ops refuse
    bulk = [f"gold-b{i}" for i in range(3)]
    for b in bulk:
        s.step(f"init {b}", {"op": "init", "as": b}, record=False)
        for j in range(100):
            s.step("bulk add", {"op": "add", "as": b, "name": f"r{j}", "description": "x" * 1000,
                                "visibility": "public"}, record=False)  # fmt: skip
    s.step("dump truncated", {"op": "dump", "as": "admin-0"})
    s.step("public too big", {"op": "public", "as": o})
    for b in bulk:
        s.step(f"drop {b}", {"op": "drop", "as": b}, record=False)

    # drop
    s.step("drop", {"op": "drop", "as": w})
    s.step("ledger dropped", {"op": "ledger", "as": o, "owner": w})
    s.step("grants after drop", {"op": "grants", "as": o})
    s.step("dump final", {"op": "dump", "as": "admin-0"})


def test_replies_match_the_reference(state_dir):
    db, log = os.path.join(state_dir, "ledger.db"), os.path.join(state_dir, "ledgerd.log")
    with open(log, "a") as err:
        proc = subprocess.Popen(LEDGERD, stderr=err, env=_service_env("LEDGER_DB", db, {}))
    wire = Wire()
    try:
        probe = Script(wire)  # its own counter, so the recorded script is unaffected; dump creates no ledger

        def answers():
            probe.send({"op": "dump", "as": "probe-0"}, resp="ledger/resp/probe")
            return wire.got.get("ledger/resp/probe")

        assert wait_for(answers, 30), open(log).read()
        s = Script(wire)
        run(s)
    finally:
        wire.close()
        proc.kill()
        proc.wait()
    golden = [json.loads(line) for line in GOLDEN.read_text().splitlines()]
    assert [g["step"] for g in golden] == [a["step"] for a in s.steps], "the script and the golden file differ"
    for g, a in zip(golden, s.steps, strict=True):
        expected = RUST.get(g["step"], g["reply"]) if s.rust else g["reply"]
        assert a["reply"] == expected, (g["step"], expected, a["reply"])
