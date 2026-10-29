"""The resource ledger end to end: semantics, a manual-ack shared client, a ledgerd kill mid-flight, hostile
payloads and floods, the admin dump, request topics and redelivery, table bounds, a multi-process lock stress test and
the stale sweep.

ledgerd is started and killed by these tests, so they use their own instead of the shared fixture. The broker runs a
minimal config without packet limits, so oversized requests reach ledgerd. Run as a script with "worker" it is one
process of the stress test.
"""

import json
import os
import random
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import paho.mqtt.client as mqtt
import pytest
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from agent_bridge import config
from agent_bridge.ledger import client as ledger

BROKER_CONF = Path(__file__).resolve().parent / "mosquitto-minimal.conf"
TMP = LOG = LD = C = None


def start_ledgerd(db="ledger.db", **env):
    from conftest import start_ledgerd as start

    global LD
    LD = start(os.path.join(TMP, db), LOG, **env)


@pytest.fixture(scope="module", autouse=True)
def ledgerd_of_this_module(fresh_broker):
    global TMP, LOG, C
    TMP = tempfile.mkdtemp()
    LOG = os.path.join(TMP, "ledgerd.log")
    C = ledger.connect()
    start_ledgerd()
    yield
    LD.kill()
    LD.wait()
    C.disconnect()
    C.loop_stop()
    shutil.rmtree(TMP, ignore_errors=True)


def alive():
    assert LD.poll() is None, open(LOG).read()


def call(op, who, **kw):
    return ledger.call(op, who, client=C, **kw)


def err(op, who, **kw):
    try:
        call(op, who, **kw)
    except ledger.LedgerError as e:
        return e.reply
    raise AssertionError(f"{op} {kw} should have failed")


def wait_for(pred, timeout=5):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        time.sleep(0.02)
    return False


class Watch:
    def __init__(self, topic):
        self.msgs = []
        self.c = ledger.connect()
        self.c.on_message = lambda _c, _u, m: self.msgs.append(
            (m.topic, m.payload, getattr(m.properties, "CorrelationData", None))
        )
        subscribed = threading.Event()
        self.c.on_subscribe = lambda *_: subscribed.set()
        self.c.subscribe(topic, qos=1)
        assert subscribed.wait(5)

    def json(self, topic):
        return [json.loads(p) for t, p, _ in self.msgs if t == topic]


def queued(resource, n=1):
    """Wait until resource has n queued waiters. ledgerd is serial, so a dump also proves earlier requests ran."""
    return wait_for(lambda: sum(w["resource"] == resource for w in call("dump", "admin-0")["waiters"]) == n)


def holders(resource):
    return {x["holder"] for x in call("dump", "admin-0")["leases"] if x["resource"] == resource}


def background(op, who, **kw):
    box = {}

    def go():
        t = time.monotonic()
        try:
            box["reply"] = ledger.call(op, who, client=C, **kw)
        except ledger.LedgerError as e:
            box["reply"] = e.reply
        box["secs"] = time.monotonic() - t

    th = threading.Thread(target=go)
    th.start()
    box["thread"] = th
    return box


def worker(who, resource, lvl, n, out):
    c = ledger.connect()
    rows = []
    for _ in range(n):
        lease = ledger.call("acquire", who, client=c, resource=resource, level=lvl, ttl=30, wait=120)["lease"]
        t0 = time.time()
        time.sleep(random.uniform(0, 0.003))
        t1 = time.time()
        ledger.call("release", who, client=c, lease_id=lease["lease_id"])
        rows.append([lvl, lease.get("token"), t0, t1])
    with open(out, "w") as f:
        json.dump(rows, f)


def test_functional():
    a, b, c, d = "alice-a1", "bob-b1", "carol-c1", "dave-d1"
    inbox = Watch("inbox/#")

    assert call("init", a)["created"] is True
    again = call("init", a)
    assert again["created"] is False and again["owner"] == a, "init reuses the existing ledger"
    assert "call init first" in err("ledger", "nobody-0")["error"]
    for x in (b, c, d):
        call("init", x)

    call("add", a, name="pub", description="shared db", visibility="public", max_level="read")
    call("add", a, name="pubw", visibility="public", max_level="write")
    call("add", a, name="priv", description="secret")
    assert "exists" in err("add", a, name="pub")["error"]
    assert [r["name"] for r in call("ledger", a)["resources"]] == ["priv", "pub", "pubw"]
    assert [r["name"] for r in call("ledger", b, owner=a)["resources"]] == ["pub", "pubw"], "others see public only"
    assert {r["id"] for r in call("public", b)["resources"]} == {f"{a}/pub", f"{a}/pubw"}
    assert call("update", a, name="priv", visibility="public")["resource"]["visibility"] == "public"
    assert f"{a}/priv" in {r["id"] for r in call("public", b)["resources"]}
    call("update", a, name="priv", visibility="private", description="still secret")
    assert f"{a}/priv" not in {r["id"] for r in call("public", b)["resources"]}
    assert "nothing to update" in err("update", a, name="priv")["error"]
    assert "no such resource" in err("update", b, name="priv")["error"], "only the owner mutates"
    call("add", a, name="tmp")
    call("remove", a, name="tmp")
    assert "no such resource" in err("remove", a, name="tmp")["error"]

    # grants
    auto = call("request", b, resource=f"{a}/pub", level="read")["request"]
    assert auto["status"] == "granted"
    assert call("request", b, resource=f"{a}/pub", level="read")["request"]["id"] == auto["id"], "idempotent"
    pend = call("request", b, resource=f"{a}/pub", level="write")["request"]
    assert pend["status"] == "pending"
    assert wait_for(lambda: any(pend["id"] in m["body"] and m["from"] == "ledgerd" for m in inbox.json(f"inbox/{a}")))
    note = inbox.json(f"inbox/{a}")[-1]
    assert set(note) == {"from", "body", "ts"}, "same shape as a bus DM"
    assert "no such request" in err("grant", b, request_id=pend["id"])["error"], "only the owner grants"
    assert call("grant", a, request_id=pend["id"])["request"]["status"] == "granted"
    assert wait_for(lambda: any("granted" in m["body"] for m in inbox.json(f"inbox/{b}")))
    secret = call("request", b, resource=f"{a}/priv", level="read")["request"]
    assert secret["status"] == "pending", "private resources are never auto-granted"
    assert call("deny", a, request_id=secret["id"])["request"]["status"] == "denied"
    assert wait_for(lambda: any("denied" in m["body"] for m in inbox.json(f"inbox/{b}")))
    assert "you own" in err("request", a, resource=f"{a}/pub", level="read")["error"]
    g = call("grants", b)
    assert {(r["resource"], r["level"], r["status"]) for r in g["held"]} == {
        (f"{a}/pub", "read", "granted"),
        (f"{a}/pub", "write", "granted"),
        (f"{a}/priv", "read", "denied"),
    }
    assert len(call("grants", a)["asked"]) == 3

    # acquire needs a grant
    assert "request it first" in err("acquire", c, resource=f"{a}/pub", level="read")["error"]
    assert "request it first" in err("acquire", b, resource=f"{a}/priv", level="read")["error"]
    call("request", c, resource=f"{a}/pub", level="read")
    assert "request it first" in err("acquire", c, resource=f"{a}/pub", level="write")["error"]
    owner = call("acquire", a, resource=f"{a}/priv", level="write", ttl=5)["lease"]
    assert owner["token"] >= 1, "owners hold implicit write"
    call("release", a, lease_id=owner["lease_id"])

    # shared readers, exclusive writer, FIFO
    readers = [f"reader-r{i}" for i in range(8)]
    for r in readers:
        call("init", r)
        call("request", r, resource=f"{a}/pub", level="read")
    with ThreadPoolExecutor(8) as ex:
        leases = list(ex.map(lambda r: call("acquire", r, resource=f"{a}/pub", level="read", ttl=30)["lease"], readers))
    assert len({x["lease_id"] for x in leases}) == 8 and all("token" not in x for x in leases), "readers share"
    busy = err("acquire", b, resource=f"{a}/pub", level="write")
    assert busy["error"] == "busy" and {h["holder"] for h in busy["holders"]} == set(readers)
    writer = background("acquire", b, resource=f"{a}/pub", level="write", ttl=30, wait=10)
    assert queued(f"{a}/pub")
    assert err("acquire", c, resource=f"{a}/pub", level="read")["error"] == "busy", "a queued writer blocks new readers"
    late_reader = background("acquire", c, resource=f"{a}/pub", level="read", ttl=30, wait=10)
    assert queued(f"{a}/pub", 2)
    for x in leases[:-1]:
        call("release", x["holder"], lease_id=x["lease_id"])
    assert holders(f"{a}/pub") == {leases[-1]["holder"]} and queued(f"{a}/pub", 2), "writer waits for the last reader"
    call("release", leases[-1]["holder"], lease_id=leases[-1]["lease_id"])
    writer["thread"].join()
    w = writer["reply"]["lease"]
    assert w["holder"] == b and w["token"] > owner["token"]
    assert queued(f"{a}/pub", 1) and "reply" not in late_reader, "the reader queued behind the writer waits its turn"
    call("release", b, lease_id=w["lease_id"])
    late_reader["thread"].join()
    assert late_reader["reply"]["lease"]["holder"] == c
    call("release", c, lease_id=late_reader["reply"]["lease"]["lease_id"])

    # fencing tokens strictly increase
    tokens = []
    for name in ("pub", "pubw", "pub", "priv"):
        x = call("acquire", a, resource=f"{a}/{name}", level="write", ttl=5)["lease"]
        tokens.append(x["token"])
        call("release", a, lease_id=x["lease_id"])
    assert tokens == sorted(tokens) and len(set(tokens)) == 4 and tokens[0] > w["token"]

    # TTL expiry frees waiters
    call("acquire", a, resource=f"{a}/pubw", level="write", ttl=1)
    call("request", b, resource=f"{a}/pubw", level="write")
    t = time.monotonic()
    got = call("acquire", b, resource=f"{a}/pubw", level="write", ttl=1, wait=5)["lease"]
    assert 0.5 < time.monotonic() - t < 3, "expired lease handed to the waiter"

    # renew and release
    renewed = call("renew", b, lease_id=got["lease_id"], ttl=6)["lease"]
    assert renewed["expires_at"] > got["expires_at"]
    time.sleep(1.2)
    assert err("acquire", a, resource=f"{a}/pubw", level="write")["error"] == "busy", "renew kept it past the first ttl"
    assert "no such lease" in err("release", c, lease_id=got["lease_id"])["error"], "only holder or owner releases"
    assert "no such lease" in err("renew", a, lease_id=got["lease_id"])["error"], "only the holder renews"
    call("release", b, lease_id=got["lease_id"])
    assert "no such lease" in err("renew", b, lease_id=got["lease_id"])["error"]
    stuck = call("acquire", b, resource=f"{a}/pubw", level="write", ttl=60)["lease"]
    call("release", a, lease_id=stuck["lease_id"])
    assert "no such lease" in err("release", b, lease_id=stuck["lease_id"])["error"], "the owner can break a lease"
    expiring = call("acquire", b, resource=f"{a}/pubw", level="write", ttl=1)["lease"]
    time.sleep(1.3)
    assert "no such lease" in err("renew", b, lease_id=expiring["lease_id"])["error"], (
        "expired leases cannot be renewed"
    )

    # revoking a grant ends the holder's leases at that level
    held = call("acquire", b, resource=f"{a}/pub", level="write", ttl=60)["lease"]
    call("deny", a, request_id=pend["id"])
    assert "no such lease" in err("renew", b, lease_id=held["lease_id"])["error"]
    assert call("acquire", b, resource=f"{a}/pub", level="read", ttl=1)["lease"], "the auto read grant remains"

    # drop cleans everything
    call("add", d, name="dres", visibility="public", max_level="write")
    call("request", b, resource=f"{d}/dres", level="read")
    call("request", d, resource=f"{a}/pubw", level="write")
    call("request", d, resource=f"{a}/priv", level="read")
    dl = call("acquire", d, resource=f"{a}/pubw", level="write", ttl=60)["lease"]
    blocker = call("acquire", d, resource=f"{d}/dres", level="write", ttl=60)["lease"]
    on_dres = background("acquire", b, resource=f"{d}/dres", level="read", ttl=5, wait=10)
    d_waits = background("acquire", d, resource=f"{a}/pubw", level="write", ttl=5, wait=10)
    assert queued(f"{d}/dres") and queued(f"{a}/pubw")
    assert blocker and "reply" not in on_dres and "reply" not in d_waits
    call("drop", d)
    on_dres["thread"].join()
    d_waits["thread"].join()
    assert "dropped" in on_dres["reply"]["error"] and "dropped" in d_waits["reply"]["error"]
    assert "call init first" in err("ledger", d)["error"]
    assert "no ledger" in err("ledger", a, owner=d)["error"]
    assert not any(r["owner"] == d for r in call("public", a)["resources"])
    assert not any(d in (r["holder"], r["owner"]) for r in call("grants", a)["asked"] + call("grants", b)["held"])
    assert call("acquire", a, resource=f"{a}/pubw", level="write", ttl=1)["lease"], f"{dl['lease_id']} was freed"
    assert call("init", d)["created"] is True and call("ledger", d)["resources"] == []
    alive()
    print("functional ok")


def test_shared_client():
    # shaped like the Bus: manual_ack, persistent session, its own on_message
    me = "busagent-b1"
    own = []
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=me, protocol=mqtt.MQTTv5, manual_ack=True)
    ready = threading.Event()
    c.on_connect = lambda *_: ready.set()
    c.on_message = lambda _c, _u, m: own.append(m.topic)
    props = Properties(PacketTypes.CONNECT)
    props.SessionExpiryInterval = 600
    props.ReceiveMaximum = 5
    c.connect(config.HOST, config.PORT, clean_start=False, properties=props)
    c.loop_start()
    assert ready.wait(5)
    for _ in range(30):
        ledger.call("init", me, client=c, timeout=3)
    assert own == [], "replies never reach the client's own on_message"
    c.unsubscribe(f"ledger/resp/{me}")
    assert ledger.call("init", me, client=c, timeout=3)["created"] is False, "a lost subscription is restored"
    c.disconnect()
    c.loop_stop()
    print("shared client ok")


def test_restart():
    a, f, g = "owner-o1", "fred-f1", "gina-g1"
    for x in (a, f, g):
        call("init", x)
    call("add", a, name="rs", visibility="public", max_level="read")
    pend = call("request", f, resource=f"{a}/rs", level="write")["request"]
    call("request", g, resource=f"{a}/rs", level="read")
    held = call("acquire", a, resource=f"{a}/rs", level="write", ttl=4)["lease"]
    waiting = background("acquire", g, resource=f"{a}/rs", level="read", ttl=5, wait=30)
    assert queued(f"{a}/rs")
    LD.kill()
    LD.wait()
    granted = background("grant", a, request_id=pend["id"], timeout=30)
    added = background("add", a, name="afterdown", timeout=30)
    time.sleep(0.3)
    assert "reply" not in granted and "reply" not in waiting
    start_ledgerd()
    for x in (granted, added, waiting):
        x["thread"].join()
    assert granted["reply"]["request"]["status"] == "granted", "request sent while ledgerd was down is processed"
    assert added["reply"]["ok"]
    assert waiting["reply"]["lease"]["holder"] == g, "a waiter queued before the restart still gets its turn"
    assert waiting["secs"] > 2, "only after the write lease taken before the restart expired"
    call("release", g, lease_id=waiting["reply"]["lease"]["lease_id"])
    assert [r["name"] for r in call("ledger", a)["resources"]] == ["afterdown", "rs"]
    assert {r["status"] for r in call("grants", f)["held"]} == {"granted"}
    later = call("acquire", a, resource=f"{a}/rs", level="write", ttl=1, wait=10)["lease"]
    assert later["token"] > held["token"], "fencing survives restarts"
    alive()
    print("restart ok")


def test_hostile():
    me = "hostile-h1"
    call("init", me)
    replies = Watch("ledger/resp/#")
    sniff = Watch("#")
    raw = ledger.connect()
    seq = [0]

    def send(payload, rt="ledger/resp/hostile", corr=True, qos=1):
        seq[0] += 1
        props = Properties(PacketTypes.PUBLISH)
        if rt is not None:
            props.ResponseTopic = rt
        cid = f"c{seq[0]}".encode()
        if corr is True:
            props.CorrelationData = cid
        elif corr:
            props.CorrelationData = corr
        payload = payload if isinstance(payload, (bytes, str)) else json.dumps(payload)
        raw.publish("ledger/req", payload, qos=qos, properties=props)
        return cid

    def answer(cid):
        assert wait_for(lambda: any(c == cid for _, _, c in replies.msgs)), cid
        return next(json.loads(p) for _, p, c in replies.msgs if c == cid)

    good = {"op": "acquire", "as": me, "resource": "alice-a1/pub", "level": "read"}
    junk = [
        b"not json",
        b"\xff\xfe\xfd",
        b"",
        b"[]",
        b'"x"',
        b"null",
        b"42",
        b"[" * 16000,
        b"{" * 5000,
        b"x" * 1_000_000,
        b'{"op": "acquire", "as": "hostile-h1", "ttl": ' + b"9" * 5000 + b"}",
        b'{"op": "acquire", "as": "hostile-h1", "resource": "alice-a1/pub", "level": "read", "ttl": NaN}',
        b'{"op": "acquire", "as": "hostile-h1", "resource": "alice-a1/pub", "level": "read", "ttl": Infinity}',
        b'{"op": "acquire", "as": "hostile-h1", "resource": "alice-a1/pub", "level": "read", "ttl": 1e400}',
        {"op": 5, "as": me},
        {"op": None, "as": me},
        {"op": "init"},
        {"op": "init", "as": "bad/addr"},
        {"op": "init", "as": ""},
        {"op": "init", "as": "a" * 65},
        {"op": "init", "as": ["x"]},
        {"op": "init", "as": "ünï"},
        {"op": "init", "as": "a+"},
        {"op": "init", "as": me, "extra": 1},
        {"op": "__init__", "as": me},
        {"op": "init" * 1000, "as": me},
        {"op": "add", "as": me, "name": "x/y"},
        {"op": "add", "as": me, "name": "#"},
        {"op": "add", "as": me, "name": ""},
        {"op": "add", "as": me, "name": ".hidden"},
        {"op": "add", "as": me, "name": 123},
        {"op": "add", "as": me, "name": {"a": 1}},
        {"op": "add", "as": me, "name": "n", "description": "bell\x07"},
        {"op": "add", "as": me, "name": "n", "description": "\ud800"},
        {"op": "add", "as": me, "name": "n", "description": "x" * 2000},
        {"op": "add", "as": me, "name": "n", "visibility": "secret"},
        {"op": "add", "as": me, "name": "n", "max_level": "admin"},
        {"op": "add", "as": me, "name": "n", "max_level": ["write"]},
        {**good, "ttl": "10"},
        {**good, "ttl": True},
        {**good, "ttl": -1},
        {**good, "ttl": 0},
        {**good, "ttl": 1e9},
        {**good, "wait": 10_000},
        {**good, "level": "admin"},
        {**good, "resource": "alice-a1/../x"},
        {**good, "resource": "alice-a1"},
        {"op": "renew", "as": me, "lease_id": "../../etc/passwd"},
        {"op": "renew", "as": me, "lease_id": 12},
        {"op": "grant", "as": me, "request_id": "ZZZZZZZZZZZZZZZZ"},
        {"op": "grant", "as": me},
        {"op": "ledger", "as": me, "owner": "x" * 100},
        {"op": "init", "as": "not-inited-0", "x": None},
        {"op": "public", "as": "never-inited-0"},
    ]
    for payload in junk:
        reply = answer(send(payload))
        assert reply["ok"] is False and isinstance(reply["error"], str) and len(reply["error"]) < 400, (
            payload[:80],
            reply,
        )
    assert "description" in answer(send({"op": "add", "as": me, "name": "n", "description": "\ud800"}))["error"]

    before = len(replies.msgs)
    for rt in (
        None,
        "inbox/victim-v1",
        "agents/victim-v1",
        "ledger/respx",
        "ledger/resp/",
        "ledger/resp/a/b",
        "ledger/resp/" + "x" * 200,
        "ledger/req",
    ):
        send({"op": "init", "as": me}, rt=rt)
    nocorr = send({"op": "init", "as": me}, corr=False)
    bigcorr = send({"op": "init", "as": me}, corr=b"z" * 1000)
    marker = send({"op": "init", "as": me})
    answer(marker)  # ledgerd is serial and the broker keeps one publisher's order: nothing earlier is still coming
    late = [m for m in replies.msgs[before:] if m[2] != marker]
    assert len(late) == 2 and all(c is None for _, _, c in late), "only the two with a valid topic were answered"
    assert all("CorrelationData" in json.loads(p)["error"] for _, p, _ in late)
    assert nocorr and bigcorr
    assert not [t for t, _, _ in sniff.msgs if t in ("inbox/victim-v1", "agents/victim-v1", "ledger/respx")]
    assert [t for t, _, _ in sniff.msgs].count("ledger/req") == len(junk) + 12, "ledgerd never published on ledger/req"

    for _ in range(3000):
        send(b"\x00junk", qos=0)
    for i in range(300):
        send({"op": "init", "as": me, "n": i})
    assert call("init", me, timeout=30)["created"] is False, "still serving after floods"
    assert call("public", me)["resources"] is not None
    alive()
    print("hostile ok")


def test_dump():
    o, r = "dumpo-d1", "dumpr-r1"
    for x in (o, r):
        call("init", x)
    call("add", o, name="dp", description="public one", visibility="public", max_level="read")
    call("add", o, name="dq", description="private one")
    granted = call("request", r, resource=f"{o}/dp", level="read")["request"]
    pending = call("request", r, resource=f"{o}/dq", level="write")["request"]
    held = call("acquire", o, resource=f"{o}/dp", level="write", ttl=60)["lease"]
    t = time.time()
    waiting = background("acquire", r, resource=f"{o}/dp", level="read", ttl=60, wait=20)
    assert queued(f"{o}/dp")

    d = call("dump", "admin-0")
    assert "truncated" not in d and abs(d["ts"] - time.time()) < 5
    assert "call init first" in err("ledger", "admin-0")["error"], "dump needs no ledger and creates none"
    led = {x["owner"]: x for x in d["ledgers"]}
    assert set(led[o]) == {"owner", "since", "last_seen"} and led[r]["last_seen"] >= led[r]["since"]
    res = {x["id"]: x for x in d["resources"]}
    assert res[f"{o}/dq"]["visibility"] == "private" and res[f"{o}/dq"]["description"] == "private one", (
        "private included"
    )
    assert res[f"{o}/dp"] == call("ledger", o)["resources"][0], "same shape as ledger"
    reqs = {x["id"]: x for x in d["requests"]}
    assert reqs[granted["id"]] == granted and reqs[pending["id"]] == pending, "same shape as request"
    assert d["requests"][0]["status"] == "pending", "pending first"
    assert [x for x in d["leases"] if x["resource"] == f"{o}/dp"] == [held], "same shape as acquire"
    w = [x for x in d["waiters"] if x["resource"] == f"{o}/dp"]
    assert (
        len(w) == 1
        and w[0]["holder"] == r
        and w[0]["level"] == "read"
        and set(w[0]) == {"resource", "holder", "level", "queued_at", "deadline"}
    )
    assert abs(w[0]["queued_at"] - t) < 1 and abs(w[0]["deadline"] - w[0]["queued_at"] - 20) < 0.01

    # admin interventions are existing ops run as the owner
    call("release", o, lease_id=held["lease_id"])
    waiting["thread"].join()
    theirs = waiting["reply"]["lease"]
    call("release", o, lease_id=theirs["lease_id"])
    call("grant", o, request_id=pending["id"])
    call("deny", o, request_id=pending["id"])
    d = call("dump", "admin-0")
    assert not [x for x in d["leases"] if x["resource"] == f"{o}/dp"], "owner release force-released the holder's lease"
    assert not [x for x in d["waiters"] if x["resource"] == f"{o}/dp"]
    assert {x["id"]: x["status"] for x in d["requests"]}[pending["id"]] == "denied", "deny revoked the grant"

    # truncation instead of an oversized reply
    bulk = [f"bulk-b{i}" for i in range(3)]
    for b in bulk:
        call("init", b)
        for j in range(100):
            call("add", b, name=f"r{j}", description="x" * 1000, visibility="public")
    raw = ledger.connect()
    sizes = Watch("ledger/resp/#")
    d = call("dump", "admin-0")
    assert d["truncated"] is True and 0 < len(d["resources"]) < 300
    assert all(len(d[k]) <= len(d["resources"]) for k in ("ledgers", "requests", "leases", "waiters")), (
        "one cap for all"
    )
    assert wait_for(lambda: sizes.msgs) and max(len(p) for _, p, _ in sizes.msgs) <= 240_000
    assert "reply over" in err("public", o)["error"], "an oversized reply is refused, not sent"
    for b in bulk:
        call("drop", b)
    assert "truncated" not in call("dump", "admin-0")

    # hostile dumps
    for bad in ({"as": "bad/x"}, {}, {"as": 5}, {"as": "a" * 65}, {"as": "admin-0", "owner": o}):
        props = Properties(PacketTypes.PUBLISH)
        props.ResponseTopic, props.CorrelationData = "ledger/resp/hostile", json.dumps(bad).encode()[:200]
        raw.publish("ledger/req", json.dumps({"op": "dump", **bad}), qos=1, properties=props)
    assert wait_for(lambda: sum(1 for t_, _, _ in sizes.msgs if t_ == "ledger/resp/hostile") == 5)
    assert all(json.loads(p)["ok"] is False for t_, p, _ in sizes.msgs if t_ == "ledger/resp/hostile")
    for _ in range(200):
        raw.publish("ledger/req", json.dumps({"op": "dump", "as": "admin-0"}), qos=0, properties=props)
    assert call("dump", "admin-0", timeout=30)["ok"]
    alive()
    print("dump ok")


def test_topics_and_redelivery():
    me, o = "topic-t1", "topic-o1"
    for x in (me, o):
        call("init", x)
    call("add", o, name="tr", visibility="public", max_level="write")
    replies = Watch("ledger/resp/topics")
    raw = ledger.connect()
    seq = [0]

    def send(topic, body, corr=None):
        seq[0] += 1
        props = Properties(PacketTypes.PUBLISH)
        props.ResponseTopic, props.CorrelationData = "ledger/resp/topics", corr or f"t{seq[0]}".encode()
        raw.publish(topic, json.dumps(body), qos=1, properties=props)
        return props.CorrelationData

    def answers(cid, n=1):
        assert wait_for(lambda: sum(c == cid for _, _, c in replies.msgs) >= n), cid
        return [json.loads(p) for _, p, c in replies.msgs if c == cid]

    # ledger/req/<addr> fixes the acting address; the body's as is optional and must agree
    assert answers(send(f"ledger/req/{me}", {"op": "grants"}))[0]["ok"] is True
    assert "must match" in answers(send(f"ledger/req/{me}", {"op": "grants", "as": o}))[0]["error"]
    assert "request topic" in answers(send("ledger/req/bad.addr", {"op": "grants"}))[0]["error"]
    assert answers(send("ledger/req", {"op": "grants", "as": me}))[0]["ok"] is True, "plain ledger/req still works"
    assert "as: required" in answers(send("ledger/req", {"op": "grants"}))[0]["error"]

    # a redelivered copy (same ResponseTopic, CorrelationData and payload) gets the first answer, no second effect
    call("request", me, resource=f"{o}/tr", level="write")
    acquire = {"op": "acquire", "resource": f"{o}/tr", "level": "write", "ttl": 30}
    send(f"ledger/req/{me}", acquire, b"same")
    send(f"ledger/req/{me}", acquire, b"same")
    first, second = answers(b"same", 2)
    assert first == second and first["ok"], "the duplicate replays the first reply"
    leases = [x for x in call("dump", "admin-0")["leases"] if x["resource"] == f"{o}/tr"]
    assert [x["holder"] for x in leases] == [me], "one lease, not two"
    other = send(f"ledger/req/{me}", {**acquire, "ttl": 31}, b"same")
    assert answers(other, 3)[-1]["error"] == "busy", "same CorrelationData, different payload: a new request"
    waiter = {"op": "acquire", "resource": f"{o}/tr", "level": "write", "ttl": 5, "wait": 20}
    send(f"ledger/req/{o}", waiter, b"wait")
    send(f"ledger/req/{o}", waiter, b"wait")
    assert queued(f"{o}/tr", 1), "a duplicate of a queued acquire queues nothing more"
    call("release", me, lease_id=first["lease"]["lease_id"])
    assert answers(b"wait")[0]["lease"]["holder"] == o
    alive()
    print("topics ok")


def test_bounds():
    from agent_bridge.ledger.store import MAX_REQUESTS

    me, o = "bound-b1", "bound-o1"
    for x in (me, o):
        call("init", x)
    call("add", o, name="bp")
    first = None
    for _ in range(MAX_REQUESTS + 3):
        rq = call("request", me, resource=f"{o}/bp", level="read")["request"]
        first = first or rq["id"]
        call("deny", o, request_id=rq["id"])
    held = call("grants", me)["held"]
    assert len(held) == MAX_REQUESTS and first not in {r["id"] for r in held}, "the oldest denied requests make room"
    alive()
    print("bounds ok")


def test_stress():
    owner, res = "stress-s1", "stress-s1/hot"
    call("init", owner)
    call("add", owner, name="hot", visibility="public", max_level="write")
    writers = [f"writer-w{i}" for i in range(8)]
    readers = [f"sreader-r{i}" for i in range(2)]
    for x in writers + readers:
        call("init", x)
        call("request", x, resource=res, level="write" if x in writers else "read")
    n = 50
    procs, outs = [], []
    for x in writers + readers:
        out = os.path.join(TMP, f"{x}.json")
        outs.append(out)
        procs.append(
            subprocess.Popen(
                [sys.executable, __file__, "worker", x, res, "write" if x in writers else "read", str(n), out]
            )
        )
    t = time.monotonic()
    for p in procs:
        assert p.wait(timeout=300) == 0
    rows = [r for out in outs for r in json.load(open(out))]
    writes = sorted((r for r in rows if r[0] == "write"), key=lambda r: r[2])
    reads = [r for r in rows if r[0] == "read"]
    assert len(writes) == len(writers) * n and len(reads) == len(readers) * n
    for prev, cur in zip(writes, writes[1:], strict=False):
        assert cur[2] >= prev[3], f"write leases overlap: {prev} {cur}"
        assert cur[1] > prev[1], f"fencing token not increasing: {prev} {cur}"
    assert len({w[1] for w in writes}) == len(writes)
    for r in reads:
        for w in writes:
            assert not (r[2] < w[3] and w[2] < r[3]), f"read overlaps write: {r} {w}"
    assert call("acquire", owner, resource=res, level="write", ttl=1)["lease"], "nothing left behind"
    alive()
    print(
        f"stress ok: {len(writes)} writes, {len(reads)} reads by {len(procs)} processes in {time.monotonic() - t:.1f}s"
    )


def test_stale():
    LD.terminate()
    LD.wait()
    start_ledgerd("stale.db", AGENT_BRIDGE_STALE_DAYS=str(1 / 86400))  # 1 s, swept every 0.1 s
    pres = ledger.connect()

    def presence(who, rec):
        pres.publish(f"agents/{who}", json.dumps(rec) if rec else b"", qos=1, retain=True).wait_for_publish(5)

    def gone(owner):
        call("init", "active-a1")  # each poll keeps the active agent's ledger fresh
        try:
            call("ledger", "active-a1", owner=owner)
        except ledger.LedgerError as e:
            return f"no ledger for {owner}" in e.reply["error"]
        return False

    presence("kept-k1", {"name": "kept", "keep": True, "seen": time.time() - 1000})
    presence("junk-j1", "not an object")
    for x in ("ghost-g1", "kept-k1", "active-a1"):
        call("init", x)
    call("add", "ghost-g1", name="gres", visibility="public")
    call("add", "kept-k1", name="kpub", visibility="public")
    call("add", "kept-k1", name="kpriv")
    call("request", "active-a1", resource="ghost-g1/gres", level="read")
    call("request", "active-a1", resource="kept-k1/kpub", level="read")
    call("request", "active-a1", resource="kept-k1/kpriv", level="read")
    assert wait_for(lambda: gone("ghost-g1"), 10), "idle, no presence: dropped"

    def held():
        return [(r["resource"], r["status"]) for r in call("grants", "active-a1")["held"]]

    assert wait_for(lambda: held() == [("kept-k1/kpub", "granted")]), (
        "the dropped owner's grant went with it, the stale pending request was pruned, the live grant stayed",
        held(),
    )
    assert call("init", "kept-k1")["created"] is False, "presence with keep protects the ledger"
    presence("kept-k1", None)
    assert wait_for(lambda: gone("kept-k1"), 10), "cleared presence no longer protects"
    alive()
    print("stale ok")


def test_the_python_ledgerd_refuses_a_newer_schema():
    """The Python ledgerd (the rollback) writes schema 1 and refuses to open a ledger.db a newer ledgerd migrated,
    before writing anything to it. Python-only, so it runs whatever AGENT_BRIDGE_LEDGERD names."""
    from agent_bridge.ledger.store import Ledger

    db = os.path.join(tempfile.mkdtemp(), "ledger.db")
    Ledger(db, lambda *_: None).db.close()
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT value FROM meta WHERE key='schema'").fetchall() == [(1,)]
        conn.execute("UPDATE meta SET value=2 WHERE key='schema'")
        conn.execute("DROP INDEX resources_owner")  # opening it would recreate this, so it shows a write
    service = subprocess.run(
        [sys.executable, "-m", "agent_bridge.ledger.service"],
        env={**os.environ, "LEDGER_DB": db},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert service.returncode == 1 and "ledger.db has schema 2; this ledgerd knows 1" in service.stderr, service
    with sqlite3.connect(db) as conn:
        assert not conn.execute("SELECT 1 FROM sqlite_master WHERE name='resources_owner'").fetchall(), "it wrote"


def test_arguments_start_nothing():
    """Only no arguments runs the Python ledgerd: --help and refused arguments exit before a connection or the
    database is opened, so a stray flag can never start a second ledgerd on the live defaults."""
    db = os.path.join(tempfile.mkdtemp(), "ledger.db")
    env = {**os.environ, "LEDGER_DB": db, "AGENT_BRIDGE_PORT": "1"}  # nothing listens there
    for args, code, out, err in (
        (["--help"], 0, "usage: agent-bridge-ledgerd", ""),
        (["-h"], 0, "usage: agent-bridge-ledgerd", ""),
        (["--bogus"], 2, "", "agent-bridge-ledgerd: error: unrecognized arguments: --bogus"),
        (["--health", "x"], 2, "", "unrecognized arguments: x"),
        (["--hea"], 2, "", "unrecognized arguments: --hea"),  # no abbreviations, as in the Rust ledgerd
        (["--health"], 1, "", "agent-bridge-ledgerd: unhealthy: broker unreachable"),
    ):
        p = subprocess.run(
            [sys.executable, "-m", "agent_bridge.ledger.service", *args],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert p.returncode == code and out in p.stdout and err in p.stderr, (args, p)
    assert not os.path.exists(db), "an argument opened the database"


def test_cli_and_log():
    call("init", "active-a1")
    cli = subprocess.run(
        [sys.executable, "-m", "agent_bridge.ledger.client", "public", "--as", "active-a1"],
        capture_output=True,
        text=True,
    )
    assert cli.returncode == 0 and json.loads(cli.stdout)["ok"], cli
    cli = subprocess.run(
        [sys.executable, "-m", "agent_bridge.ledger.client", "acquire", "--as", "active-a1", "--resource", "x-1/y",
         "--level", "read"],
        capture_output=True,
        text=True,
    )  # fmt: skip
    assert cli.returncode == 1 and "no such resource" in json.loads(cli.stdout)["error"], cli
    for flags in (["--wait", "inf"], ["--wait", "nan"], ["--wait", "1e300"], ["--timeout", "inf"], ["--ttl", "inf"]):
        cli = subprocess.run(
            [sys.executable, "-m", "agent_bridge.ledger.client", "acquire", "--as", "active-a1", "--resource", "x-1/y",
             "--level", "read", *flags],
            capture_output=True,
            text=True,
        )  # fmt: skip
        assert cli.returncode == 1 and "Traceback" not in cli.stderr, (flags, cli)
        assert json.loads(cli.stdout)["ok"] is False, (flags, cli)
    with open(LOG) as f:
        log = f.read()
    assert "Traceback" not in log, log


if __name__ == "__main__":
    if sys.argv[1:2] == ["worker"]:
        worker(sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5]), sys.argv[6])
