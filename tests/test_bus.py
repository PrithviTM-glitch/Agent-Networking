"""End to end: the CLI and the native Bus against a disposable broker with ledgerd, including a broker restart.

The tests are one scenario in file order: later ones use the agents, rooms and log entries earlier ones made.
"""

import json
import os
import shutil
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import paho.mqtt.client as mqtt
import pytest
from conftest import BIN

from agent_bridge import _native, config
from agent_bridge.errors import BridgeError
from agent_bridge.protocol import ORIGIN
from agent_bridge.validation import BODY_MAX, is_ident

Bus = _native.Bus

BRIDGE = str(BIN / "agent-bridge")
RUN = uuid.uuid4().hex[:6]
DEMO = f"demo-{RUN}"
RAW_TEXT = [b"", b"not json", b"[1, 2]", b'"str"', b"123", b"null", b'{"x": 1}', "multi\nline\ttab ✓".encode()]
JUNK = [
    b"\xff\xfe\x00binary",
    b"\xc3\x28",
    b"[" * 100_000,
    b"a\x00b",
    b"\x1b[2J",
    "\u0085".encode(),
    b"x" * 70_000,
    b'{"from": 5, "body": "x", "ts": 1}',
    b'{"from": "a/b", "body": "x", "ts": 1}',
    b'{"from": null, "body": "x", "ts": 1}',
    b'{"from": "ok-1", "body": 7, "ts": 1}',
    b'{"from": "ok-1", "body": "x", "ts": "now"}',
    b'{"from": "ok-1", "body": "x", "ts": NaN}',
    b'{"from": "ok-1", "body": "x", "ts": 1e999}',
    b'{"from": "ok-1", "body": "x", "ts": true}',
    b'{"from": "ok-1", "body": "x\\u0000y", "ts": 1}',
    b'{"from": "ok-1", "body": "\\u001b[31m", "ts": 1}',
    b'{"from": "\\ud800", "body": "x", "ts": 1}',
    b'{"from": "ok-1", "body": "\\ud800", "ts": 1}',
    b'{"from": "ok-1", "ts": 1}',
    b'{"from": "' + b"f" * 65 + b'", "body": "x", "ts": 1}',
    json.dumps({"from": "ok-1", "body": "x" * (BODY_MAX + 1), "ts": 1}).encode(),
]


def env():
    return dict(os.environ)


def run(*args, ok=True, env_extra=None):
    p = subprocess.run([BRIDGE, *args], env={**env(), **(env_extra or {})}, capture_output=True, text=True)
    assert (p.returncode == 0) == ok, (args, p.stderr)
    return p.stdout.strip()


def fails(*args, env_extra=None):
    """A clean failure: non-zero exit, one line on stderr, no traceback."""
    p = subprocess.run(
        [BRIDGE, *args], env={**env(), **(env_extra or {})}, capture_output=True, text=True, errors="replace"
    )
    err = p.stderr.strip()
    assert p.returncode != 0 and err and "\n" not in err and "Traceback" not in err, (
        args,
        env_extra,
        p.returncode,
        p.stderr,
    )
    return err


def lines(*args):
    return [json.loads(line) for line in run(*args).splitlines()]


def bodies(*args):
    return [m["body"] for m in lines(*args)]


def agents(*args):
    return {x["addr"]: x for x in lines("agents", *args)}


def waiter(addr, secs):
    return subprocess.Popen([BRIDGE, "recv", addr, "--wait", str(secs)], env=env(), stdout=subprocess.PIPE, text=True)


def raw_client():
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    c.connect(config.HOST, config.PORT)
    c.loop_start()
    return c


def raw_pub(c, topic, payload, qos=1, retain=False):
    c.publish(topic, payload, qos=qos, retain=retain).wait_for_publish(5)


def settle(bus, sender):
    """Proof bus has caught up with what other processes just did, instead of a timeout: a DM sender sends after them
    comes after their presence updates on bus's one connection, or bus was away (a takeover) and its reconnect re-read
    the retained records first. Returns what arrived up to and including the marker."""
    marker = f"marker {uuid.uuid4().hex[:6]}"
    run("dm", sender, bus.addr, marker)
    got = []
    while not any(m["body"] == marker for m in got) and (batch := bus.recv(15)):
        got += batch
    assert any(m["body"] == marker for m in got), got
    return got


def raises(fn, *args, **kw):
    try:
        fn(*args, **kw)
    except BridgeError as e:
        assert "\n" not in str(e)
        return
    raise AssertionError(f"{fn.__name__}{args} accepted bad input")


@pytest.fixture(scope="module")
def s(ledgerd, historyd):
    """What the scenario carries from one test to the next."""
    return SimpleNamespace()


def test_dm_and_topics(s):
    s.a = a = run("register", "alice", "--caps", "sentry,grafana")
    s.b = b = run("register", "bob")
    assert agents()[a]["caps"] == ["sentry", "grafana"] and b in agents()

    run("dm", a, b, "hi bob")
    assert [(m["from"], m["body"]) for m in lines("recv", b)] == [(a, "hi bob")]
    assert lines("recv", b) == [], "delivered once"
    assert lines("recv", a) == [], "DMs are private"
    assert bodies("dms", b) == bodies("dms", a, "--with", b) == ["hi bob"]

    run("sub", b, "ci")
    run("pub", a, "ci", "pipeline red")
    run("pub", a, "other", "ignored")
    assert bodies("recv", b) == ["pipeline red"]

    run("dm", a, "nobody-000000", "x", ok=False)
    run("recv", "nobody-000000", ok=False)
    run("register", "bad/name", ok=False)

    with ThreadPoolExecutor(16) as ex:
        list(ex.map(lambda i: run("pub", a, "ci", str(i)), range(100)))
    assert sorted(map(int, bodies("recv", b))) == list(range(100)), "queued while offline, nothing lost"


def test_rooms(s):
    r1, r2, r3 = (run("register", n) for n in ("r1", "r2", "r3"))
    s.r1, s.r2, s.r3 = r1, r2, r3
    for x in (r1, r2, r3):
        run("join", x, DEMO)
    run("say", r1, DEMO, "hello all")
    run("say", r2, DEMO, "hi r1")
    assert bodies("recv", r3) == ["hello all", "hi r1"], "members who joined earlier are announced only to later ones"
    joined = [f"[joined #{DEMO}] r2", f"[joined #{DEMO}] r3"]
    assert bodies("recv", r1) == [*joined, "hi r1"], "joins are announced, own messages never echo"
    assert bodies("recv", r2) == [joined[1], "hello all"]
    run("join", r2, DEMO)
    assert bodies("recv", r1) == [], "joining again announces nothing"
    assert bodies("history", DEMO, "--last", "1") == ["hi r1"]
    assert {"room": DEMO, "members": 3, "who": sorted([r1, r2, r3]), "mode": "open", "moderators": []} in lines("rooms")
    assert set(agents("--room", DEMO)) == {r1, r2, r3}
    run("part", r3, DEMO)
    run("say", r1, DEMO, "after r3 left")
    assert bodies("recv", r3) == [] and set(agents("--room", DEMO)) == {r1, r2}
    assert bodies("recv", r2) == [f"[left #{DEMO}] r3", "after r3 left"], "leaving is announced"
    r4 = run("register", "r4")
    run("join", r4, DEMO)
    run("leave", r4)
    left = [f"[left #{DEMO}] r3", f"[joined #{DEMO}] r4", f"[left #{DEMO}] r4"]
    assert bodies("recv", r1) == left, "leaving the bus is announced to its rooms too"


def test_waiting_recv(s):
    a, b = s.a, s.b
    w = waiter(b, 10)
    time.sleep(1.5)
    run("dm", a, b, "wake up")
    msg = json.loads(w.stdout.readline())
    s.lag = lag = time.time() - msg["ts"]
    w.wait()
    assert msg["body"] == "wake up" and lag < 0.3, lag

    w = waiter(b, 15)
    time.sleep(1.5)
    run("sub", b, "ci2")
    run("dm", a, b, "after takeover")
    assert json.loads(w.stdout.readline())["body"] == "after takeover", "waiter reconnects after a same-address client"
    w.wait()


def test_leave(s):
    a, r3 = s.a, s.r3
    run("dm", a, r3, "for r3")
    run("leave", r3)
    assert r3 not in agents()
    run("dm", a, r3, "x", ok=False)
    assert _native.history_call("dms", as_=r3)["messages"] == [], "its DMs go too"


def test_bus_library(s):
    """One persistent connection, messages buffer in the background until recv drains them."""
    a = s.a
    s.lib = lib = Bus.register("lib", ["x"])
    run("dm", a, lib.addr, "to lib")
    assert [(m["topic"], m["from"], m["body"]) for m in lib.recv(5)] == [(f"inbox/{lib.addr}", a, "to lib")]
    lib.join("libroom")
    run("join", a, "libroom")
    run("say", a, "libroom", "room msg")
    time.sleep(0.5)
    joined = "[joined #libroom] alice, caps: sentry, grafana"
    assert [m["body"] for m in lib.recv()] == [joined, "room msg"], "already buffered, recv(0) returns it"
    lib.say("libroom", "from lib")
    lib.dm(a, "lib to alice")
    lib.sub("libtopic")
    run("pub", a, "libtopic", "topic msg")
    assert bodies("recv", a) == ["from lib", "lib to alice"] and [m["body"] for m in lib.recv(5)] == ["topic msg"]
    assert (
        lib.recv() == []
        and lib.dms(a)[-1]["body"] == "lib to alice"
        and lib.history_call("room", room="libroom")["messages"][-1]["body"] == "from lib"
    )
    assert set(agents()[lib.addr]["topics"]) == {"room/libroom", "libtopic"}


def test_takeover(s):
    """A CLI command for the same address kicks the Bus, which reconnects; nothing buffered is lost or doubled."""
    a, lib = s.a, s.lib
    run("dm", a, lib.addr, "buffered before takeover")
    time.sleep(0.5)
    run("sub", lib.addr, "libtopic2")
    run("dm", a, lib.addr, "after takeover")
    got = lib.recv(10)
    got += lib.recv(1) if len(got) < 2 else []
    assert [m["body"] for m in got] == ["buffered before takeover", "after takeover"], got
    lib.say("libroom", "still works")
    assert bodies("recv", a) == ["still works"] and lib.recv(0.5) == []
    assert set(agents()[lib.addr]["topics"]) == {"room/libroom", "libtopic", "libtopic2"}, "presence not clobbered"


def test_stable_addresses(s):
    """--addr creates once, then reattaches to the same session, queue and subscriptions."""
    a = s.a
    st = f"stable-{RUN}"
    assert run("register", "stable", "--addr", st) == st
    run("join", st, "stableroom")
    run("sub", st, "stabletopic")
    run("dm", a, st, "queued dm")
    run("say", a, "stableroom", "queued say")
    run("pub", a, "stabletopic", "queued topic")
    assert run("register", "renamed", "--caps", "z", "--addr", st) == st
    assert agents()[st]["name"] == "stable" and set(agents()[st]["topics"]) == {"room/stableroom", "stabletopic"}
    stable = Bus.register("stable", addr=st)
    assert [m["body"] for m in stable.recv(2) + stable.recv(0.5)] == ["queued dm", "queued say", "queued topic"]
    stable.leave()
    assert run("register", "stable", "--addr", st) == st and agents()[st]["topics"] == [], "recreated fresh after leave"
    run("leave", st)


def test_broker_restart(s, fresh_broker):
    a, lib = s.a, s.lib
    fresh = run("register", "fresh")
    run("dm", a, fresh, "survives restart")
    subprocess.run(["docker", "restart", fresh_broker.name], check=True, capture_output=True)
    time.sleep(1)
    assert bodies("recv", fresh) == ["survives restart"], "sessions and queues persist across a broker restart"
    run("leave", fresh)
    run("dm", a, lib.addr, "after broker restart")
    assert [m["body"] for m in lib.recv(20)] == ["after broker restart"], (
        "the persistent Bus reconnects after a restart"
    )


def test_presence_cache(s):
    """A Bus keeps every presence record current over its own connection (mentions and DMs resolve from it), and a
    change another process made to its record survives its next read-modify-write."""
    lib, room = s.lib, f"cache-{RUN}"
    newcomer = run("register", "newcomer")
    run("join", newcomer, room)
    run("keep", lib.addr, "on")  # another process acting as lib; lib reconnects
    settle(lib, s.a)
    assert (lib.cached_agent(lib.addr) or {}).get("keep") is True
    assert (lib.cached_agent(newcomer) or {}).get("topics") == [f"room/{room}"]
    lib.join(room)
    lib.say(room, "hello @newcomer")
    lib.dm(newcomer, "found you in the cache")
    rec = _native.presence_find(lib.addr)
    assert rec["keep"] is True and f"room/{room}" in rec["topics"], "the join kept what the other process changed"
    run("keep", lib.addr, "off")
    run("leave", newcomer)
    settle(lib, s.a)
    assert lib.cached_agent(newcomer) is None, (
        "a cleared record leaves the cache",
        {"retained": _native.presence_find(newcomer), "cached": lib.cached_agent(newcomer)},
    )
    with pytest.raises(BridgeError, match="unknown recipient"):
        lib.dm(newcomer, "gone")
    lib.part(room)
    while lib.recv(0.5):  # the newcomer's room notices, so later tests start from an empty buffer
        pass


def test_idle_expiry(s):
    short = {"AGENT_BRIDGE_STALE_DAYS": str(2 / 86400)}
    brief = run("register", "brief", env_extra=short)
    s.durable = durable = run("register", "durable", "--keep", env_extra=short)
    time.sleep(3.5)
    assert brief not in agents() and durable in agents(), "idle agents expire, kept ones stay"


def test_hostile_messages(s):
    """Any local process can publish anything to the topics the bridge reads."""
    a, lib = s.a, s.lib
    s.victim = victim = run("register", "victim")
    run("join", victim, "hostile")
    run("sub", victim, "hostile/t")
    lib.join("hostile")
    evil = raw_client()
    for topic, payloads in (
        (f"inbox/{victim}", RAW_TEXT + JUNK),
        ("room/hostile", RAW_TEXT + JUNK),
        ("hostile/t", JUNK + RAW_TEXT),
    ):
        for p in payloads:
            raw_pub(evil, topic, p)
    for p in RAW_TEXT + JUNK:
        raw_pub(evil, f"inbox/{lib.addr}", p, qos=0)
        raw_pub(evil, "room/hostile", p, qos=0)
    huge = raw_client()
    huge.publish(f"inbox/{victim}", b"y" * 300_000, qos=1)  # over max_packet_size: the broker drops the sender
    time.sleep(0.5)
    huge.loop_stop()
    run("dm", a, victim, "real dm")
    run("say", a, "hostile", "real say")
    run("pub", a, "hostile/t", "real pub")
    got = lines("recv", victim)
    expected = [(None, p.decode()) for p in RAW_TEXT] + [(a, "real dm"), (a, "real say"), (a, "real pub")]
    expected.append((lib.addr, "[joined #hostile] lib, caps: x"))
    assert sorted((m["from"] or "", m["body"]) for m in got) == sorted((f or "", t) for f, t in expected), got
    assert all(m["topic"] == "hostile/t" for m in got if m["from"] is None), "plain text only passes on raw topics"
    got = lib.recv(5) + lib.recv(0.5)
    assert [m["body"] for m in got] == ["real say"], got
    lib.dm(a, "lib survived")
    assert bodies("recv", a)[-1] == "lib survived"
    evil.loop_stop()


def test_forged_presence(s):
    a = s.a
    evil = raw_client()
    forged = {f"forged{i}-{RUN}": p for i, p in enumerate(JUNK + RAW_TEXT[1:])}
    s.good = good = {"name": "x", "caps": [], "topics": [], "keep": False, "seen": time.time()}
    for key, change in {
        "inbox": {"topics": ["inbox/#"]},
        "hash": {"topics": ["#"]},
        "capsstr": {"caps": "abc"},
        "keepstr": {"keep": "yes"},
        "seen": {"seen": -1},
        "name": {"name": "a b"},
        "many": {"topics": [f"t{i}" for i in range(101)]},
        "missing": {"seen": None},
    }.items():
        forged[f"forged-{key}-{RUN}"] = json.dumps({**good, **change}).encode()
    for addr, p in forged.items():
        raw_pub(evil, f"agents/{addr}", p, retain=True)
    odd_topics = ["agents/bad addr", "agents/ünï", "agents/x/y", "agents/", f"agents/{'q' * 65}"]
    for t in odd_topics:
        raw_pub(evil, t, json.dumps(good).encode(), retain=True)
    listed = agents()
    assert not any(x.startswith("forged") or not is_ident(x) for x in listed) and a in listed, listed.keys()
    lines("rooms")
    fails("dm", a, f"forged-inbox-{RUN}", "x")
    fails("recv", f"forged0-{RUN}")
    raises(Bus.attach, f"forged-many-{RUN}")
    s.victim2 = victim2 = run("register", "victim2")
    raw_pub(evil, f"agents/{victim2}", b"{garbage", retain=True)
    assert "is not registered" in fails("recv", victim2), "junk over a real presence reads as gone"
    assert run("register", "victim2", "--addr", victim2) == victim2 and lines("recv", victim2) == []
    for t in [f"agents/{x}" for x in forged] + odd_topics:
        raw_pub(evil, t, b"", retain=True)
    evil.loop_stop()


def test_ledger_commands(s):
    """Thin wrappers over ledgerd; leave drops the agent's ledger."""
    own, user = run("register", "owner"), run("register", "user")
    db = f"{own}/db"
    added = json.loads(run("add", own, "db", "--visibility", "public", "--max-level", "write", "--description", "x"))
    assert added["resource"]["id"] == db
    assert db in [r["id"] for r in json.loads(run("public", user))["resources"]]
    assert json.loads(run("request", user, db, "write"))["request"]["status"] == "granted"
    lease = json.loads(run("acquire", user, db, "write", "--ttl", "30"))["lease"]
    assert "busy" in fails("acquire", own, db, "write")
    run("renew", user, lease["lease_id"], "--ttl", "60")
    run("release", user, lease["lease_id"])
    assert json.loads(run("acquire", own, db, "write"))["lease"]["token"] > lease["token"]
    run("update", own, "db", "--visibility", "private")
    assert json.loads(run("ledger", user, "--owner", own))["resources"] == []
    p = subprocess.run([BRIDGE, "leave", own], env=env(), capture_output=True, text=True)
    assert p.returncode == 0 and p.stderr == "", p.stderr
    assert "no ledger" in fails("ledger", user, "--owner", own)
    run("leave", user)


def test_presence_origin(s, tmp_path):
    """Presence names the Claude Code session and process an agent runs under, for the admin console."""
    good = s.good
    fake = str(tmp_path / "claude")
    shutil.copy("/bin/sh", fake)
    sid = str(uuid.uuid4())
    p = subprocess.Popen(
        [fake, "-c", f'"{BRIDGE}" register meta; exit $?'],
        env={**env(), "CLAUDE_CODE_SESSION_ID": sid},
        stdout=subprocess.PIPE,
        text=True,
    )
    with open(f"/proc/{p.pid}/stat") as f:
        start = int(f.read().rsplit(")", 1)[1].split()[19])
    meta = p.communicate()[0].strip()
    assert {k: agents()[meta].get(k) for k in ORIGIN} == {"session": sid, "pid": p.pid, "pid_start": start}
    run("join", meta, DEMO, env_extra={"CLAUDE_CODE_SESSION_ID": str(uuid.uuid4())})
    assert {k: agents()[meta].get(k) for k in ORIGIN} == {"session": sid, "pid": p.pid, "pid_start": start}, (
        "origin is written once: another session acting as the agent changes nothing"
    )
    run("leave", meta)
    evil = raw_client()
    fresh_rec = {**good, "seen": time.time()}
    metas = {
        f"meta{i}-{RUN}": extra
        for i, extra in enumerate(
            [
                {"session": "nope", "pid": -5, "pid_start": "x"},
                {"session": 5, "pid": True, "pid_start": 1},
                {"pid": 10**30, "pid_start": 1},
                {"pid": 7, "pid_start": 0},
                {"pid": 7},
                {"pid_start": 7},
                {"pid": 7.5, "pid_start": 7},
                {"session": sid, "pid": 7, "pid_start": 9},
            ]
        )
    }
    for addr, extra in metas.items():
        raw_pub(evil, f"agents/{addr}", json.dumps({**fresh_rec, **extra}).encode(), retain=True)
    listed = agents()
    for addr, extra in metas.items():
        kept = {k: listed[addr][k] for k in ORIGIN if k in listed[addr]}
        assert kept == ({"session": sid, "pid": 7, "pid_start": 9} if extra.get("session") == sid else {}), (addr, kept)
        raw_pub(evil, f"agents/{addr}", b"", retain=True)
    evil.loop_stop()


def test_subagents_and_write_once_origin(s, tmp_path):
    """A subagent's agent is marked as one and stays one; no later attach gives any agent a session it lacked."""
    sid = str(uuid.uuid4())
    in_session = {"CLAUDE_CODE_SESSION_ID": sid}
    plain = run("register", "plain")  # this test process may well run under a claude, but in no session
    run("join", plain, DEMO, env_extra=in_session)
    assert not {"session", "parent", "kind", "pid", "pid_start"} & set(agents()[plain]), (
        "registered outside a session, stays outside, and owns no session's process"
    )

    fake = str(tmp_path / "claude")
    shutil.copy("/bin/sh", fake)
    p = subprocess.Popen(
        [fake, "-c", f'"{BRIDGE}" register helper --subagent; exit $?'],
        env={**env(), **in_session},
        stdout=subprocess.PIPE,
        text=True,
    )
    with open(f"/proc/{p.pid}/stat") as f:
        start = int(f.read().rsplit(")", 1)[1].split()[19])
    sub = p.communicate()[0].strip()
    want = {"kind": "subagent", "parent": sid, "pid": p.pid, "pid_start": start}
    assert {k: agents()[sub].get(k) for k in ("session", *want)} == {"session": None, **want}
    run("join", sub, DEMO, env_extra=in_session)
    assert {k: agents()[sub].get(k) for k in ("session", "kind", "parent")} == {
        "session": None,
        "kind": "subagent",
        "parent": sid,
    }

    main = run("register", "main", env_extra=in_session)
    assert _native.relay_session_agent(sid) == main, "the relay adopts the session's own agent"
    run("leave", main)
    assert _native.relay_session_agent(sid) is None, "never a subagent's"
    with pytest.raises(BridgeError):
        Bus.register("admin", addr="admin", keep=True, admin=True, subagent=True)

    evil = raw_client()
    fresh_rec = {**s.good, "seen": time.time()}
    cases = {
        f"lineage{i}-{RUN}": (extra, kept)
        for i, (extra, kept) in enumerate(
            [
                ({"kind": "bot", "session": sid}, {"session": sid}),
                ({"kind": "subagent", "parent": "nope"}, {"kind": "subagent"}),
                ({"kind": "subagent", "parent": sid, "session": sid}, {"kind": "subagent", "parent": sid}),
                ({"kind": 5, "parent": sid}, {}),
                ({"parent": sid}, {}),
            ]
        )
    }
    for addr, (extra, _) in cases.items():
        raw_pub(evil, f"agents/{addr}", json.dumps({**fresh_rec, **extra}).encode(), retain=True)
    listed = agents()
    for addr, (_, kept) in cases.items():
        assert {k: listed[addr][k] for k in ("session", "kind", "parent") if k in listed[addr]} == kept, addr
        raw_pub(evil, f"agents/{addr}", b"", retain=True)
    evil.loop_stop()
    run("leave", plain)
    run("leave", sub)


def test_admin_is_reserved(s):
    """The admin address and name belong to the console: only the library call with admin=True takes them."""
    a, lib = s.a, s.lib
    for args in (
        ["register", "admin"],
        ["register", "Admin"],
        ["register", "x", "--addr", "admin"],
        ["register", "admin", "--addr", f"x{RUN}"],
        ["say", "admin", DEMO, "x"],
        ["recv", "admin"],
        ["keep", "admin", "on"],
        ["leave", "admin"],
        ["add", "admin", "x"],
    ):
        assert "reserved for the admin console" in fails(*args), args
    fails("register", "x", "--admin")
    for kw in (
        {"name": "admin"},
        {"name": "x", "addr": "admin"},
        {"name": "admin", "addr": "admin", "keep": True},
        {"name": "x", "addr": f"x{RUN}", "admin": True},
        {"name": "admin", "admin": True},
    ):
        raises(Bus.register, **kw)
    raises(lib.describe, "admin")
    s.console = console = Bus.register("admin", addr="admin", keep=True, admin=True)
    assert agents()["admin"]["keep"] is True and not set(ORIGIN) & set(agents()["admin"]), "no origin for admin"
    run("dm", a, "admin", "hello console")
    assert [m["body"] for m in console.recv(5)] == ["hello console"]


def test_broadcast(s):
    """Only the console's Bus sends it; every agent gets it, offline ones on their next recv."""
    lib, console = s.lib, s.console
    off, old = run("register", "offline"), run("register", "old")
    pre, up = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=old, protocol=mqtt.MQTTv5), threading.Event()
    pre.on_connect = lambda *_: up.set()
    props = mqtt.Properties(mqtt.PacketTypes.CONNECT)
    props.SessionExpiryInterval = 3600  # keep the session: this stands in for one made before broadcast existed
    pre.connect(config.HOST, config.PORT, clean_start=False, properties=props)
    pre.loop_start()
    up.wait(5)
    pre.unsubscribe("broadcast")
    time.sleep(0.3)
    pre.disconnect()
    pre.loop_stop()
    console.broadcast("missed by old")
    run("join", old, DEMO)
    console.broadcast("maintenance at noon")
    assert [(m["topic"], m["from"], m["body"]) for m in lines("recv", off)] == [
        ("broadcast", "admin", "missed by old"),
        ("broadcast", "admin", "maintenance at noon"),
    ]
    assert bodies("recv", old) == ["maintenance at noon"], (
        "a session from before broadcast picks it up on its next attach"
    )
    assert "broadcast" not in agents()[off]["topics"] and "broadcast" not in {r["room"] for r in lines("rooms")}
    logged = _native.history_call("broadcasts")["messages"][-1]
    assert {"topic": "broadcast", "from": "admin", "body": "maintenance at noon"}.items() <= logged.items()
    for args in (
        ["pub", off, "broadcast", "x"],
        ["pub", off, "broadcast/x", "x"],
        ["sub", off, "broadcast"],
        ["sub", off, "broadcast/#"],
        ["unsub", off, "broadcast"],
    ):
        assert "reserved" in fails(*args), args
    for fn, args in ((lib.broadcast, ("x",)), (lib.pub, ("broadcast", "x")), (lib.sub, ("broadcast",))):
        raises(fn, *args)
    run("leave", off)
    run("leave", old)
    console.leave()


BAD_ENV = [
    {"AGENT_BRIDGE_PORT": "abc"},
    {"AGENT_BRIDGE_PORT": "70000"},
    {"AGENT_BRIDGE_PORT": "nan"},
    {"AGENT_BRIDGE_PORT": "1"},
    {"AGENT_BRIDGE_HOST": "no-such-host.invalid"},
    {"AGENT_BRIDGE_STALE_DAYS": "nan"},
    {"AGENT_BRIDGE_STALE_DAYS": "-1"},
    {"AGENT_BRIDGE_STALE_DAYS": "1e400"},
]


def test_message_rules(s):
    """Empty text, a DM to yourself and an @word that matches nobody: each is told plainly, none is silently lost."""
    a, lib, console = s.a, s.lib, s.console
    room = f"rules-{RUN}"
    lib.join(room)
    run("join", a, room)
    while lib.recv(0.5):
        pass
    for args, error in (
        (["say", a, room, ""], "text is empty"),
        (["say", a, room, " \n\t"], "text is empty"),
        (["dm", a, lib.addr, ""], "text is empty"),
        (["dm", a, a, "note to self"], "cannot DM yourself: your own messages never reach you"),
        (["dm", a, f"nobody-{RUN}", "x"], f"unknown recipient: nobody-{RUN}"),
        (["recv", f"nobody-{RUN}"], f"nobody-{RUN} is not registered (or it left): register it first"),
    ):
        assert fails(*args) == error, args
    for fn, args in ((lib.say, (room, "  ")), (lib.dm, (lib.addr, "self")), (console.broadcast, (" ",))):
        raises(fn, *args)
    run("pub", a, f"rules{RUN}/t", "")  # a raw payload may still be empty
    said = lib.say(room, "see the @mentions field, @ALICE")
    assert said == {"mentions": [a], "unresolved": ["mentions"]}, "sent anyway; names match without case"
    assert lib.say(room, "plain") == {"mentions": [], "unresolved": []}
    p = subprocess.run([BRIDGE, "say", a, room, "ask @nobody"], env=env(), capture_output=True, text=True)
    assert p.returncode == 0 and p.stdout == "" and "@nobody" in p.stderr, (p.returncode, p.stdout, p.stderr)
    got = [(m["body"], m.get("mentions")) for m in lib.recv(5) + lib.recv(0.5) if m["topic"] == f"room/{room}"]
    assert got == [("ask @nobody", None)], "an unresolved @word is plain text, sent as written"
    lib.part(room)


def test_retained_replay_precedes_the_barrier(s):
    """A Bus's caches are complete once it is synced: with 3000 retained presence records on the broker, every one is
    in a new Bus's cache at its first read. This pins mosquitto delivering a subscription's retained records before
    the next SUBACK, which the cache barrier relies on."""
    evil = raw_client()
    rec = json.dumps({**s.good, "seen": time.time()})
    addrs = [f"bulk{i}-{RUN}" for i in range(3000)]
    for info in [evil.publish(f"agents/{x}", rec, qos=1, retain=True) for x in addrs]:
        info.wait_for_publish(10)
    fresh = Bus.register("fresh")
    try:
        missing = [x for x in addrs if fresh.cached_agent(x) is None]
        assert missing == [], f"{len(missing)} of {len(addrs)} retained records missing at the first read"
    finally:
        fresh.leave()
        for info in [evil.publish(f"agents/{x}", b"", qos=1, retain=True) for x in addrs]:
            info.wait_for_publish(10)
        evil.loop_stop()


def test_bad_arguments(s):
    """Every one is a one-line error with a non-zero exit, never a traceback."""
    a, b, lib = s.a, s.b, s.lib
    bad = [
        [],
        ["nope"],
        ["register"],
        ["register", ""],
        ["register", "bad/name"],
        ["register", "x" * 58],
        ["register", "é"],
        ["register", "-x"],
        ["register", b"\xff\xfe"],
        ["register", "x", "--caps", "a,b/c"],
        ["register", "x", "--caps", "a b"],
        ["register", "x", "--caps", ",".join(f"c{i}" for i in range(33))],
        ["register", "x", "--addr", "bad addr"],
        ["register", "x", "--addr", "y" * 65],
        ["register", "x", "--addr", ""],
        ["recv", a, "--wait", "-1"],
        ["recv", a, "--wait", "nan"],
        ["recv", a, "--wait", "inf"],
        ["recv", a, "--wait", "1e9"],
        ["recv", a, "--wait", "x"],
        ["recv", "a/b"],
        ["recv", ""],
        ["history", DEMO, "--last", "0"],
        ["history", DEMO, "--last", "-5"],
        ["history", DEMO, "--last", "100000"],
        ["history", DEMO, "--last", "x"],
        ["history", "a/b"],
        ["history", ""],
        ["dms", "a b"],
        ["dms", a, "--with", "x/y"],
        ["sub", a, "inbox/#"],
        ["sub", a, f"inbox/{b}"],
        ["sub", a, "#"],
        ["sub", a, "+"],
        ["sub", a, "+/x"],
        ["sub", a, "agents/x"],
        ["sub", a, "agents/+"],
        ["sub", a, "ledger/req"],
        ["sub", a, "ledger/#"],
        ["sub", a, "$SYS/#"],
        ["sub", a, "$share/g/t"],
        ["sub", a, "a/#/b"],
        ["sub", a, "a+"],
        ["sub", a, "a/b#"],
        ["sub", a, "a/+b"],
        ["sub", a, ""],
        ["sub", a, "a\nb"],
        ["sub", a, "a\x7fb"],
        ["sub", a, "t" * 257],
        ["sub", a, b"t\xff"],
        ["unsub", a, "inbox/#"],
        ["unsub", a, "#"],
        ["pub", a, "a/+", "x"],
        ["pub", a, "a/#", "x"],
        ["pub", a, f"agents/{a}", "{}"],
        ["pub", a, f"inbox/{b}", "x"],
        ["pub", a, "$SYS/x", "x"],
        ["pub", a, "ledger/req", "{}"],
        ["pub", a, "", "x"],
        ["pub", a, "t", b"\xff"],
        ["say", a, DEMO, "x" * (BODY_MAX + 1)],
        ["say", a, DEMO, "bell\x07"],
        ["say", a, DEMO, "esc\x1b[31m"],
        ["say", a, DEMO, "nel\u0085"],
        ["say", a, DEMO, "del\x7f"],
        ["say", a, DEMO, b"\xff"],
        ["say", a, "bad/room", "x"],
        ["say", a, DEMO],
        ["dm", a, "nobody-000000", "x"],
        ["dm", a, "bad/to", "x"],
        ["dm", a, b, "\x01"],
        ["join", a, "bad room"],
        ["join", "nobody-000000", DEMO],
        ["part", a, ""],
        ["keep", a, "maybe"],
        ["leave", "nobody-000000"],
        ["leave", "x/y"],
        ["agents", "--room", "x/y"],
        ["agents", "--room", ""],
        ["add", a, "bad/name"],
        ["add", a, "x", "--visibility", "nope"],
        ["add", a, "x", "--description", "a\x01"],
        ["request", a, "x/y", "admin"],
        ["request", a, "nobody/x", "read"],
        ["acquire", a, "nobody/x", "read"],
        ["acquire", a, f"{a}/x", "write", "--ttl", "abc"],
        ["acquire", a, f"{a}/x", "write", "--ttl", "0"],
        ["renew", a, "nope"],
        ["release", a, "nope"],
        ["grant", a, "nope"],
        ["deny", a, "nope"],
        ["remove", a, "nope"],
        ["ledger", a, "--owner", "x y"],
        ["public", "nobody-000000"],
        ["grants"],
    ]
    for args in bad:
        fails(*args)
    for extra in BAD_ENV:
        fails("agents", env_extra=extra)
    for fn, args in [
        (lib.say, ("libroom", 5)),
        (lib.say, ("libroom", "a\x00b")),
        (lib.pub, ("inbox/x", "a")),
        (lib.sub, ("#",)),
        (lib.sub, (None,)),
        (lib.dm, ("x/y", "a")),
        (lib.recv, (-1,)),
        (lib.recv, (True,)),
        (lib.recv, (float("nan"),)),
        (lib.dms, (None, 0)),
        (Bus.register, ("x", "abc")),
        (Bus.register, ("x", [], False, "a b")),
        (Bus.attach, ("nobody-000000",)),
        (_native.presence_agents, ("#",)),
    ]:
        raises(fn, *args)


def test_everyone_leaves(s):
    s.lib.leave()
    for x in (s.a, s.b, s.r1, s.r2, s.durable, s.victim, s.victim2):
        run("leave", x)
    print(f"delivery latency {s.lag * 1000:.0f}ms")
