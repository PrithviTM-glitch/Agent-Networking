"""Conformance: what every implementation of the bus must do, checked through the public API and the wire only, so it
runs unchanged against the Python modules and against the Rust bindings.

Covered: no call stalls behind a blocked recv or a queued acquire; concurrent joins keep every room; ledger replies on
a Bus's own connection never fill its receive window; leave drops the agent's ledger, and still completes when ledgerd
is down; wait_arrival (bindings only); forged history requests on the plain topic are refused.
"""

import json
import os
import threading
import time
import uuid

import paho.mqtt.client as mqtt
import pytest
from conftest import start_ledgerd, wait_for
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from agent_bridge import config, presence
from agent_bridge.bus import Bus
from agent_bridge.history import client as history
from agent_bridge.ledger import client as ledger

RUN = uuid.uuid4().hex[:6]
NATIVE = "_native" in Bus.__module__
agents = []
restarted = []  # ledgerd processes a test started in place of the fixture's


@pytest.fixture(scope="module", autouse=True)
def services(ledgerd, historyd):
    yield
    for bus in agents:
        try:
            bus.leave()
        except Exception:  # noqa: BLE001  cleanup of agents a failed test may already have closed
            pass
    for proc in restarted:
        proc.kill()
        proc.wait()


def agent(name):
    bus = Bus.register(f"{name}{RUN}")
    agents.append(bus)
    return bus


def watch(pattern):
    """A raw client collecting (topic, payload) of everything on pattern."""
    seen = []
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    c.on_message = lambda _c, _u, m: seen.append((m.topic, m.payload))
    c.connect(config.HOST, config.PORT)
    c.loop_start()
    c.subscribe(pattern, qos=1)
    time.sleep(0.3)
    return c, seen


def in_thread(fn, *args, **kw):
    box = {}

    def go():
        try:
            box["result"] = fn(*args, **kw)
        except Exception as e:  # noqa: BLE001  handed to the test
            box["error"] = e

    t = threading.Thread(target=go, daemon=True)
    t.start()
    box["thread"] = t
    return box


def test_no_call_waits_behind_a_blocked_one():
    """recv(wait) and acquire(wait) block only their own caller: say, dm and release on the same Bus return at once."""
    a, peer, holder = agent("stall"), agent("peer"), agent("holder")
    room = f"stall{RUN}"
    a.join(room)
    for bus in (a, holder):
        bus.ledger("init")
    holder.ledger("add", name="db", visibility="public", max_level="write")
    busy = f"{holder.addr}/db"
    a.ledger("request", resource=busy, level="write")
    held = holder.ledger("acquire", resource=busy, level="write", ttl=60)["lease"]
    a.ledger("add", name="own")
    own = a.ledger("acquire", resource=f"{a.addr}/own", level="write", ttl=60)["lease"]

    queued = in_thread(a.ledger, "acquire", resource=busy, level="write", ttl=30, wait=20)
    waiting = in_thread(a.recv, 30)
    time.sleep(0.5)
    assert queued["thread"].is_alive() and waiting["thread"].is_alive(), "both calls are blocked"
    started = time.monotonic()
    a.say(room, "while blocked")
    a.ledger("release", lease_id=own["lease_id"])
    elapsed = time.monotonic() - started
    assert elapsed < 1, f"say and release waited {elapsed:.2f} s behind blocked calls"
    assert queued["thread"].is_alive() and waiting["thread"].is_alive()

    peer.dm(a.addr, "wake")  # a Bus never receives its own messages, so the DM comes from another agent
    waiting["thread"].join(10)
    assert [m["body"] for m in waiting.get("result", [])] == ["wake"], waiting
    holder.ledger("release", lease_id=held["lease_id"])
    queued["thread"].join(10)
    assert queued.get("result", {}).get("lease", {}).get("holder") == a.addr, queued


@pytest.mark.xfail(
    not NATIVE, reason="the Python Bus can lose a concurrent join; the Rust Bus serialises presence writes"
)
def test_concurrent_joins_keep_every_room():
    """Ten joins at once on one Bus: presence lists all ten rooms and each room delivers."""
    a, talker = agent("joins"), agent("talker")
    rooms = [f"j{RUN}{i}" for i in range(10)]
    joins = [in_thread(a.join, r) for r in rooms]
    for j in joins:
        j["thread"].join(30)
        assert "error" not in j, j
    listed = {x["addr"]: x for x in presence.agents()}[a.addr]["topics"]
    assert {f"room/{r}" for r in rooms} <= set(listed), listed
    for r in rooms:
        talker.say(r, r)
    got = []
    end = time.monotonic() + 10
    while len(got) < len(rooms) and time.monotonic() < end:
        got += [m["body"] for m in a.recv(1)]
    assert sorted(got) == sorted(rooms)


def test_ledger_replies_never_fill_the_receive_window():
    """More ledger calls over a Bus than its ReceiveMaximum (100): replies are acked and routed, never buffered, and
    all arrive on ledger/resp/<address>."""
    a, peer = agent("calls"), agent("caller")
    watcher, seen = watch("ledger/resp/#")
    try:
        for _ in range(150):
            a.ledger("init")
        peer.dm(a.addr, "still here")
        got = a.recv(5)
        got += a.recv(0.5)
        assert [(m["topic"], m["body"]) for m in got] == [(f"inbox/{a.addr}", "still here")], got
        assert wait_for(lambda: len([t for t, _ in seen if t.startswith(f"ledger/resp/{a.addr}")]) >= 150, 5)
        assert {t for t, _ in seen if t.endswith(a.addr)} == {f"ledger/resp/{a.addr}"}
    finally:
        watcher.loop_stop()


def _dump():
    return ledger.call("dump", as_=f"inspector{RUN}")


def test_leave_drops_the_ledger():
    """leave drops the ledger first: afterwards ledgerd holds no ledger, resource, lease or waiter of that agent, and
    anyone queued on its resources gets an answer."""
    owner, reader = agent("leaver"), agent("reader")
    for bus in (owner, reader):
        bus.ledger("init")
    owner.ledger("add", name="res", visibility="public", max_level="write")
    res = f"{owner.addr}/res"
    owner.ledger("acquire", resource=res, level="write", ttl=60)
    reader.ledger("request", resource=res, level="read")
    queued = in_thread(reader.ledger, "acquire", resource=res, level="read", ttl=10, wait=20)
    assert wait_for(lambda: any(w["resource"] == res for w in _dump()["waiters"]), 5)
    agents.remove(owner)
    assert owner.leave() is None
    queued["thread"].join(10)
    assert "dropped" in str(queued.get("error", "")), queued
    d = _dump()
    assert owner.addr not in {x["owner"] for x in d["ledgers"]}
    assert not [r for r in d["resources"] if r["owner"] == owner.addr]
    assert not [x for x in d["leases"] if x["holder"] == owner.addr or x["resource"] == res]
    assert not [w for w in d["waiters"] if w["resource"] == res]


@pytest.mark.skipif(not hasattr(Bus, "wait_arrival"), reason="wait_arrival comes with the Rust bindings")
def test_wait_arrival_fires_once_per_new_arrival():
    """wait_arrival(timeout, after_seq) -> (pending, seq): it returns at once for a newer arrival, and waits out its
    timeout while messages sit unread, so a notice loop fires once per burst and never spins."""
    a, peer = agent("arrival"), agent("notifier")
    started = time.monotonic()
    pending, seq = a.wait_arrival(0.5, 0)
    assert pending == 0 and time.monotonic() - started >= 0.4, "idle: it waits out its timeout"
    for i in range(3):
        peer.dm(a.addr, f"burst {i}")
    pending, seq = a.wait_arrival(5, seq)
    assert pending >= 1
    time.sleep(0.5)
    pending, seq = a.wait_arrival(0, seq)
    started = time.monotonic()
    again, same = a.wait_arrival(0.5, seq)
    assert (again, same) == (3, seq) and time.monotonic() - started >= 0.4, "unread messages do not wake it"
    assert [m["body"] for m in a.recv(1)] == ["burst 0", "burst 1", "burst 2"]


def _raw_history(topic, request):
    """One request published as-is on topic; the reply dict."""
    reply = []
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    c.on_message = lambda _c, _u, m: reply.append(json.loads(m.payload))
    c.connect(config.HOST, config.PORT)
    c.loop_start()
    try:
        resp = f"history/resp/forger{RUN}"
        c.subscribe(resp, qos=1)
        time.sleep(0.3)
        props = Properties(PacketTypes.PUBLISH)
        props.ResponseTopic = resp
        props.CorrelationData = uuid.uuid4().bytes
        c.publish(topic, json.dumps(request), qos=1, properties=props)
        assert wait_for(lambda: reply, 5), f"no reply to {request}"
        return reply[0]
    finally:
        c.loop_stop()


def test_forged_history_requests_on_the_plain_topic():
    """dms, drop and recent name an agent, so they are refused on plain history/req whatever "as" says, and the
    victim's DMs stay readable to the victim and only through its own topic."""
    victim, sender = agent("victim"), agent("sender")
    sender.dm(victim.addr, f"secret {RUN}")
    assert wait_for(lambda: [m["body"] for m in history.dms(victim.addr)] == [f"secret {RUN}"], 5)
    for request in (
        {"op": "dms", "as": victim.addr},
        {"op": "drop", "as": victim.addr},
        {"op": "recent", "as": "admin"},
    ):
        reply = _raw_history("history/req", request)
        assert reply["ok"] is False and "history/req/" in reply["error"], (request, reply)
        assert f"secret {RUN}" not in json.dumps(reply)
    assert [m["body"] for m in history.dms(victim.addr)] == [f"secret {RUN}"], "the forged drop deleted nothing"


def test_leave_completes_with_ledgerd_down(ledgerd, state_dir):
    """With ledgerd stopped, leave still leaves the bus within its 3 s ledger budget and returns a warning."""
    ledgerd.kill()
    ledgerd.wait()
    try:
        gone = agent("unledgered")
        agents.remove(gone)
        started = time.monotonic()
        warning = gone.leave()
        assert warning and "ledger" in warning and time.monotonic() - started < 6, warning
        assert gone.addr not in {x["addr"] for x in presence.agents()}
    finally:
        restarted.append(start_ledgerd(os.path.join(state_dir, "ledger.db"), os.path.join(state_dir, "ledgerd.log")))
