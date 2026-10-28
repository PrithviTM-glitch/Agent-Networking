"""Conformance: what every implementation of the bus must do, checked through the public API and the wire only, so it
runs unchanged against the Python modules and against the Rust bindings.

Covered: no call stalls behind a blocked recv or a queued acquire; concurrent joins keep every room; ledger replies on
a Bus's own connection never fill its receive window; leave drops the agent's ledger, and still completes when ledgerd
is down; wait_arrival (bindings only); forged history requests on the plain topic are refused; wrong-typed input to
every entry point is a one-line BridgeError (LedgerError for ledger requests) with the reference message, never a bare
TypeError; and the named bus behaviours the conftest's summary reports (origin written once, subagent lineage, the
whole-record refresh, cache reset on reconnect, for_me, the relay lock's second line, moderation at recv).
"""

import fcntl
import json
import os
import subprocess
import sys
import threading
import time
import uuid
from types import SimpleNamespace

import paho.mqtt.client as mqtt
import pytest
from conftest import start_ledgerd, wait_for
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from agent_bridge import config, presence, relay, rooms
from agent_bridge.bus import Bus
from agent_bridge.history import client as history
from agent_bridge.ledger import client as ledger
from agent_bridge.validation import BODY_MAX, BridgeError

# The entry points of each implementation, for the tests that run against both side by side.
IMPLS = {
    "python": SimpleNamespace(
        native=False,
        Bus=Bus,
        attach=Bus,
        agents=presence.agents,
        room_info=rooms.info,
        ledger_call=ledger.call,
        history_call=history.call,
        relay_lock_path=relay.lock_path,
        relay_held=relay.held,
    )
}
try:
    from agent_bridge import _native

    IMPLS["native"] = SimpleNamespace(
        native=True,
        Bus=_native.Bus,
        attach=_native.Bus.attach,
        agents=_native.presence_agents,
        room_info=_native.rooms_info,
        ledger_call=_native.ledger_call,
        history_call=_native.history_call,
        relay_lock_path=_native.relay_lock_path,
        relay_held=_native.relay_held,
    )
except ImportError:
    pass
RUN = uuid.uuid4().hex[:6]
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


@pytest.fixture(scope="module", params=sorted(IMPLS))
def api(request):
    """Each test runs once per implementation present: the Python modules, and the Rust bindings when built."""
    return IMPLS[request.param]


def agent(api, name):
    bus = api.Bus.register(f"{name}{RUN}")
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


def test_no_call_waits_behind_a_blocked_one(api):
    """recv(wait) and acquire(wait) block only their own caller: say, dm and release on the same Bus return at once."""
    a, peer, holder = agent(api, "stall"), agent(api, "peer"), agent(api, "holder")
    room = f"stall{RUN}{id(api) % 997}"
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


def test_concurrent_joins_keep_every_room(api, request):
    """Ten joins at once on one Bus: presence lists all ten rooms and each room delivers."""
    if api.Bus is Bus:
        reason = "the Python Bus can lose a concurrent join; the Rust Bus serialises presence writes"
        request.applymarker(pytest.mark.xfail(reason=reason))
    a, talker = agent(api, "joins"), agent(api, "talker")
    rooms = [f"j{RUN}{id(api) % 997}x{i}" for i in range(10)]
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


def test_ledger_replies_never_fill_the_receive_window(api):
    """More ledger calls over a Bus than its ReceiveMaximum (100): replies are acked and routed, never buffered, and
    all arrive on ledger/resp/<address>."""
    a, peer = agent(api, "calls"), agent(api, "caller")
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


def test_leave_drops_the_ledger(api):
    """leave drops the ledger first: afterwards ledgerd holds no ledger, resource, lease or waiter of that agent, and
    anyone queued on its resources gets an answer."""
    owner, reader = agent(api, "leaver"), agent(api, "reader")
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


def test_wait_arrival_fires_once_per_new_arrival(api):
    """wait_arrival(timeout, after_seq) -> (pending, seq): it returns at once for a newer arrival, and waits out its
    timeout while messages sit unread, so a notice loop fires once per burst and never spins."""
    if not hasattr(api.Bus, "wait_arrival"):
        pytest.skip("wait_arrival comes with the Rust bindings")
    a, peer = agent(api, "arrival"), agent(api, "notifier")
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


def test_forged_history_requests_on_the_plain_topic(api):
    """dms, drop and recent name an agent, so they are refused on plain history/req whatever "as" says, and the
    victim's DMs stay readable to the victim and only through its own topic."""
    victim, sender = agent(api, "victim"), agent(api, "sender")
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


def test_leave_completes_with_ledgerd_down(api, ledgerd, state_dir):
    """With ledgerd stopped, leave still leaves the bus within its 3 s ledger budget and returns a warning."""
    running = restarted[-1] if restarted else ledgerd  # the second implementation's run finds the restarted one
    running.kill()
    running.wait()
    try:
        gone = agent(api, "unledgered")
        agents.remove(gone)
        started = time.monotonic()
        warning = gone.leave()
        assert warning and "ledger" in warning and time.monotonic() - started < 6, warning
        assert gone.addr not in {x["addr"] for x in presence.agents()}
    finally:
        restarted.append(start_ledgerd(os.path.join(state_dir, "ledger.db"), os.path.join(state_dir, "ledgerd.log")))


def _refusal(fn, *args, **kw):
    """The one-line message fn raises for bad input, or a description of what it did instead."""
    try:
        fn(*args, **kw)
    except (BridgeError, ledger.LedgerError) as e:
        assert "\n" not in str(e), str(e)
        return type(e).__name__, str(e)
    except Exception as e:  # noqa: BLE001  the failure this test exists to catch
        return type(e).__name__, str(e)
    return None, "accepted"


def test_wrong_types_are_one_line_bridge_errors(api):
    """Every entry point that takes user input checks its type itself: the MCP server and the console pass tool and
    form arguments straight through. The messages are the reference implementation's, for both implementations."""
    a, peer = api.Bus.register(f"types{RUN}"), api.Bus.register(f"typepeer{RUN}")
    agents.extend((a, peer))
    room = f"types{RUN}{id(api) % 997}"
    a.join(room)
    console = api.Bus.register("admin", addr="admin", keep=True, admin=True)
    console.mod(room, "add", a.addr)  # rights are checked before argument types, so a must be a moderator
    console.close()
    cases = [
        ("say text int", a.say, (room, 5), "text must be a string, got int"),
        ("say text list", a.say, (room, ["x"]), "text must be a string, got list"),
        ("say text too long", a.say, (room, "x" * (BODY_MAX + 1)), f"the limit is {BODY_MAX}"),
        ("say room int", a.say, (5, "x"), "bad room 5"),
        ("say room None", a.say, (None, "x"), "bad room None"),
        ("dm to None", a.dm, (None, "x"), "bad address None"),
        ("dm text bytes", a.dm, (peer.addr, b"x"), "text must be a string, got bytes"),
        ("pub topic None", a.pub, (None, "x"), "topic must be a string, got NoneType"),
        ("pub text int", a.pub, ("t/x", 5), "text must be a string, got int"),
        ("sub None", a.sub, (None,), "topic must be a string, got NoneType"),
        ("unsub int", a.unsub, (5,), "topic must be a string, got int"),
        ("join None", a.join, (None,), "bad room None"),
        ("part list", a.part, (["r"],), "bad room ['r']"),
        ("describe name int", a.describe, (5,), "bad name 5"),
        ("describe caps str", a.describe, (None, "abc"), "caps must be a list of at most 32"),
        ("recv wait str", a.recv, ("soon",), "wait must be 0-3600 seconds"),
        ("recv wait bool", a.recv, (True,), "wait must be 0-3600 seconds"),
        ("dms peer int", a.dms, (5,), "bad address 5"),
        ("dms last str", a.dms, (None, "x"), "last must be 1-1000"),
        ("room_set mode int", a.room_set, (room, 5), "mode must be open or announce"),
        ("mute addr None", a.mute, (room, None), "bad address None"),
        ("ban room int", a.ban, (5, peer.addr), "bad room 5"),
        ("mod action int", a.mod, (room, 5, peer.addr), "action must be add or remove"),
        ("register name int", api.Bus.register, (5,), "bad name 5"),
        ("register caps str", api.Bus.register, ("x", "abc"), "caps must be a list of at most 32"),
        ("register addr int", api.Bus.register, ("x", [], False, 5), "bad address 5"),
        ("agents room int", api.agents, (5,), "bad room 5"),
        ("room info None", api.room_info, (None,), "bad room None"),
        ("history room int", history.room, (5,), "bad room 5"),
        ("history dms None", history.dms, (None,), "bad address None"),
        ("history topic None", history.topic, (None,), "topic must be a string, got NoneType"),
    ]
    # Raw service requests are checked by the service: its own error class and message come back.
    service_cases = [
        ("ledger op int", api.ledger_call, (5, a.addr), "LedgerError", "op: one of"),
        ("ledger as None", api.ledger_call, ("dump", None), "LedgerError", "as: required"),
        ("history op int", api.history_call, (5,), "HistoryError", "history: op: one of"),
    ]
    wrong = []
    for name, fn, args, kind_wanted, message in [(*c[:3], "BridgeError", c[3]) for c in cases] + service_cases:
        kind, text = _refusal(fn, *args)
        if kind != kind_wanted or message not in text:
            wrong.append(f"{name}: got {kind}: {text}")
    assert not wrong, "\n".join(wrong)


# bridge-core's named bus behaviours, each against both implementations. Another process acting as the same agent is
# a separate Python process running the same implementation, so its origin is read from its own environment.


def _other_process(api, code, session=None):
    """Run code in a fresh process of the same implementation; `Bus` and `attach` are defined for it."""
    prelude = (
        "from agent_bridge import _native\nBus = _native.Bus\nattach = Bus.attach\n"
        if api.native
        else "from agent_bridge.bus import Bus\nattach = Bus\n"
    )
    env = {k: v for k, v in os.environ.items() if k != "CLAUDE_CODE_SESSION_ID"}
    if session:
        env["CLAUDE_CODE_SESSION_ID"] = session
    done = subprocess.run([sys.executable, "-c", prelude + code], env=env, capture_output=True, text=True, timeout=60)
    assert done.returncode == 0, done.stderr
    return done.stdout.strip()


def _record(addr):
    return {x["addr"]: x for x in presence.agents()}.get(addr) or {}


def _until_up(bus, room):
    """Wait until a Bus that was taken over is connected again."""

    def up():
        try:
            bus.say(room, "back")
            return True
        except BridgeError:
            return False

    assert wait_for(up, 10), "the Bus never reconnected"


def _heard(bus, wait=3):
    got, end = [], time.monotonic() + wait
    while time.monotonic() < end:
        got += bus.recv(0.5)
        if got and not bus.pending():
            break
    return got


def test_origin_is_written_once(api):
    """A session-less agent stays session-less when a process with a session acts as it."""
    a = agent(api, "nosession")
    room = f"origin{RUN}{id(api) % 997}"
    assert "session" not in _record(a.addr)
    _other_process(api, f"attach({a.addr!r}).join({room!r})", session=str(uuid.uuid4()))
    rec = _record(a.addr)
    assert f"room/{room}" in rec.get("topics", []) and "session" not in rec, rec


def test_subagent_keeps_its_lineage(api):
    """A subagent's kind and parent survive a later join from a process without a session."""
    parent = str(uuid.uuid4())
    addr = _other_process(api, f"b = Bus.register('sub{RUN}', subagent=True)\nprint(b.addr)\nb.close()", parent)
    sub = api.attach(addr)
    agents.append(sub)
    sub.join(f"lineage{RUN}{id(api) % 997}")
    rec = _record(addr)
    assert (rec.get("kind"), rec.get("parent"), rec.get("session")) == ("subagent", parent, None), rec


def test_a_join_keeps_what_another_process_changed(api):
    """keep set by another process acting as the agent survives the agent's next join (whole-record refresh)."""
    a = agent(api, "keeper")
    _other_process(api, f"attach({a.addr!r}).keep(True)")
    assert wait_for(lambda: _record(a.addr).get("keep") is True, 5)
    room = f"keeper{RUN}{id(api) % 997}"
    a.join(room)
    rec = _record(a.addr)
    assert rec.get("keep") is True and f"room/{room}" in rec["topics"], rec


def test_presence_cache_resets_on_reconnect(api):
    """A record cleared while the Bus was disconnected is gone from its cache once it reconnects."""
    a, ghost = agent(api, "watcher"), agent(api, "ghost")
    room = f"reset{RUN}{id(api) % 997}"
    a.join(room)
    a.dm(ghost.addr, "you are there")
    agents.remove(ghost)
    ghost.close()
    wipe = "_native.presence_wipe" if api.native else "__import__('agent_bridge.presence').presence.wipe"
    # The takeover disconnects a; the wipe happens before it reconnects, so a never sees the clear arrive.
    _other_process(api, f"attach({a.addr!r})\n{wipe}({ghost.addr!r})")
    _until_up(a, room)
    kind, text = _refusal(a.dm, ghost.addr, "still there?")
    assert (kind, text) == ("BridgeError", f"unknown address: {ghost.addr}"), (kind, text)


def test_recv_marks_mentions_for_me(api):
    """recv adds for_me to room messages with mentions: true for the mentioned agent, false for the others."""
    a, b, c = agent(api, "mentioned"), agent(api, "speaker"), agent(api, "bystander")
    room = f"mention{RUN}{id(api) % 997}"
    for bus in (a, b, c):
        bus.join(room)
    _heard(a, 1), _heard(c, 1)
    b.say(room, f"@{a.addr} over to you")
    mine = [m for m in _heard(a) if m["body"].endswith("over to you")]
    theirs = [m for m in _heard(c) if m["body"].endswith("over to you")]
    assert [m.get("for_me") for m in mine] == [True] and [m.get("for_me") for m in theirs] == [False], (mine, theirs)


def test_relay_lock_names_the_relayed_agent(api, state_dir):
    """A live relay's lock file holds its pid and, on the second line, the address it relays; held() reads it."""
    session, addr = str(uuid.uuid4()), f"relayed-{RUN}"
    path = api.relay_lock_path(session)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as lock:
        lock.write(f"{os.getpid()}\n{addr}\n")
        lock.flush()
        fcntl.flock(lock, fcntl.LOCK_EX)
        assert api.relay_held(session) == addr
        fcntl.flock(lock, fcntl.LOCK_UN)
    assert api.relay_held(session) is None, "no relay holds the lock any more"


def test_moderation_applies_at_recv(api):
    """A spoof from a muted address, queued while the receiver was offline, is dropped when it reads."""
    muted, receiver, speaker = agent(api, "muted"), agent(api, "receiver"), agent(api, "legit")
    room = f"modrecv{RUN}{id(api) % 997}"
    for bus in (muted, receiver, speaker):
        bus.join(room)
    console = api.Bus.register("admin", addr="admin", keep=True, admin=True)
    console.mute(room, muted.addr)
    console.close()
    _heard(receiver, 1)
    agents.remove(receiver)
    receiver.close()
    spoof = json.dumps({"from": muted.addr, "body": f"spoof {RUN}", "ts": time.time()})
    raw, _ = watch("none/at/all")
    raw.publish(f"room/{room}", spoof, qos=1).wait_for_publish(5)
    raw.loop_stop()
    speaker.say(room, f"legit {RUN}")
    back = api.attach(receiver.addr)
    agents.append(back)
    bodies = [m["body"] for m in _heard(back)]
    assert f"legit {RUN}" in bodies and f"spoof {RUN}" not in bodies, bodies


def test_a_closed_bus_never_redials(api):
    """A closed Bus never takes the session back from its next owner: closed while it waits to redial after a takeover,
    then the next owner attaches, and DMs sent every 0.5 s reach it in order with no reconnect-sized gap."""
    a, sender = agent(api, "closing"), agent(api, "steady")
    agents.remove(a)
    kicker = api.attach(a.addr)  # a second connection with a's client id: a loses its session and waits to redial
    a.close()
    kicker.close()
    owner = api.attach(a.addr)
    agents.append(owner)
    arrivals = []

    def read():
        end = time.monotonic() + 5
        while time.monotonic() < end and len(arrivals) < 6:
            arrivals.extend((time.monotonic(), m["body"]) for m in owner.recv(0.2))

    reader = in_thread(read)
    sent = [f"steady {i} {RUN}" for i in range(6)]
    for body in sent:
        sender.dm(owner.addr, body)
        time.sleep(0.5)
    reader["thread"].join(10)
    assert [body for _, body in arrivals] == sent, arrivals
    gaps = [later - earlier for (earlier, _), (later, _) in zip(arrivals, arrivals[1:], strict=False)]
    assert max(gaps) < 0.9, f"a gap of {max(gaps):.2f} s: something took the session over"
