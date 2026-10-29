"""The ledgerd rollback promise: the Rust and the Python ledgerd hand one ledger.db back and forth (schema 1) with
nothing lost or duplicated: ledgers, resources, requests, a held write lease and its fencing token, a queued waiter,
and the replay of a redelivered request.

Skipped unless AGENT_BRIDGE_LEDGERD names a non-Python ledgerd (the Rust one); the other side is always the Python
ledgerd. One writer at a time: each ledgerd is stopped, and seen to exit, before the other opens the database.
"""

import json
import os
import sqlite3
import subprocess
import sys
import threading

import pytest
from conftest import LEDGERD, _service_env, wait_for
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from agent_bridge.ledger import client as ledger

PYTHON = [sys.executable, "-m", "agent_bridge.ledger.service"]
RUST = LEDGERD  # conftest's, from AGENT_BRIDGE_LEDGERD

pytestmark = pytest.mark.skipif(RUST == PYTHON, reason="AGENT_BRIDGE_LEDGERD does not name a non-Python ledgerd")


def start(cmd, db, log):
    with open(log, "a") as err:
        proc = subprocess.Popen(cmd, stderr=err, env=_service_env("LEDGER_DB", db, {}))

    def answers():
        assert proc.poll() is None, open(log).read()
        try:
            return ledger.call("dump", "probe-0", timeout=1)  # dump creates no ledger, so the state stays comparable
        except ledger.LedgerError:
            return None

    assert wait_for(answers, 30), "ledgerd never answered"
    return proc


def stop(proc):
    proc.terminate()
    assert proc.wait(10) is not None, "ledgerd did not exit, so two writers could share the database"


def state():
    """dump without the fields that name the implementation or the moment."""
    d = ledger.call("dump", "admin-0")
    for k in ("ok", "ts", "ledgerd"):
        d.pop(k, None)
    return d


def test_rust_and_python_share_one_database(state_dir):
    db, log = os.path.join(state_dir, "ledger.db"), os.path.join(state_dir, "ledgerd.log")
    o, r, w = "paro-o1", "parr-r1", "parw-w1"
    c = ledger.connect()  # stays connected across every handover, so the queued waiter's reply has somewhere to go
    raw = ledger.connect()
    replies = {}
    got = threading.Condition()

    def on_reply(_c, _u, m):
        with got:
            replies.setdefault(m.properties.CorrelationData, []).append(json.loads(m.payload))
            got.notify_all()

    raw.message_callback_add("ledger/resp/parity", on_reply)
    raw.subscribe("ledger/resp/parity", qos=1)

    def send(as_, body, corr):
        props = Properties(PacketTypes.PUBLISH)
        props.ResponseTopic, props.CorrelationData = "ledger/resp/parity", corr
        raw.publish(f"ledger/req/{as_}", json.dumps(body), qos=1, properties=props)

    def answer(corr, n=1):
        with got:
            assert got.wait_for(lambda: len(replies.get(corr, [])) >= n, 10), corr
            return replies[corr][n - 1]

    def call(op, who, **kw):
        return ledger.call(op, who, client=c, **kw)

    # Rust builds the state
    proc = start(RUST, db, log)
    for x in (o, r, w):
        call("init", x)
    call("add", o, name="pub", description="shared", visibility="public", max_level="write")
    call("add", o, name="priv", description="secret")
    call("request", r, resource=f"{o}/pub", level="read")
    pending = call("request", r, resource=f"{o}/priv", level="read")["request"]
    call("request", w, resource=f"{o}/pub", level="write")
    held = call("acquire", o, resource=f"{o}/pub", level="write", ttl=120)["lease"]
    queued = {}
    waiter = threading.Thread(
        target=lambda: queued.update(call("acquire", w, resource=f"{o}/pub", level="write", ttl=60, wait=60))
    )
    waiter.start()
    assert wait_for(lambda: state()["waiters"], 10), "the write acquire never queued"
    redelivered = {"op": "request", "resource": f"{o}/priv", "level": "write"}
    send(r, redelivered, b"replay-1")
    first = answer(b"replay-1")
    assert first["request"]["status"] == "pending"
    before = state()
    stop(proc)

    # Python takes over the same file
    proc = start(PYTHON, db, log)
    assert state() == before, "the Python ledgerd reads exactly what the Rust one wrote"
    send(r, redelivered, b"replay-1")  # a redelivered copy: the Rust ledgerd's stored answer is replayed
    assert answer(b"replay-1", 2) == first and state()["requests"] == before["requests"], "no second request"
    call("deny", o, request_id=pending["id"])
    call("release", o, lease_id=held["lease_id"])  # frees the lock: the waiter queued under Rust is served by Python
    waiter.join(15)
    assert queued.get("lease", {}).get("holder") == w, queued
    assert queued["lease"]["token"] > held["token"], "fencing tokens keep growing across implementations"
    call("release", w, lease_id=queued["lease"]["lease_id"])
    after_python = state()
    stop(proc)

    # and back to Rust
    proc = start(RUST, db, log)
    assert state() == after_python, "the Rust ledgerd reads exactly what the Python one wrote"
    again = call("acquire", o, resource=f"{o}/pub", level="write", ttl=1)["lease"]
    assert again["token"] > queued["lease"]["token"]
    call("release", o, lease_id=again["lease_id"])
    stop(proc)

    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT value FROM meta WHERE key='schema'").fetchall() == [(1,)], "one schema row, 1"
    for x in (c, raw):
        x.disconnect()
        x.loop_stop()
