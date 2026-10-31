"""The ledgerd rollback promise: the Rust ledgerd and the frozen Python one in agent-bridge-ledgerd:2 (built from
v0.1.0) hand one ledger.db back and forth (schema 1) with nothing lost or duplicated: ledgers, resources, requests, a
held write lease and its fencing token, a queued waiter, and the replay of a redelivered request.

The Python side runs in Docker (AGENT_BRIDGE_ROLLBACK_LEDGERD overrides the image); skipped when the image is not built.
One writer at a time: each ledgerd is stopped, and seen to exit, before the other opens the database. This test lives
exactly as long as the README offers that image as the rollback.
"""

import json
import os
import shutil
import signal
import socket
import sqlite3
import subprocess
import threading

import paho.mqtt.client as mqtt
import pytest
from conftest import BIN, LEDGERD, _service_env, wait_for
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from agent_bridge import _native, config
from agent_bridge.errors import LedgerError

IMAGE = os.environ.get("AGENT_BRIDGE_ROLLBACK_LEDGERD", "agent-bridge-ledgerd:2")


def built():
    if not shutil.which("docker"):
        return False
    return subprocess.run(["docker", "image", "inspect", IMAGE], capture_output=True).returncode == 0


pytestmark = pytest.mark.skipif(not built(), reason=f"the rollback image {IMAGE} is not built")


def answers():
    try:
        return _native.ledger_call("dump", "probe-0", timeout=1)  # dump creates no ledger: the state stays comparable
    except LedgerError:
        return None


class Rust:
    def __init__(self, db, log):
        with open(log, "a") as err:
            self.proc = subprocess.Popen(LEDGERD, stderr=err, env=_service_env("LEDGER_DB", db, {}))
        assert wait_for(lambda: self.proc.poll() is None and answers(), 30), open(log).read()

    def stop(self):
        self.proc.terminate()
        assert self.proc.wait(10) is not None, "ledgerd did not exit, so two writers could share the database"


class Rollback:
    """The Python ledgerd in its image, on the host network and as this user, so the files it creates in data stay
    writable for the host-run Rust ledgerd; --init forwards docker stop's SIGTERM to it, since it runs as PID 1."""

    def __init__(self, data):
        self.name = f"agent-bridge-ledger-parity-{os.getpid()}"
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)
        subprocess.run(
            ["docker", "run", "-d", "--rm", "--init", "--name", self.name, "--network", "host",
             "--user", f"{os.getuid()}:{os.getgid()}", "-v", f"{data}:/data", "-e", "LEDGER_DB=/data/ledger.db",
             "-e", f"AGENT_BRIDGE_HOST={config.HOST}", "-e", f"AGENT_BRIDGE_PORT={config.PORT}", IMAGE,
             "agent-bridge-ledgerd"],
            check=True,
            capture_output=True,
        )  # fmt: skip
        assert wait_for(answers, 30), self.logs()

    def logs(self):
        return subprocess.run(["docker", "logs", self.name], capture_output=True, text=True).stderr

    def stop(self):
        subprocess.run(["docker", "stop", "-t", "10", self.name], check=True, capture_output=True)

        def stopped():  # or gone: --rm removes the container only some time after docker stop returns
            running = ["docker", "inspect", "-f", "{{.State.Running}}", self.name]
            state = subprocess.run(running, capture_output=True, text=True)
            return state.returncode != 0 or state.stdout.strip() == "false"

        assert wait_for(stopped, 10), "the rollback ledgerd is still running, so two writers could share the database"

    def remove(self):
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)


def connect():
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    up = threading.Event()
    c.on_connect = lambda *_: up.set()
    c.on_socket_open = lambda _c, _u, sock: sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    c.connect(config.HOST, config.PORT)
    c.loop_start()
    assert up.wait(5)
    return c


def call(op, who, **kw):
    return _native.ledger_call(op, who, **kw)


def state():
    """dump without the fields that name the implementation or the moment."""
    d = call("dump", "admin-0")
    for k in ("ok", "ts", "ledgerd"):
        d.pop(k, None)
    return d


def test_rust_and_python_share_one_database(state_dir, tmp_path):
    data = str(tmp_path)  # mounted at /data in the rollback container; the Rust ledgerd opens the same file
    db, log = os.path.join(data, "ledger.db"), os.path.join(state_dir, "ledgerd.log")
    o, r, w = "paro-o1", "parr-r1", "parw-w1"
    raw = connect()
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

    rollback = None
    try:
        # Rust builds the state
        ledgerd = Rust(db, log)
        for x in (o, r, w):
            call("init", x)
        call("add", o, name="pub", description="shared", visibility="public", max_level="write")
        call("add", o, name="priv", description="secret")
        call("request", r, resource=f"{o}/pub", level="read")
        pending = call("request", r, resource=f"{o}/priv", level="read")["request"]
        call("request", w, resource=f"{o}/pub", level="write")
        held = call("acquire", o, resource=f"{o}/pub", level="write", ttl=120)["lease"]
        queued = {}
        # its call keeps its own connection open across both handovers, so the reply has somewhere to go
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
        ledgerd.stop()

        # the Python rollback takes over the same file
        rollback = Rollback(data)
        assert state() == before, "the Python ledgerd reads exactly what the Rust one wrote"
        send(r, redelivered, b"replay-1")  # a redelivered copy: the Rust ledgerd's stored answer is replayed
        assert answer(b"replay-1", 2) == first and state()["requests"] == before["requests"], "no second request"
        call("deny", o, request_id=pending["id"])
        call("release", o, lease_id=held["lease_id"])  # frees the lock: Python serves the waiter queued under Rust
        waiter.join(15)
        assert queued.get("lease", {}).get("holder") == w, queued
        assert queued["lease"]["token"] > held["token"], "fencing tokens keep growing across implementations"
        call("release", w, lease_id=queued["lease"]["lease_id"])
        after_python = state()
        rollback.stop()

        # and back to Rust
        ledgerd = Rust(db, log)
        assert state() == after_python, "the Rust ledgerd reads exactly what the Python one wrote"
        again = call("acquire", o, resource=f"{o}/pub", level="write", ttl=1)["lease"]
        assert again["token"] > queued["lease"]["token"]
        call("release", o, lease_id=again["lease_id"])
        ledgerd.stop()
        with sqlite3.connect(db) as conn:
            assert conn.execute("SELECT value FROM meta WHERE key='schema'").fetchall() == [(1,)], "one schema row, 1"
    finally:
        if rollback:
            rollback.remove()
        raw.disconnect()
        raw.loop_stop()


def test_an_interrupted_acquire_against_the_rollback(tmp_path):
    """cancel is newer than v0.1.0: the Python ledgerd refuses it as an unknown op, and an interrupted command-line
    acquire still ends quietly by its signal, as it does against the Rust ledgerd."""
    rollback = Rollback(tmp_path)
    try:
        o, w = "parc-o1", "parc-w1"
        for x in (o, w):
            call("init", x)
        call("add", o, name="lk", visibility="public", max_level="write")
        call("request", w, resource=f"{o}/lk", level="write")
        call("acquire", o, resource=f"{o}/lk", level="write", ttl=60)
        proc = subprocess.Popen(
            [str(BIN / "agent-bridge"), "ledger-raw", "acquire", "--as", w, "--resource", f"{o}/lk", "--level", "write",
             "--wait", "30"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )  # fmt: skip
        assert wait_for(lambda: state()["waiters"], 10), "the acquire queued"
        proc.send_signal(signal.SIGINT)
        out, err = proc.communicate(timeout=10)
        assert proc.returncode == -signal.SIGINT and not out and "Traceback" not in err and "panicked" not in err, (
            proc.returncode,
            out,
            err,
        )
    finally:
        rollback.remove()
