"""historyd end to end on a broker: recording, dedupe, privacy, drop, the one-time import, the clients, leave, restart
persistence, health, historyd down, and its command line. It runs whichever historyd AGENT_BRIDGE_HISTORYD names (the
bundled Rust one by default). The store's own rules (pruning by receive time, the row and byte caps, the schema
refusal) are pinned by the Rust unit tests in rust/crates/historyd, and every reply's exact text by
test_history_golden.py.
"""

import json
import os
import subprocess
import time
import uuid

import paho.mqtt.client as mqtt
import pytest
from conftest import BIN, HISTORYD, start_historyd, wait_for

from agent_bridge import _native, config
from agent_bridge.errors import BridgeError, HistoryError
from agent_bridge.validation import check_topic

BRIDGE = str(BIN / "agent-bridge")
RUN = uuid.uuid4().hex[:6]


def msg(sender, body, ts=None, **extra):
    return json.dumps({"from": sender, "body": body, "ts": time.time() if ts is None else ts, **extra}).encode()


def call(op, as_=None, **fields):
    return _native.history_call(op, as_=as_, **fields)


def room(name, last=None, before=None):
    return call("room", room=name, last=last, before=before)["messages"]


def dms(addr, peer=None):
    return call("dms", as_=addr, peer=peer)["messages"]


def bodies(messages):
    return [m["body"] for m in messages]


@pytest.fixture(scope="module")
def svc(ledgerd, state_dir):
    """historyd on this module's broker, after a legacy log to import was put in place."""
    with open(config.LOG, "w") as f:
        f.write(json.dumps({"topic": f"room/old-{RUN}", "from": "al-1", "body": "from the old log", "ts": time.time()}))
        f.write("\nnot json\n")
    db = os.path.join(state_dir, "history.db")
    log = os.path.join(state_dir, "historyd.log")
    proc = start_historyd(db, log)
    yield db, log, proc
    proc.kill()
    proc.wait()


@pytest.fixture
def raw():
    """A plain MQTT client: anything any local process could publish."""
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    c.connect(config.HOST, config.PORT)
    c.loop_start()
    yield lambda topic, payload: c.publish(topic, payload, qos=1).wait_for_publish(5)
    c.disconnect()
    c.loop_stop()


def cli(*args, ok=True):
    p = subprocess.run([BRIDGE, *args], capture_output=True, text=True)
    err = p.stderr.strip()
    assert (p.returncode == 0) == ok and (ok or (err and "\n" not in err)), (args, p.returncode, err)
    return [json.loads(line) for line in p.stdout.splitlines()] if ok else err


def settled():
    """A request after what was just published: historyd handles messages and requests in order."""
    assert call("ping") == {"ok": True}


def test_records_what_is_history_and_nothing_else(svc, raw):
    r, al, bo, cy = f"rec{RUN}", f"al{RUN}", f"bo{RUN}", f"cy{RUN}"
    raw(f"room/{r}", msg(al, "one"))
    raw(f"room/{r}", msg(al, "two", mentions=[bo]))
    raw(f"inbox/{bo}", msg(al, "hi bo"))
    raw(f"inbox/{al}", msg(bo, "hi al"))
    raw(f"inbox/{cy}", msg(bo, "for cy"))
    raw("broadcast", msg("admin", f"notice {RUN}"))
    raw("broadcast", msg("mallory-1", f"fake notice {RUN}"))
    raw(f"ci{RUN}/build", b"plain text")
    for t in (f"agents/{al}", f"ledger/req/{al}", f"rooms/{r}", "history/req"):
        raw(t, msg(al, "not history"))
    raw(f"room/{r}", b"not json")
    raw(f"room/{r}", msg(al, "bad mentions", mentions=bo))
    settled()
    assert bodies(room(r)) == ["one", "two"]
    assert room(r)[1]["mentions"] == [bo]
    assert bodies(room(r, last=1)) == ["two"]
    assert bodies(dms(bo)) == ["hi bo", "hi al", "for cy"], "received and sent"
    assert bodies(dms(bo, peer=al)) == ["hi bo", "hi al"]
    assert bodies(dms(cy)) == ["for cy"], "only its own conversations"
    assert f"fake notice {RUN}" not in bodies(call("broadcasts")["messages"]), "the admin's only"
    assert f"notice {RUN}" in bodies(call("broadcasts")["messages"])
    assert call("topic", topic=f"ci{RUN}/build")["messages"] == [
        {"topic": f"ci{RUN}/build", "from": None, "body": "plain text", "ts": pytest.approx(time.time(), abs=30)}
    ]
    stamp = room(r)[1]["ts"]
    assert bodies(room(r, before=stamp)) == ["one"]


def test_redelivery_is_stored_once(svc, raw):
    r = f"once{RUN}"
    payload = msg("al-1", "once", ts=1_700_000_000.0)
    raw(f"room/{r}", payload)
    raw(f"room/{r}", payload)
    settled()
    assert bodies(room(r)) == ["once"]


def test_drop_only_own_inbox(svc, raw):
    al, bo = f"dal{RUN}", f"dbo{RUN}"
    raw(f"inbox/{bo}", msg(al, "to bo"))
    raw(f"inbox/{al}", msg(bo, "from bo"))
    settled()
    with pytest.raises(HistoryError, match="drop: an agent drops only its own inbox"):
        call("drop", as_=bo, addr=al)
    assert call("drop", as_=bo) == {"ok": True, "dropped": 1}
    assert bodies(dms(bo)) == ["from bo"], "what bo sent stays in al's inbox"


def test_service_records_and_serves(svc):
    assert bodies(room(f"old-{RUN}")) == ["from the old log"], "migrated on first start, the bad line skipped"
    al, bo = _native.Bus.register("al"), _native.Bus.register("bo")
    r = f"h{RUN}"
    al.join(r)
    al.say(r, "first")
    al.dm(bo.addr, "private")
    bo.dm(al.addr, "reply")
    al.pub(f"t{RUN}/x", "raw one")
    assert wait_for(lambda: len(room(r)) == 2, 5)
    assert bodies(room(r)) == [f"[joined #{r}] al", "first"]
    assert bodies(room(r, last=1)) == ["first"]
    assert wait_for(lambda: len(dms(bo.addr)) == 2, 5)
    assert bodies(dms(bo.addr, peer=al.addr)) == ["private", "reply"]
    assert bodies(call("topic", topic=f"t{RUN}/x")["messages"]) == ["raw one"]
    assert dms(f"nobody-{RUN}") == []
    assert call("drop", as_=al.addr)["dropped"] == 1
    assert bodies(dms(bo.addr)) == ["private"], "everything sent to al went, bo's own inbox stays"
    with pytest.raises(BridgeError, match="bad room"):
        room("a/b")
    al.leave()
    bo.leave()


def test_clients_read_through_historyd(svc):
    """The CLI's history and dms, the Bus's dms and leave all go through historyd."""
    al, bo = _native.Bus.register("al"), _native.Bus.register("bo")
    r = f"c{RUN}"
    al.join(r)
    al.say(r, "said")
    al.dm(bo.addr, "to bo")
    bo.dm(al.addr, "to al")
    assert wait_for(lambda: len(dms(bo.addr)) == 2, 5)
    assert bodies(cli("history", r, "--last", "1")) == ["said"]
    assert bodies(cli("dms", bo.addr)) == ["to bo", "to al"]
    assert bodies(bo.dms(al.addr)) == ["to bo", "to al"]
    assert al.leave() is None, "ledger and DMs dropped without a warning"
    assert [m for m in dms(al.addr) if m["topic"] == f"inbox/{al.addr}"] == [], "al's inbox went with al"
    bo.leave()


def test_restart_keeps_history_and_catches_up(svc):
    db, log, proc = svc
    r = f"r{RUN}"
    sender = _native.Bus.register("sender")
    sender.say(r, "before restart")
    assert wait_for(lambda: room(r), 5)
    proc.kill()
    proc.wait()
    sender.say(r, "while historyd was down")  # sending never depends on historyd
    err = cli("history", r, ok=False)
    assert err.startswith("history: no reply from historyd"), err
    with pytest.raises(HistoryError):
        _native.history_call("broadcasts", timeout=0.5)
    svc_proc = start_historyd(db, log)
    try:
        assert wait_for(lambda: len(room(r)) == 2, 10), "its persistent session queued what it missed"
        assert bodies(room(r)) == ["before restart", "while historyd was down"]
        assert bodies(room(f"old-{RUN}")) == ["from the old log"], "not imported twice"
        assert subprocess.run([*HISTORYD, "--health"], capture_output=True).returncode == 0
    finally:
        sender.leave()
        svc_proc.kill()
        svc_proc.wait()
    health = subprocess.run([*HISTORYD, "--health"], capture_output=True, text=True, timeout=30)
    assert health.returncode != 0 and "historyd" in health.stderr


def test_arguments_start_nothing(tmp_path):
    """Only no arguments runs the service: --help prints usage and any other argument is refused, both before anything
    connects or opens a database, so a stray flag cannot start a second historyd against the live defaults."""
    db = tmp_path / "history.db"
    env = {
        **os.environ,
        "HISTORY_DB": str(db),
        "HISTORY_IMPORT": str(tmp_path / "none.jsonl"),
        "AGENT_BRIDGE_PORT": "1",
    }
    shown = subprocess.run([*HISTORYD, "--help"], env=env, capture_output=True, text=True, timeout=30)
    assert shown.returncode == 0 and shown.stdout.startswith("usage: agent-bridge-historyd"), shown
    for args in (["--bogus"], ["--hea"], ["--health", "x"], ["serve"]):
        refused = subprocess.run([*HISTORYD, *args], env=env, capture_output=True, text=True, timeout=30)
        assert refused.returncode == 2 and refused.stderr.startswith("usage: agent-bridge-historyd"), (args, refused)
        last = refused.stderr.splitlines()[-1]
        assert last.startswith("agent-bridge-historyd: error: unrecognized arguments: "), (args, last)
    assert not db.exists(), "no database was opened"


def test_history_is_a_reserved_level():
    for topic in ("history/req", "history/resp/x", "history/#"):
        with pytest.raises(BridgeError, match="reserved"):
            check_topic(topic, filter=True)
