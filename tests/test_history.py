"""historyd: the store on its own (queries, dedupe, migration, privacy, pruning, caps, hostile requests), then the
service end to end on a broker (recording, the clients, leave, restart persistence, health, historyd down).
"""

import json
import os
import subprocess
import time
import uuid

import pytest
from conftest import BIN, HISTORYD, start_historyd, wait_for

from agent_bridge import config
from agent_bridge.bus import Bus
from agent_bridge.history import client, store
from agent_bridge.history.store import History
from agent_bridge.validation import BridgeError, check_topic

BRIDGE = str(BIN / "agent-bridge")
RUN = uuid.uuid4().hex[:6]


def msg(sender, body, ts=None, **extra):
    return json.dumps({"from": sender, "body": body, "ts": time.time() if ts is None else ts, **extra}).encode()


class Replies:
    """A publish function that keeps what History sends back, by correlation."""

    def __init__(self):
        self.sent = []

    def __call__(self, topic, payload, corr):
        self.sent.append((topic, json.loads(payload), corr))

    def ask(self, h, req, via=None, topic="history/resp/t", corr=b"c"):
        h.handle(json.dumps(req).encode() if isinstance(req, dict) else req, topic, corr, via)
        return self.sent[-1][1] if self.sent else None


@pytest.fixture
def h(tmp_path):
    r = Replies()
    hist = History(str(tmp_path / "h.db"), r)
    hist.replies = r
    return hist


def bodies(reply):
    assert reply["ok"], reply
    return [m["body"] for m in reply["messages"]]


def test_records_rooms_dms_broadcasts_and_raw_topics(h):
    ask = h.replies.ask
    h.record("room/r", msg("al-1", "one"))
    h.record("room/r", msg("al-1", "two", mentions=["bo-1"]))
    h.record("inbox/bo-1", msg("al-1", "hi bo"))
    h.record("inbox/al-1", msg("bo-1", "hi al"))
    h.record("inbox/cy-1", msg("bo-1", "for cy"))
    h.record("broadcast", msg("admin", "notice"))
    h.record("broadcast", msg("mallory-1", "fake notice"))
    h.record("ci/build", b"plain text")
    for t in ("agents/al-1", "ledger/req/al-1", "rooms/r", "history/req", "$SYS/x"):
        h.record(t, msg("al-1", "not history"))
    h.record("room/r", b"not json")
    h.record("room/r", msg("al-1", "bad mentions", mentions="bo-1"))
    assert bodies(ask(h, {"op": "room", "room": "r"})) == ["one", "two"]
    assert ask(h, {"op": "room", "room": "r"})["messages"][1]["mentions"] == ["bo-1"]
    assert bodies(ask(h, {"op": "room", "room": "r", "last": 1})) == ["two"]
    assert bodies(ask(h, {"op": "dms"}, via="bo-1")) == ["hi bo", "hi al", "for cy"], "received and sent"
    assert bodies(ask(h, {"op": "dms", "peer": "al-1"}, via="bo-1")) == ["hi bo", "hi al"]
    assert bodies(ask(h, {"op": "dms"}, via="cy-1")) == ["for cy"], "only its own conversations"
    assert bodies(ask(h, {"op": "broadcasts"})) == ["notice"], "admin's only"
    assert ask(h, {"op": "topic", "topic": "ci/build"})["messages"] == [
        {"topic": "ci/build", "from": None, "body": "plain text", "ts": pytest.approx(time.time(), abs=5)}
    ]
    assert bodies(ask(h, {"op": "recent"}, via="admin"))[-1] == "plain text"
    stamp = ask(h, {"op": "room", "room": "r"})["messages"][1]["ts"]
    assert bodies(ask(h, {"op": "room", "room": "r", "before": stamp})) == ["one"]


def test_redelivery_is_stored_once(h):
    payload = msg("al-1", "once", ts=1_700_000_000.0)
    h.record("room/r", payload)
    h.record("room/r", payload)
    assert bodies(h.replies.ask(h, {"op": "room", "room": "r"})) == ["once"]


def test_drop_only_own_inbox(h):
    ask = h.replies.ask
    h.record("inbox/bo-1", msg("al-1", "to bo"))
    h.record("inbox/al-1", msg("bo-1", "from bo"))
    refused = {"ok": False, "error": "drop: an agent drops only its own inbox"}
    assert ask(h, {"op": "drop", "addr": "al-1"}, via="bo-1") == refused
    assert ask(h, {"op": "drop"}, via="bo-1") == {"ok": True, "dropped": 1}
    assert bodies(ask(h, {"op": "dms"}, via="bo-1")) == ["from bo"], "what bo sent stays in al's inbox"


def test_migration_imports_once_and_keeps_the_file(h, tmp_path):
    legacy = tmp_path / "history.jsonl"
    good = [{"topic": "room/r", "from": "al-1", "body": f"old {i}", "ts": time.time() - 100 + i} for i in range(3)]
    lines = [json.dumps(e) for e in good] + ["not json", "{}", json.dumps({**good[0], "from": "a b"}), "[" * 50_000]
    legacy.write_text("\n".join(lines) + "\n")
    h.record("room/r", json.dumps({k: good[2][k] for k in ("from", "body", "ts")}).encode())  # arrived live too
    assert h.migrate(str(legacy)) == 2
    assert h.migrate(str(legacy)) == 0, "once"
    assert legacy.exists() and legacy.read_text().count("\n") == len(lines), "never deleted or changed"
    assert bodies(h.replies.ask(h, {"op": "room", "room": "r"})) == ["old 2", "old 0", "old 1"]
    assert h.migrate(str(tmp_path / "missing.jsonl")) == 0


def test_pruning_follows_receive_time_and_caps(h, monkeypatch):
    now = time.time()
    h.record("room/r", msg("al-1", "far future ts", ts=9_999_999_999.0), now=now - config.STALE_S - 10)
    h.record("room/r", msg("al-1", "fresh"), now=now)
    h.prune(now=now)
    assert bodies(h.replies.ask(h, {"op": "room", "room": "r"})) == ["fresh"], "a forged ts does not keep a row"
    h.record("room/r", msg("al-1", "old again"), now=now - config.STALE_S - 10)
    h.prune(now=now + 1)
    assert "old again" in bodies(h.replies.ask(h, {"op": "room", "room": "r"})), "at most once per hour"
    monkeypatch.setattr(store, "MAX_ROWS", 1500)
    for i in range(2000):
        h.add("room/flood", "al-1", f"m{i}", float(i), None, now)
    h.trim()
    assert h.q("SELECT count(*) FROM messages").fetchone()[0] <= 1500
    assert bodies(h.replies.ask(h, {"op": "room", "room": "flood", "last": 1})) == ["m1999"], "the oldest go"


def test_hostile_requests(h):
    ask, r = h.replies.ask, h.replies
    h.handle(b"{}", "elsewhere/resp", b"c")
    h.handle(b"{}", None, b"c")
    assert r.sent == [], "no ResponseTopic under history/resp/: dropped"
    bad = [
        (b"\xff\xfe", "UTF-8 JSON"),
        (b"[" * 20_000, "payload over"),
        (b"[1]", "JSON object"),
        ({"op": "nope"}, "op: one of"),
        ({"op": "room"}, "room: required"),
        ({"op": "room", "room": "a/b"}, "room: bad room"),
        ({"op": "room", "room": "r", "last": 0}, "last:"),
        ({"op": "room", "room": "r", "last": True}, "last:"),
        ({"op": "room", "room": "r", "before": "now"}, "before:"),
        ({"op": "room", "room": "r", "extra": 1}, "unknown fields"),
        ({"op": "dms"}, "history/req/<your address>"),
        ({"op": "dms", "as": "al-1"}, "history/req/<your address>"),
        ({"op": "drop", "as": "al-1"}, "history/req/<your address>"),
        ({"op": "topic", "topic": "inbox/al-1"}, "reserved"),
        ({"op": "topic", "topic": "a/#"}, "wildcards"),
        ({"op": "room", "room": "r", "as": "x y"}, "as:"),
        ({"op": "broadcasts", "since": float("nan")}, "since:"),
    ]
    for req, error in bad:
        reply = ask(h, req)
        assert reply["ok"] is False and error in reply["error"] and "\n" not in reply["error"], (req, reply)
    assert ask(h, {"op": "dms", "as": "bo-1"}, via="al-1")["error"].startswith("as: must match")
    assert "peer:" in ask(h, {"op": "dms", "peer": 5}, via="al-1")["error"]
    assert "admin console only" in ask(h, {"op": "recent"}, via="al-1")["error"]
    assert "integer from 1 to" in ask(h, {"op": "recent", "last": 9999}, via="admin")["error"]
    assert ask(h, {"op": "recent", "last": 2000}, via="admin")["ok"]
    assert ask(h, {"op": "ping"}, corr=b"")["ok"] is False, "CorrelationData required"
    h.record("room/big", msg("al-1", "x" * 60_000))
    for i in range(6):
        h.record("room/big", msg("al-1", "x" * 60_000 + str(i)))
    reply = ask(h, {"op": "room", "room": "big", "last": 1000})
    assert reply["truncated"] and len(json.dumps(reply)) < store.MAX_REPLY and len(reply["messages"]) < 7


# The service on a broker.


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


def cli(*args, ok=True):
    p = subprocess.run([BRIDGE, *args], capture_output=True, text=True)
    err = p.stderr.strip()
    assert (p.returncode == 0) == ok and (ok or (err and "\n" not in err)), (args, p.returncode, err)
    return [json.loads(line) for line in p.stdout.splitlines()] if ok else err


def test_service_records_and_serves(svc):
    assert [m["body"] for m in client.room(f"old-{RUN}")] == ["from the old log"], "migrated on first start"
    al, bo = Bus.register("al"), Bus.register("bo")
    room = f"h{RUN}"
    al.join(room)
    al.say(room, "first")
    al.dm(bo.addr, "private")
    bo.dm(al.addr, "reply")
    al.pub(f"t{RUN}/x", "raw one")
    assert wait_for(lambda: len(client.room(room)) == 2, 5)
    assert [m["body"] for m in client.room(room)] == [f"[joined #{room}] al", "first"]
    assert [m["body"] for m in client.room(room, last=1)] == ["first"]
    assert wait_for(lambda: len(client.dms(bo.addr)) == 2, 5)
    assert [m["body"] for m in client.dms(bo.addr, peer=al.addr)] == ["private", "reply"]
    assert [m["body"] for m in client.topic(f"t{RUN}/x")] == ["raw one"]
    assert client.dms(f"nobody-{RUN}") == []
    assert client.drop(al.addr) == 1
    assert [m["body"] for m in client.dms(bo.addr)] == ["private"], "everything sent to al went, bo's own inbox stays"
    with pytest.raises(BridgeError, match="bad room"):
        client.room("a/b")
    al.leave()
    bo.leave()


def test_restart_keeps_history_and_catches_up(svc):
    db, log, proc = svc
    room = f"r{RUN}"
    sender = Bus.register("sender")
    sender.say(room, "before restart")
    assert wait_for(lambda: client.room(room), 5)
    proc.kill()
    proc.wait()
    sender.say(room, "while historyd was down")  # sending never depends on historyd
    with pytest.raises(client.HistoryError):
        client.broadcasts(timeout=0.5)
    svc_proc = start_historyd(db, log)
    try:
        assert wait_for(lambda: len(client.room(room)) == 2, 10), "its persistent session queued what it missed"
        assert [m["body"] for m in client.room(room)] == ["before restart", "while historyd was down"]
        assert [m["body"] for m in client.room(f"old-{RUN}")] == ["from the old log"], "not imported twice"
        assert subprocess.run([*HISTORYD, "--health"], capture_output=True).returncode == 0
    finally:
        sender.leave()
        svc_proc.kill()
        svc_proc.wait()
    health = subprocess.run([*HISTORYD, "--health"], capture_output=True, text=True, timeout=30)
    assert health.returncode != 0 and "historyd" in health.stderr


def test_history_is_a_reserved_level():
    for topic in ("history/req", "history/resp/x", "history/#"):
        with pytest.raises(BridgeError, match="reserved"):
            check_topic(topic, filter=True)
