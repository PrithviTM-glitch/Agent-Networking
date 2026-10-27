"""@mentions in rooms: resolution when sending, validation when receiving, and for_me in recv. The relay side (waking
only the mentioned agent, the quiet backlog) is in test_relay.py. The tests are one scenario in file order.
"""

import json
import uuid
from types import SimpleNamespace

import paho.mqtt.client as mqtt
import pytest

from agent_bridge import config, history
from agent_bridge.bus import Bus
from agent_bridge.validation import MENTIONS_MAX, BridgeError

RUN = uuid.uuid4().hex[:6]
ROOM = f"at-{RUN}"
S = SimpleNamespace()


@pytest.fixture(scope="module", autouse=True)
def with_ledgerd(ledgerd):
    """leave drops the agent's ledger first; without ledgerd each leave would wait out its timeout."""


def heard(bus, wait=3):
    """The room messages bus receives now, as {mentions, for_me, body} (missing keys left out)."""
    msgs = bus.recv(wait)
    while more := bus.recv(0.5):
        msgs += more
    keys = ("mentions", "for_me", "body")
    return [{k: m[k] for k in keys if k in m} for m in msgs if m["topic"] == f"room/{ROOM}"]


def raw(topic, payload):
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    c.connect(config.HOST, config.PORT)
    c.loop_start()
    c.publish(topic, json.dumps(payload), qos=1).wait_for_publish(5)
    c.disconnect()
    c.loop_stop()


def test_names_addresses_and_all_resolve():
    S.alice, S.bob1, S.bob2, S.carol = (Bus.register(n) for n in ("alice", "bob", "bob", "carol"))
    for b in (S.alice, S.bob1, S.bob2, S.carol):
        b.join(ROOM)
    for b in (S.alice, S.bob1, S.bob2, S.carol):
        heard(b, 0.5)
    bobs = sorted([S.bob1.addr, S.bob2.addr])
    S.alice.say(ROOM, "hey @bob, ready?")
    assert heard(S.bob1) == [{"mentions": bobs, "for_me": True, "body": "hey @bob, ready?"}], "a name: every bob"
    assert heard(S.carol) == [{"mentions": bobs, "for_me": False, "body": "hey @bob, ready?"}]
    S.alice.say(ROOM, f"ping @{S.carol.addr}")
    assert heard(S.carol) == [{"mentions": [S.carol.addr], "for_me": True, "body": f"ping @{S.carol.addr}"}]
    S.alice.say(ROOM, "@all standup now, @bob too")
    S.alice.say(ROOM, "mail me at alice@example.com")
    assert heard(S.carol) == [{"body": "@all standup now, @bob too"}, {"body": "mail me at alice@example.com"}], (
        "@all means no mentions, and a@b is not a mention"
    )
    logged = [e for e in history.history(ROOM, 1000) if e["body"] == "hey @bob, ready?"]
    assert logged[0]["mentions"] == bobs


def test_unknown_mentions_are_refused():
    outsider = Bus.register("outsider")
    for body, name in (("hi @nobody", "@nobody"), (f"hi @{outsider.addr}", f"@{outsider.addr}")):
        with pytest.raises(BridgeError, match=name) as e:
            S.alice.say(ROOM, body)
        assert "\n" not in str(e.value)
    outsider.close()


def test_moderation_still_applies():
    S.admin = Bus.register("admin", addr="admin", keep=True, admin=True)
    S.admin.room_set(ROOM, mode="announce")
    S.admin.mod(ROOM, "add", S.alice.addr)
    with pytest.raises(BridgeError, match="only moderators"):
        S.bob1.say(ROOM, f"@{S.alice.addr} may I?")
    S.alice.say(ROOM, "@bob noted")
    S.admin.room_set(ROOM, mode="open")
    for b in (S.alice, S.bob1, S.bob2, S.carol):
        heard(b, 1)


def test_spoofed_and_malformed_mentions():
    base = {"from": "mallory-000000", "body": "spoof", "ts": 1e9}
    raw(f"room/{ROOM}", {**base, "mentions": [S.bob1.addr]})
    for bad in ("bob", [5], [], ["x/y"], [f"a{i}" for i in range(MENTIONS_MAX + 1)], None):
        raw(f"room/{ROOM}", {**base, "body": f"bad {bad!r}"[:40], "mentions": bad})
    raw(f"inbox/{S.carol.addr}", {**base, "body": "dm", "mentions": [S.carol.addr]})
    assert heard(S.bob1) == [{"mentions": [S.bob1.addr], "for_me": True, "body": "spoof"}], "a spoof is just a mention"
    got = S.carol.recv(3) + S.carol.recv(0.5)
    rooms = [m for m in got if m["topic"] == f"room/{ROOM}"]
    dms = [m for m in got if m["topic"].startswith("inbox/")]
    assert [(m["body"], m["for_me"]) for m in rooms] == [("spoof", False)], "malformed mentions drop the message"
    assert [(m["body"], "mentions" in m, "for_me" in m) for m in dms] == [("dm", False, False)], "rooms only"
    for b in (S.alice, S.bob1, S.bob2, S.carol, S.admin):
        b.leave()
