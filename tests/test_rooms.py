"""Room moderation end to end: settings records, sender refusals, receiver filtering, kicks, and the CLI and MCP
surfaces. The tests are one scenario in file order: later ones use the agents and settings earlier ones made.
"""

import contextlib
import json
import logging
import os
import subprocess
import uuid
from types import SimpleNamespace

import anyio
import paho.mqtt.client as mqtt
import pytest
from conftest import BIN, wait_for
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from agent_bridge import _native, config
from agent_bridge.errors import BridgeError

Bus = _native.Bus

BRIDGE, SERVER = str(BIN / "agent-bridge"), str(BIN / "agent-bridge-mcp")
RUN = uuid.uuid4().hex[:6]
ROOM = f"mod-{RUN}"
S = SimpleNamespace()


@pytest.fixture(scope="module", autouse=True)
def quiet(ledgerd, historyd):
    """The MCP client logs every channel notice as an unknown notification."""
    logging.disable(logging.WARNING)
    yield
    logging.disable(logging.NOTSET)


def refused(fn, *args, match=""):
    with pytest.raises(BridgeError, match=match) as e:
        fn(*args)
    assert "\n" not in str(e.value)
    return str(e.value)


def synced(*buses, room=ROOM):
    """Wait until every bus has the room record its moderator just published."""
    want = _native.rooms_info(room)
    assert wait_for(lambda: all(_native.rooms_view(room, b.cached_room(room)) == want for b in buses), 5), want


def heard(bus, wait=3):
    """(from, body) of the room messages bus receives now."""
    msgs = bus.recv(wait)
    while more := bus.recv(0.5):
        msgs += more
    return [(m["from"], m["body"]) for m in msgs if m["topic"] == f"room/{ROOM}"]


def raw_say(sender, body):
    """A room message published by a raw client claiming to be sender."""
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    c.connect(config.HOST, config.PORT)
    c.loop_start()
    c.publish(f"room/{ROOM}", json.dumps({"from": sender, "body": body, "ts": 1e9}), qos=1).wait_for_publish(5)
    c.disconnect()
    c.loop_stop()


def cli(*args, ok=True):
    p = subprocess.run([BRIDGE, *args], capture_output=True, text=True, errors="replace")
    err = p.stderr.strip()
    assert (p.returncode == 0) == ok and (ok or (err and "\n" not in err and "Traceback" not in err)), (args, err)
    return p.stdout.strip() if ok else err


def test_a_room_without_settings_is_open():
    S.alice, S.bob, S.carol, S.watch = (Bus.register(n) for n in ("alice", "bob", "carol", "watch"))
    for b in (S.alice, S.bob, S.carol, S.watch):
        b.join(ROOM)
    heard(S.watch)
    assert _native.rooms_info(ROOM) == {"room": ROOM, **_native.ROOM_OPEN}
    S.bob.say(ROOM, "anyone may post")
    assert heard(S.watch) == [(S.bob.addr, "anyone may post")]
    refused(S.alice.room_set, ROOM, "announce", match="admin names the first")
    refused(S.alice.mute, ROOM, S.bob.addr, match="only admin or a moderator")


def test_admin_names_the_first_moderator():
    S.admin = Bus.register("admin", addr="admin", keep=True, admin=True)
    settings = S.admin.mod(ROOM, "add", S.alice.addr)
    assert settings["moderators"] == [S.alice.addr] and settings["by"] == "admin" and settings["mode"] == "open"
    assert heard(S.watch) == [("admin", f"[moderator #{ROOM}] +{S.alice.addr} by admin")]
    listed = {r["room"]: r for r in _native.rooms_listing()}[ROOM]
    assert listed["mode"] == "open" and listed["moderators"] == [S.alice.addr] and listed["members"] == 4


def test_announce_rooms_take_posts_from_moderators_only():
    S.alice.room_set(ROOM, mode="announce")
    synced(S.bob, S.watch, S.admin)
    refused(S.bob.say, ROOM, "can I?", match="only moderators post in an announce room")
    S.alice.say(ROOM, "moderator speaking")
    S.admin.say(ROOM, "admin overrides")
    raw_say(S.bob.addr, "spoofed non-moderator")
    assert heard(S.watch) == [
        (S.alice.addr, f"[mode #{ROOM}] announce by {S.alice.addr}"),
        (S.alice.addr, "moderator speaking"),
        ("admin", "admin overrides"),
    ]
    S.alice.room_set(ROOM, mode="open")
    synced(S.bob, S.watch)
    assert heard(S.watch) == [(S.alice.addr, f"[mode #{ROOM}] open by {S.alice.addr}")]


def test_mute_stops_posts_and_spoofed_ones():
    S.alice.mute(ROOM, S.bob.addr)
    synced(S.bob, S.watch)
    refused(S.bob.say, ROOM, "let me talk", match="muted")
    raw_say(S.bob.addr, "spoofed from a muted address")
    raw_say(S.alice.addr, "spoofed moderator: advisory moderation lets this through")
    S.carol.say(ROOM, "carol is fine")
    assert heard(S.watch) == [
        (S.alice.addr, f"[muted #{ROOM}] {S.bob.addr} by {S.alice.addr}"),
        (S.alice.addr, "spoofed moderator: advisory moderation lets this through"),
        (S.carol.addr, "carol is fine"),
    ]
    S.alice.unmute(ROOM, S.bob.addr)
    synced(S.bob)
    S.bob.say(ROOM, "unmuted")
    assert heard(S.watch) == [
        (S.alice.addr, f"[unmuted #{ROOM}] {S.bob.addr} by {S.alice.addr}"),
        (S.bob.addr, "unmuted"),
    ]


def test_rules_and_bad_settings():
    assert S.alice.room_set(ROOM, rules="Be kind.\nNo spam.")["rules"] == "Be kind.\nNo spam."
    assert _native.rooms_info(ROOM)["rules"] == "Be kind.\nNo spam."
    assert heard(S.watch) == [(S.alice.addr, f"[rules #{ROOM}] updated by {S.alice.addr}")]
    refused(S.alice.room_set, ROOM, None, "x" * (_native.RULES_MAX + 1), match="rules is")
    refused(S.alice.room_set, ROOM, None, "bell\x07", match="control character")
    refused(S.alice.room_set, ROOM, "loud", match="mode must be")
    refused(S.alice.room_set, ROOM, match="nothing to change")
    refused(S.alice.mod, ROOM, "promote", S.bob.addr, match="add or remove")
    refused(S.alice.mute, ROOM, "x/y", match="bad address")
    refused(S.alice.mute, "bad room", S.bob.addr, match="bad room")
    refused(S.alice.mute, ROOM, "admin", match="cannot be moderated")
    refused(S.carol.pub, f"rooms/{ROOM}", "{}", match="reserved")
    refused(S.carol.sub, "rooms/+", match="reserved")


def test_non_moderators_are_refused():
    for fn, args in (
        (S.carol.mute, (ROOM, S.bob.addr)),
        (S.carol.ban, (ROOM, S.bob.addr)),
        (S.carol.kick, (ROOM, S.bob.addr)),
        (S.carol.unban, (ROOM, S.bob.addr)),
        (S.carol.mod, (ROOM, "add", S.carol.addr)),
        (S.carol.room_set, (ROOM, "announce")),
    ):
        refused(fn, *args, match="only admin or a moderator")
    assert _native.rooms_info(ROOM)["moderators"] == [S.alice.addr]


def test_kick_bans_and_drops_the_room_and_the_target_keeps_working():
    # bob is a persistent Bus, as a relay holds: the kick takes its session over and it reconnects.
    S.alice.kick(ROOM, S.bob.addr)
    assert wait_for(lambda: f"room/{ROOM}" not in _native.presence_find(S.bob.addr)["topics"], 5)
    synced(S.bob, S.watch)
    assert heard(S.watch) == [(S.alice.addr, f"[kicked #{ROOM}] {S.bob.addr} by {S.alice.addr}")]
    refused(S.bob.join, ROOM, match="banned")
    refused(S.bob.say, ROOM, "back door", match="banned")
    raw_say(S.bob.addr, "spoofed from a banned address")
    S.carol.dm(S.bob.addr, "still reachable")
    S.alice.say(ROOM, "bob should not hear this")
    got = S.bob.recv(10) + S.bob.recv(1)
    assert [m["body"] for m in got if m["topic"] == f"inbox/{S.bob.addr}"] == ["still reachable"]
    assert not [m for m in got if m["topic"] == f"room/{ROOM}" and m["body"] == "bob should not hear this"]
    assert heard(S.watch) == [(S.alice.addr, "bob should not hear this")]
    S.alice.unban(ROOM, S.bob.addr)
    synced(S.bob)
    S.bob.join(ROOM)
    S.bob.say(ROOM, "back")
    assert heard(S.watch)[-1] == (S.bob.addr, "back")


def test_ban_without_kick_keeps_the_subscription_but_not_the_voice():
    S.alice.ban(ROOM, S.carol.addr)
    synced(S.carol, S.watch)
    refused(S.carol.say, ROOM, "hello?", match="banned")
    assert f"room/{ROOM}" in _native.presence_find(S.carol.addr)["topics"]
    S.carol.part(ROOM)
    refused(S.carol.join, ROOM, match="banned")


def test_admin_overrides_moderators():
    S.admin.mod(ROOM, "remove", S.alice.addr)
    synced(S.alice)
    refused(S.alice.unban, ROOM, S.carol.addr, match="only admin or a moderator")
    S.admin.unban(ROOM, S.carol.addr)
    synced(S.carol)
    S.carol.join(ROOM)
    assert _native.rooms_info(ROOM)["moderators"] == [] and _native.rooms_info(ROOM)["banned"] == []


def test_announcements_are_in_the_transcript():
    verbs = ("[moderator", "[muted", "[unmuted", "[banned", "[unbanned", "[kicked", "[rules", "[mode")
    transcript = _native.history_call("room", room=ROOM, last=1000)["messages"]
    notes = [e["body"] for e in transcript if e["body"].startswith(verbs)]
    by_a, by_admin = f"by {S.alice.addr}", "by admin"
    assert notes == [
        f"[moderator #{ROOM}] +{S.alice.addr} {by_admin}",
        f"[mode #{ROOM}] announce {by_a}",
        f"[mode #{ROOM}] open {by_a}",
        f"[muted #{ROOM}] {S.bob.addr} {by_a}",
        f"[unmuted #{ROOM}] {S.bob.addr} {by_a}",
        f"[rules #{ROOM}] updated {by_a}",
        f"[kicked #{ROOM}] {S.bob.addr} {by_a}",
        f"[unbanned #{ROOM}] {S.bob.addr} {by_a}",
        f"[banned #{ROOM}] {S.carol.addr} {by_a}",
        f"[moderator #{ROOM}] -{S.alice.addr} {by_admin}",
        f"[unbanned #{ROOM}] {S.carol.addr} {by_admin}",
    ]


def test_malformed_records_count_as_open():
    junk_room = f"junk-{RUN}"
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    c.connect(config.HOST, config.PORT)
    c.loop_start()
    bad = {"mode": "announce", "moderators": ["x/y"], "rules": "", "muted": [], "banned": [], "by": "a", "ts": 1}
    c.publish(f"rooms/{junk_room}", json.dumps(bad), qos=1, retain=True).wait_for_publish(5)
    assert _native.rooms_info(junk_room) == {"room": junk_room, **_native.ROOM_OPEN}
    S.watch.join(junk_room)
    S.watch.say(junk_room, "still open")
    c.publish(f"rooms/{junk_room}", b"", qos=1, retain=True).wait_for_publish(5)
    c.disconnect()
    c.loop_stop()


def test_cli():
    S.admin.mod(ROOM, "add", S.alice.addr)
    synced(S.alice)
    assert json.loads(cli("room", "info", ROOM))["moderators"] == [S.alice.addr]
    out = json.loads(cli("room", "set", S.alice.addr, ROOM, "--mode", "announce", "--rules", "CLI rules"))
    assert out["mode"] == "announce" and out["rules"] == "CLI rules" and out["by"] == S.alice.addr
    assert json.loads(cli("mute", S.alice.addr, ROOM, S.bob.addr))["muted"] == [S.bob.addr]
    assert json.loads(cli("mod", S.alice.addr, ROOM, "add", S.carol.addr))["moderators"] == sorted(
        [S.alice.addr, S.carol.addr]
    )
    assert "muted" in cli("say", S.bob.addr, ROOM, "x", ok=False)
    assert "only admin or a moderator" in cli("unmute", S.bob.addr, ROOM, S.bob.addr, ok=False)
    for args in (
        ["room", "set", S.alice.addr, ROOM, "--mode", "loud"],
        ["room", "set", S.alice.addr, ROOM, "--rules", "a\x01b"],
        ["room", "set", S.alice.addr, ROOM],
        ["room", "info", "bad/room"],
        ["mod", S.alice.addr, ROOM, "promote", S.bob.addr],
        ["kick", S.alice.addr, ROOM, "admin"],
        ["ban", "admin", ROOM, S.bob.addr],
        ["pub", S.alice.addr, f"rooms/{ROOM}", "{}"],
    ):
        cli(*args, ok=False)
    listed = {json.loads(line)["room"]: json.loads(line) for line in cli("rooms").splitlines()}[ROOM]
    assert listed["mode"] == "announce" and listed["moderators"] == sorted([S.alice.addr, S.carol.addr])
    assert json.loads(cli("kick", S.alice.addr, ROOM, S.bob.addr))["banned"] == [S.bob.addr]
    assert wait_for(lambda: f"room/{ROOM}" not in _native.presence_find(S.bob.addr)["topics"], 5)


def test_mcp():
    anyio.run(_mcp)


async def _mcp():
    params = StdioServerParameters(command=SERVER, env={**os.environ, "CLAUDE_PROJECT_DIR": "/work/moderation"})
    with open(os.devnull, "w") as quiet:
        async with stdio_client(params, errlog=quiet) as (r, w), ClientSession(r, w) as s:
            await s.initialize()

            async def call(tool, ok=True, **args):
                res = await s.call_tool(tool, args)
                text = res.content[0].text
                assert res.isError != ok, (tool, args, text)
                if not ok:
                    assert "\n" not in text.strip() and "Traceback" not in text, text
                    return text
                return json.loads(text.split("\n", 1)[1] if text.startswith("UNTRUSTED") else text)

            me = (await call("connect", name=f"mcp{RUN}"))["addr"]
            await call("join", room=ROOM)
            assert (await call("room_info", room=ROOM))["mode"] == "announce"
            assert "only moderators" in await call("say", ok=False, room=ROOM, text="x")
            assert "only admin or a moderator" in await call("mute", ok=False, room=ROOM, addr=S.alice.addr)
            await anyio.to_thread.run_sync(S.admin.mod, ROOM, "add", me)
            await anyio.sleep(0.5)  # the record reaches the server's Bus over its rooms/+ subscription
            assert (await call("mute", room=ROOM, addr=S.carol.addr))["muted"] == sorted([S.bob.addr, S.carol.addr])
            assert (await call("room_set", room=ROOM, mode="open"))["mode"] == "open"
            assert (await call("unmute", room=ROOM, addr=S.carol.addr))["muted"] == [S.bob.addr]
            assert (await call("ban", room=ROOM, addr=S.carol.addr))["banned"] == sorted([S.bob.addr, S.carol.addr])
            assert (await call("unban", room=ROOM, addr=S.carol.addr))["banned"] == [S.bob.addr]
            assert (await call("mod", room=ROOM, action="remove", addr=S.carol.addr))["moderators"] == sorted(
                [S.alice.addr, me]
            )
            assert "bad arguments" in await call("mod", ok=False, room=ROOM, action="promote", addr=S.bob.addr)
            await call("say", room=ROOM, text="moderated through MCP")
            room = {r["room"]: r for r in await call("rooms")}[ROOM]
            assert room["mode"] == "open" and me in room["moderators"]
            await call("leave")
    with contextlib.suppress(BridgeError):
        for b in (S.alice, S.bob, S.carol, S.watch):
            b.leave()
    S.admin.leave()
