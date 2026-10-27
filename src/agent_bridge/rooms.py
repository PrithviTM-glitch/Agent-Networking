"""Room settings: a retained record at rooms/<room> holding mode, moderators, rules, muted and banned.

A room without a record is open and unmoderated. admin moderates every room and alone names a room's first
moderators; after that admin or a current moderator may change the record. Mode "open" lets anyone post, "announce"
only moderators. Muted agents may not post, banned ones may neither post nor join.
Enforcement is advisory, like every identity on the bus: senders refuse what the record forbids and every Bus drops
room messages it forbids, but anyone can claim to be a moderator, or overwrite the record with a raw client.
"""

import re

from paho.mqtt.subscribeoptions import SubscribeOptions

from . import broker, presence
from .protocol import PAYLOAD_MAX, parse_json
from .validation import ADMIN, BridgeError, check_ident, check_mentions, check_text, check_ts, is_ident

MODES = ("open", "announce")
MODS_MAX, LIST_MAX, RULES_MAX = 32, 256, 2048
OPEN = {"mode": "open", "moderators": [], "rules": "", "muted": [], "banned": []}
TOPICS = "rooms/+"
# QoS 0: the retained records arrive outside the inflight window, so they come straight after the SUBACK.
SUB = SubscribeOptions(qos=0)
# @token not glued to a word before it, so mail addresses (a@b.c) are not mentions.
MENTION = re.compile(r"(?<![A-Za-z0-9_@-])@([A-Za-z0-9_-]{1,64})")


def check_mode(value):
    if not isinstance(value, str) or value not in MODES:
        raise BridgeError(f"mode must be {' or '.join(MODES)}")
    return value


def check_rules(value):
    return check_text(value, "rules", RULES_MAX)


def check_addrs(values, cap, what):
    if not isinstance(values, list) or len(values) > cap:
        raise BridgeError(f"{what} must be a list of at most {cap} addresses")
    return sorted({check_ident(v, "address") for v in values})


def check_record(d):
    """A room record with every field checked; raises BridgeError or KeyError/TypeError on a bad shape."""
    return {
        "mode": check_mode(d["mode"]),
        "moderators": check_addrs(d["moderators"], MODS_MAX, "moderators"),
        "rules": check_rules(d["rules"]),
        "muted": check_addrs(d["muted"], LIST_MAX, "muted"),
        "banned": check_addrs(d["banned"], LIST_MAX, "banned"),
        "by": check_ident(d["by"], "address"),
        "ts": check_ts(d["ts"]),
    }


def parse_room(payload):
    """A retained room record, or None if it is malformed: the room then counts as open."""
    try:
        return check_record(parse_json(payload, PAYLOAD_MAX))
    except (BridgeError, KeyError, TypeError):
        return None


def is_moderator(rec, addr):
    return addr == ADMIN or bool(rec) and addr in rec["moderators"]


def refusal(rec, addr):
    """Why addr may not post in a room with record rec, or None if it may."""
    if not rec or addr == ADMIN:
        return None
    if addr in rec["banned"]:
        return "banned"
    if addr in rec["muted"]:
        return "muted"
    if rec["mode"] == "announce" and addr not in rec["moderators"]:
        return "only moderators post in an announce room"
    return None


def mentions(room, body, members):
    """The addresses body mentions among members (agents() rows): @address, or @name for every member with that
    name. None for no mention or @all, which mentions nobody in particular. Raises BridgeError for anything else."""
    tokens = list(dict.fromkeys(MENTION.findall(body)))
    if not tokens or any(t.lower() == "all" for t in tokens):
        return None
    found = set()
    for token in tokens:
        named = {m["addr"] for m in members if m["name"] == token}
        if token in {m["addr"] for m in members}:
            found.add(token)
        elif named:
            found |= named
        else:
            raise BridgeError(f"@{token} is neither a member of #{room} nor the name of one")
    return check_mentions(sorted(found))


def view(room, rec):
    """What info shows for a room: its settings, open defaults when it has no record."""
    return {"room": room, **OPEN, **(rec or {})}


def records(retained):
    """Room -> record of every well-formed record among retained topic -> payload."""
    return {
        t[6:]: rec
        for t, payload in retained.items()
        if t.startswith("rooms/") and is_ident(t[6:]) and (rec := parse_room(payload))
    }


def info(room):
    room = check_ident(room, "room")
    return view(room, records(broker.retained([f"rooms/{room}"], first=True)).get(room))


def listing():
    """Every room with members or settings: members, mode and moderators."""
    retained = broker.retained(["agents/+", TOPICS])
    members = {}
    for addr, rec in presence.parse_all(retained).items():
        for topic in rec["topics"]:
            if topic.startswith("room/") and is_ident(topic[5:]):
                members.setdefault(topic[5:], []).append(addr)
    settings = records(retained)
    return [
        {"room": name, "members": len(who := sorted(members.get(name, []))), "who": who,
         "mode": settings.get(name, OPEN)["mode"], "moderators": settings.get(name, OPEN)["moderators"]}
        for name in sorted(members.keys() | settings.keys())
    ]  # fmt: skip
