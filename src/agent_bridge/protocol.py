"""The wire format: reserved topics, presence records and message payloads, parsed as hostile input.

Incoming messages that are over 256 KiB, not UTF-8, or break the input rules are dropped; plain text on a raw topic
arrives with "from": None. "from" is checked for shape only: any local process can claim any address.
"""

import json

from .validation import (
    TOPICS_MAX,
    UUID,
    BridgeError,
    check_caps,
    check_ident,
    check_mentions,
    check_text,
    check_topic,
    check_ts,
    is_count,
)

# A 64 KiB body can grow to ~192 KiB as JSON; config/mosquitto.conf max_packet_size must stay above this.
PAYLOAD_MAX = 256 * 1024
BROADCAST = "broadcast"
NEVER = 0xFFFFFFFF  # MQTT 5 session expiry value meaning the broker keeps the session forever
ORIGIN = ("session", "pid", "pid_start")
LINEAGE = ("kind", "parent")  # a subagent's agent: kind "subagent" and the creating session, never its own session
PID_MAX, TICKS_MAX = 4194304, 2**53


def parse_json(payload, cap):
    if len(payload) > cap:
        return None
    try:
        return json.loads(payload.decode())
    except (ValueError, RecursionError):
        return None


def parse_presence(payload):
    """A retained presence record, or None if it is malformed."""
    d = parse_json(payload, PAYLOAD_MAX)
    try:
        if not isinstance(d["topics"], list) or len(d["topics"]) > TOPICS_MAX or not isinstance(d["keep"], bool):
            return None
        rec = {
            "name": check_ident(d["name"]),
            "caps": check_caps(d["caps"]),
            "topics": [check_topic(t, filter=True) for t in d["topics"]],
            "keep": d["keep"],
            "seen": check_ts(d["seen"]),
        }
    except (BridgeError, KeyError, TypeError):
        return None
    if d.get("kind") == "subagent":
        rec["kind"] = "subagent"
        if isinstance(parent := d.get("parent"), str) and UUID.fullmatch(parent):
            rec["parent"] = parent
    elif isinstance(session := d.get("session"), str) and UUID.fullmatch(session):
        rec["session"] = session
    if is_count(d.get("pid"), PID_MAX) and is_count(d.get("pid_start"), TICKS_MAX):
        rec.update(pid=d["pid"], pid_start=d["pid_start"])
    return rec


def parse_message(topic, payload):
    """A received payload as a message dict, or None if it breaks the input rules."""
    d = parse_json(payload, PAYLOAD_MAX)
    try:
        if isinstance(d, dict) and "from" in d:
            msg = {
                "topic": topic,
                "from": check_ident(d["from"], "address"),
                "body": check_text(d["body"]),
                "ts": check_ts(d["ts"]),
            }
            if topic.startswith("room/") and "mentions" in d:
                msg["mentions"] = check_mentions(d["mentions"])
            return msg
        if topic.startswith(("room/", "inbox/")) or topic == BROADCAST or len(payload) > PAYLOAD_MAX:
            return None
        return {"topic": topic, "from": None, "body": check_text(payload.decode())}
    except (BridgeError, KeyError, ValueError):
        return None
