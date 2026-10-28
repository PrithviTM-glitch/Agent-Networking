"""Message history. historyd (service.py) is its only writer: it records every room message, DM, broadcast and raw
topic message from the bus in SQLite (store.py) and answers requests over MQTT 5 (client.py has the calls).

The legacy history log (one JSON line per message, written by bridge clients before historyd) is only read here:
historyd imports it once on its first start, and the file is never deleted.
"""

import contextlib
import fcntl
import json
import os
import time

from .. import config
from ..validation import TOPIC_MAX, BridgeError, check_ident, check_last, check_mentions, check_text, check_ts

PRUNE_S = 3600


def parse_entry(line):
    """A legacy history log line as a dict, or None if it is malformed."""
    try:
        d = json.loads(line)
        if isinstance(d["topic"], str) and len(d["topic"]) <= TOPIC_MAX:
            entry = {
                "topic": d["topic"],
                "from": check_ident(d["from"]),
                "body": check_text(d["body"]),
                "ts": check_ts(d["ts"]),
            }
            if "mentions" in d:
                entry["mentions"] = check_mentions(d["mentions"])
            return entry
    except (ValueError, RecursionError, BridgeError, KeyError, TypeError):
        pass
    return None


def read_log(path):
    """Every well-formed entry of a legacy history log, oldest first; none if the file is missing."""
    try:
        with open(path, "rb") as f:
            return [e for e in map(parse_entry, f) if e]
    except FileNotFoundError:
        return []


# Until bridge clients read from historyd (phase 2), they keep appending to and reading the legacy log through these.


def _open():
    config.data_dir()
    return open(config.LOG, "a+", encoding="utf-8", errors="replace")


def append(topic, entry):
    with _open() as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.write(json.dumps({"topic": topic, **entry}) + "\n")


def entries():
    with _open() as f:
        fcntl.flock(f, fcntl.LOCK_SH)
        f.seek(0)
        return [e for e in map(parse_entry, f) if e]


def prune(drop_topic=None):
    """Drop entries older than the stale period, malformed lines, and with drop_topic every entry on that topic.
    Without drop_topic it runs at most once per PRUNE_S (or per stale period if shorter): it rewrites the whole log."""
    stamp = config.data_path("prune.stamp")
    with contextlib.suppress(OSError):
        if drop_topic is None and time.time() - os.path.getmtime(stamp) < min(PRUNE_S, config.STALE_S):
            return
    cutoff = time.time() - config.STALE_S
    with _open() as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.seek(0)
        kept = [
            json.dumps(e) + "\n" for e in map(parse_entry, f) if e and e["ts"] >= cutoff and e["topic"] != drop_topic
        ]
        f.seek(0)
        f.truncate()
        f.writelines(kept)  # rewrites the whole log; rotate by day if it ever grows past a few MB
        with open(stamp, "a"):
            os.utime(stamp)


def history(room, last=20):
    """A chat room's messages, oldest first."""
    topic = f"room/{check_ident(room, 'room')}"
    return [e for e in entries() if e["topic"] == topic][-check_last(last) :]


def dms(addr, peer=None, last=20):
    """addr's direct messages in both directions, optionally only those with peer."""
    inbox = f"inbox/{check_ident(addr, 'address')}"
    peer = peer is not None and check_ident(peer, "address")
    mine = [e for e in entries() if e["topic"] == inbox or (e["from"] == addr and e["topic"].startswith("inbox/"))]
    return [e for e in mine if not peer or peer in (e["from"], e["topic"][6:])][-check_last(last) :]
