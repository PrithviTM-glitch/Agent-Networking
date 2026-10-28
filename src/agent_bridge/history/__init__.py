"""Message history. historyd (service.py) is its only writer: it records every room message, DM, broadcast and raw
topic message from the bus in SQLite (store.py) and answers requests over MQTT 5 (client.py has the calls).

The legacy history log (one JSON line per message, written by bridge clients before historyd) is only read here:
historyd imports it once on its first start, and the file is never deleted.
"""

import json

from ..validation import TOPIC_MAX, BridgeError, check_ident, check_mentions, check_text, check_ts


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
