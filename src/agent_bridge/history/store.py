"""The history store: SQLite, one transaction per message or request, written only by historyd.

Messages come from the bus already validated by protocol.parse_message. Requests are hostile input: every field is
checked, unknown fields are refused, replies are capped. Retention follows receive time, not the sender's ts, so a
forged timestamp cannot keep a row forever; row and byte caps bound what a flood on raw topics can store.
"""

import hashlib
import json
import os
import re
import sqlite3
import sys
import time
import traceback

from .. import config
from ..protocol import BROADCAST, parse_message
from ..validation import (
    ADMIN,
    RESERVED,
    BridgeError,
    check_ident,
    check_last,
    check_topic,
    check_ts,
)
from . import read_log

SCHEMA_VERSION = 1  # meta row "schema"; a historyd refuses a database whose version it does not know
MAX_PAYLOAD = 16384
MAX_CORR = 256
# Under config/mosquitto.conf max_packet_size 270000: a bigger publish would get historyd disconnected.
MAX_REPLY = 240_000
MAX_ROWS, MAX_BYTES = 500_000, 256 * 1024 * 1024
RECENT_MAX = 5000  # the console's backlog; the reply cap still bounds what goes back
PRUNE_S = 3600
RESP = re.compile(r"history/resp/[A-Za-z0-9_-]{1,128}")
STORED = ("room", "inbox", BROADCAST)  # the reserved first levels whose messages are history; others never are
REQUIRED = object()

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages(id INTEGER PRIMARY KEY, topic TEXT NOT NULL, sender TEXT, body TEXT NOT NULL,
  ts REAL NOT NULL, mentions TEXT, received REAL NOT NULL, digest BLOB NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS messages_topic ON messages(topic, id);
CREATE INDEX IF NOT EXISTS messages_sender ON messages(sender, id);
CREATE INDEX IF NOT EXISTS messages_received ON messages(received);
"""


class SchemaError(Exception):
    """The database was written by a historyd with another schema version; opening it could damage it."""


class Bad(Exception):
    """A request historyd refuses; the message goes back to the caller."""


def log(*parts):
    print(time.strftime("%Y-%m-%dT%H:%M:%S"), *parts, file=sys.stderr, flush=True)


def checked(check, *args):
    def run(key, v):
        try:
            return check(v, *args)
        except BridgeError as e:
            raise Bad(f"{key}: {e}") from None

    return run


def count(cap):
    def check(key, v):
        if isinstance(v, bool) or not isinstance(v, int) or not 1 <= v <= cap:
            raise Bad(f"{key}: integer from 1 to {cap}")
        return v

    return check


address = checked(check_ident, "address")
OPS = {
    "room": {"room": (checked(check_ident, "room"), REQUIRED), "last": (checked(check_last), 20),
             "before": (checked(check_ts), None)},
    "dms": {"peer": (address, None), "last": (checked(check_last), 20)},
    "topic": {"topic": (checked(check_topic), REQUIRED), "last": (checked(check_last), 20)},
    "broadcasts": {"since": (checked(check_ts), 0.0), "last": (checked(check_last), 20)},
    "recent": {"last": (count(RECENT_MAX), 100)},
    "drop": {"addr": (address, None)},
    "ping": {},
}  # fmt: skip
# Ops that read or change one agent's data take its address only from the topic history/req/<as>, never from the
# body, so an ACL on that topic is all the auth phase needs.
NEEDS_AS = {"dms", "recent", "drop"}


def parse(req, via=None):
    """op, acting address (None when the op needs none) and checked fields; via, from history/req/<addr>, fixes it."""
    if not isinstance(req, dict):
        raise Bad("request must be a JSON object")
    op = req.get("op")
    if not isinstance(op, str) or op not in OPS:
        raise Bad(f"op: one of {', '.join(OPS)}")
    fields = OPS[op]
    if extra := [k[:32] for k in req if k not in fields and k not in ("op", "as")]:
        raise Bad(f"unknown fields for {op}: {', '.join(extra[:5])}")
    if via is not None:
        address("request topic address", via)
        if req.get("as", via) != via:
            raise Bad(f"as: must match the request topic history/req/{via}")
    if op in NEEDS_AS and via is None:
        raise Bad(f"{op}: send it on history/req/<your address>")
    me = via if via is not None else req.get("as")
    if me is not None:
        address("as", me)
    args = {}
    for key, (check, default) in fields.items():
        if key in req and req[key] is not None:
            args[key] = check(key, req[key])
        elif default is REQUIRED:
            raise Bad(f"{key}: required")
        else:
            args[key] = default
    return op, me, args


def stored(topic):
    """True for the topics historyd records: rooms, inboxes, broadcasts and raw topics, never other reserved ones."""
    return not topic.startswith("$") and (topic.split("/", 1)[0] not in RESERVED or topic.split("/", 1)[0] in STORED)


class History:
    def __init__(self, path, publish):
        """publish(topic, payload, correlation) sends one QoS 1 reply."""
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        # Checked before anything is written: a database another version migrated is left exactly as it is.
        if self.q("SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'").fetchone():
            row = self.q("SELECT value FROM meta WHERE key='schema'").fetchone()
            if row is not None and str(row[0]) != str(SCHEMA_VERSION):
                self.db.close()
                raise SchemaError(f"{path} has history schema {row[0]}; this historyd knows {SCHEMA_VERSION}")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(SCHEMA)
        with self.db:
            self.q("INSERT OR IGNORE INTO meta VALUES ('schema', ?)", str(SCHEMA_VERSION))
        self.publish = publish
        self.pruned = 0.0
        self.since_trim = 0

    def q(self, sql, *args):
        return self.db.execute(sql, args)

    # recording

    def add(self, topic, sender, body, ts, mentions, received):
        """Store one message unless an identical one (same topic, sender, ts and body) already is; True if new."""
        digest = hashlib.sha256(json.dumps([topic, sender, ts, body]).encode()).digest()
        cur = self.q(
            "INSERT OR IGNORE INTO messages(topic, sender, body, ts, mentions, received, digest)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            topic, sender, body, ts, json.dumps(mentions) if mentions else None, received, digest,
        )  # fmt: skip
        return cur.rowcount == 1

    def record(self, topic, payload, now=None):
        """One message from the bus, if it is history and well formed."""
        if not stored(topic) or not (msg := parse_message(topic, payload)):
            return
        now = time.time() if now is None else now
        with self.db:
            if self.add(topic, msg["from"], msg["body"], msg.get("ts", now), msg.get("mentions"), now):
                self.since_trim += 1
        if self.since_trim >= 1000:
            self.trim()

    def migrate(self, path):
        """Import a legacy history log once: the path is remembered, and messages already stored are skipped."""
        key = f"imported:{os.path.abspath(path)}"
        if self.q("SELECT 1 FROM meta WHERE key=?", key).fetchone() or not os.path.exists(path):
            return 0
        entries = read_log(path)
        with self.db:
            n = sum(self.add(e["topic"], e["from"], e["body"], e["ts"], e.get("mentions"), e["ts"]) for e in entries)
            self.q("INSERT INTO meta VALUES (?, ?)", key, json.dumps({"at": time.time(), "entries": n}))
        log(f"imported {n} of {len(entries)} entries from {path}; the file stays where it is")
        return n

    def prune(self, now=None):
        """Drop what is older than the stale period, at most once per PRUNE_S; then hold the row and byte caps."""
        now = time.time() if now is None else now
        if now - self.pruned < min(PRUNE_S, config.STALE_S):
            return
        with self.db:
            self.q("DELETE FROM messages WHERE received < ?", now - config.STALE_S)
        self.pruned = now
        self.trim()

    def trim(self):
        self.since_trim = 0
        with self.db:
            while True:
                rows, size = self.q("SELECT count(*), coalesce(sum(length(body)), 0) FROM messages").fetchone()
                if rows <= MAX_ROWS and size <= MAX_BYTES:
                    return
                self.q("DELETE FROM messages WHERE id IN (SELECT id FROM messages ORDER BY id LIMIT 1000)")

    # requests

    def handle(self, payload, topic, corr, via=None):
        """One request; via is the <addr> of a history/req/<addr> topic, None for plain history/req."""
        if not isinstance(topic, str) or not RESP.fullmatch(topic):
            return log("dropped request without a ResponseTopic under history/resp/")
        if not isinstance(corr, bytes) or not 0 < len(corr) <= MAX_CORR:
            error = f"CorrelationData of 1 to {MAX_CORR} bytes required"
            return self.reply(topic, None, {"ok": False, "error": error})
        try:
            if len(payload) > MAX_PAYLOAD:
                raise Bad(f"payload over {MAX_PAYLOAD} bytes")
            try:
                req = json.loads(payload.decode("utf-8"))
            except (ValueError, RecursionError):
                raise Bad("payload must be UTF-8 JSON") from None
            op, me, args = parse(req, via)
            with self.db:
                body = {"ok": True, **getattr(self, f"op_{op}")(me, **args)}
        except Bad as e:
            body = {"ok": False, "error": str(e)}
        except Exception:
            log("internal error", traceback.format_exc())
            body = {"ok": False, "error": "internal error"}
        self.reply(topic, corr, body)

    def reply(self, topic, corr, body):
        try:
            self.publish(topic, json.dumps(body), corr)
        except Exception as e:
            log("reply failed", topic, repr(e))

    def messages(self, sql, args, last):
        """The last rows of a query, oldest first, as wire entries; the oldest are left out if the reply is too big."""
        rows = self.q(f"SELECT * FROM messages WHERE {sql} ORDER BY id DESC LIMIT ?", *args, last).fetchall()
        out, size = [], 0
        for r in rows:
            entry = {"topic": r["topic"], "from": r["sender"], "body": r["body"], "ts": r["ts"]}
            if r["mentions"]:
                entry["mentions"] = json.loads(r["mentions"])
            size += len(json.dumps(entry)) + 2
            if size > MAX_REPLY - 1000:
                return {"messages": out[::-1], "truncated": True}
            out.append(entry)
        return {"messages": out[::-1]}

    def op_room(self, _me, room, last, before):
        if before is None:
            return self.messages("topic=?", (f"room/{room}",), last)
        return self.messages("topic=? AND ts<?", (f"room/{room}", before), last)

    def op_dms(self, me, peer, last):
        if peer is None:
            sql = "topic=? OR (sender=? AND substr(topic, 1, 6)='inbox/')"
            return self.messages(sql, (f"inbox/{me}", me), last)
        return self.messages("(topic=? AND sender=?) OR (topic=? AND sender=?)",
                             (f"inbox/{me}", peer, f"inbox/{peer}", me), last)  # fmt: skip

    def op_topic(self, _me, topic, last):
        return self.messages("topic=?", (topic,), last)

    def op_broadcasts(self, _me, since, last):
        return self.messages("topic=? AND sender=? AND ts>?", (BROADCAST, ADMIN, since), last)

    def op_recent(self, me, last):
        if me != ADMIN:
            raise Bad("recent is for the admin console only")
        return self.messages("1", (), last)

    def op_drop(self, me, addr):
        if addr is not None and addr != me:
            raise Bad("drop: an agent drops only its own inbox")
        return {"dropped": self.q("DELETE FROM messages WHERE topic=?", f"inbox/{me}").rowcount}

    def op_ping(self, _me):
        return {}
