"""The ledger's state: SQLite schema, request validation and every op, one transaction per request.

Ledger is the only writer of ledgers, grants and leases. The service feeds it requests, lease expiry and presence
updates one at a time, in order; that serialization is the whole concurrency guarantee.
"""

import json
import math
import os
import re
import secrets
import sqlite3
import sys
import time
import traceback

from .. import config

STALE_S = max(1.0, config.STALE_DAYS * 86400)
MAX_PAYLOAD = 16384
MAX_CORR = 256
# Under config/mosquitto.conf max_packet_size 270000: a bigger publish gets ledgerd disconnected, then resent forever.
MAX_REPLY = 240_000
MAX_TTL, MAX_WAIT = 3600, 300
MAX_LEDGERS, MAX_RESOURCES, MAX_PENDING, MAX_LEASES, MAX_WAITERS = 1000, 128, 64, 256, 256
LEVELS = {"read": 1, "write": 2}
ADDR = r"[A-Za-z0-9_-]{1,64}"
NAME = r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,63}"
RESP = re.compile(r"ledger/resp/[A-Za-z0-9_-]{1,128}")
BAD_TEXT = re.compile(r"[\x00-\x08\x0b-\x1f\x7f\ud800-\udfff]")
REQUIRED = object()

SCHEMA = """
CREATE TABLE IF NOT EXISTS ledgers(owner TEXT PRIMARY KEY, created REAL NOT NULL, seen REAL NOT NULL);
CREATE TABLE IF NOT EXISTS presence(addr TEXT PRIMARY KEY, seen REAL NOT NULL, keep INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS resources(id TEXT PRIMARY KEY, owner TEXT NOT NULL, name TEXT NOT NULL,
  description TEXT NOT NULL, visibility TEXT NOT NULL, max_level TEXT NOT NULL, created REAL NOT NULL,
  updated REAL NOT NULL);
CREATE TABLE IF NOT EXISTS requests(id TEXT PRIMARY KEY, resource TEXT NOT NULL, owner TEXT NOT NULL,
  holder TEXT NOT NULL, level TEXT NOT NULL, status TEXT NOT NULL, ts REAL NOT NULL);
CREATE TABLE IF NOT EXISTS leases(id TEXT PRIMARY KEY, resource TEXT NOT NULL, holder TEXT NOT NULL,
  level TEXT NOT NULL, token INTEGER, expires REAL NOT NULL);
CREATE TABLE IF NOT EXISTS waiters(seq INTEGER PRIMARY KEY AUTOINCREMENT, resource TEXT NOT NULL,
  holder TEXT NOT NULL, level TEXT NOT NULL, ttl REAL NOT NULL, deadline REAL NOT NULL, topic TEXT NOT NULL,
  corr BLOB);
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value INTEGER NOT NULL);
INSERT OR IGNORE INTO meta VALUES ('fence', 0);
CREATE INDEX IF NOT EXISTS resources_owner ON resources(owner);
CREATE INDEX IF NOT EXISTS requests_resource ON requests(resource, holder);
CREATE INDEX IF NOT EXISTS requests_holder ON requests(holder);
CREATE INDEX IF NOT EXISTS requests_owner ON requests(owner);
CREATE INDEX IF NOT EXISTS leases_resource ON leases(resource);
CREATE INDEX IF NOT EXISTS leases_holder ON leases(holder);
CREATE INDEX IF NOT EXISTS waiters_resource ON waiters(resource, seq);
"""


class Bad(Exception):
    """A request ledgerd refuses; the message goes back to the caller."""


def log(*parts):
    print(time.strftime("%Y-%m-%dT%H:%M:%S"), *parts, file=sys.stderr, flush=True)


def text(pattern):
    rx = re.compile(pattern)

    def check(key, v):
        if not isinstance(v, str) or not rx.fullmatch(v):
            raise Bad(f"{key}: must match {pattern}")
        return v

    return check


def description(key, v):
    if not isinstance(v, str) or len(v) > 1024 or BAD_TEXT.search(v):
        raise Bad(f"{key}: string up to 1024 chars, no control characters")
    return v


def enum(*values):
    def check(key, v):
        if v not in values or not isinstance(v, str):
            raise Bad(f"{key}: one of {', '.join(values)}")
        return v

    return check


def number(lo, hi):
    def check(key, v):
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not lo <= v <= hi:
            raise Bad(f"{key}: number between {lo} and {hi}")
        return float(v)

    return check


addr, name, rid, rowid = text(ADDR), text(NAME), text(f"{ADDR}/{NAME}"), text(r"[0-9a-f]{16}")
visibility, level = enum("public", "private"), enum("read", "write")


class Ledger:
    def __init__(self, path, publish):
        """publish(topic, payload, correlation or None) sends one QoS 1 message; replies leave after commit."""
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript(SCHEMA)
        # databases created before dump existed lack waiters.queued
        if "queued" not in {r["name"] for r in self.q("PRAGMA table_info(waiters)")}:
            self.db.execute("ALTER TABLE waiters ADD COLUMN queued REAL")
        self.publish = publish
        self.out = []
        self.swept = 0.0

    def q(self, sql, *args):
        return self.db.execute(sql, args)

    def one(self, sql, *args):
        return self.db.execute(sql, args).fetchone()

    def reply(self, topic, corr, body):
        payload = json.dumps(body)
        if len(payload) > MAX_REPLY:
            payload = json.dumps({"ok": False, "error": f"reply over {MAX_REPLY} bytes"})
        self.out.append((topic, payload, corr))

    def notify(self, to, body):
        self.out.append((f"inbox/{to}", json.dumps({"from": "ledgerd", "body": body, "ts": time.time()}), None))

    def flush(self):
        for topic, payload, corr in self.out:
            try:
                self.publish(topic, payload, corr)
            except Exception as e:
                log("publish failed", topic, repr(e))
        self.out = []

    def run(self, fn, *args):
        """One transaction: replies and notifications leave only after it commits."""
        try:
            with self.db:
                result = fn(*args)
        except Exception:
            self.out = []
            raise
        self.flush()
        return result

    # requests

    def handle(self, payload, topic, corr):
        if not isinstance(topic, str) or not RESP.fullmatch(topic):
            return log("dropped request without a ResponseTopic under ledger/resp/")
        if not isinstance(corr, bytes) or len(corr) > MAX_CORR:
            return self.reply_now(
                topic, None, {"ok": False, "error": f"CorrelationData of 1 to {MAX_CORR} bytes required"}
            )
        try:
            if len(payload) > MAX_PAYLOAD:
                raise Bad(f"payload over {MAX_PAYLOAD} bytes")
            try:
                req = json.loads(payload.decode("utf-8"))
            except (ValueError, RecursionError):
                raise Bad("payload must be UTF-8 JSON") from None
            op, me, args = parse(req)
            body = self.run(self.dispatch, op, me, args, topic, corr)
        except Bad as e:
            body = {"ok": False, "error": str(e)}
        except Exception:
            log("internal error", traceback.format_exc())
            body = {"ok": False, "error": "internal error"}
        if body is not None:
            self.reply_now(topic, corr, body)

    def reply_now(self, topic, corr, body):
        self.reply(topic, corr, body)
        self.flush()

    def dispatch(self, op, me, args, topic, corr):
        now = time.time()
        if op == "init":
            return {"ok": True, **self.op_init(me, now)}
        if op == "dump":
            return {"ok": True, **self.op_dump(now)}
        if not self.q("UPDATE ledgers SET seen=? WHERE owner=?", now, me).rowcount:
            raise Bad(f"no ledger for {me}, call init first")
        body = getattr(self, f"op_{op}")(me, now, topic=topic, corr=corr, **args)
        return body if body is None or "ok" in body else {"ok": True, **body}

    def op_init(self, me, now):
        row = self.one("SELECT * FROM ledgers WHERE owner=?", me)
        if row:
            self.q("UPDATE ledgers SET seen=? WHERE owner=?", now, me)
            return {"created": False, "owner": me, "since": row["created"]}
        if self.one("SELECT count(*) FROM ledgers")[0] >= MAX_LEDGERS:
            raise Bad("ledger limit reached")
        self.q("INSERT INTO ledgers VALUES (?, ?, ?)", me, now, now)
        return {"created": True, "owner": me, "since": now}

    def resource(self, rid, me=None):
        row = self.one("SELECT * FROM resources WHERE id=?", rid)
        if not row or (me and row["owner"] != me):
            raise Bad(f"no such resource: {rid}")
        return row

    def op_add(self, me, now, name, description, visibility, max_level, **_):
        rid = f"{me}/{name}"
        if self.one("SELECT 1 FROM resources WHERE id=?", rid):
            raise Bad(f"{rid} exists, use update")
        if self.one("SELECT count(*) FROM resources WHERE owner=?", me)[0] >= MAX_RESOURCES:
            raise Bad(f"at most {MAX_RESOURCES} resources per ledger")
        self.q(
            "INSERT INTO resources VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rid,
            me,
            name,
            description,
            visibility,
            max_level,
            now,
            now,
        )
        return {"resource": dict(self.resource(rid))}

    def op_update(self, me, now, name, description, visibility, max_level, **_):
        rid = f"{me}/{name}"
        self.resource(rid, me)
        changes = {
            k: v
            for k, v in (("description", description), ("visibility", visibility), ("max_level", max_level))
            if v is not None
        }
        if not changes:
            raise Bad("nothing to update: give description, visibility or max_level")
        for k, v in changes.items():
            self.q(f"UPDATE resources SET {k}=?, updated=? WHERE id=?", v, now, rid)
        return {"resource": dict(self.resource(rid))}

    def op_remove(self, me, now, name, **_):
        rid = f"{me}/{name}"
        self.resource(rid, me)
        self.forget([rid], f"{rid} was removed")
        return {}

    def op_ledger(self, me, now, owner, **_):
        owner = owner or me
        if owner == me:
            rows = self.q("SELECT * FROM resources WHERE owner=? ORDER BY name", me)
        elif self.one("SELECT 1 FROM ledgers WHERE owner=?", owner):
            rows = self.q("SELECT * FROM resources WHERE owner=? AND visibility='public' ORDER BY name", owner)
        else:
            raise Bad(f"no ledger for {owner}")
        return {"owner": owner, "resources": [dict(r) for r in rows]}

    def op_public(self, me, now, **_):
        rows = self.q("SELECT * FROM resources WHERE visibility='public' ORDER BY owner, name")
        return {"resources": [dict(r) for r in rows]}

    def op_request(self, me, now, resource, level, **_):
        res = self.resource(resource)
        if res["owner"] == me:
            raise Bad("you own this resource, owners always hold write")
        for row in self.q(
            "SELECT * FROM requests WHERE resource=? AND holder=? AND status IN ('granted', 'pending')", resource, me
        ):
            if (
                row["status"] == "granted"
                and LEVELS[row["level"]] >= LEVELS[level]
                or row["status"] == "pending"
                and row["level"] == level
            ):
                return {"request": dict(row)}
        auto = res["visibility"] == "public" and LEVELS[level] <= LEVELS[res["max_level"]]
        if (
            not auto
            and self.one("SELECT count(*) FROM requests WHERE holder=? AND status='pending'", me)[0] >= MAX_PENDING
        ):
            raise Bad(f"at most {MAX_PENDING} pending requests per agent")
        rq = secrets.token_hex(8)
        self.q(
            "INSERT INTO requests VALUES (?, ?, ?, ?, ?, ?, ?)",
            rq,
            resource,
            res["owner"],
            me,
            level,
            "granted" if auto else "pending",
            now,
        )
        if not auto:
            self.notify(
                res["owner"],
                f"{me} requests {level} access to {resource} (request_id {rq}); "
                f"answer with ledger grant or deny request_id={rq}",
            )
        return {"request": dict(self.one("SELECT * FROM requests WHERE id=?", rq))}

    def decide(self, me, rq, status):
        row = self.one("SELECT * FROM requests WHERE id=? AND owner=?", rq, me)
        if not row:
            raise Bad(f"no such request: {rq}")
        if row["status"] != status:
            self.q("UPDATE requests SET status=?, ts=? WHERE id=?", status, time.time(), rq)
            self.notify(
                row["holder"], f"{me} {status} your {row['level']} request for {row['resource']} (request_id {rq})"
            )
        return row

    def op_grant(self, me, now, request_id, **_):
        self.decide(me, request_id, "granted")
        return {"request": dict(self.one("SELECT * FROM requests WHERE id=?", request_id))}

    def op_deny(self, me, now, request_id, **_):
        row = self.decide(me, request_id, "denied")
        left = self.held(row["holder"], row["resource"])
        for lease in self.q(
            "SELECT * FROM leases WHERE resource=? AND holder=?", row["resource"], row["holder"]
        ).fetchall():
            if LEVELS[lease["level"]] > left:
                self.q("DELETE FROM leases WHERE id=?", lease["id"])
        self.pump(row["resource"])
        return {"request": dict(self.one("SELECT * FROM requests WHERE id=?", request_id))}

    def op_grants(self, me, now, **_):
        held = self.q("SELECT * FROM requests WHERE holder=? ORDER BY ts", me)
        asked = self.q("SELECT * FROM requests WHERE owner=? ORDER BY ts", me)
        return {"held": [dict(r) for r in held], "asked": [dict(r) for r in asked]}

    # leases

    def held(self, who, resource):
        """Highest level who may lock resource at: 2 for the owner, else the best granted request, 0 for none."""
        if self.one("SELECT 1 FROM resources WHERE id=? AND owner=?", resource, who):
            return LEVELS["write"]
        row = self.one(
            "SELECT max(CASE level WHEN 'write' THEN 2 ELSE 1 END) FROM requests "
            "WHERE resource=? AND holder=? AND status='granted'",
            resource,
            who,
        )
        return row[0] or 0

    def free(self, resource, lvl):
        busy = "" if lvl == "write" else " AND level='write'"
        return not self.one(f"SELECT 1 FROM leases WHERE resource=?{busy}", resource)

    def holders(self, resource):
        rows = self.q("SELECT holder, level, expires FROM leases WHERE resource=? ORDER BY expires", resource)
        return [{"holder": r["holder"], "level": r["level"], "expires_at": r["expires"]} for r in rows]

    def lease(self, resource, holder, lvl, ttl):
        token = None
        if lvl == "write":
            self.q("UPDATE meta SET value=value+1 WHERE key='fence'")
            token = self.one("SELECT value FROM meta WHERE key='fence'")[0]
        lid = secrets.token_hex(8)
        self.q("INSERT INTO leases VALUES (?, ?, ?, ?, ?, ?)", lid, resource, holder, lvl, token, time.time() + ttl)
        return self.lease_view(self.one("SELECT * FROM leases WHERE id=?", lid))

    @staticmethod
    def lease_view(row):
        view = {
            "lease_id": row["id"],
            "resource": row["resource"],
            "holder": row["holder"],
            "level": row["level"],
            "expires_at": row["expires"],
        }
        if row["token"] is not None:
            view["token"] = row["token"]
        return view

    def op_acquire(self, me, now, resource, level, ttl, wait, topic, corr):
        self.resource(resource)
        if self.held(me, resource) < LEVELS[level]:
            raise Bad(f"no {level} grant on {resource}, request it first")
        if self.one("SELECT count(*) FROM leases WHERE holder=?", me)[0] >= MAX_LEASES:
            raise Bad(f"at most {MAX_LEASES} leases per agent")
        if not self.one("SELECT 1 FROM waiters WHERE resource=?", resource) and self.free(resource, level):
            return {"lease": self.lease(resource, me, level, ttl)}
        if not wait:
            return {"ok": False, "error": "busy", "holders": self.holders(resource)}
        if self.one("SELECT count(*) FROM waiters WHERE resource=?", resource)[0] >= MAX_WAITERS:
            raise Bad(f"at most {MAX_WAITERS} waiters per resource")
        self.q(
            "INSERT INTO waiters(resource, holder, level, ttl, deadline, topic, corr, queued) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            resource,
            me,
            level,
            ttl,
            now + wait,
            topic,
            corr,
            now,
        )
        return None

    def op_renew(self, me, now, lease_id, ttl, **_):
        if not self.q("UPDATE leases SET expires=? WHERE id=? AND holder=?", now + ttl, lease_id, me).rowcount:
            raise Bad(f"no such lease: {lease_id} (it may have expired)")
        return {"lease": self.lease_view(self.one("SELECT * FROM leases WHERE id=?", lease_id))}

    def op_release(self, me, now, lease_id, **_):
        row = self.one(
            "SELECT l.resource FROM leases l JOIN resources r ON r.id=l.resource "
            "WHERE l.id=? AND (l.holder=? OR r.owner=?)",
            lease_id,
            me,
            me,
        )
        if not row:
            raise Bad(f"no such lease: {lease_id} (it may have expired)")
        self.q("DELETE FROM leases WHERE id=?", lease_id)
        self.pump(row["resource"])
        return {}

    def pump(self, resource):
        """Serve waiters FIFO: stop at the first one that cannot run yet, so readers never overtake a writer."""
        for w in self.q("SELECT * FROM waiters WHERE resource=? ORDER BY seq", resource).fetchall():
            if self.held(w["holder"], resource) < LEVELS[w["level"]]:
                self.q("DELETE FROM waiters WHERE seq=?", w["seq"])
                self.reply(w["topic"], w["corr"], {"ok": False, "error": f"no {w['level']} grant on {resource}"})
                continue
            if not self.free(resource, w["level"]):
                break
            self.q("DELETE FROM waiters WHERE seq=?", w["seq"])
            self.reply(
                w["topic"], w["corr"], {"ok": True, "lease": self.lease(resource, w["holder"], w["level"], w["ttl"])}
            )

    def op_dump(self, now):
        """Everything, for the admin console. Not a new exposure: `as` is self-asserted, so any client could already
        read every ledger, request and lease by claiming each owner in turn. Waiters leave out their reply topic."""
        lists = {
            "ledgers": [
                {"owner": r["owner"], "since": r["created"], "last_seen": r["seen"]}
                for r in self.q("SELECT * FROM ledgers ORDER BY owner")
            ],
            "resources": [dict(r) for r in self.q("SELECT * FROM resources ORDER BY id")],
            "requests": [dict(r) for r in self.q("SELECT * FROM requests ORDER BY status='pending' DESC, ts DESC")],
            "leases": [self.lease_view(r) for r in self.q("SELECT * FROM leases ORDER BY resource, expires")],
            "waiters": [
                {
                    "resource": r["resource"],
                    "holder": r["holder"],
                    "level": r["level"],
                    "queued_at": r["queued"],
                    "deadline": r["deadline"],
                }
                for r in self.q("SELECT * FROM waiters ORDER BY resource, seq")
            ],
        }
        body, n = {**lists, "ts": now}, max(map(len, lists.values()))
        while len(json.dumps(body)) > MAX_REPLY - 100:
            n = n * 4 // 5
            body = {**{k: v[:n] for k, v in lists.items()}, "ts": now, "truncated": True}
        return body

    def op_drop(self, me, now, **_):
        self.drop(me)
        return {}

    def forget(self, rids, why):
        """Delete resources and everything hanging off them, answering anyone still waiting on them."""
        for r in rids:
            for w in self.q("SELECT topic, corr FROM waiters WHERE resource=?", r).fetchall():
                self.reply(w["topic"], w["corr"], {"ok": False, "error": why})
            for table, col in (
                ("waiters", "resource"),
                ("leases", "resource"),
                ("requests", "resource"),
                ("resources", "id"),
            ):
                self.q(f"DELETE FROM {table} WHERE {col}=?", r)

    def drop(self, who):
        self.forget(
            [r["id"] for r in self.q("SELECT id FROM resources WHERE owner=?", who).fetchall()],
            f"{who} dropped its ledger",
        )
        touched = {
            r["resource"]
            for r in self.q(
                "SELECT resource FROM leases WHERE holder=? UNION SELECT resource FROM waiters WHERE holder=?", who, who
            ).fetchall()
        }
        for w in self.q("SELECT topic, corr FROM waiters WHERE holder=?", who).fetchall():
            self.reply(w["topic"], w["corr"], {"ok": False, "error": f"{who} dropped its ledger"})
        self.q("DELETE FROM waiters WHERE holder=?", who)
        self.q("DELETE FROM leases WHERE holder=?", who)
        self.q("DELETE FROM requests WHERE holder=? OR owner=?", who, who)
        self.q("DELETE FROM ledgers WHERE owner=?", who)
        for r in touched:
            self.pump(r)
        log("dropped ledger", who)

    # timers and presence

    def tick(self):
        now = time.time()
        for r in self.q("SELECT DISTINCT resource FROM leases WHERE expires<=?", now).fetchall():
            self.q("DELETE FROM leases WHERE resource=? AND expires<=?", r[0], now)
            self.pump(r[0])
        for w in self.q("SELECT * FROM waiters WHERE deadline<=? ORDER BY seq", now).fetchall():
            if self.q("DELETE FROM waiters WHERE seq=?", w["seq"]).rowcount:
                self.reply(
                    w["topic"], w["corr"], {"ok": False, "error": "busy", "holders": self.holders(w["resource"])}
                )
                self.pump(w["resource"])
        if now - self.swept >= min(600.0, STALE_S / 10):
            self.swept = now
            cutoff = now - STALE_S
            self.q("DELETE FROM presence WHERE keep=0 AND seen<?", cutoff)
            for r in self.q(
                "SELECT owner FROM ledgers WHERE seen<? AND owner NOT IN (SELECT addr FROM presence)", cutoff
            ).fetchall():
                self.drop(r[0])

    def next_deadline(self):
        times = [self.swept + min(600.0, STALE_S / 10)]
        times += [
            r[0]
            for r in (self.one("SELECT min(expires) FROM leases"), self.one("SELECT min(deadline) FROM waiters"))
            if r[0] is not None
        ]
        return min(times)

    def presence(self, who, payload):
        if not payload:
            self.q("DELETE FROM presence WHERE addr=?", who)
            return
        try:
            rec = json.loads(payload.decode("utf-8")) if len(payload) <= MAX_PAYLOAD else None
        except (ValueError, RecursionError):
            rec = None
        if not isinstance(rec, dict):
            return log("ignored malformed presence for", who)
        now, seen = time.time(), rec.get("seen")
        ok = isinstance(seen, (int, float)) and not isinstance(seen, bool) and math.isfinite(seen)
        self.q(
            "INSERT OR REPLACE INTO presence VALUES (?, ?, ?)",
            who,
            min(now, seen) if ok else now,
            rec.get("keep") is True,
        )


OPS = {
    "init": {},
    "add": {
        "name": (name, REQUIRED),
        "description": (description, ""),
        "visibility": (visibility, "private"),
        "max_level": (level, "read"),
    },
    "update": {
        "name": (name, REQUIRED),
        "description": (description, None),
        "visibility": (visibility, None),
        "max_level": (level, None),
    },
    "remove": {"name": (name, REQUIRED)},
    "ledger": {"owner": (addr, None)},
    "public": {},
    "request": {"resource": (rid, REQUIRED), "level": (level, REQUIRED)},
    "grant": {"request_id": (rowid, REQUIRED)},
    "deny": {"request_id": (rowid, REQUIRED)},
    "grants": {},
    "acquire": {
        "resource": (rid, REQUIRED),
        "level": (level, REQUIRED),
        "ttl": (number(1, MAX_TTL), 60.0),
        "wait": (number(0, MAX_WAIT), 0.0),
    },
    "renew": {"lease_id": (rowid, REQUIRED), "ttl": (number(1, MAX_TTL), 60.0)},
    "release": {"lease_id": (rowid, REQUIRED)},
    "drop": {},
    "dump": {},
}


def parse(req):
    if not isinstance(req, dict):
        raise Bad("request must be a JSON object")
    op = req.get("op")
    if not isinstance(op, str) or op not in OPS:
        raise Bad(f"op: one of {', '.join(OPS)}")
    fields = OPS[op]
    extra = [k[:32] for k in req if k not in fields and k not in ("op", "as")]
    if extra:
        raise Bad(f"unknown fields for {op}: {', '.join(extra[:5])}")
    if "as" not in req:
        raise Bad("as: required")
    args = {}
    for key, (check, default) in fields.items():
        if key in req and req[key] is not None:
            args[key] = check(key, req[key])
        elif default is REQUIRED:
            raise Bad(f"{key}: required")
        else:
            args[key] = default
    return op, addr("as", req["as"]), args
