//! The ledger's state: SQLite schema and every op, one transaction per request.
//!
//! Ledger is the only writer of ledgers, grants and leases. The service feeds it requests, lease expiry and presence
//! updates one at a time, in order; that serialization is the whole concurrency guarantee.

use crate::request::{Bad, parse};
use agent_bridge::json::{Map, dumps, dumps_map, float, loads, now};
use agent_bridge::ledger::random_hex;
use agent_bridge::stamp::stamp;
use agent_bridge::validation::is_ident;
use rusqlite::types::{Value as Sql, ValueRef};
use rusqlite::{Connection, Params, params};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

const MAX_PAYLOAD: usize = 16384;
const MAX_CORR: usize = 256;
/// Under config/mosquitto.conf max_packet_size 270000: a bigger publish gets ledgerd disconnected, then resent forever.
const MAX_REPLY: usize = 240_000;
const MAX_LEDGERS: i64 = 1000;
const MAX_RESOURCES: i64 = 128;
const MAX_PENDING: i64 = 64;
const MAX_LEASES: i64 = 256;
const MAX_WAITERS: i64 = 256;
/// stored requests per holder, any status
const MAX_REQUESTS: i64 = 512;
/// how long a mutating request's answer is kept to replay to a redelivered copy
const DEDUPE_S: f64 = 300.0;
const READ_ONLY: [&str; 4] = ["ledger", "public", "grants", "dump"];
const WAITER: &str = "SELECT seq, resource, holder, level, topic, ttl, corr FROM waiters";

const SCHEMA: &str = "
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
CREATE TABLE IF NOT EXISTS replies(key BLOB PRIMARY KEY, body TEXT, ts REAL NOT NULL);
INSERT OR IGNORE INTO meta VALUES ('fence', 0);
CREATE INDEX IF NOT EXISTS resources_owner ON resources(owner);
CREATE INDEX IF NOT EXISTS requests_resource ON requests(resource, holder);
CREATE INDEX IF NOT EXISTS requests_holder ON requests(holder);
CREATE INDEX IF NOT EXISTS requests_owner ON requests(owner);
CREATE INDEX IF NOT EXISTS leases_resource ON leases(resource);
CREATE INDEX IF NOT EXISTS leases_holder ON leases(holder);
CREATE INDEX IF NOT EXISTS waiters_resource ON waiters(resource, seq);
CREATE INDEX IF NOT EXISTS replies_ts ON replies(ts);
";

/// Why an op failed: refused (the message goes back) or broken (logged, "internal error" goes back).
pub enum Fail {
    Bad(String),
    Internal(String),
}

impl From<rusqlite::Error> for Fail {
    fn from(e: rusqlite::Error) -> Fail {
        Fail::Internal(e.to_string())
    }
}

impl From<Bad> for Fail {
    fn from(b: Bad) -> Fail {
        Fail::Bad(b.0)
    }
}

type Res<T> = Result<T, Fail>;

fn bad<T>(message: impl Into<String>) -> Res<T> {
    Err(Fail::Bad(message.into()))
}

pub fn log(text: &str) {
    eprintln!("{} {text}", stamp());
}

fn level(l: &str) -> i64 {
    if l == "write" { 2 } else { 1 }
}

fn json_of(v: ValueRef) -> Value {
    match v {
        ValueRef::Null | ValueRef::Blob(_) => Value::Null,
        ValueRef::Integer(i) => i.into(),
        ValueRef::Real(f) => float(f),
        ValueRef::Text(t) => String::from_utf8_lossy(t).into_owned().into(),
    }
}

fn s<'a>(row: &'a Map, key: &str) -> &'a str {
    row.get(key).and_then(Value::as_str).unwrap_or_default()
}

/// topic, payload and CorrelationData of one QoS 1 publish; notifications have no CorrelationData.
pub type Outgoing = (String, String, Option<Vec<u8>>);

/// A waiter's seq, its fields, and the CorrelationData to answer it with.
type Waiter = (i64, Map, Option<Vec<u8>>);

pub struct Ledger {
    db: Connection,
    out: Vec<Outgoing>,
    swept: f64,
    stale_s: f64,
}

impl Ledger {
    pub fn open(path: &std::path::Path, stale_days: f64) -> Result<Ledger, String> {
        let abs = std::path::absolute(path).map_err(|e| e.to_string())?;
        if let Some(dir) = abs.parent() {
            std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        }
        let db = Connection::open(path).map_err(|e| e.to_string())?;
        let setup = || -> rusqlite::Result<()> {
            db.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))?;
            db.execute_batch("PRAGMA synchronous=FULL")?;
            db.execute_batch(SCHEMA)?;
            // databases created before dump existed lack waiters.queued
            let has_queued = db
                .prepare("PRAGMA table_info(waiters)")?
                .query_map([], |r| r.get::<_, String>(1))?
                .any(|c| c.is_ok_and(|c| c == "queued"));
            if !has_queued {
                db.execute_batch("ALTER TABLE waiters ADD COLUMN queued REAL")?;
            }
            Ok(())
        };
        setup().map_err(|e| e.to_string())?;
        Ok(Ledger { db, out: Vec::new(), swept: 0.0, stale_s: (stale_days * 86400.0).max(1.0) })
    }

    fn rows<P: Params>(&self, sql: &str, args: P) -> Res<Vec<Map>> {
        let mut stmt = self.db.prepare_cached(sql)?;
        let names: Vec<String> = stmt.column_names().map(str::to_owned).collect();
        let rows = stmt.query_map(args, |r| {
            names
                .iter()
                .enumerate()
                .map(|(i, n)| Ok((n.clone(), json_of(r.get_ref(i)?))))
                .collect::<rusqlite::Result<Map>>()
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<Map>>>()?)
    }

    fn one<P: Params>(&self, sql: &str, args: P) -> Res<Option<Map>> {
        Ok(self.rows(sql, args)?.into_iter().next())
    }

    fn count<P: Params>(&self, sql: &str, args: P) -> Res<i64> {
        Ok(self.db.prepare_cached(sql)?.query_row(args, |r| r.get::<_, Option<i64>>(0))?.unwrap_or(0))
    }

    fn exec<P: Params>(&self, sql: &str, args: P) -> Res<usize> {
        Ok(self.db.prepare_cached(sql)?.execute(args)?)
    }

    fn reply(&mut self, topic: &str, corr: Option<Vec<u8>>, body: &Map) {
        let mut payload = dumps_map(body);
        if payload.len() > MAX_REPLY {
            payload = dumps(&json!({"ok": false, "error": format!("reply over {MAX_REPLY} bytes")}));
        }
        self.out.push((topic.to_owned(), payload, corr));
    }

    fn notify(&mut self, to: &str, body: String) {
        let payload = json!({"from": "ledgerd", "body": body, "ts": float(now())});
        self.out.push((format!("inbox/{to}"), dumps(&payload), None));
    }

    /// One transaction: replies and notifications leave only after it commits, through publish.
    pub fn run<T>(&mut self, publish: &mut dyn FnMut(Outgoing), f: impl FnOnce(&mut Self) -> Res<T>) -> Res<T> {
        self.db.execute_batch("BEGIN")?;
        let result = f(self).and_then(|r| Ok(self.db.execute_batch("COMMIT").map(|_| r)?));
        if result.is_err() {
            let _ = self.db.execute_batch("ROLLBACK");
            self.out.clear();
            return result;
        }
        for item in self.out.drain(..) {
            publish(item);
        }
        result
    }

    // requests

    /// One request; via is the <addr> of a ledger/req/<addr> topic, None for plain ledger/req.
    pub fn handle(
        &mut self,
        publish: &mut dyn FnMut(Outgoing),
        payload: &[u8],
        topic: Option<&str>,
        corr: Option<&[u8]>,
        via: Option<&str>,
    ) {
        let Some(topic) = topic.filter(|t| t.strip_prefix("ledger/resp/").is_some_and(|id| is_ident(id, 128))) else {
            return log("dropped request without a ResponseTopic under ledger/resp/");
        };
        let Some(corr) = corr.filter(|c| c.len() <= MAX_CORR) else {
            let body = json!({"ok": false, "error": format!("CorrelationData of 1 to {MAX_CORR} bytes required")});
            publish((topic.to_owned(), dumps(&body), None));
            return;
        };
        let body = match self.answer(publish, payload, topic, corr, via) {
            Ok(body) => body,
            Err(Fail::Bad(e)) => Some(error(&e)),
            Err(Fail::Internal(e)) => {
                log(&format!("internal error {e}"));
                Some(error("internal error"))
            }
        };
        if let Some(body) = body {
            self.reply(topic, Some(corr.to_vec()), &body);
            for item in self.out.drain(..) {
                publish(item);
            }
        }
    }

    fn answer(
        &mut self,
        publish: &mut dyn FnMut(Outgoing),
        payload: &[u8],
        topic: &str,
        corr: &[u8],
        via: Option<&str>,
    ) -> Res<Option<Map>> {
        if payload.len() > MAX_PAYLOAD {
            return bad(format!("payload over {MAX_PAYLOAD} bytes"));
        }
        let Some(req) = loads(payload) else {
            return bad("payload must be UTF-8 JSON");
        };
        let (op, me, args) = parse(&req, via)?;
        self.run(publish, |l| l.serve(op, &me, &args, topic, corr, payload))
    }

    /// dispatch, once: a redelivered copy of a mutating request (ledgerd crashed or reconnected before acking it) gets
    /// the first answer again instead of a second lease or request. The key includes the payload, so a client that
    /// reuses CorrelationData for different requests is not mistaken for a duplicate.
    fn serve(&mut self, op: &str, me: &str, args: &Map, topic: &str, corr: &[u8], payload: &[u8]) -> Res<Option<Map>> {
        if READ_ONLY.contains(&op) {
            return self.dispatch(op, me, args, topic, corr);
        }
        let key = Sha256::digest([topic.as_bytes(), corr, payload].join(&0u8)).to_vec();
        let stored: Option<Option<String>> = self
            .db
            .prepare_cached("SELECT body FROM replies WHERE key=?")?
            .query_row([&key], |r| r.get(0))
            .map(Some)
            .or_else(|e| if e == rusqlite::Error::QueryReturnedNoRows { Ok(None) } else { Err(e) })?;
        if let Some(body) = stored {
            return Ok(body.and_then(|b| loads(b.as_bytes())).and_then(|v| v.as_object().cloned()));
        }
        let body = self.dispatch(op, me, args, topic, corr)?;
        let text = body.as_ref().map(dumps_map);
        self.exec("INSERT INTO replies VALUES (?, ?, ?)", params![key, text, now()])?;
        Ok(body)
    }

    fn dispatch(&mut self, op: &str, me: &str, args: &Map, topic: &str, corr: &[u8]) -> Res<Option<Map>> {
        let now = now();
        let body = match op {
            "init" => self.op_init(me, now)?,
            "dump" => self.op_dump(now)?,
            _ => {
                if self.exec("UPDATE ledgers SET seen=? WHERE owner=?", params![now, me])? == 0 {
                    return bad(format!("no ledger for {me}, call init first"));
                }
                let a = |k: &str| args.get(k).and_then(Value::as_str);
                let n = |k: &str| args.get(k).and_then(Value::as_f64).unwrap_or(0.0);
                let body = match op {
                    "add" => self.op_add(me, now, a("name").unwrap_or_default(), args)?,
                    "update" => self.op_update(me, now, a("name").unwrap_or_default(), args)?,
                    "remove" => self.op_remove(me, a("name").unwrap_or_default())?,
                    "ledger" => self.op_ledger(me, a("owner"))?,
                    "public" => self.op_public()?,
                    "request" => {
                        self.op_request(me, now, a("resource").unwrap_or_default(), a("level").unwrap_or_default())?
                    }
                    "grant" => self.op_grant(me, a("request_id").unwrap_or_default())?,
                    "deny" => self.op_deny(me, a("request_id").unwrap_or_default())?,
                    "grants" => self.op_grants(me)?,
                    "acquire" => {
                        let (res, lvl) = (a("resource").unwrap_or_default(), a("level").unwrap_or_default());
                        match self.op_acquire(me, now, res, lvl, n("ttl"), n("wait"), topic, corr)? {
                            Some(body) => body,
                            None => return Ok(None),
                        }
                    }
                    "renew" => self.op_renew(me, now, a("lease_id").unwrap_or_default(), n("ttl"))?,
                    "release" => self.op_release(me, a("lease_id").unwrap_or_default())?,
                    "drop" => {
                        self.drop_ledger(me)?;
                        Map::new()
                    }
                    _ => return bad(format!("op: {op}")),
                };
                if body.contains_key("ok") {
                    return Ok(Some(body));
                }
                body
            }
        };
        let mut out = Map::new();
        out.insert("ok".into(), true.into());
        out.extend(body);
        Ok(Some(out))
    }

    fn op_init(&mut self, me: &str, now: f64) -> Res<Map> {
        if let Some(row) = self.one("SELECT * FROM ledgers WHERE owner=?", [me])? {
            self.exec("UPDATE ledgers SET seen=? WHERE owner=?", params![now, me])?;
            return Ok(obj(json!({"created": false, "owner": me, "since": row["created"]})));
        }
        if self.count("SELECT count(*) FROM ledgers", [])? >= MAX_LEDGERS {
            return bad("ledger limit reached");
        }
        self.exec("INSERT INTO ledgers VALUES (?, ?, ?)", params![me, now, now])?;
        Ok(obj(json!({"created": true, "owner": me, "since": float(now)})))
    }

    fn resource(&self, rid: &str, owner: Option<&str>) -> Res<Map> {
        match self.one("SELECT * FROM resources WHERE id=?", [rid])? {
            Some(row) if owner.is_none_or(|o| s(&row, "owner") == o) => Ok(row),
            _ => bad(format!("no such resource: {rid}")),
        }
    }

    fn resource_body(&self, rid: &str) -> Res<Map> {
        Ok(obj(json!({"resource": self.resource(rid, None)?})))
    }

    fn op_add(&mut self, me: &str, now: f64, name: &str, args: &Map) -> Res<Map> {
        let rid = format!("{me}/{name}");
        if self.one("SELECT 1 FROM resources WHERE id=?", [&rid])?.is_some() {
            return bad(format!("{rid} exists, use update"));
        }
        if self.count("SELECT count(*) FROM resources WHERE owner=?", [me])? >= MAX_RESOURCES {
            return bad(format!("at most {MAX_RESOURCES} resources per ledger"));
        }
        let a = |k: &str| args.get(k).and_then(Value::as_str).unwrap_or_default().to_owned();
        self.exec(
            "INSERT INTO resources VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            params![rid, me, name, a("description"), a("visibility"), a("max_level"), now, now],
        )?;
        self.resource_body(&rid)
    }

    fn op_update(&mut self, me: &str, now: f64, name: &str, args: &Map) -> Res<Map> {
        let rid = format!("{me}/{name}");
        self.resource(&rid, Some(me))?;
        let changes: Vec<(&str, &str)> = ["description", "visibility", "max_level"]
            .into_iter()
            .filter_map(|k| Some((k, args.get(k)?.as_str()?)))
            .collect();
        if changes.is_empty() {
            return bad("nothing to update: give description, visibility or max_level");
        }
        for (k, v) in changes {
            self.exec(&format!("UPDATE resources SET {k}=?, updated=? WHERE id=?"), params![v, now, rid])?;
        }
        self.resource_body(&rid)
    }

    fn op_remove(&mut self, me: &str, name: &str) -> Res<Map> {
        let rid = format!("{me}/{name}");
        self.resource(&rid, Some(me))?;
        self.forget(std::slice::from_ref(&rid), &format!("{rid} was removed"))?;
        Ok(Map::new())
    }

    fn op_ledger(&mut self, me: &str, owner: Option<&str>) -> Res<Map> {
        let owner = owner.filter(|o| !o.is_empty()).unwrap_or(me);
        let rows = if owner == me {
            self.rows("SELECT * FROM resources WHERE owner=? ORDER BY name", [me])?
        } else if self.one("SELECT 1 FROM ledgers WHERE owner=?", [owner])?.is_some() {
            self.rows("SELECT * FROM resources WHERE owner=? AND visibility='public' ORDER BY name", [owner])?
        } else {
            return bad(format!("no ledger for {owner}"));
        };
        Ok(obj(json!({"owner": owner, "resources": rows})))
    }

    fn op_public(&mut self) -> Res<Map> {
        let rows = self.rows("SELECT * FROM resources WHERE visibility='public' ORDER BY owner, name", [])?;
        Ok(obj(json!({"resources": rows})))
    }

    fn request_body(&self, id: &str) -> Res<Map> {
        Ok(obj(json!({"request": self.one("SELECT * FROM requests WHERE id=?", [id])?})))
    }

    fn op_request(&mut self, me: &str, now: f64, resource: &str, lvl: &str) -> Res<Map> {
        let res = self.resource(resource, None)?;
        if s(&res, "owner") == me {
            return bad("you own this resource, owners always hold write");
        }
        for row in self.rows(
            "SELECT * FROM requests WHERE resource=? AND holder=? AND status IN ('granted', 'pending')",
            [resource, me],
        )? {
            let (status, held) = (s(&row, "status"), s(&row, "level"));
            if (status == "granted" && level(held) >= level(lvl)) || (status == "pending" && held == lvl) {
                return Ok(obj(json!({"request": row})));
            }
        }
        let auto = s(&res, "visibility") == "public" && level(lvl) <= level(s(&res, "max_level"));
        let stored = "SELECT count(*) FROM requests WHERE holder=?";
        if self.count(stored, [me])? >= MAX_REQUESTS {
            self.exec(
                "DELETE FROM requests WHERE id=(SELECT id FROM requests WHERE holder=? AND status='denied' \
                 ORDER BY ts LIMIT 1)",
                [me],
            )?;
            if self.count(stored, [me])? >= MAX_REQUESTS {
                return bad(format!("at most {MAX_REQUESTS} requests per agent"));
            }
        }
        if !auto
            && self.count("SELECT count(*) FROM requests WHERE holder=? AND status='pending'", [me])? >= MAX_PENDING
        {
            return bad(format!("at most {MAX_PENDING} pending requests per agent"));
        }
        let rq = random_hex(8);
        let owner = s(&res, "owner").to_owned();
        let status = if auto { "granted" } else { "pending" };
        self.exec(
            "INSERT INTO requests VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![rq, resource, owner, me, lvl, status, now],
        )?;
        if !auto {
            self.notify(
                &owner,
                format!(
                    "{me} requests {lvl} access to {resource} (request_id {rq}); answer with ledger grant or deny \
                     request_id={rq}"
                ),
            );
        }
        self.request_body(&rq)
    }

    fn decide(&mut self, me: &str, rq: &str, status: &str) -> Res<Map> {
        let Some(row) = self.one("SELECT * FROM requests WHERE id=? AND owner=?", [rq, me])? else {
            return bad(format!("no such request: {rq}"));
        };
        if s(&row, "status") != status {
            self.exec("UPDATE requests SET status=?, ts=? WHERE id=?", params![status, now(), rq])?;
            let text = format!(
                "{me} {status} your {} request for {} (request_id {rq})",
                s(&row, "level"),
                s(&row, "resource")
            );
            self.notify(s(&row, "holder"), text);
        }
        Ok(row)
    }

    fn op_grant(&mut self, me: &str, rq: &str) -> Res<Map> {
        self.decide(me, rq, "granted")?;
        self.request_body(rq)
    }

    fn op_deny(&mut self, me: &str, rq: &str) -> Res<Map> {
        let row = self.decide(me, rq, "denied")?;
        let (holder, resource) = (s(&row, "holder"), s(&row, "resource"));
        let left = self.held(holder, resource)?;
        for lease in self.rows("SELECT * FROM leases WHERE resource=? AND holder=?", [resource, holder])? {
            if level(s(&lease, "level")) > left {
                self.exec("DELETE FROM leases WHERE id=?", [s(&lease, "id")])?;
            }
        }
        self.pump(resource)?;
        self.request_body(rq)
    }

    fn op_grants(&mut self, me: &str) -> Res<Map> {
        let held = self.rows("SELECT * FROM requests WHERE holder=? ORDER BY ts", [me])?;
        let asked = self.rows("SELECT * FROM requests WHERE owner=? ORDER BY ts", [me])?;
        Ok(obj(json!({"held": held, "asked": asked})))
    }

    // leases

    /// Highest level who may lock resource at: 2 for the owner, else the best granted request, 0 for none.
    fn held(&self, who: &str, resource: &str) -> Res<i64> {
        if self.one("SELECT 1 FROM resources WHERE id=? AND owner=?", [resource, who])?.is_some() {
            return Ok(2);
        }
        self.count(
            "SELECT max(CASE level WHEN 'write' THEN 2 ELSE 1 END) FROM requests \
             WHERE resource=? AND holder=? AND status='granted'",
            [resource, who],
        )
    }

    fn free(&self, resource: &str, lvl: &str) -> Res<bool> {
        let busy = if lvl == "write" { "" } else { " AND level='write'" };
        Ok(self.one(&format!("SELECT 1 FROM leases WHERE resource=?{busy}"), [resource])?.is_none())
    }

    fn holders(&self, resource: &str) -> Res<Value> {
        let rows =
            self.rows("SELECT holder, level, expires FROM leases WHERE resource=? ORDER BY expires", [resource])?;
        Ok(rows
            .iter()
            .map(|r| json!({"holder": r["holder"], "level": r["level"], "expires_at": r["expires"]}))
            .collect())
    }

    fn lease(&mut self, resource: &str, holder: &str, lvl: &str, ttl: f64) -> Res<Map> {
        let mut token = None;
        if lvl == "write" {
            self.exec("UPDATE meta SET value=value+1 WHERE key='fence'", [])?;
            token = Some(self.count("SELECT value FROM meta WHERE key='fence'", [])?);
        }
        let lid = random_hex(8);
        self.exec(
            "INSERT INTO leases VALUES (?, ?, ?, ?, ?, ?)",
            params![lid, resource, holder, lvl, token, now() + ttl],
        )?;
        let row = self.one("SELECT * FROM leases WHERE id=?", [&lid])?.unwrap_or_default();
        Ok(lease_view(&row))
    }

    #[allow(clippy::too_many_arguments)]
    fn op_acquire(
        &mut self,
        me: &str,
        now: f64,
        resource: &str,
        lvl: &str,
        ttl: f64,
        wait: f64,
        topic: &str,
        corr: &[u8],
    ) -> Res<Option<Map>> {
        self.resource(resource, None)?;
        if self.held(me, resource)? < level(lvl) {
            return bad(format!("no {lvl} grant on {resource}, request it first"));
        }
        if self.count("SELECT count(*) FROM leases WHERE holder=?", [me])? >= MAX_LEASES {
            return bad(format!("at most {MAX_LEASES} leases per agent"));
        }
        if self.one("SELECT 1 FROM waiters WHERE resource=?", [resource])?.is_none() && self.free(resource, lvl)? {
            return Ok(Some(obj(json!({"lease": self.lease(resource, me, lvl, ttl)?}))));
        }
        if wait == 0.0 {
            return Ok(Some(obj(json!({"ok": false, "error": "busy", "holders": self.holders(resource)?}))));
        }
        if self.count("SELECT count(*) FROM waiters WHERE resource=?", [resource])? >= MAX_WAITERS {
            return bad(format!("at most {MAX_WAITERS} waiters per resource"));
        }
        self.exec(
            "INSERT INTO waiters(resource, holder, level, ttl, deadline, topic, corr, queued) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            params![resource, me, lvl, ttl, now + wait, topic, corr, now],
        )?;
        Ok(None)
    }

    fn op_renew(&mut self, me: &str, now: f64, lease_id: &str, ttl: f64) -> Res<Map> {
        if self.exec("UPDATE leases SET expires=? WHERE id=? AND holder=?", params![now + ttl, lease_id, me])? == 0 {
            return bad(format!("no such lease: {lease_id} (it may have expired)"));
        }
        let row = self.one("SELECT * FROM leases WHERE id=?", [lease_id])?.unwrap_or_default();
        Ok(obj(json!({"lease": lease_view(&row)})))
    }

    fn op_release(&mut self, me: &str, lease_id: &str) -> Res<Map> {
        let Some(row) = self.one(
            "SELECT l.resource FROM leases l JOIN resources r ON r.id=l.resource WHERE l.id=? AND (l.holder=? OR r.owner=?)",
            [lease_id, me, me],
        )?
        else {
            return bad(format!("no such lease: {lease_id} (it may have expired)"));
        };
        self.exec("DELETE FROM leases WHERE id=?", [lease_id])?;
        self.pump(s(&row, "resource"))?;
        Ok(Map::new())
    }

    /// The waiters of resource with what answering them needs.
    fn waiters<P: Params>(&self, sql: &str, args: P) -> Res<Vec<Waiter>> {
        let mut stmt = self.db.prepare_cached(sql)?;
        let rows = stmt.query_map(args, |r| {
            let mut m = Map::new();
            for (i, key) in ["resource", "holder", "level", "topic"].iter().enumerate() {
                m.insert((*key).into(), json_of(r.get_ref(i + 1)?));
            }
            m.insert("ttl".into(), json_of(r.get_ref(5)?));
            let corr = match r.get::<_, Sql>(6)? {
                Sql::Blob(b) => Some(b),
                Sql::Text(t) => Some(t.into_bytes()),
                _ => None,
            };
            Ok((r.get::<_, i64>(0)?, m, corr))
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Serve waiters FIFO: stop at the first one that cannot run yet, so readers never overtake a writer.
    fn pump(&mut self, resource: &str) -> Res<()> {
        for (seq, w, corr) in self.waiters(&format!("{WAITER} WHERE resource=? ORDER BY seq"), [resource])? {
            let (holder, lvl, topic) = (s(&w, "holder"), s(&w, "level"), s(&w, "topic"));
            if self.held(holder, resource)? < level(lvl) {
                self.exec("DELETE FROM waiters WHERE seq=?", [seq])?;
                self.reply(topic, corr, &error(&format!("no {lvl} grant on {resource}")));
                continue;
            }
            if !self.free(resource, lvl)? {
                break;
            }
            self.exec("DELETE FROM waiters WHERE seq=?", [seq])?;
            let lease = self.lease(resource, holder, lvl, w["ttl"].as_f64().unwrap_or(60.0))?;
            self.reply(topic, corr, &obj(json!({"ok": true, "lease": lease})));
        }
        Ok(())
    }

    /// Everything, for the admin console. Not a new exposure: `as` is self-asserted, so any client could already read
    /// every ledger, request and lease by claiming each owner in turn. Waiters leave out their reply topic.
    fn op_dump(&mut self, now: f64) -> Res<Map> {
        let ledgers: Vec<Value> = self
            .rows("SELECT * FROM ledgers ORDER BY owner", [])?
            .iter()
            .map(|r| json!({"owner": r["owner"], "since": r["created"], "last_seen": r["seen"]}))
            .collect();
        let resources = self.rows("SELECT * FROM resources ORDER BY id", [])?;
        let requests = self.rows("SELECT * FROM requests ORDER BY status='pending' DESC, ts DESC", [])?;
        let leases: Vec<Map> =
            self.rows("SELECT * FROM leases ORDER BY resource, expires", [])?.iter().map(lease_view).collect();
        let waiters: Vec<Value> = self
            .rows("SELECT * FROM waiters ORDER BY resource, seq", [])?
            .iter()
            .map(|r| {
                json!({"resource": r["resource"], "holder": r["holder"], "level": r["level"],
                       "queued_at": r.get("queued").cloned().unwrap_or(Value::Null), "deadline": r["deadline"]})
            })
            .collect();
        let lists: Vec<(&str, Vec<Value>)> = vec![
            ("ledgers", ledgers),
            ("resources", resources.into_iter().map(Value::Object).collect()),
            ("requests", requests.into_iter().map(Value::Object).collect()),
            ("leases", leases.into_iter().map(Value::Object).collect()),
            ("waiters", waiters),
        ];
        let build = |n: Option<usize>| {
            let mut body = Map::new();
            for (k, v) in &lists {
                body.insert((*k).into(), v[..n.unwrap_or(v.len()).min(v.len())].to_vec().into());
            }
            body.insert("ts".into(), float(now));
            if n.is_some() {
                body.insert("truncated".into(), true.into());
            }
            body
        };
        let mut n = lists.iter().map(|(_, v)| v.len()).max().unwrap_or(0);
        let mut body = build(None);
        while dumps_map(&body).len() > MAX_REPLY - 100 {
            n = n * 4 / 5;
            body = build(Some(n));
        }
        Ok(body)
    }

    /// Delete resources and everything hanging off them, answering anyone still waiting on them.
    fn forget(&mut self, rids: &[String], why: &str) -> Res<()> {
        for r in rids {
            for (_, w, corr) in self.waiters(&format!("{WAITER} WHERE resource=?"), [r])? {
                self.reply(s(&w, "topic"), corr, &error(why));
            }
            for (table, col) in
                [("waiters", "resource"), ("leases", "resource"), ("requests", "resource"), ("resources", "id")]
            {
                self.exec(&format!("DELETE FROM {table} WHERE {col}=?"), [r])?;
            }
        }
        Ok(())
    }

    fn drop_ledger(&mut self, who: &str) -> Res<()> {
        let owned: Vec<String> =
            self.rows("SELECT id FROM resources WHERE owner=?", [who])?.iter().map(|r| s(r, "id").to_owned()).collect();
        let why = format!("{who} dropped its ledger");
        self.forget(&owned, &why)?;
        let touched: BTreeSet<String> = self
            .rows(
                "SELECT resource FROM leases WHERE holder=? UNION SELECT resource FROM waiters WHERE holder=?",
                [who, who],
            )?
            .iter()
            .map(|r| s(r, "resource").to_owned())
            .collect();
        for (_, w, corr) in self.waiters(&format!("{WAITER} WHERE holder=?"), [who])? {
            self.reply(s(&w, "topic"), corr, &error(&why));
        }
        self.exec("DELETE FROM waiters WHERE holder=?", [who])?;
        self.exec("DELETE FROM leases WHERE holder=?", [who])?;
        self.exec("DELETE FROM requests WHERE holder=? OR owner=?", [who, who])?;
        self.exec("DELETE FROM ledgers WHERE owner=?", [who])?;
        for r in touched {
            self.pump(&r)?;
        }
        log(&format!("dropped ledger {who}"));
        Ok(())
    }

    // timers and presence

    fn sweep_every(&self) -> f64 {
        600f64.min(self.stale_s / 10.0)
    }

    pub fn tick(&mut self) -> Res<()> {
        let now = now();
        self.exec("DELETE FROM replies WHERE ts<?", [now - DEDUPE_S])?;
        for r in self.rows("SELECT DISTINCT resource FROM leases WHERE expires<=?", [now])? {
            self.exec("DELETE FROM leases WHERE resource=? AND expires<=?", params![s(&r, "resource"), now])?;
            self.pump(s(&r, "resource"))?;
        }
        for (seq, w, corr) in self.waiters(&format!("{WAITER} WHERE deadline<=? ORDER BY seq"), [now])? {
            if self.exec("DELETE FROM waiters WHERE seq=?", [seq])? > 0 {
                let holders = self.holders(s(&w, "resource"))?;
                self.reply(s(&w, "topic"), corr, &obj(json!({"ok": false, "error": "busy", "holders": holders})));
                self.pump(s(&w, "resource"))?;
            }
        }
        if now - self.swept >= self.sweep_every() {
            self.swept = now;
            let cutoff = now - self.stale_s;
            self.exec("DELETE FROM presence WHERE keep=0 AND seen<?", [cutoff])?;
            // Granted requests are live permissions and stay; undecided and denied ones go once stale.
            self.exec("DELETE FROM requests WHERE status IN ('pending', 'denied') AND ts<?", [cutoff])?;
            for r in self
                .rows("SELECT owner FROM ledgers WHERE seen<? AND owner NOT IN (SELECT addr FROM presence)", [cutoff])?
            {
                self.drop_ledger(s(&r, "owner"))?;
            }
        }
        Ok(())
    }

    pub fn next_deadline(&self) -> f64 {
        let mut t = self.swept + self.sweep_every();
        for sql in ["SELECT min(expires) FROM leases", "SELECT min(deadline) FROM waiters"] {
            if let Ok(Some(v)) = self.db.query_row(sql, [], |r| r.get::<_, Option<f64>>(0)) {
                t = t.min(v);
            }
        }
        t
    }

    pub fn presence(&mut self, who: &str, payload: &[u8]) -> Res<()> {
        if payload.is_empty() {
            self.exec("DELETE FROM presence WHERE addr=?", [who])?;
            return Ok(());
        }
        let rec = (payload.len() <= MAX_PAYLOAD).then(|| loads(payload)).flatten();
        let Some(rec) = rec.as_ref().and_then(Value::as_object) else {
            log(&format!("ignored malformed presence for {who}"));
            return Ok(());
        };
        let now = now();
        let seen = rec.get("seen").filter(|v| v.is_number()).and_then(Value::as_f64).filter(|v| v.is_finite());
        let keep = rec.get("keep") == Some(&Value::Bool(true));
        self.exec(
            "INSERT OR REPLACE INTO presence VALUES (?, ?, ?)",
            params![who, seen.map_or(now, |s| s.min(now)), keep],
        )?;
        Ok(())
    }
}

fn error(text: &str) -> Map {
    obj(json!({"ok": false, "error": text}))
}

fn obj(v: Value) -> Map {
    match v {
        Value::Object(m) => m,
        _ => Map::new(),
    }
}

fn lease_view(row: &Map) -> Map {
    let mut view = obj(json!({"lease_id": row["id"], "resource": row["resource"], "holder": row["holder"],
                              "level": row["level"], "expires_at": row["expires"]}));
    if row.get("token").is_some_and(|t| !t.is_null()) {
        view.insert("token".into(), row["token"].clone());
    }
    view
}
