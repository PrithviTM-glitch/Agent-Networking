//! The ledger's state: SQLite schema and every op, one transaction per request.
//!
//! Ledger is the only writer of ledgers, grants and leases. The service feeds it requests, lease expiry and presence
//! updates one at a time, in order; that serialization is the whole concurrency guarantee.

use crate::request::{Bad, parse};
/// AGENT_BRIDGE_AUTH as ledgerd applies it: off is the 0.3.1 behaviour; warn logs what enforce refuses.
pub use agent_bridge::config::Auth;
use agent_bridge::json::{Map, dumps, dumps_map, float, loads, now};
use agent_bridge::ledger::{
    MAX_LEASES, MAX_LEDGERS, MAX_PENDING, MAX_PENDING_PER_OWNER, MAX_REQUESTS, MAX_RESOURCES, MAX_TTL, MAX_WAIT,
    MAX_WAITERS, random_hex, worktree_resource,
};
use agent_bridge::stamp::stamp;
use agent_bridge::validation::{ADMIN, is_ident};
use rusqlite::types::{Value as Sql, ValueRef};
use rusqlite::{Connection, Params, params};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};

const MAX_PAYLOAD: usize = 16384;
const MAX_CORR: usize = 256;
/// Under config/mosquitto.conf max_packet_size 270000: a bigger publish gets ledgerd disconnected, then resent forever.
const MAX_REPLY: usize = 240_000;
/// how long a mutating request's answer is kept to replay to a redelivered copy, at least; a request whose
/// MessageExpiryInterval is longer keeps its answer that long, up to MAX_KEEP
const DEDUPE_S: f64 = 300.0;
const MAX_KEEP: f64 = MAX_TTL + MAX_WAIT;
/// A waiter is dropped this long before its client stops listening, so its lease reply still reaches it.
const REPLY_MARGIN: f64 = 1.0;
const READ_ONLY: [&str; 10] =
    ["ledger", "public", "grants", "dump", "leases", "lease_history", "ping", "claims", "holders", "worktrees"];
/// worktree resources admin's ledger may hold (admin/wt-*), apart from MAX_RESOURCES
const MAX_WORKTREES: i64 = 1024;
/// the per-identity request rate under auth: a token bucket of RATE_BURST refilled at RATE_PER_S
const RATE_PER_S: f64 = 20.0;
const RATE_BURST: f64 = 100.0;
/// identities tracked at once, by the rate limit and by the once-an-hour auth warnings
const MAX_TRACKED: usize = 4096;
const WARN_EVERY_S: f64 = 3600.0;
/// claims one agent may hold, and one room may have
const MAX_CLAIMS_HELD: i64 = 256;
const MAX_CLAIMS_ROOM: i64 = 1024;
/// lease events kept per resource, newest
const MAX_EVENTS: i64 = 200;
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
CREATE TABLE IF NOT EXISTS lease_events(seq INTEGER PRIMARY KEY AUTOINCREMENT, resource TEXT NOT NULL, ts REAL NOT NULL,
  event TEXT NOT NULL, lease_id TEXT NOT NULL, holder TEXT NOT NULL, level TEXT NOT NULL, token INTEGER, actor TEXT);
INSERT OR IGNORE INTO meta VALUES ('fence', 0);
CREATE INDEX IF NOT EXISTS resources_owner ON resources(owner);
CREATE INDEX IF NOT EXISTS requests_resource ON requests(resource, holder);
CREATE INDEX IF NOT EXISTS requests_holder ON requests(holder);
CREATE INDEX IF NOT EXISTS requests_owner ON requests(owner);
CREATE INDEX IF NOT EXISTS leases_resource ON leases(resource);
CREATE INDEX IF NOT EXISTS leases_holder ON leases(holder);
CREATE INDEX IF NOT EXISTS waiters_resource ON waiters(resource, seq);
CREATE INDEX IF NOT EXISTS replies_ts ON replies(ts);
CREATE INDEX IF NOT EXISTS lease_events_resource ON lease_events(resource, seq);
CREATE TABLE IF NOT EXISTS claims(room TEXT NOT NULL, key TEXT NOT NULL, holder TEXT NOT NULL, since REAL NOT NULL,
  expires REAL, note TEXT NOT NULL, PRIMARY KEY(room, key));
CREATE INDEX IF NOT EXISTS claims_holder ON claims(holder);
CREATE TABLE IF NOT EXISTS specs(resource TEXT PRIMARY KEY, spec TEXT NOT NULL);
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

/// Where a request's answer goes, and how long its client listens (the request's remaining MessageExpiryInterval).
struct Req<'a> {
    topic: &'a str,
    corr: &'a [u8],
    expiry: Option<f64>,
}

pub struct Ledger {
    db: Connection,
    out: Vec<Outgoing>,
    swept: f64,
    stale_s: f64,
    /// each room's moderators, from its retained rooms/<room> record
    moderators: HashMap<String, Vec<String>>,
    pub auth: Auth,
    /// the rate limit's buckets by identity: tokens left and when they were counted
    buckets: HashMap<String, (f64, f64)>,
    /// when each auth warning was last logged
    warned: HashMap<String, f64>,
}

/// Why Ledger::open failed: a database another schema version wrote (refused before anything is written), or
/// anything else.
pub enum Open {
    Schema(String),
    Failed(String),
}

/// The meta schema row of an existing database, as Python prints it, when it is not 1 (compared as a number, as
/// the Python ledgerd does with its INTEGER column).
fn other_schema(db: &Connection) -> rusqlite::Result<Option<String>> {
    let has_meta = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='meta'";
    if db.query_row(has_meta, [], |r| r.get::<_, i64>(0))? == 0 {
        return Ok(None);
    }
    let mut stmt = db.prepare("SELECT value FROM meta WHERE key='schema'")?;
    let mut rows = stmt.query([])?;
    let Some(row) = rows.next()? else { return Ok(None) };
    Ok(match row.get_ref(0)? {
        ValueRef::Integer(1) => None,
        ValueRef::Real(1.0) => None,
        ValueRef::Integer(n) => Some(n.to_string()),
        ValueRef::Real(f) => Some(agent_bridge::json::float_repr(f)),
        ValueRef::Text(t) | ValueRef::Blob(t) => Some(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Null => Some("None".into()),
    })
}

impl Ledger {
    pub fn open(path: &std::path::Path, stale_days: f64) -> Result<Ledger, Open> {
        let failed = |e: &dyn std::fmt::Display| Open::Failed(e.to_string());
        let abs = std::path::absolute(path).map_err(|e| failed(&e))?;
        if let Some(dir) = abs.parent() {
            std::fs::create_dir_all(dir).map_err(|e| Open::Failed(format!("{}: {e}", dir.display())))?;
        }
        let db = Connection::open(path).map_err(|e| failed(&e))?;
        // Before anything is written, so a database a newer ledgerd migrated is left exactly as it is.
        if let Some(n) = other_schema(&db).map_err(|e| failed(&e))? {
            return Err(Open::Schema(format!("ledger.db has schema {n}; this ledgerd knows 1")));
        }
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
            // Schema 1 is all of the above. A later migration checks this row, migrates, then raises it; the first
            // one retires the Python ledgerd (image :2) as a rollback, which only knows schema 1.
            db.execute_batch("INSERT OR IGNORE INTO meta VALUES ('schema', 1)")?;
            Ok(())
        };
        setup().map_err(|e| failed(&e))?;
        Ok(Ledger {
            db,
            out: Vec::new(),
            swept: 0.0,
            stale_s: (stale_days * 86400.0).max(1.0),
            moderators: HashMap::new(),
            auth: Auth::Off,
            buckets: HashMap::new(),
            warned: HashMap::new(),
        })
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
        expiry: Option<u32>,
    ) {
        let Some(topic) = topic else {
            return log("dropped request without a ResponseTopic under ledger/resp/");
        };
        // ledger/resp/<id>, or ledger/resp/<addr>/<conn>, which only the agent at addr may read under auth
        let owner = match topic.strip_prefix("ledger/resp/").map(|rest| rest.split_once('/')) {
            Some(None) if is_ident(&topic["ledger/resp/".len()..], 128) => {
                if self.auth == Auth::Enforce {
                    return log(
                        "dropped request with a one-level ResponseTopic: under enforce it is ledger/resp/<addr>/<conn>",
                    );
                }
                None
            }
            Some(Some((addr, conn))) if is_ident(addr, agent_bridge::validation::ID_MAX) && is_ident(conn, 128) => {
                Some(addr)
            }
            _ => return log("dropped request without a ResponseTopic under ledger/resp/"),
        };
        let Some(corr) = corr.filter(|c| c.len() <= MAX_CORR) else {
            let body = json!({"ok": false, "error": format!("CorrelationData of 1 to {MAX_CORR} bytes required")});
            publish((topic.to_owned(), dumps(&body), None));
            return;
        };
        let body = match self.answer(publish, payload, topic, owner, corr, via, expiry.map(f64::from)) {
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

    /// Log text unless the same key was logged within WARN_EVERY_S.
    fn warn_once(&mut self, key: &str, text: &str) {
        let now = now();
        if self.warned.get(key).is_some_and(|t| now - t < WARN_EVERY_S) {
            return;
        }
        if self.warned.len() >= MAX_TRACKED {
            self.warned.retain(|_, t| now - *t < WARN_EVERY_S);
        }
        if self.warned.len() < MAX_TRACKED {
            self.warned.insert(key.to_owned(), now);
        }
        log(&format!("auth {}: {text}", self.auth.as_str()));
    }

    /// One token from who's bucket, false when it is empty.
    fn take_token(&mut self, who: &str) -> bool {
        let now = now();
        let level = |(tokens, at): (f64, f64)| (tokens + (now - at) * RATE_PER_S).min(RATE_BURST);
        if self.buckets.len() >= MAX_TRACKED && !self.buckets.contains_key(who) {
            // a full bucket is the same as none
            self.buckets.retain(|_, b| level(*b) < RATE_BURST);
            // still full: the longest-idle bucket goes, so new identities are never shut out
            if let Some(idle) = self.buckets.iter().min_by(|a, b| a.1.1.total_cmp(&b.1.1)).map(|(k, _)| k.clone()) {
                self.buckets.remove(&idle);
            }
        }
        let tokens = self.buckets.get(who).map_or(RATE_BURST, |b| level(*b));
        if tokens < 1.0 {
            return false;
        }
        self.buckets.insert(who.to_owned(), (tokens - 1.0, now));
        true
    }

    #[allow(clippy::too_many_arguments)]
    fn answer(
        &mut self,
        publish: &mut dyn FnMut(Outgoing),
        payload: &[u8],
        topic: &str,
        owner: Option<&str>,
        corr: &[u8],
        via: Option<&str>,
        expiry: Option<f64>,
    ) -> Res<Option<Map>> {
        if payload.len() > MAX_PAYLOAD {
            return bad(format!("payload over {MAX_PAYLOAD} bytes"));
        }
        let Some(req) = loads(payload) else {
            return bad("payload must be UTF-8 JSON");
        };
        let (op, me, args) = parse(&req, via)?;
        // Never published into another agent's reply space.
        if owner.is_some_and(|o| o != me) {
            log(&format!("dropped {op} from {me} with ResponseTopic {}", &topic[..topic.len().min(120)]));
            return Ok(None);
        }
        if via.is_none() {
            match self.auth {
                Auth::Off => {}
                Auth::Warn => self.warn_once(&format!("plain {me}"), &format!("{me} sent {op} on plain ledger/req")),
                Auth::Enforce => return bad("send requests on ledger/req/<your address>"),
            }
        }
        if self.auth != Auth::Off && me != ADMIN && !self.take_token(&me) {
            return bad(format!("rate limited: at most {RATE_PER_S} requests a second"));
        }
        let req = Req { topic, corr, expiry };
        self.run(publish, |l| l.serve(op, &me, &args, &req, payload))
    }

    /// dispatch, once: a redelivered copy of a mutating request (ledgerd crashed or reconnected before acking it) gets
    /// the first answer again instead of a second lease or request. The key includes the payload, so a client that
    /// reuses CorrelationData for different requests is not mistaken for a duplicate.
    fn serve(&mut self, op: &str, me: &str, args: &Map, req: &Req, payload: &[u8]) -> Res<Option<Map>> {
        if READ_ONLY.contains(&op) {
            return self.dispatch(op, me, args, req);
        }
        let key = Sha256::digest([req.topic.as_bytes(), req.corr, payload].join(&0u8)).to_vec();
        let stored: Option<Option<String>> = self
            .db
            .prepare_cached("SELECT body FROM replies WHERE key=?")?
            .query_row([&key], |r| r.get(0))
            .map(Some)
            .or_else(|e| if e == rusqlite::Error::QueryReturnedNoRows { Ok(None) } else { Err(e) })?;
        if let Some(body) = stored {
            return Ok(body.and_then(|b| loads(b.as_bytes())).and_then(|v| v.as_object().cloned()));
        }
        let body = self.dispatch(op, me, args, req)?;
        let text = body.as_ref().map(dumps_map);
        // The broker can redeliver the request until its expiry, so the answer is kept at least that long. Rows are
        // purged DEDUPE_S after ts, here and in the v0.1.0 rollback alike, so a longer stay moves ts forward.
        let keep = req.expiry.map_or(DEDUPE_S, |e| e.clamp(DEDUPE_S, MAX_KEEP));
        self.exec("INSERT INTO replies VALUES (?, ?, ?)", params![key, text, now() + keep - DEDUPE_S])?;
        Ok(body)
    }

    fn dispatch(&mut self, op: &str, me: &str, args: &Map, req: &Req) -> Res<Option<Map>> {
        let now = now();
        let body = match op {
            "init" => self.op_init(me, now)?,
            // Under auth, identities are real, so everything at once is for admin (the console) alone.
            "dump" if me != ADMIN && self.auth == Auth::Enforce => return bad("dump: admin only"),
            "dump" => {
                if me != ADMIN && self.auth == Auth::Warn {
                    self.warn_once(&format!("dump {me}"), &format!("{me} read the whole ledger with dump"));
                }
                self.op_dump(now)?
            }
            "ping" => {
                let mut body = obj(json!({"service": "ledgerd", "version": env!("CARGO_PKG_VERSION")}));
                if self.auth != Auth::Off {
                    body.insert("auth".into(), self.auth.as_str().into());
                }
                body
            }
            "worktrees" => self.op_worktrees(args.get("all") == Some(&Value::Bool(true)))?,
            "claims" => self.op_claims(me, args.get("room").and_then(Value::as_str))?,
            "holders" => self.op_holders(args.get("resource").and_then(Value::as_str).unwrap_or_default())?,
            // the holder, a moderator or admin, whether or not they have a ledger: it touches no resource
            "unclaim" => {
                let a = |k: &str| args.get(k).and_then(Value::as_str).unwrap_or_default();
                self.op_unclaim(me, a("room"), a("key"))?
            }
            _ => {
                // admin acting on another agent's ledger: as that owner, recorded and announced as admin
                let by = args.get("on_behalf").and_then(Value::as_str);
                let me = match by {
                    Some(_) if me != ADMIN => return bad("on_behalf: only admin acts for another ledger"),
                    Some(owner) => {
                        if self.one("SELECT 1 FROM ledgers WHERE owner=?", [owner])?.is_none() {
                            return bad(format!("no ledger for {owner}"));
                        }
                        owner
                    }
                    None => {
                        if self.exec("UPDATE ledgers SET seen=? WHERE owner=?", params![now, me])? == 0 {
                            return bad(format!("no ledger for {me}, call init first"));
                        }
                        me
                    }
                };
                let by = by.map(|_| ADMIN);
                let a = |k: &str| args.get(k).and_then(Value::as_str);
                let n = |k: &str| args.get(k).and_then(Value::as_f64).unwrap_or(0.0);
                let body = match op {
                    "add" => self.op_add(me, now, a("name").unwrap_or_default(), args)?,
                    "update" => self.op_update(me, now, a("name").unwrap_or_default(), args)?,
                    "remove" => self.op_remove(me, a("name").unwrap_or_default(), by)?,
                    "ledger" => self.op_ledger(me, a("owner"))?,
                    "public" => self.op_public()?,
                    "request" => {
                        self.op_request(me, now, a("resource").unwrap_or_default(), a("level").unwrap_or_default())?
                    }
                    "grant" => self.op_grant(me, a("request_id").unwrap_or_default())?,
                    "deny" => self.op_deny(me, a("request_id").unwrap_or_default(), by)?,
                    "grants" => self.op_grants(me)?,
                    "acquire" => {
                        let (res, lvl) = (a("resource").unwrap_or_default(), a("level").unwrap_or_default());
                        let place = a("path").map(|p| (p, a("git_common_dir")));
                        match self.op_acquire(me, now, res, lvl, n("ttl"), n("wait"), place, req)? {
                            Some(body) => body,
                            None => return Ok(None),
                        }
                    }
                    "renew" => self.op_renew(me, now, a("lease_id").unwrap_or_default(), n("ttl"))?,
                    "release" => self.op_release(me, a("lease_id").unwrap_or_default(), by)?,
                    "cancel" => self.op_cancel(me, req.topic, a("correlation").unwrap_or_default())?,
                    "leases" => self.op_leases(me)?,
                    "claim" => {
                        let ttl = args.get("ttl").and_then(Value::as_f64);
                        let (room, key) = (a("room").unwrap_or_default(), a("key").unwrap_or_default());
                        self.op_claim(me, now, room, key, ttl, a("note").unwrap_or_default())?
                    }
                    "lease_history" => {
                        self.op_lease_history(me, a("resource").unwrap_or_default(), n("last") as i64)?
                    }
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
        if me == ADMIN && name.starts_with("wt-") {
            return bad("wt- names in admin's ledger are worktrees, made by acquire with a path");
        }
        if self.one("SELECT 1 FROM resources WHERE id=?", [&rid])?.is_some() {
            return bad(format!("{rid} exists, use update"));
        }
        let mine = "SELECT count(*) FROM resources WHERE owner=? AND NOT (owner='admin' AND name LIKE 'wt-%')";
        if self.count(mine, [me])? >= MAX_RESOURCES {
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

    fn op_remove(&mut self, me: &str, name: &str, by: Option<&str>) -> Res<Map> {
        let rid = format!("{me}/{name}");
        self.resource(&rid, Some(me))?;
        let why = match by {
            Some(by) => format!("{rid} was removed by {by}"),
            None => format!("{rid} was removed"),
        };
        self.forget(std::slice::from_ref(&rid), &why)?;
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
        let owner = s(&res, "owner").to_owned();
        if !auto {
            if self.count("SELECT count(*) FROM requests WHERE holder=? AND status='pending'", [me])? >= MAX_PENDING {
                return bad(format!("at most {MAX_PENDING} pending requests per agent"));
            }
            // each pending request DMs the owner, so its inbox is bounded too
            let waiting = self.count("SELECT count(*) FROM requests WHERE owner=? AND status='pending'", [&owner])?;
            if waiting >= MAX_PENDING_PER_OWNER {
                return bad(format!(
                    "{owner} has {MAX_PENDING_PER_OWNER} pending requests; try again once it has answered some"
                ));
            }
        }
        let rq = random_hex(8);
        let status = if auto { "granted" } else { "pending" };
        self.exec(
            "INSERT INTO requests VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![rq, resource, owner, me, lvl, status, now],
        )?;
        if !auto {
            self.notify(
                &owner,
                format!(
                    "{me} requests {lvl} access to {resource} (request_id {rq}); answer with agent-bridge grant \
                     {owner} {rq} or agent-bridge deny {owner} {rq}"
                ),
            );
        }
        self.request_body(&rq)
    }

    /// The request rq on one of me's resources, set to status; None when it already had that status.
    fn decide(&mut self, me: &str, rq: &str, status: &str) -> Res<Option<Map>> {
        let Some(row) = self.one("SELECT * FROM requests WHERE id=? AND owner=?", [rq, me])? else {
            return bad(format!("no such request: {rq}"));
        };
        if s(&row, "status") == status {
            return Ok(None);
        }
        self.exec("UPDATE requests SET status=?, ts=? WHERE id=?", params![status, now(), rq])?;
        Ok(Some(row))
    }

    fn op_grant(&mut self, me: &str, rq: &str) -> Res<Map> {
        if let Some(row) = self.decide(me, rq, "granted")? {
            let text =
                format!("{me} granted your {} request for {} (request_id {rq})", s(&row, "level"), s(&row, "resource"));
            self.notify(s(&row, "holder"), text);
        }
        self.request_body(rq)
    }

    fn op_deny(&mut self, owner: &str, rq: &str, by: Option<&str>) -> Res<Map> {
        let Some(row) = self.decide(owner, rq, "denied")? else { return self.request_body(rq) };
        let me = by.unwrap_or(owner);
        let (holder, resource, lvl) = (s(&row, "holder"), s(&row, "resource"), s(&row, "level"));
        let left = self.held(holder, resource)?;
        let mut ended = Vec::new();
        for lease in self.rows("SELECT * FROM leases WHERE resource=? AND holder=?", [resource, holder])? {
            if level(s(&lease, "level")) > left {
                self.exec("DELETE FROM leases WHERE id=?", [s(&lease, "id")])?;
                self.event("revoke", &lease, Some(me))?;
                ended.push(s(&lease, "id").to_owned());
            }
        }
        let text = if s(&row, "status") == "granted" {
            let ending = if ended.is_empty() { String::new() } else { format!(", ending lease {}", ended.join(", ")) };
            format!("{me} revoked your {lvl} grant on {resource} (request_id {rq}){ending}")
        } else {
            format!("{me} denied your {lvl} request for {resource} (request_id {rq})")
        };
        self.notify(holder, text);
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
        self.event("acquire", &row, None)?;
        Ok(lease_view(&row))
    }

    /// Record what happened to a lease (its row as in the leases table) in its resource's bounded history.
    fn event(&mut self, event: &str, lease: &Map, actor: Option<&str>) -> Res<()> {
        let resource = s(lease, "resource");
        // A run of renewals of one lease is one row with the latest time, so a long acquire --exec keeps the history.
        if event == "renew"
            && self.exec(
                "UPDATE lease_events SET ts=? WHERE seq=(SELECT max(seq) FROM lease_events WHERE resource=?) \
                 AND event='renew' AND lease_id=?",
                params![now(), resource, s(lease, "id")],
            )? > 0
        {
            return Ok(());
        }
        self.exec(
            "INSERT INTO lease_events(resource, ts, event, lease_id, holder, level, token, actor) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                resource,
                now(),
                event,
                s(lease, "id"),
                s(lease, "holder"),
                s(lease, "level"),
                lease.get("token").and_then(Value::as_i64),
                actor
            ],
        )?;
        self.exec(
            "DELETE FROM lease_events WHERE resource=? AND seq<=(SELECT seq FROM lease_events WHERE resource=? \
             ORDER BY seq DESC LIMIT 1 OFFSET ?)",
            params![resource, resource, MAX_EVENTS],
        )?;
        Ok(())
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
        place: Option<(&str, Option<&str>)>,
        req: &Req,
    ) -> Res<Option<Map>> {
        let mut created = None;
        if let Some((path, common)) = place {
            match self.worktree(me, now, resource, lvl, path, common)? {
                Ok(c) => created = Some(c),
                Err(refusal) => return Ok(Some(refusal)),
            }
        }
        self.resource(resource, None)?;
        if self.held(me, resource)? < level(lvl) {
            return bad(format!("no {lvl} grant on {resource}, request it first"));
        }
        if self.count("SELECT count(*) FROM leases WHERE holder=?", [me])? >= MAX_LEASES {
            return bad(format!("at most {MAX_LEASES} leases per agent"));
        }
        if self.one("SELECT 1 FROM waiters WHERE resource=?", [resource])?.is_none() && self.free(resource, lvl)? {
            let mut body = obj(json!({"lease": self.lease(resource, me, lvl, ttl)?}));
            if let Some(c) = created {
                body.insert("created".into(), c.into());
            }
            return Ok(Some(body));
        }
        let busy = if lvl == "write" { "" } else { " AND level='write'" };
        let blocking =
            self.rows(&format!("SELECT id, holder, level FROM leases WHERE resource=?{busy}"), [resource])?;
        if !blocking.is_empty() && blocking.iter().all(|l| s(l, "holder") == me) {
            let mine = &blocking[0];
            return bad(format!(
                "you already hold {} lease {} on {resource}; renew it, or release it first",
                s(mine, "level"),
                s(mine, "id")
            ));
        }
        // Not past the moment the client stops listening (its request's expiry), or the lease would go to nobody.
        let wait = req.expiry.map_or(wait, |e| wait.min(e - REPLY_MARGIN));
        if wait <= 0.0 {
            return Ok(Some(obj(json!({"ok": false, "error": "busy", "holders": self.holders(resource)?}))));
        }
        if self.count("SELECT count(*) FROM waiters WHERE resource=?", [resource])? >= MAX_WAITERS {
            return bad(format!("at most {MAX_WAITERS} waiters per resource"));
        }
        self.exec(
            "INSERT INTO waiters(resource, holder, level, ttl, deadline, topic, corr, queued) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            params![resource, me, lvl, ttl, now + wait, req.topic, req.corr, now],
        )?;
        Ok(None)
    }

    /// acquire with a path: the worktree's resource admin/wt-<hash> is made on demand (public, max write, the path as
    /// its description, an access spec) and me's request for lvl filed as any request is. Ok(Ok(created)) to go on
    /// with the acquire, Ok(Err(reply)) when the request is left pending; that reply commits, so the request stays.
    fn worktree(
        &mut self,
        me: &str,
        now: f64,
        resource: &str,
        lvl: &str,
        path: &str,
        common: Option<&str>,
    ) -> Res<Result<bool, Map>> {
        if resource != worktree_resource(path) {
            return bad(format!("path: does not match {resource} (its id is the sha256 of the realpath)"));
        }
        let name = &resource[ADMIN.len() + 1..];
        let created = self.one("SELECT 1 FROM resources WHERE id=?", [resource])?.is_none();
        if created {
            let wt = "SELECT count(*) FROM resources WHERE owner=? AND name LIKE 'wt-%'";
            if self.count(wt, [ADMIN])? >= MAX_WORKTREES {
                return bad(format!("at most {MAX_WORKTREES} worktrees"));
            }
            self.exec("INSERT OR IGNORE INTO ledgers VALUES (?, ?, ?)", params![ADMIN, now, now])?;
            self.exec(
                "INSERT INTO resources VALUES (?, ?, ?, ?, 'public', 'write', ?, ?)",
                params![resource, ADMIN, name, path, now, now],
            )?;
        }
        let spec = json!({"kind": "worktree", "path": path, "git_common_dir": common, "enforcement": "hook"});
        self.exec("INSERT OR IGNORE INTO specs VALUES (?, ?)", params![resource, dumps(&spec)])?;
        if me == ADMIN {
            return Ok(Ok(created));
        }
        let request = self.op_request(me, now, resource, lvl)?;
        let row = request.get("request").cloned().unwrap_or_default();
        if row.get("status").and_then(Value::as_str) == Some("pending") {
            let id = row.get("id").and_then(Value::as_str).unwrap_or_default();
            let mut body = error(&format!("your {lvl} request for {resource} is pending with admin (request_id {id})"));
            body.insert("request".into(), row);
            return Ok(Err(body));
        }
        Ok(Ok(created))
    }

    /// The worktrees with a live lease, or every one with all, for the guard and the console. Anyone may ask.
    fn op_worktrees(&mut self, all: bool) -> Res<Map> {
        let join = if all { "LEFT JOIN" } else { "JOIN" };
        let rows = self.rows(
            &format!(
                "SELECT r.id, r.description, s.spec, l.holder, l.level, l.id AS lease_id, l.expires \
                 FROM resources r LEFT JOIN specs s ON s.resource=r.id {join} leases l ON l.resource=r.id \
                 WHERE r.owner=? AND r.name LIKE 'wt-%' ORDER BY r.id, l.expires"
            ),
            [ADMIN],
        )?;
        let worktrees: Vec<Value> = rows
            .iter()
            .map(|r| {
                let spec = r.get("spec").and_then(Value::as_str).and_then(|t| loads(t.as_bytes())).unwrap_or_default();
                let path = spec.get("path").cloned().unwrap_or_else(|| r["description"].clone());
                json!({"resource": r["id"], "path": path,
                       "git_common_dir": spec.get("git_common_dir").cloned().unwrap_or(Value::Null),
                       "holder": r["holder"], "level": r["level"], "lease_id": r["lease_id"], "expires_at": r["expires"]})
            })
            .collect();
        Ok(obj(json!({"worktrees": worktrees})))
    }

    fn op_renew(&mut self, me: &str, now: f64, lease_id: &str, ttl: f64) -> Res<Map> {
        if self.exec("UPDATE leases SET expires=? WHERE id=? AND holder=?", params![now + ttl, lease_id, me])? == 0 {
            return self.no_lease(lease_id);
        }
        let row = self.one("SELECT * FROM leases WHERE id=?", [lease_id])?.unwrap_or_default();
        self.event("renew", &row, None)?;
        Ok(obj(json!({"lease": lease_view(&row)})))
    }

    fn op_release(&mut self, owner: &str, lease_id: &str, by: Option<&str>) -> Res<Map> {
        let Some(row) = self.one(
            "SELECT l.* FROM leases l JOIN resources r ON r.id=l.resource WHERE l.id=? AND (l.holder=? OR r.owner=?)",
            [lease_id, owner, owner],
        )?
        else {
            return self.no_lease(lease_id);
        };
        self.exec("DELETE FROM leases WHERE id=?", [lease_id])?;
        let (resource, holder) = (s(&row, "resource"), s(&row, "holder"));
        let me = by.unwrap_or(owner);
        if holder == me {
            self.event("release", &row, None)?;
        } else {
            self.event("break", &row, Some(me))?;
            self.notify(holder, format!("{me} ended your {} lease {lease_id} on {resource}", s(&row, "level")));
        }
        self.pump(resource)?;
        Ok(Map::new())
    }

    fn op_leases(&mut self, me: &str) -> Res<Map> {
        let rows = self.rows(
            "SELECT l.*, r.owner FROM leases l JOIN resources r ON r.id=l.resource WHERE l.holder=? ORDER BY l.expires",
            [me],
        )?;
        let leases: Vec<Map> = rows
            .iter()
            .map(|row| {
                let mut view = lease_view(row);
                view.insert("owner".into(), row["owner"].clone());
                view
            })
            .collect();
        Ok(obj(json!({"leases": leases})))
    }

    /// The last events of one of me's resources, oldest first.
    fn op_lease_history(&mut self, me: &str, resource: &str, last: i64) -> Res<Map> {
        self.resource(resource, Some(me))?;
        let rows = self.rows(
            "SELECT * FROM (SELECT * FROM lease_events WHERE resource=? ORDER BY seq DESC LIMIT ?) ORDER BY seq",
            params![resource, last],
        )?;
        let events: Vec<Value> = rows
            .iter()
            .map(|r| {
                let mut e = obj(json!({"ts": r["ts"], "event": r["event"], "lease_id": r["lease_id"],
                                       "holder": r["holder"], "level": r["level"]}));
                if !r["token"].is_null() {
                    e.insert("token".into(), r["token"].clone());
                }
                if !r["actor"].is_null() {
                    e.insert("by".into(), r["actor"].clone());
                }
                Value::Object(e)
            })
            .collect();
        Ok(obj(json!({"resource": resource, "events": events})))
    }

    /// Claim key in room for me, or refresh my own claim's ttl and note; another holder's claim is refused, naming it.
    fn op_claim(&mut self, me: &str, now: f64, room: &str, key: &str, ttl: Option<f64>, note: &str) -> Res<Map> {
        let expires = ttl.map(|t| now + t);
        match self.one("SELECT * FROM claims WHERE room=? AND key=?", [room, key])? {
            Some(c) if s(&c, "holder") != me => {
                let since = c["since"].as_f64().unwrap_or_default();
                let mut body =
                    error(&format!("{key} in #{room} is claimed by {} since {}", s(&c, "holder"), utc_hms(since)));
                body.insert("claim".into(), Value::Object(claim_view(&c)));
                return Ok(body);
            }
            Some(_) => {
                self.exec(
                    "UPDATE claims SET expires=?, note=? WHERE room=? AND key=?",
                    params![expires, note, room, key],
                )?;
            }
            None => {
                if self.count("SELECT count(*) FROM claims WHERE holder=?", [me])? >= MAX_CLAIMS_HELD {
                    return bad(format!("at most {MAX_CLAIMS_HELD} claims per agent"));
                }
                if self.count("SELECT count(*) FROM claims WHERE room=?", [room])? >= MAX_CLAIMS_ROOM {
                    return bad(format!("at most {MAX_CLAIMS_ROOM} claims per room"));
                }
                self.exec("INSERT INTO claims VALUES (?, ?, ?, ?, ?, ?)", params![room, key, me, now, expires, note])?;
            }
        }
        let c = self.one("SELECT * FROM claims WHERE room=? AND key=?", [room, key])?.unwrap_or_default();
        Ok(obj(json!({"claim": claim_view(&c)})))
    }

    /// Release a claim: its holder, admin, or a moderator of its room; someone else's release DMs the holder.
    fn op_unclaim(&mut self, me: &str, room: &str, key: &str) -> Res<Map> {
        let Some(c) = self.one("SELECT * FROM claims WHERE room=? AND key=?", [room, key])? else {
            return bad(format!("no claim {key} in #{room}"));
        };
        let holder = s(&c, "holder");
        let moderator = me == ADMIN || self.moderators.get(room).is_some_and(|m| m.iter().any(|x| x == me));
        if holder != me && !moderator {
            return bad(format!(
                "{key} in #{room} is claimed by {holder}; only they, a moderator of #{room} or admin can release it"
            ));
        }
        self.exec("DELETE FROM claims WHERE room=? AND key=?", [room, key])?;
        if holder != me {
            self.notify(holder, format!("{me} released your claim {key} in #{room}"));
        }
        Ok(Map::new())
    }

    /// One room's claims, oldest first; every room's for admin alone.
    fn op_claims(&mut self, me: &str, room: Option<&str>) -> Res<Map> {
        let rows = match room {
            Some(room) => self.rows("SELECT * FROM claims WHERE room=? ORDER BY since", [room])?,
            None if me == ADMIN => self.rows("SELECT * FROM claims ORDER BY room, since", [])?,
            None => return bad("room: required (only admin lists every room)"),
        };
        let claims: Vec<Value> = rows.iter().map(|c| Value::Object(claim_view(c))).collect();
        let mut body = obj(json!({"claims": claims}));
        if let Some(room) = room {
            body.insert("room".into(), room.into());
        }
        Ok(body)
    }

    /// Who holds resource now, and who waits for it, for anyone who asks.
    fn op_holders(&mut self, resource: &str) -> Res<Map> {
        self.resource(resource, None)?;
        let holders: Vec<Value> = self
            .rows("SELECT id, holder, level, expires FROM leases WHERE resource=? ORDER BY expires", [resource])?
            .iter()
            .map(|r| json!({"holder": r["holder"], "level": r["level"], "expires_at": r["expires"], "lease_id": r["id"]}))
            .collect();
        let waiting: Vec<Value> = self
            .rows("SELECT holder, level, queued FROM waiters WHERE resource=? ORDER BY seq", [resource])?
            .iter()
            .map(|w| json!({"holder": w["holder"], "level": w["level"], "queued_at": w["queued"]}))
            .collect();
        Ok(obj(json!({"resource": resource, "holders": holders, "waiting": waiting})))
    }

    /// The moderators of room, from its retained record (published at QoS 1, so this ledgerd's persistent session
    /// receives every change in order); an empty payload deletes the room.
    pub fn room(&mut self, room: &str, payload: &[u8]) {
        match agent_bridge::rooms::parse_room(payload) {
            Some(rec) => {
                let mods = rec.get("moderators").and_then(Value::as_array).cloned().unwrap_or_default();
                let mods = mods.iter().filter_map(Value::as_str).map(str::to_owned).collect();
                self.moderators.insert(room.to_owned(), mods);
            }
            None => {
                self.moderators.remove(room);
            }
        }
    }

    /// The refusal for a lease me may not touch: another agent's, or one that is gone.
    fn no_lease<T>(&self, lease_id: &str) -> Res<T> {
        match self.one("SELECT holder FROM leases WHERE id=?", [lease_id])? {
            Some(row) => bad(format!("lease {lease_id} is not yours, {} holds it", s(&row, "holder"))),
            None => bad(format!("no such lease: {lease_id} (it may have expired)")),
        }
    }

    /// Drop me's waiter queued under this ResponseTopic and correlation (hex), so a client that stops waiting is never
    /// handed a lease. A lease already granted to it went out on topic before this reply, so the client sees it first.
    fn op_cancel(&mut self, me: &str, topic: &str, correlation: &str) -> Res<Map> {
        let corr: Vec<u8> = (0..correlation.len() / 2)
            .filter_map(|i| u8::from_str_radix(&correlation[2 * i..2 * i + 2], 16).ok())
            .collect();
        let queued = format!("{WAITER} WHERE holder=? AND topic=? AND corr=?");
        let mut cancelled = 0;
        for (seq, w, _) in self.waiters(&queued, params![me, topic, corr])? {
            cancelled += self.exec("DELETE FROM waiters WHERE seq=?", [seq])?;
            self.pump(s(&w, "resource"))?;
        }
        Ok(obj(json!({"cancelled": cancelled})))
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
            body.insert("ledgerd".into(), json!({"impl": "rust", "version": env!("CARGO_PKG_VERSION")}));
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
            for (table, col) in [
                ("waiters", "resource"),
                ("leases", "resource"),
                ("lease_events", "resource"),
                ("specs", "resource"),
                ("requests", "resource"),
                ("resources", "id"),
            ] {
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
        for lease in self.rows("SELECT * FROM leases WHERE holder=?", [who])? {
            self.event("drop", &lease, Some(who))?;
        }
        self.exec("DELETE FROM leases WHERE holder=?", [who])?;
        self.exec("DELETE FROM requests WHERE holder=? OR owner=?", [who, who])?;
        self.exec("DELETE FROM claims WHERE holder=?", [who])?;
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
        let expired = self.rows("SELECT * FROM leases WHERE expires<=? ORDER BY resource, expires", [now])?;
        let mut freed = BTreeSet::new();
        for lease in &expired {
            self.exec("DELETE FROM leases WHERE id=?", [s(lease, "id")])?;
            self.event("expire", lease, None)?;
            let (id, resource) = (s(lease, "id"), s(lease, "resource"));
            let text = format!("your {} lease {id} on {resource} expired without renewal", s(lease, "level"));
            self.notify(s(lease, "holder"), text);
            freed.insert(resource.to_owned());
        }
        for resource in freed {
            self.pump(&resource)?;
        }
        for (seq, w, corr) in self.waiters(&format!("{WAITER} WHERE deadline<=? ORDER BY seq"), [now])? {
            if self.exec("DELETE FROM waiters WHERE seq=?", [seq])? > 0 {
                let holders = self.holders(s(&w, "resource"))?;
                self.reply(s(&w, "topic"), corr, &obj(json!({"ok": false, "error": "busy", "holders": holders})));
                self.pump(s(&w, "resource"))?;
            }
        }
        for c in self.rows("SELECT * FROM claims WHERE expires<=?", [now])? {
            let (room, key) = (s(&c, "room"), s(&c, "key"));
            self.exec("DELETE FROM claims WHERE room=? AND key=?", [room, key])?;
            self.notify(s(&c, "holder"), format!("your claim {key} in #{room} expired"));
        }
        if now - self.swept >= self.sweep_every() {
            self.swept = now;
            let cutoff = now - self.stale_s;
            self.exec("DELETE FROM presence WHERE keep=0 AND seen<?", [cutoff])?;
            // Granted requests are live permissions and stay; undecided and denied ones go once stale.
            self.exec("DELETE FROM requests WHERE status IN ('pending', 'denied') AND ts<?", [cutoff])?;
            // events of resources a rollback ledgerd removed, which knows nothing of this table
            self.exec("DELETE FROM lease_events WHERE resource NOT IN (SELECT id FROM resources)", [])?;
            self.exec("DELETE FROM specs WHERE resource NOT IN (SELECT id FROM resources)", [])?;
            for r in self
                // admin's ledger holds the worktrees, which outlive any session
                .rows(
                    "SELECT owner FROM ledgers WHERE seen<? AND owner NOT IN (SELECT addr FROM presence) \
                     AND owner<>'admin'",
                    [cutoff],
                )?
            {
                self.drop_ledger(s(&r, "owner"))?;
            }
        }
        Ok(())
    }

    pub fn next_deadline(&self) -> f64 {
        let mut t = self.swept + self.sweep_every();
        for sql in
            ["SELECT min(expires) FROM leases", "SELECT min(deadline) FROM waiters", "SELECT min(expires) FROM claims"]
        {
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

fn claim_view(c: &Map) -> Map {
    obj(json!({"room": c["room"], "key": c["key"], "holder": c["holder"], "since": c["since"],
               "expires_at": c.get("expires").cloned().unwrap_or(Value::Null), "note": c["note"]}))
}

/// HH:MM:SS UTC of a timestamp, for refusals that name a moment.
fn utc_hms(ts: f64) -> String {
    let day = (ts.max(0.0) as u64) % 86400;
    format!("{:02}:{:02}:{:02} UTC", day / 3600, day % 3600 / 60, day % 60)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A temp dir removed when the test ends, a failed one too.
    struct Dir(std::path::PathBuf);

    impl Dir {
        fn new(name: &str) -> Dir {
            Dir(std::env::temp_dir().join(format!("ledgerd-{name}-{}", std::process::id())))
        }
        fn join(&self, file: &str) -> std::path::PathBuf {
            self.0.join(file)
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// One request as me through handle, with the reply and every notification it published.
    fn ask(ledger: &mut Ledger, n: &mut u32, me: &str, body: Value) -> (Map, Vec<Outgoing>) {
        let mut out = Vec::new();
        *n += 1;
        let corr = n.to_be_bytes();
        ledger.handle(
            &mut |o| out.push(o),
            dumps(&body).as_bytes(),
            Some("ledger/resp/t"),
            Some(&corr),
            Some(me),
            None,
        );
        let reply = out.iter().find(|o| o.2.as_deref() == Some(&corr[..])).map(|o| o.1.clone()).unwrap_or_default();
        (loads(reply.as_bytes()).and_then(|v| v.as_object().cloned()).unwrap_or_default(), out)
    }

    #[test]
    fn lease_events_are_bounded_and_expiry_tells_the_holder() {
        let dir = Dir::new("events");
        let mut ledger = Ledger::open(&dir.join("ledger.db"), 3.0).ok().expect("opens");
        let n = &mut 0;
        for who in ["o-1", "h-1"] {
            ask(&mut ledger, n, who, json!({"op": "init"}));
        }
        ask(&mut ledger, n, "o-1", json!({"op": "add", "name": "r", "visibility": "public", "max_level": "write"}));
        ask(&mut ledger, n, "h-1", json!({"op": "request", "resource": "o-1/r", "level": "write"}));
        let (pong, _) = ask(&mut ledger, n, "nobody-0", json!({"op": "ping"}));
        assert_eq!(pong["service"], "ledgerd", "ping needs no ledger");
        for _ in 0..(MAX_EVENTS + 5) {
            let (r, _) = ask(&mut ledger, n, "h-1", json!({"op": "acquire", "resource": "o-1/r", "level": "write"}));
            ask(&mut ledger, n, "h-1", json!({"op": "release", "lease_id": r["lease"]["lease_id"]}));
        }
        let (mine, _) = ask(&mut ledger, n, "h-1", json!({"op": "leases"}));
        assert_eq!(mine["leases"], json!([]));
        let history = json!({"op": "lease_history", "resource": "o-1/r", "last": 500});
        let (h, _) = ask(&mut ledger, n, "o-1", history.clone());
        assert_eq!(h["events"].as_array().map(Vec::len), Some(MAX_EVENTS as usize), "bounded per resource");
        assert_eq!(h["events"][MAX_EVENTS as usize - 1]["event"], "release", "oldest first, newest last");
        let (refused, _) = ask(&mut ledger, n, "h-1", history);
        assert_eq!(refused["error"], "no such resource: o-1/r", "only the owner reads it");

        let (r, _) =
            ask(&mut ledger, n, "h-1", json!({"op": "acquire", "resource": "o-1/r", "level": "write", "ttl": 1}));
        let lid = r["lease"]["lease_id"].as_str().unwrap_or_default().to_owned();
        let (mine, _) = ask(&mut ledger, n, "h-1", json!({"op": "leases"}));
        assert_eq!(mine["leases"][0]["owner"], "o-1");
        ledger.db.execute("UPDATE leases SET expires=0", []).expect("ages the lease");
        let mut out = Vec::new();
        ledger.run(&mut |o| out.push(o), Ledger::tick).ok().expect("ticks");
        let dm: Vec<&Outgoing> = out.iter().filter(|o| o.0 == "inbox/h-1").collect();
        assert_eq!(dm.len(), 1, "{out:?}");
        assert!(dm[0].1.contains(&format!("your write lease {lid} on o-1/r expired without renewal")), "{}", dm[0].1);
        let (h, _) = ask(&mut ledger, n, "o-1", json!({"op": "lease_history", "resource": "o-1/r", "last": 1}));
        assert_eq!(h["events"][0]["event"], "expire");
        ask(&mut ledger, n, "o-1", json!({"op": "remove", "name": "r"}));
        assert_eq!(ledger.count("SELECT count(*) FROM lease_events", []).ok(), Some(0), "removed with the resource");
    }

    #[test]
    fn claims_are_held_until_released_expired_or_dropped() {
        let dir = Dir::new("claims");
        let mut ledger = Ledger::open(&dir.join("ledger.db"), 3.0).ok().expect("opens");
        let n = &mut 0;
        for who in ["a-1", "b-1"] {
            ask(&mut ledger, n, who, json!({"op": "init"}));
        }
        let claim = |key: &str| json!({"op": "claim", "room": "mrs", "key": key, "note": "on it"});
        let (mine, _) = ask(&mut ledger, n, "a-1", claim("BX-1"));
        assert_eq!((mine["claim"]["holder"].as_str(), mine["claim"]["expires_at"].is_null()), (Some("a-1"), true));
        let (taken, _) = ask(&mut ledger, n, "b-1", claim("BX-1"));
        let err = taken["error"].as_str().unwrap_or_default();
        assert!(err.starts_with("BX-1 in #mrs is claimed by a-1 since ") && err.ends_with(" UTC"), "{err}");
        assert_eq!(taken["claim"]["holder"], "a-1", "the refusal carries the claim");
        let (again, _) = ask(&mut ledger, n, "a-1", json!({"op": "claim", "room": "mrs", "key": "BX-1", "ttl": 60}));
        assert!(again["claim"]["expires_at"].is_number() && again["claim"]["note"] == "", "a re-claim replaces both");

        let (no, _) = ask(&mut ledger, n, "b-1", json!({"op": "unclaim", "room": "mrs", "key": "BX-1"}));
        assert_eq!(
            no["error"],
            "BX-1 in #mrs is claimed by a-1; only they, a moderator of #mrs or admin can release it"
        );
        let record = json!({"mode": "open", "moderators": ["m-1"], "rules": "", "muted": [], "banned": [], "by": "admin",
                            "ts": 1.0});
        ledger.room("mrs", dumps(&record).as_bytes());
        let (ok, out) = ask(&mut ledger, n, "m-1", json!({"op": "unclaim", "room": "mrs", "key": "BX-1"}));
        assert_eq!(ok["ok"], true, "{ok:?}");
        assert!(out.iter().any(|o| o.0 == "inbox/a-1" && o.1.contains("m-1 released your claim BX-1 in #mrs")));
        ledger.room("mrs", b"");
        ask(&mut ledger, n, "a-1", claim("BX-2"));
        let (no, _) = ask(&mut ledger, n, "m-1", json!({"op": "unclaim", "room": "mrs", "key": "BX-2"}));
        assert!(no["error"].as_str().is_some_and(|e| e.contains("only they")), "the room record was cleared");
        let (ok, _) = ask(&mut ledger, n, "admin", json!({"op": "unclaim", "room": "mrs", "key": "BX-2"}));
        assert_eq!(ok["ok"], true);
        let ledgers = ledger.count("SELECT count(*) FROM ledgers WHERE owner IN ('admin', 'm-1')", []).ok();
        assert_eq!(ledgers, Some(0), "a moderator and admin release claims without a ledger, and none is made");
        let (gone, _) = ask(&mut ledger, n, "a-1", json!({"op": "unclaim", "room": "mrs", "key": "BX-2"}));
        assert_eq!(gone["error"], "no claim BX-2 in #mrs");

        ask(&mut ledger, n, "a-1", claim("CC-1"));
        ask(&mut ledger, n, "b-1", json!({"op": "claim", "room": "dev", "key": "H1"}));
        let (list, _) = ask(&mut ledger, n, "nobody-0", json!({"op": "claims", "room": "mrs"}));
        assert_eq!(
            (list["room"].as_str(), list["claims"].as_array().map(Vec::len)),
            (Some("mrs"), Some(1)),
            "no ledger needed"
        );
        let (refused, _) = ask(&mut ledger, n, "a-1", json!({"op": "claims"}));
        assert_eq!(refused["error"], "room: required (only admin lists every room)");
        let (all, _) = ask(&mut ledger, n, "admin", json!({"op": "claims"}));
        let rooms: Vec<&str> =
            all["claims"].as_array().into_iter().flatten().filter_map(|c| c["room"].as_str()).collect();
        assert_eq!(rooms, ["dev", "mrs"]);

        ask(&mut ledger, n, "a-1", json!({"op": "claim", "room": "mrs", "key": "T-1", "ttl": 30}));
        ledger.db.execute("UPDATE claims SET expires=0 WHERE key='T-1'", []).expect("ages the claim");
        let mut out = Vec::new();
        ledger.run(&mut |o| out.push(o), Ledger::tick).ok().expect("ticks");
        assert!(out.iter().any(|o| o.0 == "inbox/a-1" && o.1.contains("your claim T-1 in #mrs expired")), "{out:?}");
        ask(&mut ledger, n, "a-1", json!({"op": "drop"}));
        let (left, _) = ask(&mut ledger, n, "admin", json!({"op": "claims"}));
        assert_eq!(left["claims"].as_array().map(Vec::len), Some(1), "only b-1's claim is left: {left:?}");

        ask(&mut ledger, n, "b-1", json!({"op": "add", "name": "db", "visibility": "public", "max_level": "write"}));
        ask(&mut ledger, n, "b-1", json!({"op": "acquire", "resource": "b-1/db", "level": "write"}));
        let (h, _) = ask(&mut ledger, n, "nobody-0", json!({"op": "holders", "resource": "b-1/db"}));
        assert_eq!((h["holders"][0]["holder"].as_str(), h["waiting"].as_array().map(Vec::len)), (Some("b-1"), Some(0)));
    }

    /// One request on any topic form: via None is plain ledger/req, reply the ResponseTopic. The reply, if any.
    fn ask_on(ledger: &mut Ledger, n: &mut u32, via: Option<&str>, reply: &str, body: Value) -> Option<Map> {
        let mut out = Vec::new();
        *n += 1;
        let corr = n.to_be_bytes();
        ledger.handle(&mut |o| out.push(o), dumps(&body).as_bytes(), Some(reply), Some(&corr), via, None);
        let found = out.iter().find(|o| o.2.as_deref() == Some(&corr[..]))?;
        assert_eq!(found.0, reply, "a reply goes only to the request's ResponseTopic");
        loads(found.1.as_bytes()).and_then(|v| v.as_object().cloned())
    }

    #[test]
    fn worktrees_are_made_on_demand_and_listed() {
        let dir = Dir::new("worktrees");
        let mut ledger = Ledger::open(&dir.join("ledger.db"), 3.0).ok().expect("opens");
        let n = &mut 0;
        for who in ["a-1", "b-1"] {
            ask(&mut ledger, n, who, json!({"op": "init"}));
        }
        let path = "/home/u/wt";
        let wt = worktree_resource(path);
        let take = |who_ttl: f64, wait: f64| {
            json!({"op": "acquire", "resource": wt, "level": "write", "ttl": who_ttl, "wait": wait, "path": path,
                   "git_common_dir": "/home/u/repo/.git"})
        };
        let (bad, _) = ask(
            &mut ledger,
            n,
            "a-1",
            json!({"op": "acquire", "resource": "admin/wt-0000000000000000",
                                                         "level": "write", "path": path}),
        );
        assert_eq!(
            bad["error"],
            "path: does not match admin/wt-0000000000000000 (its id is the sha256 of the realpath)"
        );
        let (got, _) = ask(&mut ledger, n, "a-1", take(60.0, 0.0));
        assert_eq!((got["ok"].as_bool(), got["created"].as_bool()), (Some(true), Some(true)), "{got:?}");
        let lease_id = got["lease"]["lease_id"].as_str().unwrap_or_default().to_owned();
        let (busy, _) = ask(&mut ledger, n, "b-1", take(60.0, 0.0));
        assert_eq!(busy["error"], "busy", "{busy:?}");
        let (h, _) = ask(&mut ledger, n, "c-1", json!({"op": "holders", "resource": wt}));
        assert_eq!(
            (h["holders"][0]["holder"].as_str(), h["holders"][0]["lease_id"].as_str()),
            (Some("a-1"), Some(lease_id.as_str()))
        );

        let (held, _) = ask(&mut ledger, n, "c-1", json!({"op": "worktrees"}));
        assert_eq!(
            held["worktrees"],
            json!([{"resource": wt, "path": path, "git_common_dir": "/home/u/repo/.git",
            "holder": "a-1", "level": "write", "lease_id": lease_id, "expires_at": got["lease"]["expires_at"]}])
        );
        let (res, _) = ask(&mut ledger, n, "a-1", json!({"op": "ledger", "owner": "admin"}));
        assert_eq!(
            (res["resources"][0]["description"].as_str(), res["resources"][0]["visibility"].as_str()),
            (Some(path), Some("public"))
        );

        ask(&mut ledger, n, "a-1", json!({"op": "release", "lease_id": lease_id}));
        let (none, _) = ask(&mut ledger, n, "c-1", json!({"op": "worktrees"}));
        assert_eq!(none["worktrees"], json!([]));
        let (all, _) = ask(&mut ledger, n, "c-1", json!({"op": "worktrees", "all": true}));
        assert_eq!((all["worktrees"][0]["path"].as_str(), all["worktrees"][0]["holder"].is_null()), (Some(path), true));
        let (again, _) = ask(&mut ledger, n, "b-1", take(60.0, 0.0));
        assert_eq!((again["ok"].as_bool(), again["created"].as_bool()), (Some(true), Some(false)), "{again:?}");

        // admin makes it private: a newcomer's request waits for admin and stays filed
        ask(&mut ledger, n, "admin", json!({"op": "update", "name": &wt[6..], "visibility": "private"}));
        ask(&mut ledger, n, "d-1", json!({"op": "init"}));
        let (pending, out) = ask(&mut ledger, n, "d-1", take(60.0, 0.0));
        let err = pending["error"].as_str().unwrap_or_default();
        assert!(err.starts_with(&format!("your write request for {wt} is pending with admin (request_id ")), "{err}");
        assert!(out.iter().any(|o| o.0 == "inbox/admin"), "admin is asked: {out:?}");
        let (grants, _) = ask(&mut ledger, n, "d-1", json!({"op": "grants"}));
        assert_eq!(grants["held"][0]["status"], "pending", "the request was kept");

        let (named, _) = ask(&mut ledger, n, "admin", json!({"op": "add", "name": "wt-x"}));
        assert_eq!(named["error"], "wt- names in admin's ledger are worktrees, made by acquire with a path");
        // admin's ledger outlives the stale sweep
        ledger.exec("UPDATE ledgers SET seen=0", []).ok();
        ledger.swept = 0.0;
        ledger.run(&mut |_| {}, Ledger::tick).ok();
        assert_eq!(ledger.count("SELECT count(*) FROM resources WHERE owner='admin'", []).ok(), Some(1));
        assert_eq!(ledger.count("SELECT count(*) FROM specs", []).ok(), Some(1));
    }

    #[test]
    fn admin_acts_on_behalf_and_is_named() {
        let dir = Dir::new("behalf");
        let mut ledger = Ledger::open(&dir.join("ledger.db"), 3.0).ok().expect("opens");
        let n = &mut 0;
        for who in ["o-1", "h-1"] {
            ask(&mut ledger, n, who, json!({"op": "init"}));
        }
        ask(&mut ledger, n, "o-1", json!({"op": "add", "name": "db", "visibility": "public", "max_level": "write"}));
        ask(&mut ledger, n, "h-1", json!({"op": "request", "resource": "o-1/db", "level": "write"}));
        let (got, _) = ask(&mut ledger, n, "h-1", json!({"op": "acquire", "resource": "o-1/db", "level": "write"}));
        let lease_id = got["lease"]["lease_id"].as_str().unwrap_or_default().to_owned();
        let release = json!({"op": "release", "lease_id": lease_id, "on_behalf": "o-1"});
        let (no, _) = ask(&mut ledger, n, "h-1", release.clone());
        assert_eq!(no["error"], "on_behalf: only admin acts for another ledger");
        let (gone, _) =
            ask(&mut ledger, n, "admin", json!({"op": "release", "lease_id": lease_id, "on_behalf": "x-9"}));
        assert_eq!(gone["error"], "no ledger for x-9");
        let (ok, out) = ask(&mut ledger, n, "admin", release);
        assert_eq!(ok["ok"], true, "{ok:?}");
        assert!(
            out.iter().any(|o| o.0 == "inbox/h-1" && o.1.contains(&format!("admin ended your write lease {lease_id}")))
        );
        let (hist, _) = ask(&mut ledger, n, "o-1", json!({"op": "lease_history", "resource": "o-1/db"}));
        assert_eq!(
            (hist["events"][1]["event"].as_str(), hist["events"][1]["by"].as_str()),
            (Some("break"), Some("admin"))
        );

        let (grants, _) = ask(&mut ledger, n, "o-1", json!({"op": "grants"}));
        let rq = grants["asked"][0]["id"].as_str().unwrap_or_default().to_owned();
        let (_, out) = ask(&mut ledger, n, "admin", json!({"op": "deny", "request_id": rq, "on_behalf": "o-1"}));
        assert!(out.iter().any(|o| o.0 == "inbox/h-1" && o.1.contains("admin revoked your write grant on o-1/db")));
        let (_, _) = ask(&mut ledger, n, "admin", json!({"op": "remove", "name": "db", "on_behalf": "o-1"}));
        assert_eq!(ledger.count("SELECT count(*) FROM resources", []).ok(), Some(0));
    }

    #[test]
    fn auth_modes_check_topics_rate_and_dump() {
        let dir = Dir::new("auth");
        let mut ledger = Ledger::open(&dir.join("ledger.db"), 3.0).ok().expect("opens");
        let n = &mut 0;
        let ping = json!({"op": "ping", "as": "a-1"});
        // off: today's behaviour, plus two-level reply topics that name the requester
        let off = ask_on(&mut ledger, n, None, "ledger/resp/c1", ping.clone()).expect("plain works when off");
        assert!(off.get("auth").is_none(), "{off:?}");
        assert!(ask_on(&mut ledger, n, Some("a-1"), "ledger/resp/a-1/s", ping.clone()).is_some());
        assert!(ask_on(&mut ledger, n, Some("a-1"), "ledger/resp/b-1/s", ping.clone()).is_none(), "not into b-1's");
        assert!(ask_on(&mut ledger, n, Some("a-1"), "ledger/resp/a-1/s/x", ping.clone()).is_none());

        ledger.auth = Auth::Warn;
        let warn = ask_on(&mut ledger, n, None, "ledger/resp/c1", ping.clone()).expect("plain is logged, not refused");
        assert_eq!(warn["auth"], "warn");
        let dump = ask_on(&mut ledger, n, Some("a-1"), "ledger/resp/a-1/s", json!({"op": "dump"})).expect("dump");
        assert_eq!(dump["ok"], true, "logged in warn");
        let mut limited = 0;
        for _ in 0..150 {
            let r = ask_on(&mut ledger, n, Some("r-1"), "ledger/resp/r-1/s", json!({"op": "ping"})).expect("reply");
            if r.get("error") == Some(&json!("rate limited: at most 20 requests a second")) {
                limited += 1;
            }
        }
        assert!((40..=55).contains(&limited), "a burst of 100, then about 20 a second: {limited} limited");
        for _ in 0..150 {
            let r = ask_on(&mut ledger, n, Some("admin"), "ledger/resp/admin/s", json!({"op": "ping"})).expect("reply");
            assert_eq!(r["ok"], true, "admin is exempt");
        }

        ledger.auth = Auth::Enforce;
        let plain = ask_on(&mut ledger, n, None, "ledger/resp/a-1/s", ping.clone()).expect("refused with a reply");
        assert_eq!(plain["error"], "send requests on ledger/req/<your address>");
        assert!(ask_on(&mut ledger, n, Some("a-1"), "ledger/resp/c1", ping.clone()).is_none(), "one level is dropped");
        let dump = ask_on(&mut ledger, n, Some("a-1"), "ledger/resp/a-1/s", json!({"op": "dump"})).expect("reply");
        assert_eq!(dump["error"], "dump: admin only");
        let dump = ask_on(&mut ledger, n, Some("admin"), "ledger/resp/admin/s", json!({"op": "dump"})).expect("reply");
        assert_eq!(dump["ok"], true);
    }

    #[test]
    fn schema_row_and_dump_names_the_implementation() {
        let dir = Dir::new("store");
        let mut ledger = Ledger::open(&dir.join("ledger.db"), 3.0).ok().expect("opens");
        assert_eq!(ledger.count("SELECT value FROM meta WHERE key='schema'", []).ok(), Some(1));
        let dump = ledger.run(&mut |_| {}, |l| l.op_dump(now())).ok().expect("dump");
        assert_eq!(dump["ledgerd"], json!({"impl": "rust", "version": env!("CARGO_PKG_VERSION")}));
        drop(ledger);
        let reopened = Ledger::open(&dir.join("ledger.db"), 3.0).ok().expect("opens");
        assert_eq!(reopened.count("SELECT count(*) FROM meta WHERE key='schema'", []).ok(), Some(1));
    }

    #[test]
    fn a_newer_schema_is_refused_before_anything_is_written() {
        let dir = Dir::new("schema");
        let path = dir.join("ledger.db");
        drop(Ledger::open(&path, 3.0).ok().expect("opens"));
        let db = Connection::open(&path).expect("reopens");
        db.execute_batch("DROP INDEX resources_owner; UPDATE meta SET value=2 WHERE key='schema'").expect("migrates");
        drop(db);
        match Ledger::open(&path, 3.0) {
            Err(Open::Schema(e)) => assert_eq!(e, "ledger.db has schema 2; this ledgerd knows 1"),
            _ => panic!("schema 2 opened"),
        }
        let db = Connection::open(&path).expect("reopens");
        let index = db.query_row("SELECT count(*) FROM sqlite_master WHERE name='resources_owner'", [], |r| r.get(0));
        assert_eq!(index, Ok(0), "nothing was written");
    }
}
