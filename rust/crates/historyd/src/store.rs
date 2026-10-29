//! The history store: SQLite, one transaction per message or request, written only by historyd.
//!
//! Messages come from the bus already validated by protocol::parse_message. Requests are hostile input: every field
//! is checked, unknown fields are refused, replies are capped. Retention follows receive time, not the sender's ts, so
//! a forged timestamp cannot keep a row forever; row and byte caps bound what a flood on raw topics can store. The
//! schema and digests are the Python historyd's, so either implementation can serve the same database.

use crate::request::{Bad, Request, parse};
use agent_bridge::json::{Map, dumps, float, loads, now};
use agent_bridge::protocol::{BROADCAST, parse_message};
use agent_bridge::stamp::stamp;
use agent_bridge::validation::{ADMIN, RESERVED};
use rusqlite::{Connection, OptionalExtension, Params, params};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::path::{Component, Path, PathBuf};

const MAX_PAYLOAD: usize = 16384;
const MAX_CORR: usize = 256;
/// Under config/mosquitto.conf max_packet_size 270000: a bigger publish would get historyd disconnected.
const MAX_REPLY: usize = 240_000;
const MAX_ROWS: i64 = 500_000;
const MAX_BYTES: i64 = 256 * 1024 * 1024;
const PRUNE_S: f64 = 3600.0;
/// the reserved first levels whose messages are history; others never are
const STORED: [&str; 3] = ["room", "inbox", BROADCAST];

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS messages(id INTEGER PRIMARY KEY, topic TEXT NOT NULL, sender TEXT, body TEXT NOT NULL,
  ts REAL NOT NULL, mentions TEXT, received REAL NOT NULL, digest BLOB NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS messages_topic ON messages(topic, id);
CREATE INDEX IF NOT EXISTS messages_sender ON messages(sender, id);
CREATE INDEX IF NOT EXISTS messages_received ON messages(received);
";

/// One reply: topic, payload and CorrelationData.
pub type Outgoing = (String, String, Option<Vec<u8>>);

pub fn log(text: &str) {
    eprintln!("{} {text}", stamp());
}

/// True for the topics historyd records: rooms, inboxes, broadcasts and raw topics, never other reserved ones.
pub fn stored(topic: &str) -> bool {
    let first = topic.split('/').next().unwrap_or_default();
    !topic.starts_with('$') && (!RESERVED.contains(&first) || STORED.contains(&first))
}

/// The row's identity, as Python's sha256(json.dumps([topic, sender, ts, body])): both implementations dedupe
/// against the same database.
fn digest(topic: &Value, sender: &Value, ts: f64, body: &Value) -> Vec<u8> {
    Sha256::digest(dumps(&json!([topic, sender, float(ts), body])).as_bytes()).to_vec()
}

/// os.path.abspath: absolute and lexically normalised, symlinks left alone.
fn abspath(path: &Path) -> PathBuf {
    let mut out = std::env::current_dir().unwrap_or_default();
    if path.is_absolute() {
        out = PathBuf::from("/");
    }
    for c in path.components() {
        match c {
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(p) => out.push(p),
            _ => {}
        }
    }
    out
}

pub struct History {
    db: Connection,
    stale_s: f64,
    pruned: f64,
    since_trim: u32,
}

impl History {
    pub fn open(path: &Path, stale_s: u64) -> Result<History, String> {
        let open = || -> rusqlite::Result<Connection> {
            if let Some(dir) = abspath(path).parent() {
                let _ = std::fs::create_dir_all(dir);
            }
            let db = Connection::open(path)?;
            db.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))?;
            db.execute_batch(SCHEMA)?;
            Ok(db)
        };
        let db = open().map_err(|e| e.to_string())?;
        Ok(History { db, stale_s: stale_s as f64, pruned: 0.0, since_trim: 0 })
    }

    /// f in one transaction, rolled back if it fails.
    fn tx<T>(&mut self, f: impl FnOnce(&Connection) -> rusqlite::Result<T>) -> rusqlite::Result<T> {
        let tx = self.db.transaction()?;
        let out = f(&tx)?;
        tx.commit()?;
        Ok(out)
    }

    // recording

    /// Store one message unless an identical one (same topic, sender, ts and body) already is; true if new.
    fn add(db: &Connection, msg: &Map, ts: f64, received: f64) -> rusqlite::Result<bool> {
        let (topic, sender, body) = (&msg["topic"], &msg["from"], &msg["body"]);
        let digest = digest(topic, sender, ts, body);
        let mentions = msg.get("mentions").filter(|m| m.as_array().is_some_and(|a| !a.is_empty())).map(dumps);
        let n = db.execute(
            "INSERT OR IGNORE INTO messages(topic, sender, body, ts, mentions, received, digest) VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![topic.as_str(), sender.as_str(), body.as_str(), ts, mentions, received, digest],
        )?;
        Ok(n == 1)
    }

    /// One message from the bus, if it is history and well formed.
    pub fn record(&mut self, topic: &str, payload: &[u8]) -> rusqlite::Result<()> {
        let Some(msg) = stored(topic).then(|| parse_message(topic, payload)).flatten() else { return Ok(()) };
        let received = now();
        let ts = msg.get("ts").and_then(Value::as_f64).unwrap_or(received);
        if self.tx(|db| History::add(db, &msg, ts, received))? {
            self.since_trim += 1;
        }
        if self.since_trim >= 1000 {
            self.trim()?;
        }
        Ok(())
    }

    /// Import a legacy history log once: the path is remembered, and messages already stored are skipped.
    pub fn migrate(&mut self, path: &Path) -> Result<usize, String> {
        let key = format!("imported:{}", abspath(path).display());
        let done = self.db.query_row("SELECT 1 FROM meta WHERE key=?", [&key], |_| Ok(())).optional();
        if done.map_err(|e| e.to_string())?.is_some() || !path.exists() {
            return Ok(0);
        }
        let entries = agent_bridge::history::read_log(path).map_err(|e| e.0)?;
        let n = self
            .tx(|db| {
                let mut n = 0;
                for e in &entries {
                    let ts = e["ts"].as_f64().unwrap_or_default();
                    n += History::add(db, e, ts, ts)? as usize;
                }
                let value = dumps(&json!({"at": float(now()), "entries": n}));
                db.execute("INSERT INTO meta VALUES (?, ?)", [&key, &value])?;
                Ok(n)
            })
            .map_err(|e| e.to_string())?;
        log(&format!("imported {n} of {} entries from {}; the file stays where it is", entries.len(), path.display()));
        Ok(n)
    }

    /// Drop what is older than the stale period, at most once per PRUNE_S; then hold the row and byte caps.
    pub fn prune(&mut self) -> rusqlite::Result<()> {
        let now = now();
        if now - self.pruned < PRUNE_S.min(self.stale_s) {
            return Ok(());
        }
        let cutoff = now - self.stale_s;
        self.tx(|db| db.execute("DELETE FROM messages WHERE received < ?", [cutoff]))?;
        self.pruned = now;
        self.trim()
    }

    fn trim(&mut self) -> rusqlite::Result<()> {
        self.since_trim = 0;
        self.tx(|db| {
            loop {
                let (rows, size): (i64, i64) =
                    db.query_row("SELECT count(*), coalesce(sum(length(body)), 0) FROM messages", [], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })?;
                if rows <= MAX_ROWS && size <= MAX_BYTES {
                    return Ok(());
                }
                db.execute("DELETE FROM messages WHERE id IN (SELECT id FROM messages ORDER BY id LIMIT 1000)", [])?;
            }
        })
    }

    // requests

    /// One request; via is the <addr> of a history/req/<addr> topic, None for plain history/req.
    pub fn handle(
        &mut self,
        publish: &mut dyn FnMut(Outgoing),
        payload: &[u8],
        topic: Option<&str>,
        corr: Option<&[u8]>,
        via: Option<&str>,
    ) {
        let resp = |id: &str| {
            !id.is_empty() && id.len() <= 128 && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        };
        let Some(topic) = topic.filter(|t| t.strip_prefix("history/resp/").is_some_and(resp)) else {
            return log("dropped request without a ResponseTopic under history/resp/");
        };
        let Some(corr) = corr.filter(|c| (1..=MAX_CORR).contains(&c.len())) else {
            let body = json!({"ok": false, "error": format!("CorrelationData of 1 to {MAX_CORR} bytes required")});
            return publish((topic.to_owned(), dumps(&body), None));
        };
        let body = match self.answer(payload, via) {
            Ok(Ok(mut result)) => {
                result.shift_insert(0, "ok".into(), true.into());
                Value::Object(result)
            }
            Ok(Err(Bad(e))) => json!({"ok": false, "error": e}),
            Err(e) => {
                log(&format!("internal error {e}"));
                json!({"ok": false, "error": "internal error"})
            }
        };
        publish((topic.to_owned(), dumps(&body), Some(corr.to_vec())));
    }

    fn answer(&mut self, payload: &[u8], via: Option<&str>) -> rusqlite::Result<Result<Map, Bad>> {
        if payload.len() > MAX_PAYLOAD {
            return Ok(Err(Bad(format!("payload over {MAX_PAYLOAD} bytes"))));
        }
        let Some(req) = std::str::from_utf8(payload).ok().and_then(|_| loads(payload)) else {
            return Ok(Err(Bad("payload must be UTF-8 JSON".into())));
        };
        let Request { op, me, args } = match parse(&req, via) {
            Ok(r) => r,
            Err(b) => return Ok(Err(b)),
        };
        let me = me.unwrap_or_default();
        let (inbox, last) = (format!("inbox/{me}"), args.get("last").and_then(Value::as_i64).unwrap_or(0));
        let text = |k: &str| args.get(k).and_then(Value::as_str).map(str::to_owned);
        let number = |k: &str| args.get(k).and_then(Value::as_f64);
        self.tx(|db| {
            Ok(Ok(match op {
                "room" => {
                    let topic = format!("room/{}", text("room").unwrap_or_default());
                    match number("before") {
                        None => messages(db, "topic=?", params![topic, last])?,
                        Some(before) => messages(db, "topic=? AND ts<?", params![topic, before, last])?,
                    }
                }
                "dms" => match text("peer") {
                    None => messages(
                        db,
                        "topic=? OR (sender=? AND substr(topic, 1, 6)='inbox/')",
                        params![inbox, me, last],
                    )?,
                    Some(peer) => messages(
                        db,
                        "(topic=? AND sender=?) OR (topic=? AND sender=?)",
                        params![inbox, peer, format!("inbox/{peer}"), me, last],
                    )?,
                },
                "topic" => messages(db, "topic=?", params![text("topic"), last])?,
                "broadcasts" => messages(
                    db,
                    "topic=? AND sender=? AND ts>?",
                    params![BROADCAST, ADMIN, number("since").unwrap_or_default(), last],
                )?,
                "recent" if me != ADMIN => return Ok(Err(Bad("recent is for the admin console only".into()))),
                "recent" => messages(db, "1", params![last])?,
                "drop" if text("addr").is_some_and(|a| a != me) => {
                    return Ok(Err(Bad("drop: an agent drops only its own inbox".into())));
                }
                "drop" => {
                    let n = db.execute("DELETE FROM messages WHERE topic=?", [&inbox])?;
                    Map::from_iter([("dropped".into(), n.into())])
                }
                _ => Map::new(),
            }))
        })
    }
}

/// The last rows of a query, oldest first, as wire entries; the oldest are left out if the reply is too big. The
/// query's last parameter is the row limit.
fn messages<P: Params>(db: &Connection, filter: &str, args: P) -> rusqlite::Result<Map> {
    let sql = format!("SELECT topic, sender, body, ts, mentions FROM messages WHERE {filter} ORDER BY id DESC LIMIT ?");
    let mut stmt = db.prepare(&sql)?;
    let rows = stmt.query_map(args, |r| {
        let mut entry = Map::new();
        entry.insert("topic".into(), r.get::<_, String>(0)?.into());
        entry.insert("from".into(), r.get::<_, Option<String>>(1)?.into());
        entry.insert("body".into(), r.get::<_, String>(2)?.into());
        entry.insert("ts".into(), float(r.get(3)?));
        if let Some(m) = r.get::<_, Option<String>>(4)?.filter(|m| !m.is_empty()) {
            entry.insert("mentions".into(), loads(m.as_bytes()).unwrap_or(Value::Null));
        }
        Ok(entry)
    })?;
    let (mut out, mut size) = (Vec::new(), 0);
    let mut result = Map::new();
    for entry in rows {
        let entry = entry?;
        size += dumps(&Value::Object(entry.clone())).len() + 2;
        if size > MAX_REPLY - 1000 {
            result.insert("truncated".into(), true.into());
            break;
        }
        out.push(Value::Object(entry));
    }
    out.reverse();
    result.shift_insert(0, "messages".into(), out.into());
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("agent-bridge-historyd-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("a temp dir");
        dir
    }

    /// The reply to one request, as JSON.
    fn ask(h: &mut History, req: &str, via: Option<&str>) -> Value {
        let mut sent = Vec::new();
        h.handle(&mut |o| sent.push(o), req.as_bytes(), Some("history/resp/t"), Some(b"c"), via);
        loads(sent.pop().expect("a reply").1.as_bytes()).expect("JSON")
    }

    fn bodies(reply: &Value) -> Vec<&str> {
        reply["messages"].as_array().expect("messages").iter().map(|m| m["body"].as_str().unwrap_or_default()).collect()
    }

    #[test]
    fn digests_are_pythons() {
        let hex = |d: Vec<u8>| d.iter().map(|b| format!("{b:02x}")).collect::<String>();
        let d = digest(&json!("room/r"), &json!("a-1"), 1793000001.1234567, &json!("hi \u{2713}"));
        assert_eq!(hex(d), "2d4e848ed82ee9e4b9d45efda17b2e46269a839c45d9461f087263a04ac9acf1");
        let raw = digest(&json!("ci"), &Value::Null, 1e-05, &json!("x"));
        assert_eq!(hex(raw), "faa7ff1464b85dbd8ca49a0170263b43048f062aff82920d7d9c4e837bf13ad2");
    }

    #[test]
    fn records_history_once_and_serves_it() {
        let dir = temp("record");
        let mut h = History::open(&dir.join("h.db"), 86400).expect("opens");
        let m = |from: &str, body: &str, ts: f64| json!({"from": from, "body": body, "ts": ts}).to_string();
        for (topic, payload) in [
            ("room/r", m("a-1", "one", 1.0)),
            ("room/r", m("a-1", "one", 1.0)),
            ("room/r", m("b-1", "two", 2.0)),
            ("inbox/b-1", m("a-1", "dm", 3.0)),
            ("agents/a-1", m("a-1", "presence", 4.0)),
            ("history/resp/x", m("a-1", "reply", 5.0)),
            ("ci/x", "plain".into()),
        ] {
            h.record(topic, payload.as_bytes()).expect("records");
        }
        assert_eq!(bodies(&ask(&mut h, r#"{"op": "room", "room": "r"}"#, None)), ["one", "two"]);
        assert_eq!(bodies(&ask(&mut h, r#"{"op": "room", "room": "r", "before": 2}"#, None)), ["one"]);
        assert_eq!(bodies(&ask(&mut h, r#"{"op": "dms"}"#, Some("b-1"))), ["dm"]);
        assert_eq!(bodies(&ask(&mut h, r#"{"op": "topic", "topic": "ci/x"}"#, None)), ["plain"]);
        assert_eq!(
            ask(&mut h, r#"{"op": "recent", "last": 9}"#, Some("admin"))["messages"].as_array().map(Vec::len),
            Some(4)
        );
        assert_eq!(ask(&mut h, r#"{"op": "drop"}"#, Some("b-1")), json!({"ok": true, "dropped": 1}));
        assert_eq!(ask(&mut h, r#"{"op": "ping"}"#, None), json!({"ok": true}));
        let big = "x".repeat(60_000);
        for i in 0..7 {
            h.record("room/big", m("a-1", &format!("{big}{i}"), 10.0 + i as f64).as_bytes()).expect("records");
        }
        let reply = ask(&mut h, r#"{"op": "room", "room": "big", "last": 1000}"#, None);
        assert_eq!(reply["truncated"], true);
        assert!(dumps(&reply).len() < MAX_REPLY && bodies(&reply).len() < 7);
        assert!(bodies(&reply).last().is_some_and(|b| b.ends_with('6')), "the newest are kept");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn imports_a_legacy_log_once_and_prunes_by_receive_time() {
        let dir = temp("migrate");
        let log = dir.join("history.jsonl");
        let line = r#"{"topic": "room/old", "from": "a-1", "body": "old", "ts": 5}"#;
        std::fs::write(&log, format!("{line}\nnot json\n{line}\n")).expect("writes");
        let mut h = History::open(&dir.join("h.db"), 86400).expect("opens");
        assert_eq!(h.migrate(&log), Ok(1));
        assert_eq!(h.migrate(&log), Ok(0), "imported once");
        assert!(log.exists(), "the file stays");
        h.record("room/new", br#"{"from": "a-1", "body": "new", "ts": 1}"#).expect("records");
        h.prune().expect("prunes");
        assert_eq!(
            bodies(&ask(&mut h, r#"{"op": "room", "room": "old"}"#, None)),
            Vec::<&str>::new(),
            "received at ts 5"
        );
        assert_eq!(
            bodies(&ask(&mut h, r#"{"op": "room", "room": "new"}"#, None)),
            ["new"],
            "received now, though its ts is old"
        );
        let _ = std::fs::remove_dir_all(dir);
    }
}
