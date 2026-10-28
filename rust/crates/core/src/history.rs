//! The history log: one JSON line per room message, DM and broadcast sent by a bridge client on this host.
//!
//! Writers append under an exclusive flock and readers take a shared one. Entries older than the stale period are
//! pruned when an agent registers, at most hourly, and leaving drops the DMs an agent received.

use crate::config;
use crate::json::{Map, dumps_map, float, now};
use crate::validation::{
    Result, TOPIC_MAX, check_body_value, check_ident, check_last, check_mentions, check_ts, is_ident, os_error,
};
use serde_json::Value;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::time::SystemTime;

const PRUNE_S: u64 = 3600;

/// A history log line as an entry, or None if it is malformed.
pub fn parse_entry(line: &str) -> Option<Map> {
    let d: Value = crate::json::loads(line.as_bytes())?;
    let topic = d.get("topic")?.as_str().filter(|t| t.chars().count() <= TOPIC_MAX)?;
    let from = d.get("from")?.as_str().filter(|f| is_ident(f, crate::validation::ID_MAX))?;
    let mut entry = Map::new();
    entry.insert("topic".into(), topic.into());
    entry.insert("from".into(), from.into());
    entry.insert("body".into(), check_body_value(d.get("body")?).ok()?.into());
    entry.insert("ts".into(), float(check_ts(d.get("ts")?).ok()?));
    if let Some(mentions) = d.get("mentions") {
        entry.insert("mentions".into(), check_mentions(mentions).ok()?.into());
    }
    Some(entry)
}

fn open() -> Result<File> {
    config::data_dir()?;
    let path = &config::get().log;
    OpenOptions::new().read(true).append(true).create(true).open(path).map_err(|e| os_error(&e, Some(path)))
}

fn io(e: std::io::Error) -> crate::Error {
    os_error(&e, Some(&config::get().log))
}

/// entry is {from, body, ts[, mentions]}; the line gets topic first, as Python writes it.
pub fn append(topic: &str, entry: &Map) -> Result<()> {
    let f = open()?;
    f.lock().map_err(io)?;
    let mut line = Map::new();
    line.insert("topic".into(), topic.into());
    line.extend(entry.clone());
    (&f).write_all(format!("{}\n", dumps_map(&line)).as_bytes()).map_err(io)
}

fn read_entries(mut f: &File) -> Result<Vec<Map>> {
    let mut raw = Vec::new();
    f.seek(SeekFrom::Start(0)).and_then(|_| f.read_to_end(&mut raw)).map_err(io)?;
    // Python reads the log in text mode: universal newlines, undecodable bytes replaced.
    let text = String::from_utf8_lossy(&raw).replace("\r\n", "\n").replace('\r', "\n");
    Ok(text.split_inclusive('\n').filter_map(parse_entry).collect())
}

pub fn entries() -> Result<Vec<Map>> {
    let f = open()?;
    f.lock_shared().map_err(io)?;
    read_entries(&f)
}

/// Drop entries older than the stale period, malformed lines, and with drop_topic every entry on that topic.
/// Without drop_topic it runs at most once per PRUNE_S (or per stale period if shorter): it rewrites the whole log.
pub fn prune(drop_topic: Option<&str>) -> Result<()> {
    let cfg = config::get();
    let stamp = config::data_path(&["prune.stamp"]);
    if drop_topic.is_none()
        && let Ok(modified) = std::fs::metadata(&stamp).and_then(|m| m.modified())
        && now() - unix(modified) < PRUNE_S.min(cfg.stale_s) as f64
    {
        return Ok(());
    }
    let cutoff = now() - cfg.stale_s as f64;
    let mut f = open()?;
    f.lock().map_err(io)?;
    let kept: String = read_entries(&f)?
        .into_iter()
        .filter(|e| e["ts"].as_f64().unwrap_or(0.0) >= cutoff && Some(e["topic"].as_str().unwrap_or("")) != drop_topic)
        .map(|e| format!("{}\n", dumps_map(&e)))
        .collect();
    f.set_len(0).and_then(|_| f.write_all(kept.as_bytes())).map_err(io)?;
    let touched = OpenOptions::new().append(true).create(true).open(&stamp);
    touched.and_then(|s| s.set_modified(SystemTime::now())).map_err(|e| os_error(&e, Some(&stamp)))
}

fn unix(t: SystemTime) -> f64 {
    match t.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => d.as_secs_f64(),
        Err(e) => -e.duration().as_secs_f64(),
    }
}

fn topic_of(e: &Map) -> &str {
    e["topic"].as_str().unwrap_or("")
}

/// A chat room's messages, oldest first.
pub fn history(room: &str, last: i64) -> Result<Vec<Map>> {
    let topic = format!("room/{}", check_ident(room, "room")?);
    let mine: Vec<Map> = entries()?.into_iter().filter(|e| topic_of(e) == topic).collect();
    Ok(tail(mine, check_last(last)?))
}

/// addr's direct messages in both directions, optionally only those with peer.
pub fn dms(addr: &str, peer: Option<&str>, last: i64) -> Result<Vec<Map>> {
    let inbox = format!("inbox/{}", check_ident(addr, "address")?);
    let peer = peer.map(|p| check_ident(p, "address")).transpose()?;
    let mine: Vec<Map> = entries()?
        .into_iter()
        .filter(|e| topic_of(e) == inbox || (e["from"] == addr && topic_of(e).starts_with("inbox/")))
        .filter(|e| peer.is_none_or(|p| e["from"] == p || &topic_of(e)[6.min(topic_of(e).len())..] == p))
        .collect();
    Ok(tail(mine, check_last(last)?))
}

fn tail(mut v: Vec<Map>, n: usize) -> Vec<Map> {
    v.split_off(v.len().saturating_sub(n))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entries_parse_like_python() {
        let e = parse_entry(r#"{"topic": "room/r", "from": "a-1", "body": "x", "ts": 3, "extra": 1}"#).unwrap();
        assert_eq!(dumps_map(&e), r#"{"topic": "room/r", "from": "a-1", "body": "x", "ts": 3.0}"#);
        for junk in [
            "",
            "{",
            "[1]",
            r#"{"topic": 5, "from": "a", "body": "x", "ts": 1}"#,
            r#"{"topic": "t", "from": "a/b", "body": "x", "ts": 1}"#,
            r#"{"topic": "t", "from": "a", "body": "x", "ts": 1, "mentions": []}"#,
        ] {
            assert!(parse_entry(junk).is_none(), "{junk}");
        }
    }
}
