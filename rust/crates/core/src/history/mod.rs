//! Message history. historyd is its only writer: it records every room message, DM, broadcast and raw topic message
//! from the bus and answers requests over MQTT 5 (client has the calls).
//!
//! The legacy history log (one JSON line per message, written by bridge clients before historyd) is only read here:
//! historyd imports it once on its first start, and the file is never deleted.

pub mod client;

use crate::json::{Map, float};
use crate::validation::{ID_MAX, Result, TOPIC_MAX, check_body_value, check_mentions, check_ts, is_ident, os_error};
use std::path::Path;

/// A legacy history log line as an entry, or None if it is malformed.
pub fn parse_entry(line: &[u8]) -> Option<Map> {
    let d = crate::json::loads(line)?;
    let topic = d.get("topic")?.as_str().filter(|t| t.chars().count() <= TOPIC_MAX)?;
    let from = d.get("from")?.as_str().filter(|f| is_ident(f, ID_MAX))?;
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

/// Every well-formed entry of a legacy history log, oldest first; none if the file is missing.
pub fn read_log(path: &Path) -> Result<Vec<Map>> {
    match std::fs::read(path) {
        Ok(raw) => Ok(raw.split(|b| *b == b'\n').filter_map(parse_entry).collect()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(os_error(&e, Some(path))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json::dumps_map;

    #[test]
    fn entries_parse_like_python() {
        let e = parse_entry(br#"{"topic": "room/r", "from": "a-1", "body": "x", "ts": 3, "extra": 1}"#).unwrap();
        assert_eq!(dumps_map(&e), r#"{"topic": "room/r", "from": "a-1", "body": "x", "ts": 3.0}"#);
        for junk in [
            "",
            "{",
            "[1]",
            r#"{"topic": 5, "from": "a", "body": "x", "ts": 1}"#,
            r#"{"topic": "t", "from": "a/b", "body": "x", "ts": 1}"#,
            r#"{"topic": "t", "from": "a", "body": "x", "ts": 1, "mentions": []}"#,
        ] {
            assert!(parse_entry(junk.as_bytes()).is_none(), "{junk}");
        }
    }
}
