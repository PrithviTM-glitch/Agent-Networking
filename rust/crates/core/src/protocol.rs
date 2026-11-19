//! The wire format: reserved topics, presence records and message payloads, parsed as hostile input.
//!
//! Incoming messages that are over 256 KiB, not UTF-8, or break the input rules are dropped; plain text on a raw topic
//! arrives with "from": null. "from" is checked for shape only: any local process can claim any address.

use crate::json::{Map, float, float_repr};
use crate::validation::{
    ID_MAX, Result, TOPICS_MAX, check_about, check_body_value, check_caps_value, check_ident, check_mentions,
    check_topic_value, check_ts, fail, is_count, is_ident, is_message_id, is_uuid, is_version,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// A 64 KiB body can grow to ~192 KiB as JSON; config/mosquitto.conf max_packet_size must stay above this.
pub const PAYLOAD_MAX: usize = 256 * 1024;
pub const BROADCAST: &str = "broadcast";
/// MQTT 5 session expiry value meaning the broker keeps the session forever.
pub const NEVER: u32 = 0xFFFF_FFFF;
pub const ORIGIN: [&str; 3] = ["session", "pid", "pid_start"];
/// A subagent's agent: kind "subagent" and the creating session, never its own session.
pub const LINEAGE: [&str; 2] = ["kind", "parent"];
pub const PID_MAX: u64 = 4194304;
pub const TICKS_MAX: u64 = 1 << 53;

pub fn parse_json(payload: &[u8], cap: usize) -> Option<Value> {
    if payload.len() > cap {
        return None;
    }
    crate::json::loads(payload)
}

/// A retained presence record, or None if it is malformed.
pub fn parse_presence(payload: &[u8]) -> Option<Map> {
    let d = parse_json(payload, PAYLOAD_MAX)?;
    let d = d.as_object()?;
    let topics = d.get("topics")?.as_array()?;
    let keep = d.get("keep")?.as_bool()?;
    if topics.len() > TOPICS_MAX {
        return None;
    }
    let mut rec = Map::new();
    rec.insert("name".into(), Value::String(check_ident(d.get("name")?.as_str()?, "name").ok()?.to_owned()));
    rec.insert("caps".into(), check_caps_value(d.get("caps")?).ok()?.into());
    rec.insert("topics".into(), topics.iter().map(check_topic_value).collect::<Result<Vec<_>>>().ok()?.into());
    rec.insert("keep".into(), keep.into());
    rec.insert("seen".into(), float(check_ts(d.get("seen")?).ok()?));
    let uuid = |key: &str| d.get(key).and_then(Value::as_str).filter(|s| is_uuid(s)).map(str::to_owned);
    if d.get("kind").and_then(Value::as_str) == Some("subagent") {
        rec.insert("kind".into(), "subagent".into());
        if let Some(parent) = uuid("parent") {
            rec.insert("parent".into(), parent.into());
        }
    } else if let Some(session) = uuid("session") {
        rec.insert("session".into(), session.into());
    }
    if is_count(d.get("pid"), PID_MAX) && is_count(d.get("pid_start"), TICKS_MAX) {
        rec.insert("pid".into(), d["pid"].clone());
        rec.insert("pid_start".into(), d["pid_start"].clone());
    }
    // Optional, from 0.3.0: kept when well formed and dropped otherwise, never refusing the record. A writer older
    // than 0.3.0 leaves them out when it rewrites the record, and they stay gone until the agent sets them again.
    if let Some(v) = d.get("mcp").and_then(Value::as_str).filter(|v| is_version(v)) {
        rec.insert("mcp".into(), v.into());
    }
    if let Some(a) = d.get("about").and_then(Value::as_str).filter(|a| !a.is_empty() && check_about(a).is_ok()) {
        rec.insert("about".into(), a.into());
    }
    // From 0.3.2: "credentials" when the agent's Bus connected with its own credentials (doctor's warn-mode check).
    if d.get("auth").and_then(Value::as_str) == Some("credentials") {
        rec.insert("auth".into(), "credentials".into());
    }
    Some(rec)
}

/// A message's id, derived, never sent: the first 16 hex digits of sha256("<topic>\n<from>\n<ts>"), ts written as
/// the wire writes it (Python's float repr). Every client computes the same id for the same message, including one
/// from a sender older than 0.3.1, so a reply can name its parent (reply_to) from recv, history or watch alike.
pub fn message_id(topic: &str, from: &str, ts: f64) -> String {
    let digest = Sha256::digest(format!("{topic}\n{from}\n{}", float_repr(ts)).as_bytes());
    digest[..8].iter().map(|b| format!("{b:02x}")).collect()
}

/// A sender-bound topic (0.3.2, AUTH.md 4.4): `inbox/<to>/<from>` or `room/<room>/<from>`, as (today's topic, the
/// sender in the topic). The broker's ACL lets only `<from>` write its own level once auth is on.
pub fn sender_bound(topic: &str) -> Option<(String, &str)> {
    let mut levels = topic.split('/');
    let (kind, place, from) = (levels.next()?, levels.next()?, levels.next()?);
    let ok = matches!(kind, "inbox" | "room") && levels.next().is_none();
    (ok && is_ident(place, ID_MAX) && is_ident(from, ID_MAX)).then(|| (format!("{kind}/{place}"), from))
}

/// A received payload as a message, or None if it breaks the input rules.
///
/// On a sender-bound topic the sender is the topic level: a payload `"from"` that differs drops the message, the topic
/// is normalised to today's form (so recv, history, ids and threads don't change) and `"sender": "bound"` is added. A
/// message on today's forms, or any other topic with a sender, carries `"sender": "claimed"` (the payload says so).
pub fn parse_message(topic: &str, payload: &[u8]) -> Option<Map> {
    if let Some((base, from)) = sender_bound(topic) {
        let Some(Value::Object(d)) = parse_json(payload, PAYLOAD_MAX) else { return None };
        if d.get("from").and_then(Value::as_str) != Some(from) {
            return None;
        }
        let mut msg = message(&base, &d).ok()?;
        msg.insert("sender".into(), "bound".into());
        return Some(msg);
    }
    if (topic.starts_with("room/") || topic.starts_with("inbox/")) && topic.matches('/').count() != 1 {
        return None; // neither today's form nor a well-formed sender-bound one
    }
    if let Some(Value::Object(d)) = parse_json(payload, PAYLOAD_MAX)
        && d.contains_key("from")
    {
        let mut msg = message(topic, &d).ok()?;
        msg.insert("sender".into(), "claimed".into());
        return Some(msg);
    }
    if topic.starts_with("room/") || topic.starts_with("inbox/") || topic == BROADCAST || payload.len() > PAYLOAD_MAX {
        return None;
    }
    let body = check_body_value(&Value::String(std::str::from_utf8(payload).ok()?.to_owned())).ok()?;
    let mut msg = Map::new();
    msg.insert("topic".into(), topic.into());
    msg.insert("from".into(), Value::Null);
    msg.insert("body".into(), body.into());
    Some(msg)
}

fn message(topic: &str, d: &Map) -> Result<Map> {
    let from = match d["from"].as_str() {
        Some(s) => check_ident(s, "address")?.to_owned(),
        None => return fail("bad address"),
    };
    let mut msg = Map::new();
    msg.insert("topic".into(), topic.into());
    msg.insert("from".into(), from.into());
    msg.insert("body".into(), check_body_value(d.get("body").ok_or_else(|| crate::Error("body".into()))?)?.into());
    msg.insert("ts".into(), float(check_ts(d.get("ts").ok_or_else(|| crate::Error("ts".into()))?)?));
    if topic.starts_with("room/")
        && let Some(mentions) = d.get("mentions")
    {
        msg.insert("mentions".into(), check_mentions(mentions)?.into());
    }
    // From 0.3.1, optional: kept when well formed, dropped otherwise, never refusing the message.
    if (topic.starts_with("room/") || topic.starts_with("inbox/"))
        && let Some(parent) = d.get("reply_to").and_then(Value::as_str).filter(|p| is_message_id(p))
    {
        msg.insert("reply_to".into(), parent.into());
    }
    let id = message_id(topic, msg["from"].as_str().unwrap_or(""), msg["ts"].as_f64().unwrap_or(0.0));
    msg.insert("id".into(), id.into());
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json::dumps_map;

    #[test]
    fn presence_keeps_the_0_3_fields_when_well_formed() {
        let base = r#""name": "a", "caps": [], "topics": [], "keep": false, "seen": 1"#;
        let rec =
            parse_presence(format!(r#"{{{base}, "mcp": "0.3.0", "about": "on the console"}}"#).as_bytes()).unwrap();
        assert_eq!((rec["mcp"].as_str(), rec["about"].as_str()), (Some("0.3.0"), Some("on the console")));
        for bad in [r#""mcp": "0.3 0""#, r#""mcp": 3"#, r#""about": "two\nlines""#, r#""about": """#, r#""about": 5"#] {
            let rec =
                parse_presence(format!("{{{base}, {bad}}}").as_bytes()).expect("the record itself is still valid");
            assert!(!rec.contains_key("mcp") && !rec.contains_key("about"), "{bad}");
        }
    }

    #[test]
    fn messages() {
        let m = parse_message("room/r", br#"{"from": "a-1", "body": "hi", "ts": 5, "mentions": ["b", "a"]}"#).unwrap();
        assert_eq!(
            dumps_map(&m),
            r#"{"topic": "room/r", "from": "a-1", "body": "hi", "ts": 5.0, "mentions": ["a", "b"], "id": "0361f55cdae7513a", "sender": "claimed"}"#
        );
        let dm = parse_message("inbox/x", br#"{"from": "a-1", "body": "hi", "ts": 1.5, "mentions": 7}"#).unwrap();
        assert!(!dm.contains_key("mentions"), "mentions only count on rooms");
        assert!(parse_message("room/r", br#"{"from": "a-1", "body": "hi", "ts": 1, "mentions": []}"#).is_none());
        let raw = parse_message("ci", "multi\nline\ttab ✓".as_bytes()).unwrap();
        assert_eq!(dumps_map(&raw), r#"{"topic": "ci", "from": null, "body": "multi\nline\ttab \u2713"}"#);
        for raw in [&b""[..], b"not json", b"[1, 2]", b"\"str\"", b"123", b"null", br#"{"x": 1}"#] {
            assert_eq!(parse_message("ci", raw).unwrap()["body"], std::str::from_utf8(raw).unwrap());
            assert!(parse_message("room/r", raw).is_none() && parse_message("broadcast", raw).is_none());
        }
        let junk: [&[u8]; 18] = [
            b"\xff\xfe\x00binary",
            b"\xc3\x28",
            b"a\x00b",
            b"\x1b[2J",
            "\u{85}".as_bytes(),
            br#"{"from": 5, "body": "x", "ts": 1}"#,
            br#"{"from": "a/b", "body": "x", "ts": 1}"#,
            br#"{"from": null, "body": "x", "ts": 1}"#,
            br#"{"from": "ok-1", "body": 7, "ts": 1}"#,
            br#"{"from": "ok-1", "body": "x", "ts": "now"}"#,
            br#"{"from": "ok-1", "body": "x", "ts": true}"#,
            br#"{"from": "ok-1", "body": "x\u0000y", "ts": 1}"#,
            br#"{"from": "ok-1", "body": "\u001b[31m", "ts": 1}"#,
            br#"{"from": "ok-1", "ts": 1}"#,
            br#"{"from": "ok-1", "body": "x", "ts": NaN}"#,
            br#"{"from": "ok-1", "body": "x", "ts": 1e999}"#,
            br#"{"from": "\ud800", "body": "x", "ts": 1}"#,
            br#"{"from": "ok-1", "body": "\ud800", "ts": 1}"#,
        ];
        for j in junk {
            assert!(parse_message("ci", j).is_none(), "{}", String::from_utf8_lossy(j));
        }
        assert!(parse_message("ci", &vec![b'x'; 70_000]).is_none());
        assert!(parse_message("ci", &vec![b'['; 100_000]).is_none());
        assert!(parse_message("ci", &vec![b'x'; PAYLOAD_MAX + 1]).is_none());
    }

    #[test]
    fn sender_bound_topics() {
        let p = |topic: &str, payload: &str| parse_message(topic, payload.as_bytes());
        let base = r#""body": "hi", "ts": 1794518700.0857205"#;
        let bound = p("inbox/bob-2/al-1", &format!(r#"{{"from": "al-1", {base}}}"#)).expect("a bound DM");
        let today = p("inbox/bob-2", &format!(r#"{{"from": "al-1", {base}}}"#)).expect("today's form");
        assert_eq!((bound["topic"].as_str(), bound["sender"].as_str()), (Some("inbox/bob-2"), Some("bound")));
        assert_eq!(today["sender"], "claimed");
        assert_eq!(bound["id"], today["id"], "the id is over the normalised topic, so it survives the move");
        let room = p("room/ops/al-1", &format!(r#"{{"from": "al-1", "mentions": ["bob-2"], {base}}}"#)).unwrap();
        assert_eq!((room["topic"].as_str(), room["mentions"][0].as_str()), (Some("room/ops"), Some("bob-2")));
        for (topic, payload) in [
            ("inbox/bob-2/mallory-3", format!(r#"{{"from": "al-1", {base}}}"#)), // payload disagrees with the topic
            ("room/ops/al-1", format!(r#"{{{base}}}"#)),                         // no from
            ("room/ops/al-1", "plain text".to_owned()),
            ("room/ops/a/b", format!(r#"{{"from": "b", {base}}}"#)), // too deep
            ("room/ops/bad.name", format!(r#"{{"from": "bad.name", {base}}}"#)),
            ("inbox/x/", format!(r#"{{"from": "al-1", {base}}}"#)),
        ] {
            assert!(p(topic, &payload).is_none(), "{topic} {payload}");
        }
        assert_eq!(p("ci/build/x", &format!(r#"{{"from": "al-1", {base}}}"#)).unwrap()["sender"], "claimed");
        assert!(!p("ci/build", "plain").unwrap().contains_key("sender"), "no sender, nothing claimed");
        let rec = parse_presence(
            br#"{"name": "a", "caps": [], "topics": [], "keep": false, "seen": 1, "auth": "credentials"}"#,
        );
        assert_eq!(rec.unwrap()["auth"], "credentials");
    }

    /// The id is pinned against Python's hashlib.sha256(f"{topic}\n{from}\n{ts!r}"), so any client can compute it.
    #[test]
    fn ids_and_replies() {
        assert_eq!(message_id("room/r", "a-1", 5.0), "0361f55cdae7513a");
        assert_eq!(message_id("inbox/x", "a-1", 1794518700.0857205), "ebdd23bbc73877c5");
        let parent = "0361f55cdae7513a";
        let reply = |topic: &str, reply_to: &str| {
            let payload =
                format!(r#"{{"from": "a-1", "body": "yes", "ts": 1794518700.0857205, "reply_to": {reply_to}}}"#);
            parse_message(topic, payload.as_bytes()).expect("still a message")
        };
        let dm = reply("inbox/x", &format!("\"{parent}\""));
        assert_eq!((dm["reply_to"].as_str(), dm["id"].as_str()), (Some(parent), Some("ebdd23bbc73877c5")));
        assert_eq!(reply("room/r", &format!("\"{parent}\""))["reply_to"], parent);
        assert!(!reply("ci/x", &format!("\"{parent}\"")).contains_key("reply_to"), "rooms and DMs only");
        for bad in [r#""0361F55CDAE7513A""#, r#""0361f55c""#, "7", "null", r#""<script>xxxxxxxx""#] {
            assert!(!reply("room/r", bad).contains_key("reply_to"), "dropped, never refusing: {bad}");
        }
        assert!(!parse_message("ci", b"plain text").unwrap().contains_key("id"), "no sender, no id");
    }

    #[test]
    fn presence() {
        let ok = br#"{"name": "a", "caps": ["x"], "topics": ["room/r", "ci/#"], "keep": false, "seen": 10,
                      "session": "0f8fad5b-d9cb-469f-a165-70867728950e", "pid": 12, "pid_start": 34, "extra": 1}"#;
        assert_eq!(
            dumps_map(&parse_presence(ok).unwrap()),
            r#"{"name": "a", "caps": ["x"], "topics": ["room/r", "ci/#"], "keep": false, "seen": 10.0, "session": "0f8fad5b-d9cb-469f-a165-70867728950e", "pid": 12, "pid_start": 34}"#
        );
        let sub = br#"{"name": "a", "caps": [], "topics": [], "keep": true, "seen": 1, "kind": "subagent",
                       "parent": "0f8fad5b-d9cb-469f-a165-70867728950e", "session": "0f8fad5b-d9cb-469f-a165-70867728950e",
                       "pid": 0, "pid_start": 34}"#;
        let rec = parse_presence(sub).unwrap();
        assert!(rec["kind"] == "subagent" && rec.contains_key("parent") && !rec.contains_key("session"));
        assert!(!rec.contains_key("pid"), "pid 0 is not a pid");
        for bad in [
            &br#"{"name": "a/b", "caps": [], "topics": [], "keep": true, "seen": 1}"#[..],
            br#"{"name": "a", "caps": [], "topics": ["inbox/x"], "keep": true, "seen": 1}"#,
            br#"{"name": "a", "caps": [], "topics": [], "keep": 1, "seen": 1}"#,
            br#"{"name": "a", "caps": [], "topics": [], "keep": true}"#,
            br#"[1]"#,
            b"",
        ] {
            assert!(parse_presence(bad).is_none());
        }
    }
}
