//! One message as a line for people (the CLI's --text and watch): when, where, who, then the body.
//!
//! "14:02:31 #ops alice-1a2b3c: hello", "14:02:31 DM alice-1a2b3c -> bob-9f8e7d: hi", "14:02:31 broadcast admin: ..."
//! or "14:02:31 ci/build alice-1a2b3c: ..." for a raw topic ("-" when plain text has no sender). " (mentions you)"
//! follows the sender when the message mentions this agent. A message from another day starts with its date. Later
//! lines of a multi-line body are indented two spaces, and anything a terminal would act on rather than show (a
//! carriage return, a bidi or zero-width mark) is written escaped, so a body can never redraw or reorder the output.

use crate::json::{Map, is_format_control, now};
use crate::stamp::local;
use serde_json::Value;

fn when(ts: f64) -> String {
    let (at, today) = (local(ts as i64, ' '), local(now() as i64, ' '));
    match at.split_once(' ') {
        Some((date, time)) if today.starts_with(date) => time.to_owned(),
        _ => at,
    }
}

fn shown(body: &str) -> String {
    let mut out = String::with_capacity(body.len());
    for c in body.chars() {
        match c {
            '\n' => out.push_str("\n  "),
            '\r' => out.push_str("\\r"),
            c if is_format_control(c) => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// msg as received (recv, history, dms, watch): one line, or several for a multi-line body; no trailing newline.
pub fn line(msg: &Map) -> String {
    let topic = msg.get("topic").and_then(Value::as_str).unwrap_or("");
    let from = msg.get("from").and_then(Value::as_str).unwrap_or("-");
    let place = if let Some(room) = topic.strip_prefix("room/") {
        format!("#{room} {from}")
    } else if let Some(to) = topic.strip_prefix("inbox/") {
        format!("DM {from} -> {to}")
    } else {
        format!("{topic} {from}")
    };
    let mentions = if msg.get("for_me") == Some(&Value::Bool(true)) { " (mentions you)" } else { "" };
    let ts = msg.get("ts").and_then(Value::as_f64).unwrap_or_else(now);
    let body = msg.get("body").and_then(Value::as_str).unwrap_or("");
    format!("{} {place}{mentions}: {}", when(ts), shown(body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn msg(v: Value) -> Map {
        v.as_object().cloned().expect("an object")
    }

    #[test]
    fn lines() {
        let t = now();
        let time = local(t as i64, ' ').split_once(' ').map(|(_, t)| t.to_owned()).expect("a time");
        let room = msg(json!({"topic": "room/ops", "from": "al-1", "body": "hi", "ts": t}));
        assert_eq!(line(&room), format!("{time} #ops al-1: hi"));
        let dm = msg(json!({"topic": "inbox/bo-2", "from": "al-1", "body": "a\nb", "ts": t}));
        assert_eq!(line(&dm), format!("{time} DM al-1 -> bo-2: a\n  b"), "continuation lines indented");
        let at = msg(json!({"topic": "room/ops", "from": "al-1", "body": "@bo", "ts": t, "for_me": true}));
        assert_eq!(line(&at), format!("{time} #ops al-1 (mentions you): @bo"));
        let raw = msg(json!({"topic": "ci/build", "from": null, "body": "ok\r\u{202e}x\u{200b}", "ts": t}));
        assert_eq!(line(&raw), format!("{time} ci/build -: ok\\r\\u202ex\\u200b"), "nothing a terminal acts on");
        let old = msg(json!({"topic": "broadcast", "from": "admin", "body": "x", "ts": 86400.0 * 3.0}));
        assert!(line(&old).starts_with("1970-01-0"), "another day starts with its date: {}", line(&old));
    }
}
