//! Topic parsing, the sender-bound forms included. Input: the topic, a newline, the payload.
//! - parse_message never panics;
//! - a sender-bound topic (inbox/<to>/<from>, room/<room>/<from>) normalises to its one-level form, and a message
//!   read from it is marked "bound" and comes from the topic's sender;
//! - every other room/ or inbox/ topic that isn't one level deep is dropped;
//! - a message read from a one-level topic is "claimed".

#![no_main]

use agent_bridge::protocol::{message_id, parse_message, sender_bound};
use libfuzzer_sys::fuzz_target;
use serde_json::Value;

fuzz_target!(|data: &[u8]| {
    let split = data.iter().position(|b| *b == b'\n').unwrap_or(data.len());
    let Ok(topic) = std::str::from_utf8(&data[..split]) else { return };
    let payload = data.get(split + 1..).unwrap_or(&[]);
    let msg = parse_message(topic, payload);
    let room_or_inbox = topic.starts_with("room/") || topic.starts_with("inbox/");
    match sender_bound(topic) {
        Some((base, from)) => {
            assert_eq!(base.matches('/').count(), 1, "{topic} -> {base}");
            assert!(topic.starts_with(&format!("{base}/")), "{topic} -> {base}");
            assert_eq!(&topic[base.len() + 1..], from);
            if let Some(m) = &msg {
                assert_eq!(m.get("topic").and_then(Value::as_str), Some(base.as_str()), "{m:?}");
                assert_eq!(m.get("from").and_then(Value::as_str), Some(from), "{m:?}");
                assert_eq!(m.get("sender").and_then(Value::as_str), Some("bound"), "{m:?}");
            }
        }
        None if room_or_inbox && topic.matches('/').count() != 1 => {
            assert!(msg.is_none(), "a {topic} message must be dropped: {msg:?}")
        }
        None => {
            if let Some(m) = &msg
                && room_or_inbox
            {
                assert_eq!(m.get("sender").and_then(Value::as_str), Some("claimed"), "{m:?}");
            }
        }
    }
    if let Some(m) = msg {
        let topic = m.get("topic").and_then(Value::as_str).unwrap_or_default();
        let from = m.get("from").and_then(Value::as_str).unwrap_or_default();
        if let Some(ts) = m.get("ts").and_then(Value::as_f64) {
            let id = message_id(topic, from, ts);
            assert!(id.len() == 16 && id.bytes().all(|b| b.is_ascii_hexdigit()), "{id}");
            assert_eq!(id, message_id(topic, from, ts));
        }
    }
});
