//! Envelope and JSON decoding on any bytes: json::loads never panics and what it accepts writes back and reads back
//! the same (dumps then loads); presence records and messages on today's topics never panic.

#![no_main]

use agent_bridge::json::{dumps, loads};
use agent_bridge::protocol::{PAYLOAD_MAX, parse_json, parse_message, parse_presence};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Some(v) = loads(data) {
        let text = dumps(&v);
        assert_eq!(loads(text.as_bytes()).as_ref(), Some(&v), "dumps did not read back: {text}");
    }
    let _ = parse_json(data, PAYLOAD_MAX);
    if let Some(rec) = parse_presence(data) {
        // a record the reader accepts is accepted again as written
        let again = parse_presence(dumps(&serde_json::Value::Object(rec.clone())).as_bytes());
        assert_eq!(again.as_ref(), Some(&rec));
    }
    for topic in ["inbox/al-1", "room/r", "broadcast", "x/y"] {
        let _ = parse_message(topic, data);
    }
});
