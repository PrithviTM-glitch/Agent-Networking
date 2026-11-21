//! Kani harnesses (scripts/verify-kani.sh) for functions that take untrusted input or must keep a shape: built only
//! under cfg(kani), so a normal build never sees them. The ones for provision and address live next to their code.

use crate::guard::paths::lexical;
use crate::protocol::{message_id, parse_message, sender_bound};
use std::path::Path;

/// Up to N bytes of any value, as a str when they are UTF-8.
fn any_str<const N: usize>(bytes: &[u8; N]) -> Option<&str> {
    let len: usize = kani::any();
    kani::assume(len <= N);
    std::str::from_utf8(&bytes[..len]).ok()
}

/// A message id never panics, whatever the timestamp (NaN and the infinities included), and is 16 lowercase hex.
#[kani::proof]
#[kani::unwind(70)]
fn a_message_id_is_always_16_hex() {
    let ts: f64 = kani::any();
    let id = message_id("room/r", "al-a1b2c3", ts);
    assert!(id.len() == 16);
    assert!(id.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
}

/// Topic and payload parsing never panics on any bytes up to N, and a sender-bound topic's sender is a level of it.
#[kani::proof]
#[kani::unwind(7)]
fn topic_parsing_never_panics() {
    let topic_bytes: [u8; 6] = kani::any();
    let payload: [u8; 2] = kani::any();
    if let Some(topic) = any_str(&topic_bytes) {
        if let Some((base, from)) = sender_bound(topic) {
            assert!(topic.starts_with(&base) && topic.ends_with(from));
        }
        let _ = parse_message(topic, &payload);
    }
}

/// With no `..` and no root in rest, the lexical step never leaves base.
#[kani::proof]
#[kani::unwind(6)]
fn lexical_without_dotdot_stays_in_its_base() {
    let bytes: [u8; 4] = kani::any();
    let Some(rest) = any_str(&bytes) else { return };
    kani::assume(!rest.starts_with('/') && !Path::new(rest).components().any(|c| c.as_os_str() == ".."));
    let base = Path::new("/w/lane");
    assert!(lexical(base, rest).starts_with(base));
}
