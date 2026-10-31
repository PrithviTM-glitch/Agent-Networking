//! The request/response pattern the bridge's services share (ledgerd, historyd): a request on <service>/req/<as>
//! (plain <service>/req when as is absent or not an address) with ResponseTopic <service>/resp/<client id> and fresh
//! CorrelationData, and the reply matched back by that correlation. Over an agent's own connection the reader thread
//! acks replies at once and never buffers them, so they cannot fill its receive window.

use crate::ledger::random_bytes;
use crate::mqtt::{Conn, filter};
use crate::validation::{ID_MAX, is_ident};
use bytes::Bytes;
use rumqttc::v5::mqttbytes::v5::PublishProperties;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

/// Replies awaited on one connection, by CorrelationData.
#[derive(Default)]
pub struct Replies {
    slots: Mutex<HashMap<Vec<u8>, Option<Bytes>>>,
    cv: Condvar,
}

impl Replies {
    /// Keep payload if its correlation is awaited; a late or foreign reply is dropped.
    pub fn fill(&self, correlation: Option<&Bytes>, payload: Bytes) {
        let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(slot) = correlation.and_then(|c| slots.get_mut(c.as_ref())) {
            *slot = Some(payload);
            self.cv.notify_all();
        }
    }

    fn expect(&self, corr: &[u8]) {
        self.slots.lock().unwrap_or_else(|e| e.into_inner()).insert(corr.to_vec(), None);
    }

    /// The reply to corr within timeout, checking stop every 100 ms. The slot is gone afterwards unless stop ended
    /// the wait, so a reply still on its way is kept for take.
    fn wait(&self, corr: &[u8], timeout: Duration, stop: &dyn Fn() -> bool) -> Sent {
        let deadline = Instant::now() + timeout;
        let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if let Some(Some(_)) = slots.get(corr) {
                return slots.remove(corr).flatten().map_or(Sent::Timeout, Sent::Reply);
            }
            if stop() {
                return Sent::Stopped(corr.to_vec());
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                slots.remove(corr);
                return Sent::Timeout;
            }
            slots =
                self.cv.wait_timeout(slots, left.min(Duration::from_millis(100))).unwrap_or_else(|e| e.into_inner()).0;
        }
    }

    /// The reply to a stopped request if it has come, forgetting the slot.
    pub fn take(&self, corr: &[u8]) -> Option<Bytes> {
        self.slots.lock().unwrap_or_else(|e| e.into_inner()).remove(corr).flatten()
    }
}

/// What became of a request: its reply, none within the timeout, or stopped, with the CorrelationData it went with.
pub enum Sent {
    Reply(Bytes),
    Timeout,
    Stopped(Vec<u8>),
}

/// The reply topic of a client: named after its client id, so a persistent session never piles up subscriptions.
pub fn reply_topic(service: &str, client_id: &str) -> String {
    let id = if is_ident(client_id, 128) { client_id.to_owned() } else { crate::ledger::random_hex(16) };
    format!("{service}/resp/{id}")
}

/// Send one request and wait up to timeout seconds for its reply; None if none came.
pub fn send(
    conn: &Conn,
    replies: &Replies,
    reply_topic: &str,
    service: &str,
    as_: Option<&Value>,
    body: String,
    timeout: f64,
) -> Option<Bytes> {
    match send_until(conn, replies, reply_topic, service, as_, body, timeout, &|| false) {
        Sent::Reply(reply) => Some(reply),
        _ => None,
    }
}

/// send, giving up early once stop() is true.
#[allow(clippy::too_many_arguments)]
pub fn send_until(
    conn: &Conn,
    replies: &Replies,
    reply_topic: &str,
    service: &str,
    as_: Option<&Value>,
    body: String,
    timeout: f64,
    stop: &dyn Fn() -> bool,
) -> Sent {
    let corr = random_bytes(16);
    replies.expect(&corr);
    // Resubscribed every call: the broker may have lost the session since; it handles this before the request.
    let _ = conn.subscribe(vec![filter(reply_topic, true, false)]);
    let properties = PublishProperties {
        response_topic: Some(reply_topic.to_owned()),
        correlation_data: Some(Bytes::from(corr.clone())),
        // The broker discards the request if the service cannot pick it up before we stop listening.
        message_expiry_interval: Some(timeout.ceil().clamp(0.0, u32::MAX as f64) as u32),
        ..Default::default()
    };
    // Per-agent topic, so broker ACLs can later vouch for the address; the service checks it matches "as".
    let topic = match as_.and_then(Value::as_str).filter(|a| is_ident(a, ID_MAX)) {
        Some(a) => format!("{service}/req/{a}"),
        None => format!("{service}/req"),
    };
    let _ = conn.publish(&topic, body, true, false, Some(properties), false);
    replies.wait(&corr, Duration::from_secs_f64(timeout), stop)
}
