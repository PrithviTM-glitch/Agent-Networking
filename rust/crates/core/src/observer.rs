//! A read-only view of every topic, for the admin console: presence records, room records, messages and the
//! broker's $SYS statistics, parsed as hostile input like everything else and queued for whoever reads them. No
//! callbacks: the reader thread only queues, so no caller's code ever runs on it.

use crate::config;
use crate::json::{Map, now};
use crate::ledger::random_hex;
use crate::mqtt::{Conn, Handler, Message, Options, filter};
use crate::protocol::{parse_message, parse_presence};
use crate::rooms::parse_room;
use crate::validation::{ID_MAX, Result, SERVICES, is_ident, os_error};
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

/// Events kept while nobody reads; beyond it the oldest go and `dropped` counts them.
const QUEUE_MAX: usize = 10_000;
/// $SYS values are short numbers and versions; anything longer is not the broker's.
const SYS_MAX: usize = 200;
const RECONNECT_MAX: Duration = Duration::from_secs(120);

pub enum Event {
    /// A (re)connect: what was shown before may be stale, and the retained records follow afresh.
    Connected,
    /// A presence record; None when it was cleared or is malformed.
    Presence { addr: String, rec: Option<Map> },
    /// A room's settings record; None when it was cleared or is malformed (the room is then open).
    Room { room: String, rec: Option<Map> },
    /// A room, DM, broadcast or raw-topic message, with the time it arrived here.
    Message { received: f64, msg: Map },
    /// One of the broker's own statistics.
    Sys { topic: String, text: String },
}

#[derive(Default)]
struct Queue {
    events: VecDeque<Event>,
    dropped: u64,
}

#[derive(Default)]
struct Shared {
    queue: Mutex<Queue>,
    cv: Condvar,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn push(&self, event: Event) {
        let mut q = self.lock();
        if q.events.len() >= QUEUE_MAX {
            q.events.pop_front();
            q.dropped += 1;
        }
        q.events.push_back(event);
        self.cv.notify_all();
    }
}

fn parse(topic: &str, payload: &[u8]) -> Option<Event> {
    if topic.starts_with("$SYS/") {
        return (payload.len() < SYS_MAX)
            .then(|| Event::Sys { topic: topic.to_owned(), text: String::from_utf8_lossy(payload).into_owned() });
    }
    if let Some(addr) = topic.strip_prefix("agents/") {
        let rec = (!payload.is_empty()).then(|| parse_presence(payload)).flatten();
        return Some(Event::Presence { addr: addr.to_owned(), rec });
    }
    if let Some(room) = topic.strip_prefix("rooms/") {
        let rec = (!payload.is_empty()).then(|| parse_room(payload)).flatten();
        return is_ident(room, ID_MAX).then(|| Event::Room { room: room.to_owned(), rec });
    }
    if SERVICES.contains(&topic.split('/').next().unwrap_or("")) {
        return None;
    }
    parse_message(topic, payload).map(|msg| Event::Message { received: now(), msg })
}

impl Handler for Shared {
    fn connected(&self, conn: &Conn, _session_present: bool) {
        self.push(Event::Connected);
        // "#" never matches $SYS topics, so the broker's statistics need a subscription of their own.
        let _ = conn.subscribe(vec![filter("#", false, false), filter("$SYS/#", false, false)]);
    }

    fn message(&self, _conn: &Conn, m: Message) {
        if let Some(event) = m.topic.as_deref().and_then(|t| parse(t, &m.payload)) {
            self.push(event);
        }
    }
}

pub struct Observer {
    conn: Conn,
    shared: Arc<Shared>,
}

impl Observer {
    /// Start observing; it connects in the background and reconnects on its own.
    pub fn open() -> Result<Observer> {
        let cfg = config::get()?;
        let shared = Arc::new(Shared::default());
        let opts = Options {
            client_id: format!("monitor-{}", random_hex(4)),
            clean_start: true,
            expiry: 0,
            receive_max: None,
            manual_ack: false,
            reconnect: Some(RECONNECT_MAX),
        };
        let conn = Conn::start(&cfg.host, cfg.port, opts, shared.clone()).map_err(|e| os_error(&e, None))?;
        Ok(Observer { conn, shared })
    }

    /// The next event, waiting up to timeout for one.
    pub fn next(&self, timeout: Duration) -> Option<Event> {
        let q = self.shared.lock();
        let (mut q, _) =
            self.shared.cv.wait_timeout_while(q, timeout, |q| q.events.is_empty()).unwrap_or_else(|e| e.into_inner());
        q.events.pop_front()
    }

    /// Events lost to a full queue since the start.
    pub fn dropped(&self) -> u64 {
        self.shared.lock().dropped
    }

    pub fn close(&self) {
        self.conn.close();
    }
}

impl Drop for Observer {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_like_the_console() {
        let presence = br#"{"name": "a", "caps": [], "topics": [], "keep": false, "seen": 1}"#;
        assert!(matches!(parse("agents/a-1", presence), Some(Event::Presence { rec: Some(_), .. })));
        assert!(matches!(parse("agents/a-1", b""), Some(Event::Presence { rec: None, .. })));
        assert!(matches!(parse("rooms/r", b"junk"), Some(Event::Room { rec: None, .. })));
        assert!(parse("rooms/bad/room", b"").is_none());
        assert!(matches!(parse("$SYS/broker/uptime", b"12 seconds"), Some(Event::Sys { .. })));
        assert!(parse("$SYS/broker/x", &[b'1'; SYS_MAX]).is_none());
        assert!(parse("ledger/resp/x", br#"{"ok": true}"#).is_none());
        assert!(parse("history/req/x", b"{}").is_none());
        assert!(matches!(parse("ci/build", b"red"), Some(Event::Message { .. })));
        assert!(parse("room/r", b"not json").is_none());
    }
}
