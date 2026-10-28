//! The bridge's MQTT helpers: agent and one-shot connections with the bridge's error messages, retained lookups and
//! confirmed QoS 1 publishes.

use crate::config;
use crate::mqtt::{Conn, DialError, Handler, Message, Options, filter};
use crate::validation::{Error, Result, strerror};
use bytes::Bytes;
use rumqttc::v5::mqttbytes::v5::PublishProperties;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub use crate::mqtt::TIMEOUT;
pub const RECEIVE_MAX: u16 = 100;
const RECONNECT_MAX: Duration = Duration::from_secs(120);

/// An OS error the way Python's str() shows it.
pub fn os_text(e: &std::io::Error) -> String {
    match e.raw_os_error() {
        Some(code) => format!("[Errno {code}] {}", strerror(code)),
        None if e.kind() == std::io::ErrorKind::TimedOut => "timed out".into(),
        None => e.to_string(),
    }
}

pub fn connect(client_id: &str, clean: bool, expiry: u32, agent: bool, handler: Arc<dyn Handler>) -> Result<Conn> {
    let cfg = config::get();
    let opts = Options {
        client_id: client_id.to_owned(),
        clean_start: clean,
        expiry,
        receive_max: Some(RECEIVE_MAX),
        manual_ack: agent,
        reconnect: agent.then_some(RECONNECT_MAX),
    };
    Conn::open(&cfg.host, cfg.port, opts, handler).map_err(|e| match e {
        DialError::Unreachable(e) => Error(format!(
            "broker unreachable at {}:{} ({}), start it with: docker compose up -d",
            cfg.host,
            cfg.port,
            os_text(&e)
        )),
        DialError::NotAccepted => Error("broker did not accept the connection".into()),
    })
}

/// A connection for a single lookup or publish; its session ends when it closes, which dropping it does.
pub struct OneShot(Conn);

impl Deref for OneShot {
    type Target = Conn;
    fn deref(&self) -> &Conn {
        &self.0
    }
}

impl Drop for OneShot {
    fn drop(&mut self) {
        self.0.close();
    }
}

pub fn one_shot(client_id: &str, handler: Arc<dyn Handler>) -> Result<OneShot> {
    connect(client_id, true, 0, false, handler).map(OneShot)
}

pub struct Ignore;

impl Handler for Ignore {
    fn message(&self, _conn: &Conn, _msg: Message) {}
}

/// Topic -> payload of what arrives, in arrival order, a repeated topic keeping its place.
#[derive(Default)]
struct Collect(Mutex<Vec<(String, Bytes)>>);

impl Handler for Collect {
    fn message(&self, _conn: &Conn, msg: Message) {
        let Some(topic) = msg.topic else { return };
        let mut found = self.0.lock().unwrap_or_else(|e| e.into_inner());
        match found.iter_mut().find(|(t, _)| *t == topic) {
            Some(entry) => entry.1 = msg.payload,
            None => found.push((topic, msg.payload)),
        }
    }
}

/// Topic -> payload of every retained message matching patterns, through a short-lived client. Retained records at
/// QoS 0 come straight after the SUBACK, outside the inflight window, so the ack of the next request on the connection
/// marks the moment all of them are in.
pub fn retained(patterns: &[&str]) -> Result<Vec<(String, Bytes)>> {
    let found = Arc::new(Collect::default());
    let c = one_shot("", found.clone())?;
    let _ = c.subscribe(patterns.iter().map(|p| filter(p, false, false)).collect());
    let barrier = c.unsubscribe(patterns.iter().map(|p| p.to_string()).collect()).ok();
    if barrier.and_then(|pkid| c.wait_ack(pkid, TIMEOUT)).is_none() {
        return Err(Error("the broker did not finish sending retained records".into()));
    }
    drop(c);
    Ok(std::mem::take(&mut *found.0.lock().unwrap_or_else(|e| e.into_inner())))
}

/// Publish at QoS 1 and wait for the broker to confirm it.
pub fn publish(c: &Conn, topic: &str, payload: impl Into<Bytes>, retain: bool, expire: Option<u32>) -> Result<()> {
    let properties =
        expire.filter(|e| *e > 0).map(|e| PublishProperties { message_expiry_interval: Some(e), ..Default::default() });
    let up = c.is_up();
    let confirmed = c
        .publish(topic, payload, true, retain, properties, true)
        .ok()
        .and_then(|pkid| c.wait_ack(pkid, if up { TIMEOUT } else { Duration::ZERO }));
    if confirmed.is_none() {
        return Err(Error(format!("broker did not confirm the publish to {topic}")));
    }
    Ok(())
}
