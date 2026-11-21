//! The bridge's MQTT helpers: agent and one-shot connections with the bridge's error messages, retained lookups and
//! confirmed QoS 1 publishes.

use crate::config;
use crate::config::Credentials;
use crate::mqtt::{Acked, Conn, DialError, Handler, Message, Options, filter};
use crate::validation::{Error, Result, strerror};
use bytes::Bytes;
use rumqttc::v5::mqttbytes::v5::{Login, PublishProperties};
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
    connect_with(client_id, None, clean, expiry, agent, handler)
}

/// connect, logging in with creds when given. client_id is used as given, so an empty one stays broker-assigned;
/// an owning connection passes creds.client_id.
pub fn connect_with(
    client_id: &str,
    creds: Option<&Credentials>,
    clean: bool,
    expiry: u32,
    agent: bool,
    handler: Arc<dyn Handler>,
) -> Result<Conn> {
    let cfg = config::get()?;
    let opts = Options {
        client_id: client_id.to_owned(),
        clean_start: clean,
        expiry,
        receive_max: Some(RECEIVE_MAX),
        manual_ack: agent,
        reconnect: agent.then_some(RECONNECT_MAX),
    };
    let login = creds.map(|c| Login::new(c.username.as_str(), c.password.as_str()));
    Conn::open_as(&cfg.host, cfg.port, opts, login, handler).map_err(|e| match e {
        DialError::Unreachable(e) => {
            let at = config::socket().map_or_else(|| format!("{}:{}", cfg.host, cfg.port), |p| p.display().to_string());
            Error(format!("broker unreachable at {at} ({}), start it with: docker compose up -d", os_text(&e)))
        }
        DialError::NotAccepted => not_accepted(creds),
    })
}

/// Why the broker turned a connection away, for every client that dials it: a refused login, or under auth enforce
/// no credentials at all.
pub fn not_accepted(creds: Option<&Credentials>) -> Error {
    match creds {
        Some(c) => Error(format!("broker did not accept the login of {}", c.username)),
        None if config::auth_mode().is_ok_and(|m| m == config::Auth::Enforce) => {
            Error("this broker requires credentials (auth enforce)".into())
        }
        None => Error("broker did not accept the connection".into()),
    }
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

/// A one-shot logged in through provision::helper_credentials(None), so it works under auth enforce; anonymous
/// when there are none.
pub fn one_shot(client_id: &str, handler: Arc<dyn Handler>) -> Result<OneShot> {
    one_shot_as(client_id, None, handler)
}

/// one_shot for a caller that knows its own address: addr's credentials first.
pub fn one_shot_as(client_id: &str, addr: Option<&str>, handler: Arc<dyn Handler>) -> Result<OneShot> {
    one_shot_with(client_id, crate::provision::helper_credentials(addr)?.as_ref(), handler)
}

pub fn one_shot_with(client_id: &str, creds: Option<&Credentials>, handler: Arc<dyn Handler>) -> Result<OneShot> {
    connect_with(client_id, creds, true, 0, false, handler).map(OneShot)
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
    retained_as(patterns, None)
}

/// retained for a caller that knows its own address: logged in with addr's credentials first.
pub fn retained_as(patterns: &[&str], addr: Option<&str>) -> Result<Vec<(String, Bytes)>> {
    let found = Arc::new(Collect::default());
    let c = one_shot_as("", addr, found.clone())?;
    let sub = c
        .subscribe(patterns.iter().map(|p| filter(p, false, false)).collect())
        .map_err(|e| Error(format!("cannot subscribe: {}", os_text(&e))))?;
    let barrier = c.unsubscribe(patterns.iter().map(|p| p.to_string()).collect()).ok();
    if barrier.and_then(|pkid| c.wait_ack(pkid, TIMEOUT)).is_none() {
        return Err(Error("the broker did not finish sending retained records".into()));
    }
    // the SUBACK came before that UNSUBACK: a refused read is an error, never an empty answer
    if c.wait_refused(sub, Duration::ZERO) == Some(Acked::Refused) {
        return Err(crate::validation::refused("subscribe to", &patterns.join(", ")));
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
        .and_then(|pkid| c.wait_refused(pkid, if up { TIMEOUT } else { Duration::ZERO }));
    match confirmed {
        None => Err(Error(format!("broker did not confirm the publish to {topic}"))),
        Some(Acked::Refused) => Err(crate::validation::refused("publish on", topic)),
        Some(_) => Ok(()),
    }
}

/// Whether the broker checks logins: false when it accepts a made-up username and password, which only a broker
/// without the dynamic-security plugin does (auth off, whatever the auth-mode file says).
pub fn runs_auth() -> Result<bool> {
    let probe = Credentials {
        username: format!("agent-bridge-probe-{}", crate::ledger::random_hex(4)),
        password: crate::ledger::random_hex(16),
        client_id: String::new(),
    };
    match one_shot_with("", Some(&probe), Arc::new(Ignore)) {
        Ok(_) => Ok(false),
        Err(e) if e.0.starts_with("broker unreachable") => Err(e),
        Err(_) => Ok(true),
    }
}

/// Subscribe and wait for the SUBACK; a topic the broker's ACL refuses is a "not authorized" error naming it.
pub fn subscribe(c: &Conn, topics: &[&str], qos1: bool, no_local: bool) -> Result<()> {
    let pkid = c
        .subscribe(topics.iter().map(|t| filter(t, qos1, no_local)).collect())
        .map_err(|e| Error(format!("cannot subscribe: {}", os_text(&e))))?;
    match c.wait_refused(pkid, TIMEOUT) {
        Some(Acked::Yes) => Ok(()),
        Some(Acked::Refused) => Err(crate::validation::refused("subscribe to", &topics.join(", "))),
        _ => Err(Error(format!("broker did not confirm the subscription to {}", topics.join(", ")))),
    }
}
