//! Client for ledgerd: call(op, as, fields) returns the reply or a LedgerError, over a connection of its own.

use crate::config;
use crate::json::{Map, dumps};
use crate::mqtt::{Conn, DialError, Handler, Message, Options, filter};
use crate::validation::{ID_MAX, is_ident};
use bytes::Bytes;
use rumqttc::v5::mqttbytes::v5::PublishProperties;
use serde_json::Value;
use std::fmt;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

/// An ok:false reply, a timeout or a malformed reply; reply holds the reply.
#[derive(Debug)]
pub struct LedgerError {
    pub reply: Map,
}

impl LedgerError {
    fn new(error: impl Into<String>) -> LedgerError {
        let mut reply = Map::new();
        reply.insert("ok".into(), false.into());
        reply.insert("error".into(), error.into().into());
        LedgerError { reply }
    }
}

impl fmt::Display for LedgerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.reply.get("error") {
            None => f.write_str("ledger error"),
            Some(Value::String(s)) => f.write_str(s),
            Some(other) => f.write_str(&dumps(other)),
        }
    }
}

pub fn random_bytes(n: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; n];
    getrandom::fill(&mut bytes).expect("the OS random source is available");
    bytes
}

pub fn random_hex(n: usize) -> String {
    random_bytes(n).iter().map(|b| format!("{b:02x}")).collect()
}

/// Python's "{:g}" for the timeouts shown in messages.
pub fn fmt_g(v: f64) -> String {
    if v == 0.0 || !v.is_finite() {
        return format!("{v}");
    }
    let decimals = (5 - v.abs().log10().floor() as i32).max(0) as usize;
    let s = format!("{v:.decimals$}");
    if s.contains('.') { s.trim_end_matches('0').trim_end_matches('.').to_owned() } else { s }
}

struct Reply {
    topic: String,
    corr: Bytes,
    got: Mutex<Option<Bytes>>,
    cv: Condvar,
}

impl Handler for Reply {
    fn message(&self, _conn: &Conn, msg: Message) {
        if msg.topic.as_deref() == Some(&self.topic) && msg.correlation.as_ref() == Some(&self.corr) {
            *self.got.lock().unwrap_or_else(|e| e.into_inner()) = Some(msg.payload);
            self.cv.notify_all();
        }
    }
}

/// One ledgerd request as as_; timeout defaults to 10 s plus the request's wait.
pub fn call(op: &str, as_: &str, fields: Map, timeout: Option<f64>) -> Result<Map, LedgerError> {
    let wait =
        fields.get("wait").and_then(Value::as_f64).filter(|w| w.is_finite()).map_or(0.0, |w| w.clamp(0.0, 300.0));
    let mut timeout = timeout.filter(|t| *t != 0.0).unwrap_or(10.0 + wait);
    // A bad wait is left for ledgerd to refuse; the timeout itself stays usable.
    if !(timeout > 0.0 && timeout <= 3600.0) {
        timeout = if timeout > 3600.0 { 3600.0 } else { 10.0 + wait };
    }
    let mut req = Map::new();
    req.insert("op".into(), op.into());
    req.extend(fields);
    req.insert("as".into(), as_.into());
    let reply = Arc::new(Reply {
        topic: format!("ledger/resp/{}", random_hex(16)),
        corr: Bytes::from(random_bytes(16)),
        got: Mutex::new(None),
        cv: Condvar::new(),
    });
    let cfg = config::get();
    let opts = Options {
        client_id: String::new(),
        clean_start: true,
        expiry: 0,
        receive_max: None,
        manual_ack: false,
        reconnect: None,
    };
    let c = Conn::open(&cfg.host, cfg.port, opts, reply.clone()).map_err(|e| match e {
        DialError::Unreachable(e) => LedgerError::new(format!(
            "broker unreachable at {}:{} ({})",
            cfg.host,
            cfg.port,
            crate::broker::os_text(&e)
        )),
        DialError::NotAccepted => LedgerError::new("broker did not accept the connection"),
    })?;
    let _ = c.subscribe(vec![filter(&reply.topic, true, false)]);
    let properties = PublishProperties {
        response_topic: Some(reply.topic.clone()),
        correlation_data: Some(reply.corr.clone()),
        // The broker discards the request if ledgerd cannot pick it up before we stop listening.
        message_expiry_interval: Some(timeout.ceil().clamp(0.0, u32::MAX as f64) as u32),
        ..Default::default()
    };
    // Per-agent topic, so broker ACLs can later vouch for the address; ledgerd checks it matches "as".
    let topic = if is_ident(as_, ID_MAX) { format!("ledger/req/{as_}") } else { "ledger/req".into() };
    let _ = c.publish(&topic, dumps(&Value::Object(req)), true, false, Some(properties), false);
    let got = reply.got.lock().unwrap_or_else(|e| e.into_inner());
    let (mut got, _) = reply
        .cv
        .wait_timeout_while(got, Duration::from_secs_f64(timeout), |g| g.is_none())
        .unwrap_or_else(|e| e.into_inner());
    let payload = got.take();
    c.close();
    let payload = payload
        .ok_or_else(|| LedgerError::new(format!("no reply from ledgerd within {}s, is it running?", fmt_g(timeout))))?;
    match crate::json::loads(&payload) {
        Some(Value::Object(reply)) if reply.get("ok") == Some(&Value::Bool(true)) => Ok(reply),
        Some(Value::Object(reply)) => Err(LedgerError { reply }),
        _ => Err(LedgerError::new("malformed reply")),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn g_format() {
        for (v, s) in
            [(10.0, "10"), (13.0, "13"), (10.5, "10.5"), (3.0, "3"), (10.123456789, "10.1235"), (0.25, "0.25")]
        {
            assert_eq!(super::fmt_g(v), s);
        }
    }
}
