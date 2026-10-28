//! Client for ledgerd: call(op, as, fields) returns the reply or a LedgerError, over a connection of its own;
//! call_over does the same over an existing one, such as an agent's Bus.

use crate::config;
use crate::json::{Map, dumps};
use crate::mqtt::{Conn, DialError, Handler, Message, Options};
use crate::request::{self, Replies};
use serde_json::Value;
use std::fmt;
use std::sync::Arc;

/// An ok:false reply, a timeout or a malformed reply; reply holds the reply.
#[derive(Debug)]
pub struct LedgerError {
    pub reply: Map,
}

impl LedgerError {
    pub fn new(error: impl Into<String>) -> LedgerError {
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

/// The limits ledgerd enforces, exported so clients and tests follow the implementation.
pub const MAX_LEDGERS: i64 = 1000;
pub const MAX_RESOURCES: i64 = 128;
pub const MAX_PENDING: i64 = 64;
pub const MAX_LEASES: i64 = 256;
pub const MAX_WAITERS: i64 = 256;
/// stored requests per holder, any status
pub const MAX_REQUESTS: i64 = 512;
pub const MAX_TTL: f64 = 3600.0;
pub const MAX_WAIT: f64 = 300.0;

/// Collects the replies to a connection of its own.
struct Own(Replies);

impl Handler for Own {
    fn message(&self, _conn: &Conn, msg: Message) {
        self.0.fill(msg.correlation.as_ref(), msg.payload);
    }
}

/// How long to wait: timeout, else 10 s plus the request's wait. A bad wait is left for ledgerd to refuse; the
/// timeout itself stays usable.
fn timeout_for(fields: &Map, timeout: Option<f64>) -> f64 {
    let wait =
        fields.get("wait").and_then(Value::as_f64).filter(|w| w.is_finite()).map_or(0.0, |w| w.clamp(0.0, MAX_WAIT));
    let timeout = timeout.filter(|t| *t != 0.0).unwrap_or(10.0 + wait);
    if timeout > 0.0 && timeout <= 3600.0 {
        timeout
    } else if timeout > 3600.0 {
        3600.0
    } else {
        10.0 + wait
    }
}

/// One ledgerd request as as_ over conn, whose reader thread fills replies from reply_topic.
pub fn call_over(
    conn: &Conn,
    replies: &Replies,
    reply_topic: &str,
    op: Value,
    as_: Option<Value>,
    fields: Map,
    timeout: Option<f64>,
) -> Result<Map, LedgerError> {
    let timeout = timeout_for(&fields, timeout);
    let mut req = Map::new();
    req.insert("op".into(), op);
    req.extend(fields);
    if let Some(a) = as_.clone() {
        req.insert("as".into(), a);
    }
    let payload =
        request::send(conn, replies, reply_topic, "ledger", as_.as_ref(), dumps(&Value::Object(req)), timeout)
            .ok_or_else(|| {
                LedgerError::new(format!("no reply from ledgerd within {}s, is it running?", fmt_g(timeout)))
            })?;
    match crate::json::loads(&payload) {
        Some(Value::Object(reply)) if reply.get("ok") == Some(&Value::Bool(true)) => Ok(reply),
        Some(Value::Object(reply)) => Err(LedgerError { reply }),
        _ => Err(LedgerError::new("malformed reply")),
    }
}

/// One ledgerd request as as_, over a connection of its own.
pub fn call(op: &str, as_: &str, fields: Map, timeout: Option<f64>) -> Result<Map, LedgerError> {
    call_value(op.into(), Some(as_.into()), fields, timeout)
}

/// call with op and as_ as the caller gave them; ledgerd refuses what is wrong with them, as it does for any client.
pub fn call_value(op: Value, as_: Option<Value>, fields: Map, timeout: Option<f64>) -> Result<Map, LedgerError> {
    let cfg = config::get().map_err(|e| LedgerError::new(e.0))?;
    let own = Arc::new(Own(Replies::default()));
    let opts = Options {
        client_id: String::new(),
        clean_start: true,
        expiry: 0,
        receive_max: None,
        manual_ack: false,
        reconnect: None,
    };
    let c = Conn::open(&cfg.host, cfg.port, opts, own.clone()).map_err(|e| match e {
        DialError::Unreachable(e) => LedgerError::new(format!(
            "broker unreachable at {}:{} ({})",
            cfg.host,
            cfg.port,
            crate::broker::os_text(&e)
        )),
        DialError::NotAccepted => LedgerError::new("broker did not accept the connection"),
    })?;
    let reply = call_over(&c, &own.0, &request::reply_topic("ledger", ""), op, as_, fields, timeout);
    c.close();
    reply
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
