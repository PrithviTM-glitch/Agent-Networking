//! Client for historyd. Every call fails with a HistoryError, one line ("history: ..."), when historyd refuses, is down
//! or the broker is. Calls go over a connection of their own, or over an agent's Bus (Bus::history_call).

use crate::broker;
use crate::json::{Map, dumps, float};
use crate::ledger::fmt_g;
use crate::mqtt::{Conn, Handler, Message};
use crate::request::{self, Replies};
use crate::validation::{ADMIN, Error, check_ident, check_last, check_topic};
use serde_json::Value;
use std::fmt;
use std::sync::Arc;

pub const TIMEOUT: f64 = 5.0;
/// The broadcast hook's: it runs on every prompt.
pub const BROADCASTS_TIMEOUT: f64 = 1.0;
/// leave's
pub const DROP_TIMEOUT: f64 = 3.0;

/// An ok:false reply, a timeout, a malformed reply or no broker; reply holds the reply.
#[derive(Debug)]
pub struct HistoryError {
    pub reply: Map,
}

impl HistoryError {
    fn new(error: impl Into<String>) -> HistoryError {
        let mut reply = Map::new();
        reply.insert("ok".into(), false.into());
        reply.insert("error".into(), error.into().into());
        HistoryError { reply }
    }
}

impl fmt::Display for HistoryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let error = match self.reply.get("error") {
            None => "error".to_owned(),
            Some(Value::String(s)) => s.clone(),
            Some(other) => dumps(other),
        };
        write!(f, "history: {}", error.split_whitespace().collect::<Vec<_>>().join(" "))
    }
}

impl From<HistoryError> for Error {
    fn from(e: HistoryError) -> Error {
        Error(e.to_string())
    }
}

/// Collects the replies to a connection of its own.
struct Own(Replies);

impl Handler for Own {
    fn message(&self, _conn: &Conn, msg: Message) {
        self.0.fill(msg.correlation.as_ref(), msg.payload);
    }
}

/// One historyd request over conn, whose reader thread fills replies from reply_topic. Fields given as null are
/// left out, so historyd uses its defaults.
pub fn call_over(
    conn: &Conn,
    replies: &Replies,
    reply_topic: &str,
    op: Value,
    as_: Option<Value>,
    fields: Map,
    timeout: f64,
) -> Result<Map, HistoryError> {
    let mut req = Map::new();
    req.insert("op".into(), op);
    req.extend(fields.into_iter().filter(|(_, v)| !v.is_null()));
    if let Some(a) = as_.clone() {
        req.insert("as".into(), a);
    }
    let body = dumps(&Value::Object(req));
    let payload =
        request::send(conn, replies, reply_topic, "history", as_.as_ref(), body, timeout).ok_or_else(|| {
            HistoryError::new(format!("no reply from historyd within {}s, is it running?", fmt_g(timeout)))
        })?;
    match crate::json::loads(&payload) {
        Some(Value::Object(reply)) if reply.get("ok") == Some(&Value::Bool(true)) => Ok(reply),
        Some(Value::Object(reply)) => Err(HistoryError { reply }),
        _ => Err(HistoryError::new("malformed reply from historyd")),
    }
}

/// One historyd request over a connection of its own.
pub fn call(op: &str, as_: Option<&str>, fields: Map, timeout: f64) -> Result<Map, HistoryError> {
    call_value(op.into(), as_.map(Value::from), fields, timeout)
}

/// call with op and as_ as the caller gave them; historyd refuses what is wrong with them.
pub fn call_value(op: Value, as_: Option<Value>, fields: Map, timeout: f64) -> Result<Map, HistoryError> {
    let own = Arc::new(Own(Replies::default()));
    let c = broker::one_shot("", own.clone()).map_err(|e| HistoryError::new(e.0))?;
    call_over(&c, &own.0, &request::reply_topic("history", ""), op, as_, fields, timeout)
}

fn messages(reply: Map) -> Vec<Map> {
    match reply.get("messages") {
        Some(Value::Array(items)) => items.iter().filter_map(|m| m.as_object().cloned()).collect(),
        _ => Vec::new(),
    }
}

fn fields(pairs: &[(&str, Value)]) -> Map {
    pairs.iter().map(|(k, v)| ((*k).to_owned(), v.clone())).collect()
}

/// A room's last messages, oldest first; before (a ts) pages back.
pub fn room(room: &str, last: i64, before: Option<f64>) -> crate::Result<Vec<Map>> {
    check_ident(room, "room")?;
    check_last(last)?;
    let f = fields(&[("room", room.into()), ("last", last.into()), ("before", before.map_or(Value::Null, float))]);
    Ok(messages(call("room", None, f, TIMEOUT)?))
}

/// addr's direct messages in both directions, oldest first; with peer, only those with peer.
pub fn dms(addr: &str, peer: Option<&str>, last: i64) -> crate::Result<Vec<Map>> {
    check_ident(addr, "address")?;
    if let Some(p) = peer {
        check_ident(p, "address")?;
    }
    check_last(last)?;
    let f = fields(&[("peer", peer.map_or(Value::Null, Value::from)), ("last", last.into())]);
    Ok(messages(call("dms", Some(addr), f, TIMEOUT)?))
}

/// A raw topic's last messages, oldest first.
pub fn topic(topic: &str, last: i64) -> crate::Result<Vec<Map>> {
    check_topic(topic, false)?;
    check_last(last)?;
    Ok(messages(call("topic", None, fields(&[("topic", topic.into()), ("last", last.into())]), TIMEOUT)?))
}

/// The admin's broadcasts newer than since (a ts), oldest first.
pub fn broadcasts(since: f64, last: i64) -> crate::Result<Vec<Map>> {
    let f = fields(&[("since", float(since)), ("last", last.into())]);
    Ok(messages(call("broadcasts", None, f, BROADCASTS_TIMEOUT)?))
}

/// The last messages of every topic, oldest first; for the admin console.
pub fn recent(last: i64) -> crate::Result<Vec<Map>> {
    Ok(messages(call("recent", Some(ADMIN), fields(&[("last", last.into())]), TIMEOUT)?))
}

/// Delete addr's received DMs; the number deleted.
pub fn drop(addr: &str) -> crate::Result<i64> {
    Ok(call("drop", Some(addr), Map::new(), DROP_TIMEOUT)?.get("dropped").and_then(Value::as_i64).unwrap_or(0))
}
