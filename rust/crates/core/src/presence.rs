//! The directory of agents: retained presence records under agents/<addr>, read through short-lived clients.

use crate::broker::{self, Ignore};
use crate::config;
use crate::json::{Map, now};
use crate::protocol::{LINEAGE, NEVER, ORIGIN, parse_presence};
use crate::validation::{Error, ID_MAX, Result, check_ident, is_ident};
use bytes::Bytes;
use serde_json::Value;
use std::sync::Arc;

pub const TOPICS: &str = "agents/+";

/// Address -> presence record of every well-formed record among retained topic -> payload, in arrival order.
pub fn parse_all(retained: &[(String, Bytes)]) -> Vec<(String, Map)> {
    retained
        .iter()
        .filter_map(|(t, payload)| {
            let addr = t.strip_prefix("agents/").filter(|a| is_ident(a, ID_MAX))?;
            Some((addr.to_owned(), parse_presence(payload)?))
        })
        .collect()
}

pub fn presences(pattern: &str) -> Result<Vec<(String, Map)>> {
    Ok(parse_all(&broker::retained(&[pattern])?))
}

pub fn find(addr: &str) -> Result<Option<Map>> {
    let addr = check_ident(addr, "address")?;
    Ok(presences(&format!("agents/{addr}"))?.into_iter().find(|(a, _)| a == addr).map(|(_, rec)| rec))
}

pub fn need(addr: &str) -> Result<Map> {
    find(addr)?.ok_or_else(|| Error(format!("unknown address: {addr}")))
}

/// End addr's broker session and clear its presence.
pub fn wipe(addr: &str) -> Result<()> {
    // A clean start with expiry 0: the session ends with this connection.
    let c = broker::one_shot(addr, Arc::new(Ignore))?;
    broker::publish(&c, &format!("agents/{addr}"), Bytes::new(), true, None)
}

pub fn topics(rec: &Map) -> impl Iterator<Item = &str> {
    rec.get("topics").and_then(Value::as_array).into_iter().flatten().filter_map(Value::as_str)
}

pub fn in_room(rec: &Map, room: &str) -> bool {
    let topic = format!("room/{room}");
    topics(rec).any(|t| topic_matches(t, &topic))
}

/// Whether an MQTT subscription filter matches a topic, as paho's topic_matches_sub decides it.
pub fn topic_matches(filter: &str, topic: &str) -> bool {
    let (f, t): (Vec<&str>, Vec<&str>) = (filter.split('/').collect(), topic.split('/').collect());
    let dollar = topic.starts_with('$');
    for (i, level) in f.iter().enumerate() {
        if *level == "#" {
            return !(i == 0 && dollar);
        }
        match t.get(i) {
            None => return false,
            Some(_) if *level == "+" && i == 0 && dollar => return false,
            Some(part) if *level != "+" && level != part => return false,
            _ => {}
        }
    }
    f.len() == t.len()
}

pub fn seen(rec: &Map) -> f64 {
    rec.get("seen").and_then(Value::as_f64).unwrap_or(0.0)
}

/// Every live agent, most recently seen first; with room, only its members.
pub fn agents(room: Option<&str>) -> Result<Vec<Map>> {
    let room = room.map(|r| check_ident(r, "room")).transpose()?;
    let mut all = presences(TOPICS)?;
    all.sort_by(|a, b| seen(&b.1).total_cmp(&seen(&a.1)));
    let now = now();
    Ok(all.iter().filter(|(_, rec)| room.is_none_or(|r| in_room(rec, r))).map(|(a, rec)| row(a, rec, now)).collect())
}

fn row(addr: &str, rec: &Map, now: f64) -> Map {
    let mut out = Map::new();
    out.insert("addr".into(), addr.into());
    for key in ["name", "caps", "topics", "keep"] {
        out.insert(key.into(), rec[key].clone());
    }
    out.insert("idle_s".into(), ((now - seen(rec)).round_ties_even() as i64).into());
    for key in ORIGIN.iter().chain(LINEAGE.iter()) {
        if let Some(v) = rec.get(*key) {
            out.insert((*key).into(), v.clone());
        }
    }
    out
}

pub fn keep(rec: &Map) -> bool {
    rec.get("keep").and_then(Value::as_bool).unwrap_or(false)
}

/// The session expiry for an agent's record: forever with keep, else the stale period.
pub fn expiry(rec: &Map) -> u32 {
    if keep(rec) { NEVER } else { config::get().stale_s.min(u32::MAX as u64) as u32 }
}

#[cfg(test)]
mod tests {
    use super::topic_matches;

    #[test]
    fn matching() {
        for (f, t, yes) in [
            ("room/x", "room/x", true),
            ("room/#", "room/x", true),
            ("room/#", "room", true),
            ("#", "room/x", true),
            ("room/+", "room/x", true),
            ("room/+", "room", false),
            ("+/x", "room/x", true),
            ("room/y", "room/x", false),
            ("room/x/#", "room/x", true),
            ("room", "room/x", false),
            ("#", "$SYS/x", false),
        ] {
            assert_eq!(topic_matches(f, t), yes, "{f} {t}");
        }
    }
}
