//! Room settings: a retained record at rooms/<room> holding mode, moderators, rules, muted and banned.
//!
//! A room without a record is open and unmoderated. admin moderates every room and alone names a room's first
//! moderators; after that admin or a current moderator may change the record. Mode "open" lets anyone post,
//! "announce" only moderators. Muted agents may not post, banned ones may neither post nor join.
//! Enforcement is advisory, like every identity on the bus: senders refuse what the record forbids and every Bus drops
//! room messages it forbids, but anyone can claim to be a moderator, or overwrite the record with a raw client.
//! A room may also have a pinned message, retained at rooms/<room>/pin (from 0.3.1), set and cleared by the same
//! people and just as advisory.

use crate::broker;
use crate::json::{Map, float};
use crate::presence;
use crate::protocol::{PAYLOAD_MAX, parse_json};
use crate::validation::{
    ADMIN, Error, ID_MAX, Result, check_addrs, check_body_value, check_ident, check_mentions, check_rules_value,
    check_ts, fail, is_ident,
};
use bytes::Bytes;
use serde_json::{Value, json};
use std::collections::BTreeMap;

pub const MODES: [&str; 2] = ["open", "announce"];
pub const MODS_MAX: usize = 32;
pub const LIST_MAX: usize = 256;
pub const RULES_MAX: usize = 2048;
pub const TOPICS: &str = "rooms/+";

/// The retained topic of room's pinned message (one level below rooms/+, so no Bus caches it).
pub fn pin_topic(room: &str) -> String {
    format!("rooms/{room}/pin")
}

/// A retained pin, or None if it is malformed or cleared: {"text", "by", "ts"}, text under the message rules.
pub fn parse_pin(payload: &[u8]) -> Option<Map> {
    let d = parse_json(payload, PAYLOAD_MAX)?;
    let d = d.as_object()?;
    let text = check_body_value(d.get("text")?).ok()?;
    if text.trim().is_empty() {
        return None;
    }
    let mut pin = Map::new();
    pin.insert("text".into(), text.into());
    pin.insert("by".into(), check_ident(d.get("by")?.as_str()?, "address").ok()?.into());
    pin.insert("ts".into(), float(check_ts(d.get("ts")?).ok()?));
    Some(pin)
}

/// The settings of a room without a record.
pub fn open() -> Map {
    let Value::Object(m) = json!({"mode": "open", "moderators": [], "rules": "", "muted": [], "banned": []}) else {
        unreachable!()
    };
    m
}

pub fn check_mode(value: &Value) -> Result<String> {
    match value.as_str() {
        Some(m) if MODES.contains(&m) => Ok(m.to_owned()),
        _ => fail(format!("mode must be {}", MODES.join(" or "))),
    }
}

pub fn check_rules(value: &Value) -> Result<String> {
    check_rules_value(value, RULES_MAX)
}

fn addrs(d: &Map, key: &str, cap: usize) -> Result<Value> {
    let v = d.get(key).ok_or_else(|| Error(key.into()))?;
    Ok(check_addrs(v, 0, cap, &format!("{key} must be a list of at most {cap} addresses"))?.into())
}

/// A room record with every field checked.
pub fn check_record(d: &Map) -> Result<Map> {
    let field = |key: &str| d.get(key).ok_or_else(|| Error(key.into()));
    let mut rec = Map::new();
    rec.insert("mode".into(), check_mode(field("mode")?)?.into());
    rec.insert("moderators".into(), addrs(d, "moderators", MODS_MAX)?);
    rec.insert("rules".into(), check_rules(field("rules")?)?.into());
    rec.insert("muted".into(), addrs(d, "muted", LIST_MAX)?);
    rec.insert("banned".into(), addrs(d, "banned", LIST_MAX)?);
    let by = field("by")?.as_str().ok_or_else(|| Error("by".into()))?;
    rec.insert("by".into(), check_ident(by, "address")?.into());
    rec.insert("ts".into(), float(check_ts(field("ts")?)?));
    Ok(rec)
}

/// A retained room record, or None if it is malformed: the room then counts as open.
pub fn parse_room(payload: &[u8]) -> Option<Map> {
    check_record(parse_json(payload, PAYLOAD_MAX)?.as_object()?).ok()
}

pub fn listed(rec: &Map, field: &str, addr: &str) -> bool {
    rec.get(field).and_then(Value::as_array).is_some_and(|l| l.iter().any(|v| v == addr))
}

pub fn is_moderator(rec: Option<&Map>, addr: &str) -> bool {
    addr == ADMIN || rec.is_some_and(|r| listed(r, "moderators", addr))
}

/// Why addr may not post in a room with record rec, or None if it may.
pub fn refusal(rec: Option<&Map>, addr: &str) -> Option<&'static str> {
    let rec = rec.filter(|_| addr != ADMIN)?;
    if listed(rec, "banned", addr) {
        Some("banned")
    } else if listed(rec, "muted", addr) {
        Some("muted")
    } else if rec.get("mode").is_some_and(|m| m == "announce") && !listed(rec, "moderators", addr) {
        Some("only moderators post in an announce room")
    } else {
        None
    }
}

fn mention_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

/// Every @token in body, in order: 1-64 identifier characters after an @ not glued to a word before it, so mail
/// addresses (a@b.c) are not mentions.
pub fn mention_tokens(body: &str) -> Vec<String> {
    let chars: Vec<char> = body.chars().collect();
    let (mut out, mut i) = (Vec::new(), 0);
    while i < chars.len() {
        if chars[i] == '@' && (i == 0 || !(mention_char(chars[i - 1]) || chars[i - 1] == '@')) {
            let start = i + 1;
            let mut end = start;
            while end < chars.len() && end - start < ID_MAX && mention_char(chars[end]) {
                end += 1;
            }
            if end > start {
                out.push(chars[start..end].iter().collect());
                i = end;
                continue;
            }
        }
        i += 1;
    }
    out
}

/// What a room message's @words came to (see mentions).
#[derive(Debug, Default, PartialEq)]
pub struct Resolved {
    /// the addresses mentioned, None for no mention (or @all)
    pub mentions: Option<Vec<String>>,
    /// @words that mention nobody and stay plain text
    pub unresolved: Vec<String>,
    /// those of them that name a live agent who is not in the room: it left, or never joined
    pub absent: Vec<String>,
}

/// Every (address, record) among agents whose address or name is word, compared without case.
fn named<'a>(agents: &'a [(String, Map)], word: &str) -> impl Iterator<Item = &'a String> {
    agents
        .iter()
        .filter(move |(a, r)| {
            a.eq_ignore_ascii_case(word)
                || r.get("name").and_then(Value::as_str).is_some_and(|n| n.eq_ignore_ascii_case(word))
        })
        .map(|(a, _)| a)
}

/// What a room message mentions among members (address, presence record): @address, or @name for every member with
/// that name, both compared without case. @admin always mentions the admin console, which joins no room. @all
/// mentions nobody in particular, like no mention, and @me nobody at all (your own posts never reach you). Any other
/// @word matches nobody: it stays plain text and comes back in unresolved so the sender can be told, and in absent
/// too when it names a live agent (among live) that is not in the room.
pub fn mentions(body: &str, members: &[(String, Map)], live: &[(String, Map)]) -> Result<Resolved> {
    let mut tokens = mention_tokens(body);
    let mut seen = std::collections::HashSet::new();
    tokens.retain(|t| seen.insert(t.to_ascii_lowercase()));
    let all = tokens.iter().any(|t| t.eq_ignore_ascii_case("all"));
    let (mut found, mut out) = (std::collections::BTreeSet::new(), Resolved::default());
    for token in tokens.into_iter().filter(|t| !t.eq_ignore_ascii_case("all") && !t.eq_ignore_ascii_case("me")) {
        if token.eq_ignore_ascii_case(ADMIN) {
            found.insert(ADMIN.to_owned());
            continue;
        }
        let matched: Vec<&String> = named(members, &token).collect();
        if matched.is_empty() {
            if named(live, &token).next().is_some() {
                out.absent.push(token.clone());
            }
            out.unresolved.push(token);
        } else {
            found.extend(matched.into_iter().cloned());
        }
    }
    if !all && !found.is_empty() {
        out.mentions = Some(check_mentions(&found.into_iter().collect::<Vec<_>>().into())?);
    }
    Ok(out)
}

/// What info shows for a room: its settings, open defaults when it has no record.
pub fn view(room: &str, rec: Option<&Map>) -> Map {
    let mut out = Map::new();
    out.insert("room".into(), room.into());
    out.extend(open());
    if let Some(r) = rec {
        out.extend(r.clone());
    }
    out
}

/// Room -> record of every well-formed record among retained topic -> payload.
pub fn records(retained: &[(String, Bytes)]) -> Vec<(String, Map)> {
    retained
        .iter()
        .filter_map(|(t, payload)| {
            let room = t.strip_prefix("rooms/").filter(|r| is_ident(r, ID_MAX))?;
            Some((room.to_owned(), parse_room(payload)?))
        })
        .collect()
}

/// A room's settings (open defaults without a record) and, as "pin", its pinned message or null.
pub fn info(room: &str) -> Result<Map> {
    let room = check_ident(room, "room")?;
    let (settings, pin) = (format!("rooms/{room}"), pin_topic(room));
    let retained = broker::retained(&[&settings, &pin])?;
    let found = records(&retained);
    let mut out = view(room, found.iter().find(|(r, _)| r == room).map(|(_, rec)| rec));
    let pinned = retained.iter().find(|(t, _)| *t == pin).and_then(|(_, p)| parse_pin(p));
    out.insert("pin".into(), pinned.map_or(Value::Null, Value::Object));
    Ok(out)
}

/// Every room with members or settings: members, mode and moderators.
pub fn listing() -> Result<Vec<Map>> {
    let retained = broker::retained(&[presence::TOPICS, TOPICS])?;
    let mut rooms: BTreeMap<String, (Vec<String>, Option<Map>)> = BTreeMap::new();
    for (addr, rec) in presence::parse_all(&retained) {
        for room in presence::topics(&rec).filter_map(|t| t.strip_prefix("room/")).filter(|r| is_ident(r, ID_MAX)) {
            rooms.entry(room.to_owned()).or_default().0.push(addr.clone());
        }
    }
    for (room, rec) in records(&retained) {
        rooms.entry(room).or_default().1 = Some(rec);
    }
    Ok(rooms
        .into_iter()
        .map(|(name, (mut who, rec))| {
            who.sort();
            let settings = rec.unwrap_or_else(open);
            let mut row = Map::new();
            row.insert("room".into(), name.into());
            row.insert("members".into(), who.len().into());
            row.insert("who".into(), who.into());
            row.insert("mode".into(), settings["mode"].clone());
            row.insert("moderators".into(), settings["moderators"].clone());
            row
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(addr: &str, name: &str) -> (String, Map) {
        let Value::Object(m) = json!({"name": name}) else { unreachable!() };
        (addr.to_owned(), m)
    }

    #[test]
    fn tokens() {
        assert_eq!(mention_tokens("@a hi @b-1, mail a@b.c @@x x@y -@z (@w"), ["a", "b-1", "w"]);
        assert_eq!(mention_tokens(&format!("@{}", "x".repeat(70)))[0].len(), 64);
        assert!(mention_tokens("no mentions @ here").is_empty());
    }

    #[test]
    fn resolve() {
        let members = [member("alice-1", "alice"), member("alice-2", "alice"), member("bob-1", "bob")];
        let live = [member("carol-3", "carol"), member("dan-4", "dan")];
        let m = |body: &str| mentions(body, &members, &live).unwrap();
        let words = |w: &[&str]| w.iter().map(|w| (*w).to_owned()).collect::<Vec<_>>();
        assert_eq!(m("@alice @bob-1").mentions.unwrap(), ["alice-1", "alice-2", "bob-1"]);
        assert_eq!(m("@bob-1 and @ALL"), Resolved::default());
        assert_eq!(m("@ALICE and @Bob-1").mentions.unwrap(), ["alice-1", "alice-2", "bob-1"], "no case");
        let r = m("@carol, see the @mentions field, @bob");
        assert_eq!(r.mentions.unwrap(), ["bob-1"]);
        assert_eq!((r.unresolved, r.absent), (words(&["carol", "mentions"]), words(&["carol"])), "carol is not here");
        assert_eq!(m("@dan-4 @Carol").absent, words(&["dan-4", "Carol"]), "by address or name, any case");
        assert_eq!(m("only @nobody @Nobody").unresolved, words(&["nobody"]), "once each");
        let admin = m("@Admin please, and @admin again");
        assert_eq!((admin.mentions.unwrap(), admin.unresolved), (vec![ADMIN.to_owned()], vec![]), "not a member");
        assert_eq!(m("ping @me or @ME"), Resolved::default(), "never a mention, never reported");
        assert_eq!(m("@me @bob").mentions.unwrap(), ["bob-1"]);
        assert_eq!(
            m("@bob-1 then @bob").unresolved,
            Vec::<String>::new(),
            "a member mentioned twice is still a member"
        );
    }

    #[test]
    fn pins() {
        let pin = parse_pin(br#"{"text": "digest\n- a", "by": "mod-1", "ts": 5, "x": 1}"#).unwrap();
        assert_eq!(crate::json::dumps_map(&pin), r#"{"text": "digest\n- a", "by": "mod-1", "ts": 5.0}"#);
        assert_eq!(pin_topic("ops"), "rooms/ops/pin");
        for bad in [
            &b""[..],
            br#"{"text": "  ", "by": "m", "ts": 1}"#,
            br#"{"text": "x", "by": "a/b", "ts": 1}"#,
            br#"{"text": "\u001b[2J", "by": "m", "ts": 1}"#,
            br#"{"text": "x", "ts": 1}"#,
            b"[1]",
        ] {
            assert!(parse_pin(bad).is_none(), "{}", String::from_utf8_lossy(bad));
        }
    }

    #[test]
    fn records_and_refusal() {
        let ok = br#"{"mode": "announce", "moderators": ["m", "a"], "rules": "be kind", "muted": ["x"],
                      "banned": ["y"], "by": "admin", "ts": 1}"#;
        let rec = parse_room(ok).unwrap();
        assert_eq!(rec["moderators"], json!(["a", "m"]));
        assert_eq!(refusal(Some(&rec), "y"), Some("banned"));
        assert_eq!(refusal(Some(&rec), "x"), Some("muted"));
        assert_eq!(refusal(Some(&rec), "z"), Some("only moderators post in an announce room"));
        assert_eq!(refusal(Some(&rec), "m"), None);
        assert_eq!(refusal(Some(&rec), ADMIN), None);
        assert_eq!(refusal(None, "z"), None);
        assert!(is_moderator(None, ADMIN) && !is_moderator(None, "m") && is_moderator(Some(&rec), "m"));
        for bad in [
            &br#"{"mode": "closed"}"#[..],
            b"[]",
            b"",
            br#"{"mode": "open", "moderators": [], "rules": "",
                    "muted": [], "banned": [], "by": "a/b", "ts": 1}"#,
        ] {
            assert!(parse_room(bad).is_none());
        }
        let v = view("r", None);
        assert_eq!(
            crate::json::dumps_map(&v),
            r#"{"room": "r", "mode": "open", "moderators": [], "rules": "", "muted": [], "banned": []}"#
        );
    }
}
