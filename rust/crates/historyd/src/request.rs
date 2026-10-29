//! Request validation: the ops, their fields and every field check, with historyd's error texts.

use agent_bridge::json::{Map, float};
use agent_bridge::validation::{LAST_MAX, check_ident_value, check_topic, check_ts, py_type};
use serde_json::Value;

/// A request historyd refuses; the message goes back to the caller.
pub struct Bad(pub String);

pub const OPS: [&str; 7] = ["room", "dms", "topic", "broadcasts", "recent", "drop", "ping"];
/// Ops that read or change one agent's data take its address only from the topic history/req/<as>, never from the
/// body, so an ACL on that topic is all the auth phase needs.
const NEEDS_AS: [&str; 3] = ["dms", "recent", "drop"];
/// the console's backlog; the reply cap still bounds what goes back
pub const RECENT_MAX: i64 = 5000;

#[derive(Clone, Copy)]
enum Check {
    /// an identifier, shown in errors as what
    Ident(&'static str),
    Last,
    Count(i64),
    Ts,
    Topic,
}

#[derive(Clone, Copy)]
enum Fallback {
    Required,
    Null,
    Int(i64),
    Float(f64),
}

type Field = (&'static str, Check, Fallback);

fn fields(op: &str) -> &'static [Field] {
    use Check as C;
    use Fallback as D;
    match op {
        "room" => &[("room", C::Ident("room"), D::Required), ("last", C::Last, D::Int(20)), ("before", C::Ts, D::Null)],
        "dms" => &[("peer", C::Ident("address"), D::Null), ("last", C::Last, D::Int(20))],
        "topic" => &[("topic", C::Topic, D::Required), ("last", C::Last, D::Int(20))],
        "broadcasts" => &[("since", C::Ts, D::Float(0.0)), ("last", C::Last, D::Int(20))],
        "recent" => &[("last", C::Count(RECENT_MAX), D::Int(100))],
        "drop" => &[("addr", C::Ident("address"), D::Null)],
        _ => &[],
    }
}

/// A checked request: the op, the acting address (None when the op needs none) and every field, defaults filled in.
pub struct Request {
    pub op: &'static str,
    pub me: Option<String>,
    pub args: Map,
}

fn int(v: &Value) -> Option<i64> {
    v.as_i64().filter(|_| !v.is_f64())
}

fn check(key: &str, check: Check, v: &Value) -> Result<Value, Bad> {
    let field = |e: agent_bridge::Error| Bad(format!("{key}: {}", e.0));
    Ok(match check {
        Check::Ident(what) => check_ident_value(v, what).map_err(field)?.into(),
        Check::Last => match int(v).filter(|n| (1..=LAST_MAX).contains(n)) {
            Some(n) => n.into(),
            None => return Err(Bad(format!("{key}: last must be 1-{LAST_MAX}"))),
        },
        Check::Count(cap) => match int(v).filter(|n| (1..=cap).contains(n)) {
            Some(n) => n.into(),
            None => return Err(Bad(format!("{key}: integer from 1 to {cap}"))),
        },
        Check::Ts => float(check_ts(v).map_err(field)?),
        Check::Topic => match v.as_str() {
            Some(s) => check_topic(s, false).map_err(field)?.into(),
            None => return Err(Bad(format!("{key}: topic must be a string, got {}", py_type(v)))),
        },
    })
}

/// op, acting address and checked fields; via, from history/req/<addr>, fixes the address.
pub fn parse(req: &Value, via: Option<&str>) -> Result<Request, Bad> {
    let Some(req) = req.as_object() else { return Err(Bad("request must be a JSON object".into())) };
    let Some(op) = req.get("op").and_then(Value::as_str).and_then(|o| OPS.iter().find(|x| **x == o)) else {
        return Err(Bad(format!("op: one of {}", OPS.join(", "))));
    };
    let known = fields(op);
    let extra: Vec<String> = req
        .keys()
        .filter(|k| !known.iter().any(|(f, ..)| f == k) && *k != "op" && *k != "as")
        .map(|k| k.chars().take(32).collect())
        .collect();
    if !extra.is_empty() {
        return Err(Bad(format!("unknown fields for {op}: {}", extra[..extra.len().min(5)].join(", "))));
    }
    if let Some(via) = via {
        check("request topic address", Check::Ident("address"), &Value::String(via.into()))?;
        if req.get("as").is_some_and(|a| a.as_str() != Some(via)) {
            return Err(Bad(format!("as: must match the request topic history/req/{via}")));
        }
    }
    if NEEDS_AS.contains(op) && via.is_none() {
        return Err(Bad(format!("{op}: send it on history/req/<your address>")));
    }
    let me = match via {
        Some(v) => Some(v.to_owned()),
        None => match req.get("as").filter(|a| !a.is_null()) {
            Some(a) => Some(check("as", Check::Ident("address"), a)?.as_str().unwrap_or_default().to_owned()),
            None => None,
        },
    };
    let mut args = Map::new();
    for &(key, c, fallback) in known {
        let value = match (req.get(key).filter(|v| !v.is_null()), fallback) {
            (Some(v), _) => check(key, c, v)?,
            (None, Fallback::Required) => return Err(Bad(format!("{key}: required"))),
            (None, Fallback::Null) => Value::Null,
            (None, Fallback::Int(n)) => n.into(),
            (None, Fallback::Float(x)) => float(x),
        };
        args.insert(key.into(), value);
    }
    Ok(Request { op, me, args })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn error(req: Value, via: Option<&str>) -> String {
        match parse(&req, via) {
            Ok(_) => "accepted".into(),
            Err(Bad(e)) => e,
        }
    }

    #[test]
    fn messages_match_python() {
        assert_eq!(error(json!([1]), None), "request must be a JSON object");
        assert_eq!(error(json!({"op": 5}), None), "op: one of room, dms, topic, broadcasts, recent, drop, ping");
        assert_eq!(error(json!({"op": "ping", "x": 1, "y": 2}), None), "unknown fields for ping: x, y");
        assert_eq!(error(json!({"op": "dms"}), None), "dms: send it on history/req/<your address>");
        assert_eq!(
            error(json!({"op": "ping", "as": "b"}), Some("a")),
            "as: must match the request topic history/req/a"
        );
        assert_eq!(
            error(json!({"op": "ping", "as": null}), Some("a")),
            "as: must match the request topic history/req/a"
        );
        assert_eq!(
            error(json!({"op": "ping"}), Some("a/b")),
            "request topic address: bad address 'a/b': use 1-64 ASCII letters, digits, _ or -"
        );
        assert_eq!(error(json!({"op": "room"}), None), "room: required");
        assert_eq!(
            error(json!({"op": "room", "room": ["r"]}), None),
            "room: bad room ['r']: use 1-64 ASCII letters, digits, _ or -"
        );
        assert_eq!(error(json!({"op": "room", "room": "r", "last": 5.0}), None), "last: last must be 1-1000");
        assert_eq!(error(json!({"op": "room", "room": "r", "last": true}), None), "last: last must be 1-1000");
        assert_eq!(error(json!({"op": "room", "room": "r", "before": true}), None), "before: bad timestamp");
        assert_eq!(error(json!({"op": "recent", "last": 5001}), Some("admin")), "last: integer from 1 to 5000");
        assert_eq!(error(json!({"op": "topic", "topic": 5}), None), "topic: topic must be a string, got int");
        assert_eq!(
            error(json!({"op": "topic", "topic": "a/+"}), None),
            "topic: + and # are wildcards, they only work in sub"
        );
        assert_eq!(
            error(json!({"op": "ping", "as": 5}), None),
            "as: bad address 5: use 1-64 ASCII letters, digits, _ or -"
        );
        let r = parse(&json!({"op": "room", "room": "r", "before": null}), None).ok().unwrap();
        assert_eq!(agent_bridge::json::dumps_map(&r.args), r#"{"room": "r", "last": 20, "before": null}"#);
        let b = parse(&json!({"op": "broadcasts"}), None).ok().unwrap();
        assert_eq!(agent_bridge::json::dumps_map(&b.args), r#"{"since": 0.0, "last": 20}"#);
    }
}
