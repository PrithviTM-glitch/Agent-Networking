//! Request validation: the ops, their fields and every field check, with ledgerd's error texts.

use agent_bridge::json::{Map, float};
use agent_bridge::validation::{ID_MAX, is_ident};
use serde_json::Value;

/// A request ledgerd refuses; the message goes back to the caller.
pub struct Bad(pub String);

pub const ADDR: &str = "[A-Za-z0-9_-]{1,64}";
const NAME: &str = "[A-Za-z0-9_][A-Za-z0-9_.-]{0,63}";
const ROWID: &str = "[0-9a-f]{16}";
pub const MAX_TTL: f64 = 3600.0;
pub const MAX_WAIT: f64 = 300.0;

#[derive(Clone, Copy)]
enum Check {
    Addr,
    Name,
    Resource,
    RowId,
    Description,
    Visibility,
    Level,
    Number(f64, f64),
}

#[derive(Clone, Copy)]
enum Fallback {
    Required,
    Null,
    Text(&'static str),
    Number(f64),
}

type Field = (&'static str, Check, Fallback);

pub const OPS: [&str; 15] = [
    "init", "add", "update", "remove", "ledger", "public", "request", "grant", "deny", "grants", "acquire", "renew",
    "release", "drop", "dump",
];

fn fields(op: &str) -> &'static [Field] {
    use Check as C;
    use Fallback as D;
    match op {
        "add" => &[
            ("name", C::Name, D::Required),
            ("description", C::Description, D::Text("")),
            ("visibility", C::Visibility, D::Text("private")),
            ("max_level", C::Level, D::Text("read")),
        ],
        "update" => &[
            ("name", C::Name, D::Required),
            ("description", C::Description, D::Null),
            ("visibility", C::Visibility, D::Null),
            ("max_level", C::Level, D::Null),
        ],
        "remove" => &[("name", C::Name, D::Required)],
        "ledger" => &[("owner", C::Addr, D::Null)],
        "request" => &[("resource", C::Resource, D::Required), ("level", C::Level, D::Required)],
        "grant" | "deny" => &[("request_id", C::RowId, D::Required)],
        "acquire" => &[
            ("resource", C::Resource, D::Required),
            ("level", C::Level, D::Required),
            ("ttl", C::Number(1.0, MAX_TTL), D::Number(60.0)),
            ("wait", C::Number(0.0, MAX_WAIT), D::Number(0.0)),
        ],
        "renew" => &[("lease_id", C::RowId, D::Required), ("ttl", C::Number(1.0, MAX_TTL), D::Number(60.0))],
        "release" => &[("lease_id", C::RowId, D::Required)],
        _ => &[],
    }
}

pub fn is_addr(v: &str) -> bool {
    is_ident(v, ID_MAX)
}

fn is_name(v: &str) -> bool {
    let mut chars = v.chars();
    let first_ok = chars.next().is_some_and(|c| c.is_ascii_alphanumeric() || c == '_');
    first_ok && v.len() <= 64 && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
}

fn pattern(key: &str, v: &Value, pattern: &str, ok: fn(&str) -> bool) -> Result<Value, Bad> {
    match v.as_str() {
        Some(s) if ok(s) => Ok(v.clone()),
        _ => Err(Bad(format!("{key}: must match {pattern}"))),
    }
}

fn one_of(key: &str, v: &Value, values: &[&str]) -> Result<Value, Bad> {
    match v.as_str() {
        Some(s) if values.contains(&s) => Ok(v.clone()),
        _ => Err(Bad(format!("{key}: one of {}", values.join(", ")))),
    }
}

fn check(key: &str, v: &Value, c: Check) -> Result<Value, Bad> {
    match c {
        Check::Addr => pattern(key, v, ADDR, is_addr),
        Check::Name => pattern(key, v, NAME, is_name),
        Check::Resource => pattern(key, v, &format!("{ADDR}/{NAME}"), |s| {
            s.split_once('/').is_some_and(|(a, n)| is_addr(a) && is_name(n))
        }),
        Check::RowId => {
            pattern(key, v, ROWID, |s| s.len() == 16 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')))
        }
        Check::Description => match v.as_str() {
            Some(s)
                if s.chars().count() <= 1024
                    && !s.chars().any(|c| matches!(c, '\0'..='\x08' | '\x0b'..='\x1f' | '\x7f')) =>
            {
                Ok(v.clone())
            }
            _ => Err(Bad(format!("{key}: string up to 1024 chars, no control characters"))),
        },
        Check::Visibility => one_of(key, v, &["public", "private"]),
        Check::Level => one_of(key, v, &["read", "write"]),
        Check::Number(lo, hi) => match v.as_f64() {
            Some(n) if (lo..=hi).contains(&n) => Ok(float(n)),
            _ => Err(Bad(format!("{key}: number between {lo} and {hi}"))),
        },
    }
}

/// op, acting address and checked fields; via, from a ledger/req/<addr> topic, fixes the address.
pub fn parse(req: &Value, via: Option<&str>) -> Result<(&'static str, String, Map), Bad> {
    let Some(req) = req.as_object() else {
        return Err(Bad("request must be a JSON object".into()));
    };
    let Some(op) = req.get("op").and_then(Value::as_str).and_then(|op| OPS.iter().find(|o| **o == op)) else {
        return Err(Bad(format!("op: one of {}", OPS.join(", "))));
    };
    let fields = fields(op);
    let extra: Vec<String> = req
        .keys()
        .filter(|k| !fields.iter().any(|f| f.0 == *k) && *k != "op" && *k != "as")
        .map(|k| k.chars().take(32).collect())
        .collect();
    if !extra.is_empty() {
        return Err(Bad(format!("unknown fields for {op}: {}", extra[..extra.len().min(5)].join(", "))));
    }
    match via {
        Some(via) => {
            if !is_addr(via) {
                return Err(Bad(format!("request topic address: must match {ADDR}")));
            }
            if req.get("as").is_some_and(|a| a != via) {
                return Err(Bad(format!("as: must match the request topic ledger/req/{via}")));
            }
        }
        None if !req.contains_key("as") => return Err(Bad("as: required".into())),
        None => {}
    }
    let mut args = Map::new();
    for (key, c, default) in fields {
        let value = match (req.get(*key).filter(|v| !v.is_null()), default) {
            (Some(v), _) => check(key, v, *c)?,
            (None, Fallback::Required) => return Err(Bad(format!("{key}: required"))),
            (None, Fallback::Null) => Value::Null,
            (None, Fallback::Text(t)) => (*t).into(),
            (None, Fallback::Number(n)) => float(*n),
        };
        args.insert((*key).into(), value);
    }
    let me = match via {
        Some(via) => via.to_owned(),
        None => check("as", &req["as"], Check::Addr)?.as_str().unwrap_or_default().to_owned(),
    };
    Ok((op, me, args))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn err(req: Value, via: Option<&str>) -> String {
        parse(&req, via).err().map(|b| b.0).unwrap_or_default()
    }

    #[test]
    fn errors_like_python() {
        assert_eq!(err(json!([1]), None), "request must be a JSON object");
        assert!(err(json!({"op": "x", "as": "a"}), None).starts_with("op: one of init, add, update,"));
        assert_eq!(err(json!({"op": "init", "as": "a", "zz": 1, "yy": 2}), None), "unknown fields for init: zz, yy");
        assert_eq!(err(json!({"op": "init"}), None), "as: required");
        assert_eq!(err(json!({"op": "init", "as": "a/b"}), None), "as: must match [A-Za-z0-9_-]{1,64}");
        assert_eq!(err(json!({"op": "init", "as": "b"}), Some("a")), "as: must match the request topic ledger/req/a");
        assert_eq!(err(json!({"op": "add", "as": "a"}), None), "name: required");
        assert_eq!(
            err(json!({"op": "add", "as": "a", "name": ".x"}), None),
            "name: must match [A-Za-z0-9_][A-Za-z0-9_.-]{0,63}"
        );
        assert_eq!(
            err(json!({"op": "add", "as": "a", "name": "x", "description": "a\rb"}), None),
            "description: string up to 1024 chars, no control characters"
        );
        assert_eq!(
            err(json!({"op": "add", "as": "a", "name": "x", "visibility": 1}), None),
            "visibility: one of public, private"
        );
        assert_eq!(
            err(json!({"op": "acquire", "as": "a", "resource": "a/x", "level": "read", "ttl": true}), None),
            "ttl: number between 1 and 3600"
        );
        assert_eq!(
            err(json!({"op": "grant", "as": "a", "request_id": "ABCDEF0123456789"}), None),
            "request_id: must match [0-9a-f]{16}"
        );
        let (op, me, args) =
            parse(&json!({"op": "acquire", "resource": "a/x", "level": "write", "wait": null}), Some("b"))
                .ok()
                .unwrap();
        assert_eq!((op, me.as_str()), ("acquire", "b"));
        assert_eq!(
            agent_bridge::json::dumps_map(&args),
            r#"{"resource": "a/x", "level": "write", "ttl": 60.0, "wait": 0.0}"#
        );
    }
}
