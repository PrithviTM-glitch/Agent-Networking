//! Input rules shared by every boundary. Each check returns its value or an Error with a one-line message.
//!
//! Names, addresses, rooms and caps: 1-64 ASCII letters, digits, _ or - (names at most 57, leaving room for the
//! address suffix), at most 32 caps. Text: at most 64 KiB of UTF-8; tab, newline and CR are kept, any other control
//! character (C0, DEL, C1) is refused. Raw topics: at most 256 bytes of UTF-8, no control characters, never starting
//! with $ or the reserved levels agents, inbox, ledger, broadcast, rooms and history. + and # work only in subscription
//! filters, as whole levels, # only last, and not as the first level.

use serde_json::Value;
use std::fmt;
use std::io;
use std::path::Path;

pub const ID_MAX: usize = 64;
pub const NAME_MAX: usize = 57;
pub const CAPS_MAX: usize = 32;
pub const TOPICS_MAX: usize = 100;
pub const TOPIC_MAX: usize = 256;
pub const MENTIONS_MAX: usize = 32;
pub const BODY_MAX: usize = 64 * 1024;
pub const WAIT_MAX: f64 = 3600.0;
pub const LAST_MAX: i64 = 1000;
pub const RESERVED: [&str; 6] = ["agents", "inbox", "ledger", "broadcast", "rooms", "history"];
/// The services' request/response levels: RPC traffic, never shown as messages.
pub const SERVICES: [&str; 2] = ["ledger", "history"];
pub const ADMIN: &str = "admin";

/// Bad input, an unknown address or a broker problem; the message is one line.
#[derive(Debug, Clone, PartialEq)]
pub struct Error(pub String);

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

pub fn fail<T>(message: impl Into<String>) -> Result<T> {
    Err(Error(message.into()))
}

/// An OS error as Python reports it: `FileNotFoundError: [Errno 2] No such file or directory: 'path'`.
pub fn os_error(e: &io::Error, path: Option<&Path>) -> Error {
    let Some(code) = e.raw_os_error() else {
        return Error(format!("OSError: {e}"));
    };
    let name = match code {
        libc::ENOENT => "FileNotFoundError",
        libc::EACCES | libc::EPERM => "PermissionError",
        libc::EEXIST => "FileExistsError",
        libc::ENOTDIR => "NotADirectoryError",
        libc::EISDIR => "IsADirectoryError",
        libc::ESRCH => "ProcessLookupError",
        libc::ECONNREFUSED => "ConnectionRefusedError",
        _ => "OSError",
    };
    let at = path.map(|p| format!(": {}", py_repr(&p.to_string_lossy()))).unwrap_or_default();
    Error(format!("{name}: [Errno {code}] {}{at}", strerror(code)))
}

pub fn strerror(code: i32) -> &'static str {
    nix::errno::Errno::from_raw(code).desc()
}

/// Python's repr() of a string.
pub fn py_repr(s: &str) -> String {
    let quote = if s.contains('\'') && !s.contains('"') { '"' } else { '\'' };
    let mut out = String::with_capacity(s.len() + 2);
    out.push(quote);
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c if (c as u32) < 0x100 && (c.is_control() || c == '\u{a0}' || c == '\u{ad}') => {
                out.push_str(&format!("\\x{:02x}", c as u32))
            }
            c if c.is_control() || (c.is_whitespace() && c != ' ') => {
                if (c as u32) < 0x10000 {
                    out.push_str(&format!("\\u{:04x}", c as u32))
                } else {
                    out.push_str(&format!("\\U{:08x}", c as u32))
                }
            }
            c => out.push(c),
        }
    }
    out.push(quote);
    out
}

/// What error messages show of a bad value: its Python repr, cut to 60 characters.
pub fn show(value: &str) -> String {
    py_repr(value).chars().take(60).collect()
}

/// show() for a decoded JSON value, as Python would print it.
pub fn show_value(value: &Value) -> String {
    py_repr_value(value).chars().take(60).collect()
}

/// Python's repr() of what json.loads made of value.
fn py_repr_value(value: &Value) -> String {
    let join = |items: Vec<String>| items.join(", ");
    match value {
        Value::String(s) => py_repr(s),
        Value::Null => "None".into(),
        Value::Bool(b) => if *b { "True" } else { "False" }.into(),
        Value::Number(n) => match n.as_f64() {
            Some(f) if n.is_f64() => crate::json::float_repr(f),
            _ => n.to_string(),
        },
        Value::Array(a) => format!("[{}]", join(a.iter().map(py_repr_value).collect())),
        Value::Object(o) => {
            format!("{{{}}}", join(o.iter().map(|(k, v)| format!("{}: {}", py_repr(k), py_repr_value(v))).collect()))
        }
    }
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

pub fn is_ident(value: &str, cap: usize) -> bool {
    !value.is_empty() && value.len() <= cap && value.chars().all(is_ident_char)
}

pub fn is_ident_value(value: &Value) -> bool {
    value.as_str().is_some_and(|s| is_ident(s, ID_MAX))
}

pub fn check_ident<'a>(value: &'a str, what: &str) -> Result<&'a str> {
    check_ident_cap(value, what, ID_MAX)
}

pub fn check_ident_cap<'a>(value: &'a str, what: &str, cap: usize) -> Result<&'a str> {
    if is_ident(value, cap) {
        Ok(value)
    } else {
        fail(format!("bad {what} {}: use 1-{cap} ASCII letters, digits, _ or -", show(value)))
    }
}

pub fn check_ident_value(value: &Value, what: &str) -> Result<String> {
    match value.as_str() {
        Some(s) => check_ident(s, what).map(str::to_owned),
        None => fail(format!("bad {what} {}: use 1-{ID_MAX} ASCII letters, digits, _ or -", show_value(value))),
    }
}

pub fn not_admin(value: Option<&str>, what: &str) -> Result<()> {
    match value {
        Some(v) if v.to_lowercase() == ADMIN => {
            fail(format!("{what} {} is reserved for the admin console", py_repr(v)))
        }
        _ => Ok(()),
    }
}

pub fn is_uuid(value: &str) -> bool {
    let b = value.as_bytes();
    b.len() == 36
        && b.iter().enumerate().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => *c == b'-',
            _ => c.is_ascii_hexdigit(),
        })
}

pub fn check_session(value: Option<&str>) -> Result<&str> {
    match value {
        Some(v) if is_uuid(v) => Ok(v),
        Some(v) => fail(format!("bad session id {}: expected a UUID", show(v))),
        None => fail("bad session id None: expected a UUID"),
    }
}

fn is_body_ctrl(c: char) -> bool {
    matches!(c, '\0'..='\x08' | '\x0b' | '\x0c' | '\x0e'..='\x1f' | '\x7f'..='\u{9f}')
}

pub fn check_text<'a>(value: &'a str, what: &str, cap: usize) -> Result<&'a str> {
    if value.len() > cap {
        return fail(format!("{what} is {} bytes, the limit is {cap}", value.len()));
    }
    if value.chars().any(is_body_ctrl) {
        return fail(format!("{what} contains a control character other than tab, newline or CR"));
    }
    Ok(value)
}

pub fn check_body(value: &str) -> Result<&str> {
    check_text(value, "text", BODY_MAX)
}

fn check_text_value(value: &Value, what: &str, cap: usize) -> Result<String> {
    match value {
        Value::String(s) => check_text(s, what, cap).map(str::to_owned),
        other => fail(format!("{what} must be a string, got {}", py_type(other))),
    }
}

pub fn check_body_value(value: &Value) -> Result<String> {
    check_text_value(value, "text", BODY_MAX)
}

pub fn check_rules_value(value: &Value, cap: usize) -> Result<String> {
    check_text_value(value, "rules", cap)
}

pub fn py_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "NoneType",
        Value::Bool(_) => "bool",
        Value::Number(n) if n.is_f64() => "float",
        Value::Number(_) => "int",
        Value::String(_) => "str",
        Value::Array(_) => "list",
        Value::Object(_) => "dict",
    }
}

/// A raw topic (or with filter a subscription filter) outside the bridge's reserved namespaces.
pub fn check_topic(value: &str, filter: bool) -> Result<&str> {
    if value.is_empty() || value.len() > TOPIC_MAX {
        return fail(format!("topic must be 1-{TOPIC_MAX} bytes"));
    }
    if value.chars().any(|c| matches!(c, '\0'..='\x1f' | '\x7f'..='\u{9f}')) {
        return fail("topic contains a control character");
    }
    let levels: Vec<&str> = value.split('/').collect();
    if value.starts_with('$') || RESERVED.contains(&levels[0]) {
        return fail(format!("topic {} is reserved: $, {} belong to the bridge", show(value), RESERVED.join(", ")));
    }
    if !filter {
        if value.contains(['+', '#']) {
            return fail("+ and # are wildcards, they only work in sub");
        }
    } else if levels[0] == "+" || levels[0] == "#" {
        return fail("a filter's first level must be literal, a wildcard there would reach reserved topics");
    } else if levels.iter().enumerate().any(|(i, level)| {
        (level.contains('#') && (*level != "#" || i < levels.len() - 1)) || (level.contains('+') && *level != "+")
    }) {
        return fail("+ must be a whole level and # a whole last level");
    }
    Ok(value)
}

pub fn check_topic_value(value: &Value) -> Result<String> {
    match value.as_str() {
        Some(s) => check_topic(s, true).map(str::to_owned),
        None => fail(format!("topic must be a string, got {}", py_type(value))),
    }
}

pub fn check_caps(values: &[String]) -> Result<Vec<String>> {
    if values.len() > CAPS_MAX {
        return fail(format!("caps must be a list of at most {CAPS_MAX}"));
    }
    values.iter().map(|c| check_ident(c, "cap").map(str::to_owned)).collect()
}

pub fn check_caps_value(value: &Value) -> Result<Vec<String>> {
    match value.as_array() {
        Some(list) if list.len() <= CAPS_MAX => list.iter().map(|c| check_ident_value(c, "cap")).collect(),
        _ => fail(format!("caps must be a list of at most {CAPS_MAX}")),
    }
}

/// Sorted, unique addresses of 1 to max entries.
pub fn check_addrs(value: &Value, min: usize, max: usize, message: &str) -> Result<Vec<String>> {
    let Some(list) = value.as_array().filter(|l| (min..=max).contains(&l.len())) else {
        return fail(message);
    };
    let mut out = list.iter().map(|v| check_ident_value(v, "address")).collect::<Result<Vec<_>>>()?;
    out.sort();
    out.dedup();
    Ok(out)
}

pub fn check_mentions(value: &Value) -> Result<Vec<String>> {
    check_addrs(value, 1, MENTIONS_MAX, &format!("mentions must be a list of 1-{MENTIONS_MAX} addresses"))
}

pub fn check_wait(value: f64) -> Result<f64> {
    if (0.0..=WAIT_MAX).contains(&value) { Ok(value) } else { fail(format!("wait must be 0-{WAIT_MAX} seconds")) }
}

pub fn check_last(value: i64) -> Result<usize> {
    if (1..=LAST_MAX).contains(&value) { Ok(value as usize) } else { fail(format!("last must be 1-{LAST_MAX}")) }
}

pub fn check_ts(value: &Value) -> Result<f64> {
    match value.as_f64() {
        Some(v) if (0.0..1e10).contains(&v) => Ok(v),
        _ => fail("bad timestamp"),
    }
}

/// A JSON integer (not a float or bool) in 1..=cap.
pub fn is_count(value: Option<&Value>, cap: u64) -> bool {
    value.and_then(Value::as_u64).is_some_and(|v| v > 0 && v <= cap) && !value.is_some_and(|v| v.is_f64())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn idents() {
        assert!(is_ident("a-B_9", ID_MAX));
        assert!(!is_ident("", ID_MAX) && !is_ident("a/b", ID_MAX) && !is_ident("é", ID_MAX));
        assert!(is_ident(&"x".repeat(64), ID_MAX) && !is_ident(&"x".repeat(65), ID_MAX));
        assert_eq!(
            check_ident_cap("bad/name", "name", NAME_MAX).unwrap_err().0,
            "bad name 'bad/name': use 1-57 ASCII letters, digits, _ or -"
        );
        assert_eq!(
            not_admin(Some("Admin"), "address").unwrap_err().0,
            "address 'Admin' is reserved for the admin console"
        );
        assert!(not_admin(None, "address").is_ok());
    }

    #[test]
    fn values_show_as_python_reprs() {
        assert_eq!(
            show_value(&json!(["r", 1, null, true, 2.5, {"k": "it's"}])),
            r#"['r', 1, None, True, 2.5, {'k': "it's"}]"#
        );
        assert_eq!(show_value(&json!(1e16)), "1e+16");
        assert_eq!(show_value(&json!(["x".repeat(70)])).chars().count(), 60);
    }

    #[test]
    fn reprs() {
        assert_eq!(py_repr("it's"), "\"it's\"");
        assert_eq!(py_repr("a\nb\x1b"), "'a\\nb\\x1b'");
        assert_eq!(show(&"y".repeat(100)).chars().count(), 60);
    }

    #[test]
    fn texts() {
        assert!(check_body("tab\tnewline\ncr\r ✓").is_ok());
        for bad in ["a\0b", "\x1b[2J", "\u{85}", "\x7f"] {
            assert_eq!(
                check_body(bad).unwrap_err().0,
                "text contains a control character other than tab, newline or CR"
            );
        }
        assert_eq!(check_body(&"x".repeat(BODY_MAX + 1)).unwrap_err().0, "text is 65537 bytes, the limit is 65536");
        assert_eq!(check_body_value(&json!(7)).unwrap_err().0, "text must be a string, got int");
    }

    #[test]
    fn topics() {
        assert!(check_topic("ci/build", false).is_ok() && check_topic("ci/+/x/#", true).is_ok());
        let cases = [
            ("", false, "topic must be 1-256 bytes"),
            ("a\tb", false, "topic contains a control character"),
            (
                "inbox/x",
                false,
                "topic 'inbox/x' is reserved: $, agents, inbox, ledger, broadcast, rooms, history belong to the bridge",
            ),
            (
                "$SYS/x",
                true,
                "topic '$SYS/x' is reserved: $, agents, inbox, ledger, broadcast, rooms, history belong to the bridge",
            ),
            ("a/+", false, "+ and # are wildcards, they only work in sub"),
            ("#", true, "a filter's first level must be literal, a wildcard there would reach reserved topics"),
            ("a/#/b", true, "+ must be a whole level and # a whole last level"),
            ("a/b+", true, "+ must be a whole level and # a whole last level"),
        ];
        for (topic, filter, message) in cases {
            assert_eq!(check_topic(topic, filter).unwrap_err().0, message, "{topic}");
        }
        assert!(check_topic(&"t".repeat(257), false).is_err());
    }

    #[test]
    fn lists_and_numbers() {
        assert_eq!(check_mentions(&json!(["b", "a", "b"])).unwrap(), ["a", "b"]);
        assert!(check_mentions(&json!([])).is_err() && check_mentions(&json!(["a/b"])).is_err());
        assert!(check_mentions(&json!((0..33).map(|i| format!("a{i}")).collect::<Vec<_>>())).is_err());
        assert_eq!(check_caps(&vec!["c".to_string(); 33]).unwrap_err().0, "caps must be a list of at most 32");
        assert_eq!(check_wait(3601.0).unwrap_err().0, "wait must be 0-3600 seconds");
        assert!(check_wait(f64::NAN).is_err() && check_wait(0.0).is_ok());
        assert_eq!(check_last(0).unwrap_err().0, "last must be 1-1000");
        assert!(check_ts(&json!(true)).is_err() && check_ts(&json!("now")).is_err() && check_ts(&json!(1e10)).is_err());
        assert_eq!(check_ts(&json!(5)).unwrap(), 5.0);
        assert!(is_count(Some(&json!(3)), 4) && !is_count(Some(&json!(3.0)), 4) && !is_count(Some(&json!(0)), 4));
        assert!(!is_count(Some(&json!(true)), 4) && !is_count(Some(&json!(5)), 4) && !is_count(None, 4));
        assert!(is_uuid("0f8fad5b-d9cb-469f-a165-70867728950e") && !is_uuid("0f8fad5bd9cb469fa16570867728950e"));
    }
}
