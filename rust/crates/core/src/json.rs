//! JSON text exactly as Python's json.dumps() writes it: ", " and ": " separators, every non-ASCII character
//! escaped, floats in repr() form. Outputs, the wire and the history log stay byte-identical to the Python side.

use serde_json::ser::Formatter;
use serde_json::{Map as JsonMap, Value};
use std::io::{self, Write};

pub type Map = JsonMap<String, Value>;

struct Python;

impl Formatter for Python {
    fn begin_array_value<W: ?Sized + Write>(&mut self, w: &mut W, first: bool) -> io::Result<()> {
        if first { Ok(()) } else { w.write_all(b", ") }
    }

    fn begin_object_key<W: ?Sized + Write>(&mut self, w: &mut W, first: bool) -> io::Result<()> {
        if first { Ok(()) } else { w.write_all(b", ") }
    }

    fn begin_object_value<W: ?Sized + Write>(&mut self, w: &mut W) -> io::Result<()> {
        w.write_all(b": ")
    }

    fn write_string_fragment<W: ?Sized + Write>(&mut self, w: &mut W, fragment: &str) -> io::Result<()> {
        for c in fragment.chars() {
            if c.is_ascii() && c != '\x7f' {
                w.write_all(&[c as u8])?;
            } else {
                for unit in c.encode_utf16(&mut [0; 2]) {
                    write!(w, "\\u{unit:04x}")?;
                }
            }
        }
        Ok(())
    }

    fn write_f64<W: ?Sized + Write>(&mut self, w: &mut W, value: f64) -> io::Result<()> {
        w.write_all(float_repr(value).as_bytes())
    }
}

/// Python's repr() of a finite float: the shortest round-trip digits, exponent form below 1e-4 and from 1e16.
pub fn float_repr(value: f64) -> String {
    let sci = format!("{:e}", value.abs());
    let (mantissa, exp) = sci.split_once('e').unwrap_or((&sci, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let sign = if value.is_sign_negative() { "-" } else { "" };
    if (-4..16).contains(&exp) {
        let n = digits.len() as i32;
        let body = if exp < 0 {
            format!("0.{}{digits}", "0".repeat((-exp - 1) as usize))
        } else if exp >= n - 1 {
            format!("{digits}{}.0", "0".repeat((exp - n + 1) as usize))
        } else {
            format!("{}.{}", &digits[..=exp as usize], &digits[exp as usize + 1..])
        };
        return format!("{sign}{body}");
    }
    let (first, rest) = digits.split_at(1);
    let rest = if rest.is_empty() { String::new() } else { format!(".{rest}") };
    format!("{sign}{first}{rest}e{}{:02}", if exp < 0 { '-' } else { '+' }, exp.abs())
}

pub fn dumps(value: &Value) -> String {
    let mut out = Vec::with_capacity(128);
    let mut ser = serde_json::Serializer::with_formatter(&mut out, Python);
    serde::Serialize::serialize(value, &mut ser).expect("serializing a Value to memory cannot fail");
    String::from_utf8(out).expect("the formatter writes ASCII only")
}

pub fn dumps_map(map: &Map) -> String {
    dumps(&Value::Object(map.clone()))
}

/// JSON text as Python's json.loads reads it. Python also takes NaN, Infinity, -Infinity, numbers too big for a
/// float and lone surrogate escapes, which serde_json refuses; those are rewritten so that every check refuses the
/// result just as Python's checks refuse the original: non-finite numbers become {} (neither number, text, list nor
/// bool) and lone surrogates \u0000 (a control character, refused wherever text is checked). Nesting deeper than
/// serde_json's 128 levels stays refused, where Python goes on for a few thousand.
pub fn loads(text: &[u8]) -> Option<Value> {
    serde_json::from_slice(text).ok().or_else(|| serde_json::from_slice(&pythonic(text)?).ok())
}

fn hex4(text: &[u8], at: usize) -> Option<u16> {
    u16::from_str_radix(std::str::from_utf8(text.get(at..at + 4)?).ok()?, 16).ok()
}

fn pythonic(text: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(text.len());
    let mut i = 0;
    while i < text.len() {
        let rest = &text[i..];
        if rest[0] == b'"' {
            out.push(b'"');
            i += 1;
            loop {
                match *text.get(i)? {
                    b'"' => break,
                    b'\\' if text.get(i + 1) == Some(&b'u') => {
                        let unit = hex4(text, i + 2)?;
                        let low = (text.get(i + 6..i + 8) == Some(b"\\u")).then(|| hex4(text, i + 8)).flatten();
                        if (0xD800..0xDC00).contains(&unit) && low.is_some_and(|l| (0xDC00..0xE000).contains(&l)) {
                            out.extend_from_slice(&text[i..i + 12]);
                            i += 12;
                        } else if (0xD800..0xE000).contains(&unit) {
                            out.extend_from_slice(b"\\u0000");
                            i += 6;
                        } else {
                            out.extend_from_slice(&text[i..i + 6]);
                            i += 6;
                        }
                    }
                    b'\\' => {
                        out.extend_from_slice(text.get(i..i + 2)?);
                        i += 2;
                    }
                    c => {
                        out.push(c);
                        i += 1;
                    }
                }
            }
            out.push(b'"');
            i += 1;
        } else if let Some(word) = [&b"NaN"[..], b"Infinity", b"-Infinity"].into_iter().find(|w| rest.starts_with(w)) {
            out.extend_from_slice(b"{}");
            i += word.len();
        } else if rest[0] == b'-' || rest[0].is_ascii_digit() {
            let n = rest.iter().take_while(|c| matches!(c, b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9')).count();
            let number = std::str::from_utf8(&rest[..n]).ok()?.parse::<f64>().ok();
            out.extend_from_slice(if number.is_some_and(f64::is_infinite) { b"{}" } else { &rest[..n] });
            i += n;
        } else {
            out.push(rest[0]);
            i += 1;
        }
    }
    Some(out)
}

/// A float as a JSON number; non-finite values, which JSON cannot hold, become null.
pub fn float(value: f64) -> Value {
    serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number)
}

pub fn now() -> f64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0.0, |d| d.as_secs_f64())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn like_python() {
        let v = json!({"topic": "room/x", "from": "a-1", "body": "é ✓ 😀 \"q\" \n\t\u{1f}\u{7f}", "ts": 1793140000.25,
                       "n": [1, 2.0, true, null], "e": {}});
        assert_eq!(
            dumps(&v),
            r#"{"topic": "room/x", "from": "a-1", "body": "\u00e9 \u2713 \ud83d\ude00 \"q\" \n\t\u001f\u007f", "ts": 1793140000.25, "n": [1, 2.0, true, null], "e": {}}"#
        );
    }

    #[test]
    fn loads_like_python() {
        let v = loads(
            br#"{"a": NaN, "b": -Infinity, "c": [1e999, 2.5], "d": "\ud800x\udc00", "e": "\ud83d\ude00", "f": "N"}"#,
        );
        assert_eq!(
            dumps(&v.unwrap()),
            r#"{"a": {}, "b": {}, "c": [{}, 2.5], "d": "\u0000x\u0000", "e": "\ud83d\ude00", "f": "N"}"#
        );
        assert!(loads(b"[1, 2").is_none() && loads(b"nan").is_none() && loads(&[b'['; 200]).is_none());
    }

    #[test]
    fn floats_round_trip() {
        let mut x = 1793145568.0f64;
        for _ in 0..20000 {
            x += 0.0000123457;
            assert_eq!(loads(float_repr(x).as_bytes()).and_then(|v| v.as_f64()), Some(x));
        }
    }

    #[test]
    fn floats() {
        for (v, s) in [
            (0.0, "0.0"),
            (60.0, "60.0"),
            (0.1, "0.1"),
            (1e-4, "0.0001"),
            (1e-5, "1e-05"),
            (1.5e-7, "1.5e-07"),
            (1e15, "1000000000000000.0"),
            (1e16, "1e+16"),
            (1.25e17, "1.25e+17"),
            (-2.5, "-2.5"),
            (1793140000.123456, "1793140000.123456"),
            (123.456, "123.456"),
        ] {
            assert_eq!(float_repr(v), s);
        }
    }
}
