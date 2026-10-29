//! Arguments as Python passes them, checked the way the Python validators check them: a wrong type is a one-line
//! BridgeError with the same message, never a bare TypeError. The core then applies every other rule.

use crate::errors::bridge;
use agent_bridge::validation::{CAPS_MAX, Error, ID_MAX, WAIT_MAX};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyFloat, PyInt, PyList, PyString, PyTuple};

fn fail<T>(message: String) -> PyResult<T> {
    Err(bridge(Error(message)))
}

/// A str as Rust text; None for any other type. A str holding lone surrogates is "<what> is not valid UTF-8".
fn as_text(value: &Bound<'_, PyAny>, what: &str) -> PyResult<Option<String>> {
    match value.cast::<PyString>() {
        Ok(s) => match s.to_str() {
            Ok(t) => Ok(Some(t.to_owned())),
            Err(_) => fail(format!("{what} is not valid UTF-8")),
        },
        Err(_) => Ok(None),
    }
}

fn type_name(value: &Bound<'_, PyAny>) -> PyResult<String> {
    Ok(value.get_type().name()?.to_string())
}

/// Text checked as check_text does: anything but a str is "<what> must be a string, got <type>".
pub fn text(value: &Bound<'_, PyAny>, what: &str) -> PyResult<String> {
    match as_text(value, what)? {
        Some(t) => Ok(t),
        None => fail(format!("{what} must be a string, got {}", type_name(value)?)),
    }
}

/// An identifier as check_ident sees it: anything but a str is "bad <what> <repr>: use 1-64 ...".
pub fn ident(value: &Bound<'_, PyAny>, what: &str) -> PyResult<String> {
    ident_cap(value, what, ID_MAX)
}

/// A str that is valid UTF-8, else None: an identifier check shows anything else by its repr, lone surrogates included.
fn utf8(value: &Bound<'_, PyAny>) -> Option<String> {
    value.cast::<PyString>().ok().and_then(|s| s.to_str().ok().map(str::to_owned))
}

/// ident with check_ident's cap (57 for a register name without an address).
pub fn ident_cap(value: &Bound<'_, PyAny>, what: &str, cap: usize) -> PyResult<String> {
    match utf8(value) {
        Some(t) => Ok(t),
        None => {
            let shown: String = value.repr()?.to_str()?.chars().take(60).collect();
            fail(format!("bad {what} {shown}: use 1-{cap} ASCII letters, digits, _ or -"))
        }
    }
}

/// ident, with None (absent or Python None) left as None.
pub fn opt_ident(value: Option<&Bound<'_, PyAny>>, what: &str) -> PyResult<Option<String>> {
    value.filter(|v| !v.is_none()).map(|v| ident(v, what)).transpose()
}

/// A list or tuple of caps, as check_caps takes them.
pub fn caps(value: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if !(value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>()) || value.len()? > CAPS_MAX {
        return fail(format!("caps must be a list of at most {CAPS_MAX}"));
    }
    value.try_iter()?.map(|c| ident(&c?, "cap")).collect()
}

/// A number that is not a bool, else message.
fn number(value: &Bound<'_, PyAny>, message: &str) -> PyResult<f64> {
    let numeric =
        !value.is_instance_of::<PyBool>() && (value.is_instance_of::<PyInt>() || value.is_instance_of::<PyFloat>());
    match numeric.then(|| value.extract::<f64>().ok()).flatten() {
        Some(n) => Ok(n),
        None => fail(message.to_owned()),
    }
}

/// recv's wait, as check_wait takes it; the core checks the range with the same message.
pub fn wait(value: &Bound<'_, PyAny>) -> PyResult<f64> {
    number(value, &format!("wait must be 0-{WAIT_MAX} seconds"))
}

/// A count such as last, as check_last takes it: an int, not a bool.
pub fn last(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    let message = format!("last must be 1-{}", agent_bridge::validation::LAST_MAX);
    if value.is_instance_of::<PyBool>() || !value.is_instance_of::<PyInt>() {
        return fail(message);
    }
    // An int too big for i64 is out of range anyway.
    Ok(value.extract::<i64>().unwrap_or(i64::MAX))
}

/// A session id as check_session takes it.
pub fn session(value: &Bound<'_, PyAny>) -> PyResult<String> {
    match utf8(value) {
        Some(t) => Ok(t),
        None => {
            let shown: String = value.repr()?.to_str()?.chars().take(60).collect();
            fail(format!("bad session id {shown}: expected a UUID"))
        }
    }
}
