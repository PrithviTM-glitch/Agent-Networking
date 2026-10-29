//! JSON values to Python objects and back: dicts keep their key order, so Python sees what the wire carries.

use agent_bridge::json::Map;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};
use serde_json::Value;

pub fn to_py<'py>(py: Python<'py>, v: &Value) -> PyResult<Bound<'py, PyAny>> {
    match v {
        Value::Null => Ok(py.None().into_bound(py)),
        Value::Bool(b) => b.into_bound_py_any(py),
        Value::Number(n) => match (n.as_i64(), n.as_u64()) {
            (Some(i), _) => i.into_bound_py_any(py),
            (_, Some(u)) => u.into_bound_py_any(py),
            _ => n.as_f64().unwrap_or(f64::NAN).into_bound_py_any(py),
        },
        Value::String(s) => s.into_bound_py_any(py),
        Value::Array(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(to_py(py, item)?)?;
            }
            Ok(list.into_any())
        }
        Value::Object(m) => Ok(dict(py, m)?.into_any()),
    }
}

pub fn dict<'py>(py: Python<'py>, m: &Map) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    for (k, v) in m {
        d.set_item(k, to_py(py, v)?)?;
    }
    Ok(d)
}

pub fn dicts<'py>(py: Python<'py>, items: &[Map]) -> PyResult<Bound<'py, PyList>> {
    let list = PyList::empty(py);
    for m in items {
        list.append(dict(py, m)?)?;
    }
    Ok(list)
}

/// A Python value as JSON. A float JSON cannot hold (NaN, infinities) becomes {}, which every number check refuses,
/// as it refuses the NaN Python's json.dumps would have sent.
/// A str as the services see it after json.dumps: a lone surrogate (from a "\ud800" escape) becomes \u0000, as
/// json::loads reads it, and a surrogate pair its character.
fn text_of(s: &Bound<'_, PyString>) -> PyResult<String> {
    if let Ok(t) = s.to_str() {
        return Ok(t.to_owned());
    }
    let encoded = s.call_method1("encode", ("utf-16-le", "surrogatepass"))?;
    let units = encoded.cast::<PyBytes>()?.as_bytes().as_chunks::<2>().0.iter().map(|c| u16::from_le_bytes(*c));
    Ok(char::decode_utf16(units).map(|c| c.unwrap_or('\0')).collect())
}

pub fn from_py(o: &Bound<'_, PyAny>) -> PyResult<Value> {
    if o.is_none() {
        Ok(Value::Null)
    } else if o.is_instance_of::<PyBool>() {
        Ok(Value::Bool(o.extract()?))
    } else if o.is_instance_of::<PyInt>() {
        Ok(match o.extract::<i64>() {
            Ok(i) => i.into(),
            Err(_) => agent_bridge::json::float(o.extract::<f64>().unwrap_or(f64::INFINITY)),
        })
    } else if o.is_instance_of::<PyFloat>() {
        let f: f64 = o.extract()?;
        Ok(if f.is_finite() { agent_bridge::json::float(f) } else { Value::Object(Map::new()) })
    } else if let Ok(s) = o.cast::<PyString>() {
        Ok(Value::String(text_of(s)?))
    } else if let Ok(d) = o.cast::<PyDict>() {
        let mut m = Map::new();
        for (k, v) in d.iter() {
            let key = k.cast::<PyString>().map_err(|_| PyTypeError::new_err("JSON object keys must be str"))?;
            m.insert(text_of(key)?, from_py(&v)?);
        }
        Ok(Value::Object(m))
    } else if o.is_instance_of::<PyList>() || o.is_instance_of::<PyTuple>() {
        Ok(Value::Array(o.try_iter()?.map(|x| from_py(&x?)).collect::<PyResult<_>>()?))
    } else {
        Err(PyTypeError::new_err(format!("{} is not JSON", o.get_type().name()?)))
    }
}

/// Keyword arguments as a JSON object; none gives an empty one.
pub fn object(kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Map> {
    match kwargs.map(|k| from_py(k.as_any())).transpose()? {
        Some(Value::Object(m)) => Ok(m),
        _ => Ok(Map::new()),
    }
}
