//! agent_bridge._native: the Rust core as a Python extension module. The thin modules of the agent_bridge package
//! re-export it; it grows with each step of the port.

mod bus;
mod convert;
mod errors;
mod observer;

use agent_bridge::{identity, ledger, presence, relay, rooms, validation as v};
use convert::{dict, dicts, from_py};
use errors::bridge;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::time::Duration;

/// Python's repr() of a value, cut to 60 characters, as the bridge's error messages show it.
fn show(value: &Bound<'_, PyAny>) -> PyResult<String> {
    Ok(value.repr()?.to_str()?.chars().take(60).collect())
}

#[pyfunction]
#[pyo3(signature = (value, what="name", cap=v::ID_MAX))]
fn check_ident(value: &Bound<'_, PyAny>, what: &str, cap: usize) -> PyResult<String> {
    match value.extract::<String>() {
        Ok(s) => v::check_ident_cap(&s, what, cap).map(str::to_owned).map_err(bridge),
        Err(_) => {
            Err(bridge(v::Error(format!("bad {what} {}: use 1-{cap} ASCII letters, digits, _ or -", show(value)?))))
        }
    }
}

#[pyfunction]
#[pyo3(signature = (value, cap=v::ID_MAX))]
fn is_ident(value: &Bound<'_, PyAny>, cap: usize) -> bool {
    value.extract::<String>().is_ok_and(|s| v::is_ident(&s, cap))
}

#[pyfunction]
fn not_admin(value: Option<String>, what: &str) -> PyResult<Option<String>> {
    v::not_admin(value.as_deref(), what).map_err(bridge)?;
    Ok(value)
}

#[pyfunction]
#[pyo3(signature = (value, what="text", cap=v::BODY_MAX))]
fn check_text(value: &Bound<'_, PyAny>, what: &str, cap: usize) -> PyResult<String> {
    let json = from_py(value)?;
    match json {
        serde_json::Value::String(s) => v::check_text(&s, what, cap).map(str::to_owned).map_err(bridge),
        _ => Err(bridge(v::Error(format!("{what} must be a string, got {}", value.get_type().name()?)))),
    }
}

#[pyfunction]
#[pyo3(signature = (value, filter=false))]
fn check_topic(value: String, filter: bool) -> PyResult<String> {
    v::check_topic(&value, filter).map(str::to_owned).map_err(bridge)
}

#[pyfunction]
fn check_caps(values: Vec<String>) -> PyResult<Vec<String>> {
    v::check_caps(&values).map_err(bridge)
}

#[pyfunction]
fn check_session(value: Option<String>) -> PyResult<String> {
    v::check_session(value.as_deref()).map(str::to_owned).map_err(bridge)
}

#[pyfunction]
fn check_wait(value: f64) -> PyResult<f64> {
    v::check_wait(value).map_err(bridge)
}

#[pyfunction]
fn check_last(value: i64) -> PyResult<usize> {
    v::check_last(value).map_err(bridge)
}

#[pyfunction]
#[pyo3(signature = (room=None))]
fn presence_agents<'py>(py: Python<'py>, room: Option<String>) -> PyResult<Bound<'py, PyList>> {
    let rows = py.detach(move || presence::agents(room.as_deref())).map_err(bridge)?;
    dicts(py, &rows)
}

#[pyfunction]
fn presence_find<'py>(py: Python<'py>, addr: String) -> PyResult<Option<Bound<'py, PyDict>>> {
    py.detach(move || presence::find(&addr)).map_err(bridge)?.map(|r| dict(py, &r)).transpose()
}

#[pyfunction]
fn presence_need<'py>(py: Python<'py>, addr: String) -> PyResult<Bound<'py, PyDict>> {
    dict(py, &py.detach(move || presence::need(&addr)).map_err(bridge)?)
}

/// End addr's broker session and clear its presence.
#[pyfunction]
fn presence_wipe(py: Python<'_>, addr: String) -> PyResult<()> {
    py.detach(move || presence::wipe(&addr)).map_err(bridge)
}

#[pyfunction]
fn rooms_info<'py>(py: Python<'py>, room: String) -> PyResult<Bound<'py, PyDict>> {
    dict(py, &py.detach(move || rooms::info(&room)).map_err(bridge)?)
}

#[pyfunction]
fn rooms_listing<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
    dicts(py, &py.detach(rooms::listing).map_err(bridge)?)
}

#[pyfunction]
fn relay_lock_path(session: String) -> PyResult<String> {
    Ok(relay::lock_path(&session).map_err(bridge)?.to_string_lossy().into_owned())
}

#[pyfunction]
fn relay_live(session: String) -> bool {
    relay::live(&session)
}

#[pyfunction]
fn relay_held(session: String) -> Option<String> {
    relay::held(&session)
}

#[pyfunction]
#[pyo3(signature = (session, timeout=5.0))]
fn relay_stop(py: Python<'_>, session: String, timeout: f64) -> PyResult<bool> {
    let wait = Duration::from_secs_f64(timeout.clamp(0.0, 3600.0));
    py.detach(move || relay::stop(&session, wait)).map_err(bridge)
}

#[pyfunction]
fn identity_session_address(session: String) -> String {
    identity::session_address(&session)
}

#[pyfunction]
fn identity_short_hash(text: String) -> String {
    identity::short_hash(&text)
}

#[pyfunction]
fn identity_project_name() -> String {
    identity::project_name()
}

#[pyfunction]
fn identity_origin<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
    dict(py, identity::origin())
}

/// One ledgerd request as as_, over a connection of its own; raises LedgerError with .reply.
#[pyfunction]
#[pyo3(signature = (op, as_, timeout=None, **fields))]
fn ledger_call<'py>(
    py: Python<'py>,
    op: String,
    as_: String,
    timeout: Option<f64>,
    fields: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyDict>> {
    let fields = match fields.map(|f| from_py(f.as_any())).transpose()? {
        Some(serde_json::Value::Object(m)) => m,
        _ => agent_bridge::json::Map::new(),
    };
    let reply = py.detach(move || ledger::call(&op, &as_, fields, timeout)).map_err(|e| errors::ledger(py, e))?;
    dict(py, &reply)
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("BridgeError", py.get_type::<errors::BridgeError>())?;
    m.add("LedgerError", py.get_type::<errors::LedgerError>())?;
    m.add_class::<bus::Bus>()?;
    m.add_class::<observer::Observer>()?;
    for (name, value) in [
        ("ID_MAX", v::ID_MAX),
        ("NAME_MAX", v::NAME_MAX),
        ("CAPS_MAX", v::CAPS_MAX),
        ("TOPICS_MAX", v::TOPICS_MAX),
        ("TOPIC_MAX", v::TOPIC_MAX),
        ("MENTIONS_MAX", v::MENTIONS_MAX),
        ("BODY_MAX", v::BODY_MAX),
        ("PAYLOAD_MAX", agent_bridge::protocol::PAYLOAD_MAX),
    ] {
        m.add(name, value)?;
    }
    m.add("WAIT_MAX", v::WAIT_MAX)?;
    m.add("LAST_MAX", v::LAST_MAX)?;
    m.add("ADMIN", v::ADMIN)?;
    m.add("RESERVED", v::RESERVED.to_vec())?;
    m.add("SERVICES", v::SERVICES.to_vec())?;
    m.add_function(wrap_pyfunction!(check_ident, m)?)?;
    m.add_function(wrap_pyfunction!(is_ident, m)?)?;
    m.add_function(wrap_pyfunction!(not_admin, m)?)?;
    m.add_function(wrap_pyfunction!(check_text, m)?)?;
    m.add_function(wrap_pyfunction!(check_topic, m)?)?;
    m.add_function(wrap_pyfunction!(check_caps, m)?)?;
    m.add_function(wrap_pyfunction!(check_session, m)?)?;
    m.add_function(wrap_pyfunction!(check_wait, m)?)?;
    m.add_function(wrap_pyfunction!(check_last, m)?)?;
    m.add_function(wrap_pyfunction!(presence_agents, m)?)?;
    m.add_function(wrap_pyfunction!(presence_find, m)?)?;
    m.add_function(wrap_pyfunction!(presence_need, m)?)?;
    m.add_function(wrap_pyfunction!(presence_wipe, m)?)?;
    m.add_function(wrap_pyfunction!(rooms_info, m)?)?;
    m.add_function(wrap_pyfunction!(rooms_listing, m)?)?;
    m.add_function(wrap_pyfunction!(relay_lock_path, m)?)?;
    m.add_function(wrap_pyfunction!(relay_live, m)?)?;
    m.add_function(wrap_pyfunction!(relay_held, m)?)?;
    m.add_function(wrap_pyfunction!(relay_stop, m)?)?;
    m.add_function(wrap_pyfunction!(identity_session_address, m)?)?;
    m.add_function(wrap_pyfunction!(identity_short_hash, m)?)?;
    m.add_function(wrap_pyfunction!(identity_project_name, m)?)?;
    m.add_function(wrap_pyfunction!(identity_origin, m)?)?;
    m.add_function(wrap_pyfunction!(ledger_call, m)?)?;
    Ok(())
}
