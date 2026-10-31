//! agent_bridge._native: the Rust core as a Python extension module. The thin modules of the agent_bridge package
//! re-export it; it grows with each step of the port.

mod args;
mod bus;
mod convert;
mod errors;
mod observer;

use agent_bridge::{host, identity, ledger, presence, relay, rooms, validation as v};
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
    v::check_text(&args::text(value, what)?, what, cap).map(str::to_owned).map_err(bridge)
}

#[pyfunction]
#[pyo3(signature = (value, filter=false))]
fn check_topic(value: &Bound<'_, PyAny>, filter: bool) -> PyResult<String> {
    v::check_topic(&args::text(value, "topic")?, filter).map(str::to_owned).map_err(bridge)
}

#[pyfunction]
fn check_caps(values: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    v::check_caps(&args::caps(values)?).map_err(bridge)
}

#[pyfunction]
fn check_session(value: &Bound<'_, PyAny>) -> PyResult<String> {
    let value = if value.is_none() { None } else { Some(args::session(value)?) };
    v::check_session(value.as_deref()).map(str::to_owned).map_err(bridge)
}

#[pyfunction]
fn check_wait(value: &Bound<'_, PyAny>) -> PyResult<f64> {
    v::check_wait(args::wait(value)?).map_err(bridge)
}

#[pyfunction]
fn check_last(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    v::check_last(args::last(value)?).map_err(bridge)
}

#[pyfunction]
#[pyo3(signature = (room=None))]
fn presence_agents<'py>(py: Python<'py>, room: Option<&Bound<'py, PyAny>>) -> PyResult<Bound<'py, PyList>> {
    let room = args::opt_ident(room, "room")?;
    let rows = py.detach(move || presence::agents(room.as_deref())).map_err(bridge)?;
    dicts(py, &rows)
}

#[pyfunction]
fn presence_find<'py>(py: Python<'py>, addr: &Bound<'py, PyAny>) -> PyResult<Option<Bound<'py, PyDict>>> {
    let addr = args::ident(addr, "address")?;
    py.detach(move || presence::find(&addr)).map_err(bridge)?.map(|r| dict(py, &r)).transpose()
}

#[pyfunction]
fn presence_need<'py>(py: Python<'py>, addr: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyDict>> {
    let addr = args::ident(addr, "address")?;
    dict(py, &py.detach(move || presence::need(&addr)).map_err(bridge)?)
}

/// End addr's broker session and clear its presence.
#[pyfunction]
fn presence_wipe(py: Python<'_>, addr: &Bound<'_, PyAny>) -> PyResult<()> {
    let addr = args::ident(addr, "address")?;
    py.detach(move || presence::wipe(&addr)).map_err(bridge)
}

#[pyfunction]
fn rooms_info<'py>(py: Python<'py>, room: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyDict>> {
    let room = args::ident(room, "room")?;
    dict(py, &py.detach(move || rooms::info(&room)).map_err(bridge)?)
}

#[pyfunction]
fn rooms_listing<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
    dicts(py, &py.detach(rooms::listing).map_err(bridge)?)
}

/// What info shows for a room: its settings, the open defaults when it has no record.
#[pyfunction]
#[pyo3(signature = (room, rec=None))]
fn rooms_view<'py>(
    py: Python<'py>,
    room: &Bound<'py, PyAny>,
    rec: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyDict>> {
    let room = args::ident(room, "room")?;
    let rec = match rec.map(|r| from_py(r.as_any())).transpose()? {
        Some(serde_json::Value::Object(m)) => Some(m),
        _ => None,
    };
    dict(py, &rooms::view(&room, rec.as_ref()))
}

#[pyfunction]
fn relay_lock_path(session: &Bound<'_, PyAny>) -> PyResult<String> {
    let session = args::session(session)?;
    Ok(relay::lock_path(&session).map_err(bridge)?.to_string_lossy().into_owned())
}

#[pyfunction]
fn relay_live(session: &Bound<'_, PyAny>) -> PyResult<bool> {
    Ok(relay::live(&args::session(session)?))
}

#[pyfunction]
fn relay_held(session: &Bound<'_, PyAny>) -> PyResult<Option<String>> {
    Ok(relay::held(&args::session(session)?))
}

#[pyfunction]
#[pyo3(signature = (session, timeout=5.0))]
fn relay_stop(py: Python<'_>, session: &Bound<'_, PyAny>, timeout: f64) -> PyResult<bool> {
    let session = args::session(session)?;
    let wait = Duration::from_secs_f64(timeout.clamp(0.0, 3600.0));
    py.detach(move || relay::stop(&session, wait)).map_err(bridge)
}

/// The agent a relay for session would adopt (relay.session_agent), or None.
#[pyfunction]
fn relay_session_agent(py: Python<'_>, session: &Bound<'_, PyAny>) -> PyResult<Option<String>> {
    let session = args::session(session)?;
    py.detach(move || relay::session_agent(&session)).map_err(bridge)
}

/// comm, ppid and start time in clock ticks of a live process, or None.
#[pyfunction]
fn identity_read_proc(py: Python<'_>, pid: i64) -> PyResult<Option<Bound<'_, PyDict>>> {
    let Some(p) = host::stat(pid) else { return Ok(None) };
    let d = PyDict::new(py);
    d.set_item("comm", p.comm)?;
    d.set_item("ppid", p.ppid)?;
    d.set_item("start", p.start)?;
    Ok(Some(d))
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
    op: &Bound<'py, PyAny>,
    as_: &Bound<'py, PyAny>,
    timeout: Option<f64>,
    fields: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyDict>> {
    // op and as_ go to ledgerd as given (None: no "as"), so a bad one gets ledgerd's own LedgerError.
    let (op, as_, fields) = (from_py(op)?, Some(from_py(as_)?).filter(|a| !a.is_null()), convert::object(fields)?);
    let reply = py.detach(move || ledger::call_value(op, as_, fields, timeout)).map_err(|e| errors::ledger(py, e))?;
    dict(py, &reply)
}

/// One historyd request over a connection of its own: as_ for the ops that name an agent (dms, drop, recent);
/// raises HistoryError with .reply.
#[pyfunction]
#[pyo3(signature = (op, as_=None, timeout=agent_bridge::history::client::TIMEOUT, **fields))]
fn history_call<'py>(
    py: Python<'py>,
    op: &Bound<'py, PyAny>,
    as_: Option<&Bound<'py, PyAny>>,
    timeout: f64,
    fields: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyDict>> {
    // op and as_ go to historyd as given, so a bad one gets historyd's own HistoryError.
    let as_ = as_.map(from_py).transpose()?.filter(|a| !a.is_null());
    let (op, fields) = (from_py(op)?, convert::object(fields)?);
    let reply = py
        .detach(move || agent_bridge::history::client::call_value(op, as_, fields, timeout))
        .map_err(|e| errors::history(py, e))?;
    dict(py, &reply)
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    let classes = errors::classes(py)?;
    m.add("BridgeError", classes.bridge.bind(py))?;
    m.add("LedgerError", classes.ledger.bind(py))?;
    m.add("HistoryError", classes.history.bind(py))?;
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
    for (name, value) in [
        ("MAX_LEDGERS", ledger::MAX_LEDGERS),
        ("MAX_RESOURCES", ledger::MAX_RESOURCES),
        ("MAX_PENDING", ledger::MAX_PENDING),
        ("MAX_PENDING_PER_OWNER", ledger::MAX_PENDING_PER_OWNER),
        ("MAX_LEASES", ledger::MAX_LEASES),
        ("MAX_WAITERS", ledger::MAX_WAITERS),
        ("MAX_REQUESTS", ledger::MAX_REQUESTS),
    ] {
        m.add(name, value)?;
    }
    m.add("MAX_TTL", ledger::MAX_TTL)?;
    m.add("MAX_WAIT", ledger::MAX_WAIT)?;
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
    m.add_function(wrap_pyfunction!(rooms_view, m)?)?;
    m.add("ROOM_OPEN", dict(py, &rooms::open())?)?;
    m.add("RULES_MAX", rooms::RULES_MAX)?;
    m.add("MODS_MAX", rooms::MODS_MAX)?;
    m.add("LIST_MAX", rooms::LIST_MAX)?;
    m.add("RELAY_BACKLOG", relay::BACKLOG)?;
    m.add_function(wrap_pyfunction!(relay_lock_path, m)?)?;
    m.add_function(wrap_pyfunction!(relay_live, m)?)?;
    m.add_function(wrap_pyfunction!(relay_held, m)?)?;
    m.add_function(wrap_pyfunction!(relay_stop, m)?)?;
    m.add_function(wrap_pyfunction!(relay_session_agent, m)?)?;
    m.add_function(wrap_pyfunction!(identity_read_proc, m)?)?;
    m.add_function(wrap_pyfunction!(identity_session_address, m)?)?;
    m.add_function(wrap_pyfunction!(identity_short_hash, m)?)?;
    m.add_function(wrap_pyfunction!(identity_project_name, m)?)?;
    m.add_function(wrap_pyfunction!(identity_origin, m)?)?;
    m.add_function(wrap_pyfunction!(ledger_call, m)?)?;
    m.add_function(wrap_pyfunction!(history_call, m)?)?;
    Ok(())
}
