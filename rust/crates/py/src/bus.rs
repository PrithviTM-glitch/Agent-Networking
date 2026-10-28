//! Bus for Python: one agent's connection, shared by any number of Python threads. Every call that can wait releases
//! the GIL, and none holds a lock while it waits, so a thread blocked in recv never stalls another's say.

use crate::convert::{dict, dicts};
use crate::errors::bridge;
use agent_bridge::bus;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::sync::Arc;

#[pyclass(frozen, name = "Bus", module = "agent_bridge._native")]
pub struct Bus(Arc<bus::Bus>);

/// Run f on the Bus with the GIL released.
fn call<T: Send>(
    py: Python<'_>,
    b: &Arc<bus::Bus>,
    f: impl FnOnce(&bus::Bus) -> agent_bridge::Result<T> + Send,
) -> PyResult<T> {
    let b = b.clone();
    py.detach(move || f(&b)).map_err(bridge)
}

fn settings<'py>(
    py: Python<'py>,
    b: &Arc<bus::Bus>,
    f: impl FnOnce(&bus::Bus) -> agent_bridge::Result<agent_bridge::json::Map> + Send,
) -> PyResult<Bound<'py, PyDict>> {
    let m = call(py, b, f)?;
    dict(py, &m)
}

#[pymethods]
impl Bus {
    /// Join the bus. With addr, reattach to it if it is live (its record stays as is), else create it.
    #[staticmethod]
    #[pyo3(signature = (name, caps=Vec::new(), keep=false, addr=None, subagent=false))]
    fn register(
        py: Python<'_>,
        name: String,
        caps: Vec<String>,
        keep: bool,
        addr: Option<String>,
        subagent: bool,
    ) -> PyResult<Bus> {
        let b = py.detach(move || bus::Bus::register(&name, &caps, keep, addr.as_deref(), subagent)).map_err(bridge)?;
        Ok(Bus(Arc::new(b)))
    }

    /// The admin console's agent: name and address admin, kept, the only one that may broadcast.
    #[staticmethod]
    fn register_admin(py: Python<'_>) -> PyResult<Bus> {
        Ok(Bus(Arc::new(py.detach(bus::Bus::register_admin).map_err(bridge)?)))
    }

    /// Attach to a live address.
    #[staticmethod]
    fn attach(py: Python<'_>, addr: String) -> PyResult<Bus> {
        Ok(Bus(Arc::new(py.detach(move || bus::Bus::attach(&addr)).map_err(bridge)?)))
    }

    #[getter]
    fn addr(&self) -> String {
        self.0.addr.clone()
    }

    /// The agent's presence record as this Bus last wrote or read it.
    #[getter]
    fn rec<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        dict(py, &self.0.rec())
    }

    fn join(&self, py: Python<'_>, room: String) -> PyResult<()> {
        call(py, &self.0, move |b| b.join(&room))
    }

    fn part(&self, py: Python<'_>, room: String) -> PyResult<()> {
        call(py, &self.0, move |b| b.part(&room))
    }

    fn sub(&self, py: Python<'_>, topic: String) -> PyResult<()> {
        call(py, &self.0, move |b| b.sub(&topic))
    }

    fn unsub(&self, py: Python<'_>, topic: String) -> PyResult<()> {
        call(py, &self.0, move |b| b.unsub(&topic))
    }

    fn say(&self, py: Python<'_>, room: String, body: String) -> PyResult<()> {
        call(py, &self.0, move |b| b.say(&room, &body))
    }

    fn dm(&self, py: Python<'_>, to: String, body: String) -> PyResult<()> {
        call(py, &self.0, move |b| b.dm(&to, &body))
    }

    #[pyo3(name = "pub")]
    fn publish(&self, py: Python<'_>, topic: String, body: String) -> PyResult<()> {
        call(py, &self.0, move |b| b.publish(&topic, &body))
    }

    fn broadcast(&self, py: Python<'_>, body: String) -> PyResult<()> {
        call(py, &self.0, move |b| b.broadcast(&body))
    }

    /// Up to limit buffered messages, blocking up to wait seconds for the first; acked once returned.
    #[pyo3(signature = (wait=0.0, limit=None))]
    fn recv<'py>(&self, py: Python<'py>, wait: f64, limit: Option<usize>) -> PyResult<Bound<'py, PyList>> {
        let msgs = call(py, &self.0, move |b| b.recv(wait, limit, None))?;
        dicts(py, &msgs)
    }

    fn pending(&self) -> usize {
        self.0.pending()
    }

    fn keep(&self, py: Python<'_>, on: bool) -> PyResult<()> {
        call(py, &self.0, move |b| b.keep(on))
    }

    /// Delete this agent; a warning if ledgerd could not drop its ledger. The Bus is closed afterwards.
    fn leave(&self, py: Python<'_>) -> PyResult<Option<String>> {
        call(py, &self.0, |b| b.leave())
    }

    #[pyo3(signature = (name=None, caps=None))]
    fn describe(&self, py: Python<'_>, name: Option<String>, caps: Option<Vec<String>>) -> PyResult<()> {
        call(py, &self.0, move |b| b.describe(name.as_deref(), caps.as_deref()))
    }

    #[pyo3(signature = (room, mode=None, rules=None))]
    fn room_set<'py>(
        &self,
        py: Python<'py>,
        room: String,
        mode: Option<String>,
        rules: Option<String>,
    ) -> PyResult<Bound<'py, PyDict>> {
        settings(py, &self.0, move |b| b.room_set(&room, mode.as_deref(), rules.as_deref()))
    }

    #[pyo3(name = "mod")]
    fn moderator<'py>(
        &self,
        py: Python<'py>,
        room: String,
        action: String,
        addr: String,
    ) -> PyResult<Bound<'py, PyDict>> {
        settings(py, &self.0, move |b| b.moderator(&room, &action, &addr))
    }

    fn mute<'py>(&self, py: Python<'py>, room: String, addr: String) -> PyResult<Bound<'py, PyDict>> {
        settings(py, &self.0, move |b| b.moderate_member("mute", &room, &addr))
    }

    fn unmute<'py>(&self, py: Python<'py>, room: String, addr: String) -> PyResult<Bound<'py, PyDict>> {
        settings(py, &self.0, move |b| b.moderate_member("unmute", &room, &addr))
    }

    fn ban<'py>(&self, py: Python<'py>, room: String, addr: String) -> PyResult<Bound<'py, PyDict>> {
        settings(py, &self.0, move |b| b.moderate_member("ban", &room, &addr))
    }

    fn unban<'py>(&self, py: Python<'py>, room: String, addr: String) -> PyResult<Bound<'py, PyDict>> {
        settings(py, &self.0, move |b| b.moderate_member("unban", &room, &addr))
    }

    fn kick<'py>(&self, py: Python<'py>, room: String, addr: String) -> PyResult<Bound<'py, PyDict>> {
        settings(py, &self.0, move |b| b.moderate_member("kick", &room, &addr))
    }

    /// Disconnect; the session and anything not yet received stay with the broker.
    fn close(&self, py: Python<'_>) {
        let b = self.0.clone();
        py.detach(move || b.close());
    }
}
