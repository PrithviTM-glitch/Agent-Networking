//! Observer for Python: every topic as events, read with next(timeout); no callbacks into Python.

use crate::convert::dict;
use crate::errors::bridge;
use agent_bridge::observer::{self, Event};
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use std::sync::Arc;
use std::time::Duration;

#[pyclass(frozen, name = "Observer", module = "agent_bridge._native")]
pub struct Observer(Arc<observer::Observer>);

#[pymethods]
impl Observer {
    #[new]
    fn new() -> PyResult<Observer> {
        Ok(Observer(Arc::new(observer::Observer::open().map_err(bridge)?)))
    }

    /// The next event within timeout seconds, or None: ("connected", None, None), ("presence", addr, record or
    /// None), ("room", room, record or None), ("message", received_ts, message) or ("sys", topic, text).
    #[pyo3(signature = (timeout=1.0))]
    fn next<'py>(&self, py: Python<'py>, timeout: f64) -> PyResult<Option<Bound<'py, PyAny>>> {
        let o = self.0.clone();
        let wait = Duration::from_secs_f64(timeout.clamp(0.0, 3600.0));
        let Some(event) = py.detach(move || o.next(wait)) else { return Ok(None) };
        let none = || py.None().into_bound(py);
        let maybe = |rec: Option<agent_bridge::json::Map>| -> PyResult<Bound<'py, PyAny>> {
            rec.map_or_else(|| Ok(none()), |r| Ok(dict(py, &r)?.into_any()))
        };
        let (kind, a, b): (&str, Bound<'py, PyAny>, Bound<'py, PyAny>) = match event {
            Event::Connected => ("connected", none(), none()),
            Event::Presence { addr, rec } => ("presence", addr.into_bound_py_any(py)?, maybe(rec)?),
            Event::Room { room, rec } => ("room", room.into_bound_py_any(py)?, maybe(rec)?),
            Event::Message { received, msg } => {
                ("message", received.into_bound_py_any(py)?, dict(py, &msg)?.into_any())
            }
            Event::Sys { topic, text } => ("sys", topic.into_bound_py_any(py)?, text.into_bound_py_any(py)?),
        };
        Ok(Some((kind, a, b).into_bound_py_any(py)?))
    }

    /// Events lost to a full queue since the start.
    #[getter]
    fn dropped(&self) -> u64 {
        self.0.dropped()
    }

    fn close(&self, py: Python<'_>) {
        let o = self.0.clone();
        py.detach(move || o.close());
    }
}
