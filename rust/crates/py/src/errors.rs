//! The bridge's exceptions are Python classes, defined once in agent_bridge.errors, and raised here as they are, so
//! one except clause catches what Python and Rust raise alike: BridgeError for bad input, an unknown address or a
//! broker problem; LedgerError(reply) for a ledger call; HistoryError(reply), a BridgeError, for historyd.

use crate::convert::dict;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyType;

pub struct Classes {
    pub bridge: Py<PyType>,
    pub ledger: Py<PyType>,
    pub history: Py<PyType>,
}

static CLASSES: PyOnceLock<Classes> = PyOnceLock::new();

pub fn classes(py: Python<'_>) -> PyResult<&Classes> {
    CLASSES.get_or_try_init(py, || {
        let m = py.import("agent_bridge.errors")?;
        let class = |name: &str| -> PyResult<Py<PyType>> { Ok(m.getattr(name)?.cast_into::<PyType>()?.unbind()) };
        Ok(Classes { bridge: class("BridgeError")?, ledger: class("LedgerError")?, history: class("HistoryError")? })
    })
}

pub fn bridge(e: agent_bridge::Error) -> PyErr {
    Python::attach(|py| match classes(py) {
        Ok(c) => PyErr::from_type(c.bridge.bind(py).clone(), e.0),
        Err(err) => err,
    })
}

fn with_reply(py: Python<'_>, class: impl Fn(&Classes) -> &Py<PyType>, reply: &agent_bridge::json::Map) -> PyErr {
    let raised =
        || -> PyResult<PyErr> { Ok(PyErr::from_value(class(classes(py)?).bind(py).call1((dict(py, reply)?,))?)) };
    raised().unwrap_or_else(|e| e)
}

pub fn ledger(py: Python<'_>, e: agent_bridge::ledger::LedgerError) -> PyErr {
    with_reply(py, |c| &c.ledger, &e.reply)
}

pub fn history(py: Python<'_>, e: agent_bridge::history::client::HistoryError) -> PyErr {
    with_reply(py, |c| &c.history, &e.reply)
}
