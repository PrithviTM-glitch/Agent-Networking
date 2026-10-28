//! The bridge's exceptions: BridgeError for bad input, an unknown address or a broker problem, LedgerError (with
//! .reply) for a refused or failed ledger call. Messages are one line, the same as the CLI prints.

use crate::convert::dict;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(_native, BridgeError, PyException, "Bad input, an unknown address or a broker problem.");
create_exception!(_native, LedgerError, PyException, "An ok:false reply, a timeout or a malformed reply.");

pub fn bridge(e: agent_bridge::Error) -> PyErr {
    BridgeError::new_err(e.0)
}

pub fn ledger(py: Python<'_>, e: agent_bridge::ledger::LedgerError) -> PyErr {
    let err = LedgerError::new_err(e.to_string());
    if let Ok(reply) = dict(py, &e.reply) {
        let _ = err.value(py).setattr("reply", reply);
    }
    err
}
