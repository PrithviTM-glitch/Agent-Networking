//! Bus for Python: one agent's connection, shared by any number of Python threads. Every call that can wait releases
//! the GIL, and none holds a lock while it waits, so a thread blocked in recv never stalls another's say. Arguments
//! are checked as the Python Bus checks them: a wrong type is a one-line BridgeError, never a TypeError.

use crate::args::{caps, ident, ident_cap, last, opt_ident, text, wait};
use crate::convert::{dict, dicts, from_py, object};
use crate::errors::bridge;
use agent_bridge::bus;
use agent_bridge::json::Map;
use agent_bridge::validation::{ADMIN, Error, ID_MAX, NAME_MAX};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use std::sync::Arc;
use std::time::Duration;

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
    f: impl FnOnce(&bus::Bus) -> agent_bridge::Result<Map> + Send,
) -> PyResult<Bound<'py, PyDict>> {
    let m = call(py, b, f)?;
    dict(py, &m)
}

#[pymethods]
impl Bus {
    /// Join the bus. With addr, reattach to it if it is live (its record stays as is), else create it. admin=True is
    /// only for the console: name and address admin. owner=False publishes as the agent over its own connection.
    #[staticmethod]
    #[pyo3(signature = (name, caps=None, keep=None, addr=None, admin=false, owner=true, subagent=false))]
    #[allow(clippy::too_many_arguments)]
    fn register(
        py: Python<'_>,
        name: &Bound<'_, PyAny>,
        caps: Option<&Bound<'_, PyAny>>,
        keep: Option<&Bound<'_, PyAny>>,
        addr: Option<&Bound<'_, PyAny>>,
        admin: bool,
        owner: bool,
        subagent: bool,
    ) -> PyResult<Bus> {
        let keep = keep.map(|k| k.is_truthy()).transpose()?.unwrap_or(false);
        if admin {
            let named_admin = name.extract::<String>().is_ok_and(|n| n == ADMIN);
            let at_admin = addr.and_then(|a| a.extract::<String>().ok()).is_some_and(|a| a == ADMIN);
            if !named_admin || !at_admin || subagent {
                return Err(bridge(Error("admin=True is only for name and address admin".into())));
            }
            return Ok(Bus(Arc::new(py.detach(bus::Bus::register_admin).map_err(bridge)?)));
        }
        let cap = if addr.is_some_and(|a| a.is_truthy().unwrap_or(false)) { ID_MAX } else { NAME_MAX };
        let name = ident_cap(name, "name", cap)?;
        let caps = match caps {
            Some(c) if !c.is_none() => self::caps(c)?,
            _ => Vec::new(),
        };
        let addr = opt_ident(addr, "address")?;
        let b = py
            .detach(move || bus::Bus::register(&name, &caps, keep, addr.as_deref(), subagent, owner))
            .map_err(bridge)?;
        Ok(Bus(Arc::new(b)))
    }

    /// The admin console's agent: name and address admin, kept, the only one that may broadcast.
    #[staticmethod]
    fn register_admin(py: Python<'_>) -> PyResult<Bus> {
        Ok(Bus(Arc::new(py.detach(bus::Bus::register_admin).map_err(bridge)?)))
    }

    /// Attach to a live address; owner=False publishes as the agent over its own connection and leaves the agent's
    /// session to its owner (the relay): no recv, and sub/unsub/keep go through a brief owner connection.
    #[staticmethod]
    #[pyo3(signature = (addr, owner=true))]
    fn attach(py: Python<'_>, addr: &Bound<'_, PyAny>, owner: bool) -> PyResult<Bus> {
        let addr = ident(addr, "address")?;
        Ok(Bus(Arc::new(py.detach(move || bus::Bus::attach_as(&addr, owner)).map_err(bridge)?)))
    }

    #[getter]
    fn addr(&self) -> String {
        self.0.addr.clone()
    }

    #[getter]
    fn owner(&self) -> bool {
        self.0.owner()
    }

    /// The agent's presence record as this Bus last wrote or read it.
    #[getter]
    fn rec<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        dict(py, &self.0.rec())
    }

    fn join(&self, py: Python<'_>, room: &Bound<'_, PyAny>) -> PyResult<()> {
        let room = ident(room, "room")?;
        call(py, &self.0, move |b| b.join(&room))
    }

    fn part(&self, py: Python<'_>, room: &Bound<'_, PyAny>) -> PyResult<()> {
        let room = ident(room, "room")?;
        call(py, &self.0, move |b| b.part(&room))
    }

    fn sub(&self, py: Python<'_>, topic: &Bound<'_, PyAny>) -> PyResult<()> {
        let topic = text(topic, "topic")?;
        call(py, &self.0, move |b| b.sub(&topic))
    }

    fn unsub(&self, py: Python<'_>, topic: &Bound<'_, PyAny>) -> PyResult<()> {
        let topic = text(topic, "topic")?;
        call(py, &self.0, move |b| b.unsub(&topic))
    }

    /// Post to a room; returns {"mentions": [addr, ...], "unresolved": [word, ...]}: whom it mentioned, and the @words
    /// that matched no member of the room (sent as plain text, mentioning nobody).
    fn say<'py>(
        &self,
        py: Python<'py>,
        room: &Bound<'py, PyAny>,
        body: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let (room, body) = (ident(room, "room")?, text(body, "text")?);
        let said = call(py, &self.0, move |b| b.say(&room, &body))?;
        let mut out = Map::new();
        out.insert("mentions".into(), said.mentions.into());
        out.insert("unresolved".into(), said.unresolved.into());
        dict(py, &out)
    }

    fn dm(&self, py: Python<'_>, to: &Bound<'_, PyAny>, body: &Bound<'_, PyAny>) -> PyResult<()> {
        let (body, to) = (text(body, "text")?, ident(to, "address")?);
        call(py, &self.0, move |b| b.dm(&to, &body))
    }

    #[pyo3(name = "pub")]
    fn publish(&self, py: Python<'_>, topic: &Bound<'_, PyAny>, body: &Bound<'_, PyAny>) -> PyResult<()> {
        let (topic, body) = (text(topic, "topic")?, text(body, "text")?);
        call(py, &self.0, move |b| b.publish(&topic, &body))
    }

    fn broadcast(&self, py: Python<'_>, body: &Bound<'_, PyAny>) -> PyResult<()> {
        let body = text(body, "text")?;
        call(py, &self.0, move |b| b.broadcast(&body))
    }

    /// Up to limit buffered messages, blocking up to wait seconds for the first; acked once returned.
    #[pyo3(signature = (wait=None, limit=None))]
    fn recv<'py>(
        &self,
        py: Python<'py>,
        wait: Option<&Bound<'py, PyAny>>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyList>> {
        let wait = wait.map(self::wait).transpose()?.unwrap_or(0.0);
        let msgs = call(py, &self.0, move |b| b.recv(wait, limit, None))?;
        dicts(py, &msgs)
    }

    /// addr's presence record from this connection's cache (waiting until it is connected), or None.
    fn cached_agent<'py>(&self, py: Python<'py>, addr: &Bound<'py, PyAny>) -> PyResult<Option<Bound<'py, PyDict>>> {
        let addr = text(addr, "address")?;
        call(py, &self.0, move |b| b.cached_agent(&addr))?.map(|m| dict(py, &m)).transpose()
    }

    /// room's settings record from this connection's cache as it is now, or None.
    fn cached_room<'py>(&self, py: Python<'py>, room: &Bound<'py, PyAny>) -> PyResult<Option<Bound<'py, PyDict>>> {
        self.0.cached_room(&text(room, "room")?).map(|m| dict(py, &m)).transpose()
    }

    fn pending(&self) -> usize {
        self.0.pending()
    }

    /// Wait up to timeout seconds for a message newer than arrival number after_seq: (pending, newest arrival
    /// number). Feed the number back in; unread messages that arrived before never wake it.
    #[pyo3(signature = (timeout, after_seq=0))]
    fn wait_arrival(&self, py: Python<'_>, timeout: &Bound<'_, PyAny>, after_seq: u64) -> PyResult<(usize, u64)> {
        let wait = Duration::from_secs_f64(self::wait(timeout)?.clamp(0.0, 3600.0));
        let b = self.0.clone();
        py.detach(move || b.wait_arrival(wait, after_seq)).map_err(bridge)
    }

    fn keep(&self, py: Python<'_>, on: &Bound<'_, PyAny>) -> PyResult<()> {
        let on = on.is_truthy()?;
        call(py, &self.0, move |b| b.keep(on))
    }

    /// Delete this agent; a warning if ledgerd or historyd could not drop what it holds. Closed afterwards.
    fn leave(&self, py: Python<'_>) -> PyResult<Option<String>> {
        call(py, &self.0, |b| b.leave())
    }

    #[pyo3(signature = (name=None, caps=None))]
    fn describe(
        &self,
        py: Python<'_>,
        name: Option<&Bound<'_, PyAny>>,
        caps: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let name = opt_ident(name, "name")?;
        let caps = caps.filter(|c| !c.is_none()).map(self::caps).transpose()?;
        call(py, &self.0, move |b| b.describe(name.as_deref(), caps.as_deref()))
    }

    #[pyo3(signature = (room, mode=None, rules=None))]
    fn room_set<'py>(
        &self,
        py: Python<'py>,
        room: &Bound<'py, PyAny>,
        mode: Option<&Bound<'py, PyAny>>,
        rules: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        // In the Python Bus's order: nothing to change, then the room, then (once allowed) mode and rules.
        let json = |v: Option<&Bound<'py, PyAny>>| v.filter(|v| !v.is_none()).map(from_py).transpose();
        let rules_arg = rules;
        let (mode, mut rules) = (json(mode)?, json(rules)?);
        if mode.is_none() && rules.is_none() {
            return Err(bridge(Error("nothing to change: give mode or rules".into())));
        }
        let room = ident(room, "room")?;
        // Rules that are a str but not UTF-8 (lone surrogates) are refused where the rules check runs, after the
        // rights and the mode: core refuses a list there, before anything is written, and the error is Python's.
        let not_utf8 = rules_arg.is_some_and(|r| r.cast::<PyString>().is_ok_and(|s| s.to_str().is_err()));
        if not_utf8 {
            rules = Some(serde_json::Value::Array(Vec::new()));
        }
        settings(py, &self.0, move |b| match b.room_set(&room, mode, rules) {
            Err(e) if not_utf8 && e.0.starts_with("rules ") => Err(Error("rules is not valid UTF-8".into())),
            other => other,
        })
    }

    #[pyo3(name = "mod")]
    fn moderator<'py>(
        &self,
        py: Python<'py>,
        room: &Bound<'py, PyAny>,
        action: &Bound<'py, PyAny>,
        addr: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let action = action.extract::<String>().unwrap_or_default();
        let (room, addr) = (ident(room, "room")?, ident(addr, "address")?);
        settings(py, &self.0, move |b| b.moderator(&room, &action, &addr))
    }

    fn mute<'py>(
        &self,
        py: Python<'py>,
        room: &Bound<'py, PyAny>,
        addr: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        self.member(py, "mute", room, addr)
    }

    fn unmute<'py>(
        &self,
        py: Python<'py>,
        room: &Bound<'py, PyAny>,
        addr: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        self.member(py, "unmute", room, addr)
    }

    fn ban<'py>(
        &self,
        py: Python<'py>,
        room: &Bound<'py, PyAny>,
        addr: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        self.member(py, "ban", room, addr)
    }

    fn unban<'py>(
        &self,
        py: Python<'py>,
        room: &Bound<'py, PyAny>,
        addr: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        self.member(py, "unban", room, addr)
    }

    fn kick<'py>(
        &self,
        py: Python<'py>,
        room: &Bound<'py, PyAny>,
        addr: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        self.member(py, "kick", room, addr)
    }

    /// One ledgerd request over this Bus's connection, as this agent or as_. op and as_ go to ledgerd as given, so a
    /// bad one gets ledgerd's own LedgerError (with .reply), as from any client.
    #[pyo3(signature = (op, as_=None, timeout=None, **fields))]
    fn ledger<'py>(
        &self,
        py: Python<'py>,
        op: &Bound<'py, PyAny>,
        as_: Option<&Bound<'py, PyAny>>,
        timeout: Option<f64>,
        fields: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let (op, as_, fields) = (from_py(op)?, as_.map(from_py).transpose()?, object(fields)?);
        let b = self.0.clone();
        let reply =
            py.detach(move || b.ledger_value(op, as_, fields, timeout)).map_err(|e| crate::errors::ledger(py, e))?;
        dict(py, &reply)
    }

    /// One historyd request over this Bus's connection; as_agent sends it as this agent (dms, drop). op goes to
    /// historyd as given.
    #[pyo3(signature = (op, as_agent=false, timeout=agent_bridge::history::client::TIMEOUT, **fields))]
    fn history_call<'py>(
        &self,
        py: Python<'py>,
        op: &Bound<'py, PyAny>,
        as_agent: bool,
        timeout: f64,
        fields: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let (op, fields) = (from_py(op)?, object(fields)?);
        let b = self.0.clone();
        let reply = py
            .detach(move || b.history_call(op, as_agent, fields, timeout))
            .map_err(|e| crate::errors::history(py, e))?;
        dict(py, &reply)
    }

    /// This agent's DMs in both directions through historyd, oldest first; with peer, only those with peer.
    #[pyo3(signature = (peer=None, last=None))]
    fn dms<'py>(
        &self,
        py: Python<'py>,
        peer: Option<&Bound<'py, PyAny>>,
        last: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let peer = opt_ident(peer, "address")?;
        let last = last.map(self::last).transpose()?.unwrap_or(20);
        let msgs = call(py, &self.0, move |b| b.dms(peer.as_deref(), last))?;
        dicts(py, &msgs)
    }

    /// Disconnect; the session and anything not yet received stay with the broker.
    fn close(&self, py: Python<'_>) {
        let b = self.0.clone();
        py.detach(move || b.close());
    }
}

impl Bus {
    fn member<'py>(
        &self,
        py: Python<'py>,
        verb: &'static str,
        room: &Bound<'py, PyAny>,
        addr: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let (addr, room) = (ident(addr, "address")?, ident(room, "room")?);
        settings(py, &self.0, move |b| b.moderate_member(verb, &room, &addr))
    }
}
