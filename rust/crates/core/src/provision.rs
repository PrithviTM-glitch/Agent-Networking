//! Agent credentials under auth warn or enforce (docs/AUTH.md 4.2): the first registration of an address creates its
//! dynamic-security client (username = the address, a random password, no pinned client id) and its role of literal
//! topics, through the admin credentials, and writes agents/<addr>.json with a secret client id and the session the
//! agent is bound to. The CLI acts only as agents bound to its own session.

use crate::broker::{self, OneShot};
use crate::config::{self, Credentials};
use crate::json::Map;
use crate::ledger::random_hex;
use crate::mqtt::{Conn, Handler, Message, filter};
use crate::validation::{Error, Result, check_ident};
use crate::{host, validation};
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

const CONTROL: &str = "$CONTROL/dynamic-security/v1";
const ALLOW: i64 = 10;
const DENY: i64 = 5;

/// The ACLs of an agent's role: its own sender levels, inboxes and reply topics, every room and the presence and
/// room records; nothing else under the reserved first levels; raw topics freely. Today's topic forms stay allowed
/// until senders switch (AUTH.md 4.8 step S).
pub fn acls(addr: &str) -> Vec<(&'static str, String, bool, i64)> {
    let a = addr;
    let send = [
        format!("agents/{a}"),
        "inbox/+".into(),
        format!("inbox/+/{a}"),
        "room/+".into(),
        format!("room/+/{a}"),
        "rooms/+".into(),
        "rooms/+/pin".into(),
        "ledger/req".into(),
        format!("ledger/req/{a}"),
        "history/req".into(),
        format!("history/req/{a}"),
    ];
    let read = [
        "agents/+".into(),
        "rooms/+".into(),
        "rooms/+/pin".into(),
        "room/+".into(),
        "room/+/+".into(),
        format!("inbox/{a}"),
        format!("inbox/{a}/+"),
        format!("ledger/resp/{a}"),
        format!("ledger/resp/{a}/+"),
        format!("history/resp/{a}"),
        format!("history/resp/{a}/+"),
        "broadcast".into(),
    ];
    let reserved = ["agents/#", "inbox/#", "room/#", "rooms/#", "ledger/#", "history/#"];
    let mut out = Vec::new();
    for t in send {
        out.push(("publishClientSend", t, true, ALLOW));
    }
    for t in reserved.iter().chain(&["broadcast"]) {
        out.push(("publishClientSend", (*t).to_owned(), false, DENY));
    }
    out.push(("publishClientSend", "#".into(), true, 0));
    for kind in ["publishClientReceive", "subscribePattern"] {
        for t in &read {
            out.push((kind, t.clone(), true, ALLOW));
        }
        for t in reserved {
            out.push((kind, t.to_owned(), false, DENY));
        }
        out.push((kind, "#".into(), true, 0));
    }
    out
}

/// The dynsec commands that give addr's new client its own role, sent only once createClient has made the client.
/// They touch nothing but role-<addr> and that client.
fn role_commands(addr: &str) -> Vec<Value> {
    let role = format!("role-{addr}");
    let mut c = vec![json!({"command": "createRole", "rolename": role})];
    for (kind, topic, allow, priority) in acls(addr) {
        c.push(json!({"command": "addRoleACL", "rolename": role, "acltype": kind, "topic": topic, "allow": allow,
                      "priority": priority}));
    }
    c.push(json!({"command": "addClientRole", "username": addr, "rolename": role}));
    c
}

/// Errors dynsec gives for what is already there.
fn exists(error: &str) -> bool {
    error.contains("already exists") || error.contains("already in")
}

/// An address register makes: <name>-<6 hex> (a new agent) or <name>-<8 hex> (a session's own, shared by its relay
/// and MCP server). Provisioning makes no other, so never admin, a service or a name chosen to look like one.
fn provisionable(addr: &str) -> bool {
    addr.rsplit_once('-').is_some_and(|(name, hex)| {
        !name.is_empty() && matches!(hex.len(), 6 | 8) && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    })
}

#[derive(Default)]
struct Response(Mutex<Option<Value>>, Condvar);

impl Handler for Response {
    fn message(&self, _conn: &Conn, msg: Message) {
        if msg.topic.as_deref() == Some(&format!("{CONTROL}/response")) {
            *self.0.lock().unwrap_or_else(|e| e.into_inner()) = serde_json::from_slice(&msg.payload).ok();
            self.1.notify_all();
        }
    }
}

/// Send dynsec commands as admin; their responses, in order.
fn control(admin: &Credentials, commands: Vec<Value>) -> Result<Vec<Value>> {
    let got = Arc::new(Response::default());
    let c: OneShot = broker::one_shot_with("", Some(admin), got.clone())?;
    let pkid = c
        .subscribe(vec![filter(&format!("{CONTROL}/response"), true, false)])
        .map_err(|e| Error(format!("cannot subscribe to the dynsec responses: {}", broker::os_text(&e))))?;
    if c.wait_ack(pkid, broker::TIMEOUT) != Some(true) {
        return Err(Error("admin may not read the dynsec responses: is admin.json the broker's?".into()));
    }
    broker::publish(&c, CONTROL, json!({"commands": commands}).to_string(), false, None)?;
    let deadline = Instant::now() + broker::TIMEOUT;
    let mut slot = got.0.lock().unwrap_or_else(|e| e.into_inner());
    while slot.is_none() {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(Error("the broker did not answer the dynsec commands".into()));
        }
        slot = got.1.wait_timeout(slot, left.min(Duration::from_millis(200))).unwrap_or_else(|e| e.into_inner()).0;
    }
    let responses = slot.take().and_then(|v| v.get("responses").cloned()).unwrap_or_default();
    Ok(responses.as_array().cloned().unwrap_or_default())
}

/// The first error among responses, ignoring those of things that already exist when allow_existing.
fn first_error(responses: &[Value], allow_existing: bool) -> Result<()> {
    for r in responses {
        if let Some(e) = r.get("error").and_then(Value::as_str).filter(|e| !(allow_existing && exists(e))) {
            let command = r.get("command").and_then(Value::as_str).unwrap_or("?");
            return Err(Error(format!("dynsec {command} failed: {e}")));
        }
    }
    Ok(())
}

fn agents_dir() -> PathBuf {
    config::config_dir().join("agents")
}

/// The session addr's credentials file binds it to, if it records one.
pub fn bound_session(addr: &str) -> Option<String> {
    let raw = std::fs::read(agents_dir().join(format!("{addr}.json"))).ok()?;
    let v: Value = serde_json::from_slice(&raw).ok()?;
    v.get("session").and_then(Value::as_str).map(str::to_owned)
}

/// The one rule for picking addr's credentials, which every connection follows:
/// - admin's only when AGENT_BRIDGE_CREDENTIALS names admin, never from admin.json found on disk, never because a
///   caller passed "admin";
/// - an agent's file only when it isn't bound to another session (check_own; that refusal is the error);
/// - otherwise None: anonymous.
pub fn credentials_for(addr: &str) -> Result<Option<Credentials>> {
    if addr == crate::validation::ADMIN {
        return Ok(explicit().filter(|c| c.username == addr));
    }
    let Some(c) = config::credentials(addr)? else { return Ok(None) };
    check_own(addr)?;
    Ok(Some(c))
}

/// The AGENT_BRIDGE_CREDENTIALS file's credentials, whoever they name.
fn explicit() -> Option<Credentials> {
    let path = std::env::var_os("AGENT_BRIDGE_CREDENTIALS").filter(|v| !v.is_empty())?;
    let raw = std::fs::read(&path).ok()?;
    let name = serde_json::from_slice::<Value>(&raw).ok()?.get("username")?.as_str()?.to_owned();
    config::credentials(&name).ok().flatten()
}

/// The credentials a helper connection logs in with: credentials_for(addr) when the caller knows its address; else
/// the AGENT_BRIDGE_CREDENTIALS file (a service's, or admin.json set deliberately by the console); else the agent
/// bound to this process's session (the one its relay holds first, then the newest file); else None, anonymous,
/// which auth enforce refuses.
pub fn helper_credentials(addr: Option<&str>) -> Result<Option<Credentials>> {
    if let Some(c) = addr.map(credentials_for).transpose()?.flatten() {
        return Ok(Some(c));
    }
    Ok(explicit().or_else(session_credentials))
}

fn session_credentials() -> Option<Credentials> {
    let session = host::current().session()?;
    if let Some(addr) = crate::relay::held(&session)
        && bound_session(&addr).is_some_and(|s| s.eq_ignore_ascii_case(&session))
    {
        return config::credentials(&addr).ok().flatten();
    }
    let mut bound: Vec<(std::time::SystemTime, String)> = std::fs::read_dir(agents_dir())
        .ok()?
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_str()?.strip_suffix(".json")?.to_owned();
            if !bound_session(&name).is_some_and(|s| s.eq_ignore_ascii_case(&session)) {
                return None;
            }
            Some((e.metadata().ok()?.modified().ok()?, name))
        })
        .collect();
    bound.sort();
    config::credentials(&bound.pop()?.1).ok().flatten()
}

/// The CLI's own-session rule: inside a session, acting as an agent whose credentials bind it to another session is
/// refused. Agents without a credentials file (auth off, or not provisioned) and calls from outside a session pass.
pub fn check_own(addr: &str) -> Result<()> {
    let (Some(bound), Some(mine)) = (bound_session(addr), host::current().session()) else { return Ok(()) };
    if bound.eq_ignore_ascii_case(&mine) {
        return Ok(());
    }
    Err(Error(format!("agent-bridge: {addr} is not your agent (bound to another session)")))
}

/// addr's credentials, made on first use when the broker runs auth (admin.json exists): Ok(None) under auth off, and
/// for an address register doesn't make (it stays anonymous).
/// session (default this process's) is recorded in the file.
pub fn ensure(addr: &str, session: Option<&str>) -> Result<Option<Credentials>> {
    check_ident(addr, "address")?;
    validation::not_admin(Some(addr), "address")?;
    if let Some(c) = credentials_for(addr)? {
        return Ok(Some(c));
    }
    // Provisioning is the one use of admin.json from disk (AUTH.md decision 5): it only creates addr's own client.
    let Some(admin) = config::credentials(crate::validation::ADMIN)? else { return Ok(None) };
    // anything else stays anonymous: under warn it keeps working, under enforce the broker refuses it, and doctor
    // lists it
    if !provisionable(addr) {
        return Ok(None);
    }
    let password = random_hex(24);
    // create only: an existing client is never reset, so nobody can provision their way into another's address
    let created = control(&admin, vec![json!({"command": "createClient", "username": addr, "password": password})])?;
    if created.iter().any(|r| r.get("error").and_then(Value::as_str).is_some_and(exists)) {
        return Err(Error(format!("{addr} already has credentials elsewhere; ask admin to reissue")));
    }
    first_error(&created, false)?;
    first_error(&control(&admin, role_commands(addr))?, true)?;
    let creds = Credentials { username: addr.into(), password, client_id: format!("c-{}", random_hex(16)) };
    let session = session.map(str::to_owned).or_else(|| host::current().session());
    write(addr, &creds, session.as_deref())?;
    Ok(Some(creds))
}

fn write(addr: &str, c: &Credentials, session: Option<&str>) -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
    let dir = agents_dir();
    std::fs::DirBuilder::new().recursive(true).mode(0o700).create(&dir).map_err(|e| config::data_error(&e, &dir))?;
    let mut rec = Map::new();
    rec.insert("username".into(), c.username.clone().into());
    rec.insert("password".into(), c.password.clone().into());
    rec.insert("client_id".into(), c.client_id.clone().into());
    if let Some(s) = session {
        rec.insert("session".into(), s.into());
    }
    let path = dir.join(format!("{addr}.json"));
    let tmp = dir.join(format!(".{addr}.json.tmp"));
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)
        .map_err(|e| config::data_error(&e, &tmp))?;
    writeln!(f, "{}", Value::Object(rec)).map_err(|e| config::data_error(&e, &tmp))?;
    std::fs::rename(&tmp, &path).map_err(|e| config::data_error(&e, &path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_register_made_addresses_are_provisioned() {
        let session = crate::identity::session_address("0b9d6a3e-5f39-4c43-9d0a-2b1f6c1e9a01");
        for ok in ["al-a1b2c3", "bridge-rust-8e36b2", session.as_str()] {
            assert!(provisionable(ok), "{ok}");
        }
        for no in ["admin", "ledgerd", "historyd", "al-1", "al-A1B2C3", "-a1b2c3", "al-a1b2c3d", "al-a1b2c3d4e5"] {
            assert!(!provisionable(no), "{no}");
        }
    }

    #[test]
    fn an_agents_role_is_literal_topics() {
        let acl = acls("al-1");
        let has = |kind: &str, topic: &str, allow: bool| {
            acl.iter().any(|(k, t, a, _)| *k == kind && t == topic && *a == allow)
        };
        assert!(has("publishClientSend", "room/+/al-1", true) && has("publishClientSend", "inbox/+/al-1", true));
        assert!(has("publishClientSend", "agents/al-1", true) && has("publishClientSend", "agents/#", false));
        assert!(has("publishClientReceive", "inbox/al-1/+", true) && has("publishClientReceive", "inbox/#", false));
        assert!(has("subscribePattern", "ledger/resp/al-1/+", true) && has("subscribePattern", "ledger/#", false));
        assert!(!acl.iter().any(|(_, t, _, _)| t.contains("%u") || t.contains("%c")), "dynsec substitutes neither");
        // the deny rows sit between the agent's own topics and the raw-topic catch-all
        let prio = |kind: &str, topic: &str| acl.iter().find(|(k, t, _, _)| *k == kind && t == topic).map(|r| r.3);
        assert!(prio("publishClientSend", "agents/al-1") > prio("publishClientSend", "agents/#"));
        assert!(prio("publishClientSend", "agents/#") > prio("publishClientSend", "#"));
    }
}
