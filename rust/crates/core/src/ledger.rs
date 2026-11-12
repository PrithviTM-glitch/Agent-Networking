//! Client for ledgerd: call(op, as, fields) returns the reply or a LedgerError, over a connection of its own;
//! call_over does the same over an existing one, such as an agent's Bus.

use crate::config;
use crate::json::{Map, dumps};
use crate::mqtt::{Conn, DialError, Handler, Message, Options};
use crate::request::{self, Replies, Sent};
use serde_json::Value;
use std::fmt;
use std::process::{Command, ExitStatus};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::time::{Duration, Instant};

/// An ok:false reply, a timeout or a malformed reply; reply holds the reply.
#[derive(Debug)]
pub struct LedgerError {
    pub reply: Map,
}

impl LedgerError {
    pub fn new(error: impl Into<String>) -> LedgerError {
        let mut reply = Map::new();
        reply.insert("ok".into(), false.into());
        reply.insert("error".into(), error.into().into());
        LedgerError { reply }
    }
}

impl fmt::Display for LedgerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.reply.get("error") {
            None => f.write_str("ledger error"),
            Some(Value::String(s)) if s == "busy" => f.write_str(&busy_text(&self.reply, crate::json::now())),
            Some(Value::String(s)) => f.write_str(s),
            Some(other) => f.write_str(&dumps(other)),
        }
    }
}

/// "busy, held by HOLDER (LEVEL, N s left), ..." from a busy reply's holders; Python's LedgerError says the same.
fn busy_text(reply: &Map, now: f64) -> String {
    let held: Vec<String> = reply
        .get("holders")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|h| {
            let left = h.get("expires_at").and_then(Value::as_f64).map_or(0.0, |t| (t - now).max(0.0)).ceil();
            let text = |k: &str| h.get(k).and_then(Value::as_str).unwrap_or("?").to_owned();
            format!("{} ({}, {left} s left)", text("holder"), text("level"))
        })
        .collect();
    if held.is_empty() { "busy".into() } else { format!("busy, held by {}", held.join(", ")) }
}

pub fn random_bytes(n: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; n];
    getrandom::fill(&mut bytes).expect("the OS random source is available");
    bytes
}

pub fn random_hex(n: usize) -> String {
    random_bytes(n).iter().map(|b| format!("{b:02x}")).collect()
}

/// Python's "{:g}" for the timeouts shown in messages.
pub fn fmt_g(v: f64) -> String {
    if v == 0.0 || !v.is_finite() {
        return format!("{v}");
    }
    let decimals = (5 - v.abs().log10().floor() as i32).max(0) as usize;
    let s = format!("{v:.decimals$}");
    if s.contains('.') { s.trim_end_matches('0').trim_end_matches('.').to_owned() } else { s }
}

/// The limits ledgerd enforces, exported so clients and tests follow the implementation.
pub const MAX_LEDGERS: i64 = 1000;
pub const MAX_RESOURCES: i64 = 128;
pub const MAX_PENDING: i64 = 64;
/// pending requests on one owner's resources, from everyone; each one DMs the owner
pub const MAX_PENDING_PER_OWNER: i64 = 256;
pub const MAX_LEASES: i64 = 256;
pub const MAX_WAITERS: i64 = 256;
/// stored requests per holder, any status
pub const MAX_REQUESTS: i64 = 512;
pub const MAX_TTL: f64 = 3600.0;
pub const MAX_WAIT: f64 = 300.0;

/// Collects the replies to a connection of its own.
struct Own(Replies);

impl Handler for Own {
    fn message(&self, _conn: &Conn, msg: Message) {
        self.0.fill(msg.correlation.as_ref(), msg.payload);
    }
}

/// How long to wait: timeout, else 10 s plus the request's wait. A bad wait is left for ledgerd to refuse; the
/// timeout itself stays usable.
fn timeout_for(fields: &Map, timeout: Option<f64>) -> f64 {
    let wait =
        fields.get("wait").and_then(Value::as_f64).filter(|w| w.is_finite()).map_or(0.0, |w| w.clamp(0.0, MAX_WAIT));
    let timeout = timeout.filter(|t| *t != 0.0).unwrap_or(10.0 + wait);
    if timeout > 0.0 && timeout <= 3600.0 {
        timeout
    } else if timeout > 3600.0 {
        3600.0
    } else {
        10.0 + wait
    }
}

/// One ledgerd request as as_ over conn, whose reader thread fills replies from reply_topic.
pub fn call_over(
    conn: &Conn,
    replies: &Replies,
    reply_topic: &str,
    op: Value,
    as_: Option<Value>,
    fields: Map,
    timeout: Option<f64>,
) -> Result<Map, LedgerError> {
    let timeout = timeout_for(&fields, timeout);
    let newer = op.as_str().filter(|o| NEWER_OPS.contains(o)).map(str::to_owned);
    let mut req = Map::new();
    req.insert("op".into(), op);
    req.extend(fields);
    if let Some(a) = as_.clone() {
        req.insert("as".into(), a);
    }
    let wait = fields_wait(&req);
    let watch = CANCEL_ON_SIGNALS.load(Ordering::SeqCst) && req.get("op") == Some(&"acquire".into()) && wait > 0.0;
    let _handlers = watch.then(Handlers::install);
    let body = dumps(&Value::Object(req));
    let stop = || SIGNALLED.load(Ordering::SeqCst) != 0;
    let payload = match request::send_until(conn, replies, reply_topic, "ledger", as_.as_ref(), body, timeout, &stop) {
        Sent::Reply(payload) => payload,
        Sent::Timeout if wait >= timeout => {
            return Err(LedgerError::new(format!(
                "no reply from ledgerd within {}s, and this acquire may wait {}s: give a timeout above the wait",
                fmt_g(timeout),
                fmt_g(wait)
            )));
        }
        Sent::Timeout => {
            return Err(LedgerError::new(format!("no reply from ledgerd within {}s, is it running?", fmt_g(timeout))));
        }
        Sent::Stopped(corr) => cancel(conn, replies, reply_topic, as_.as_ref(), &corr),
    };
    match crate::json::loads(&payload) {
        Some(Value::Object(reply)) if reply.get("ok") == Some(&Value::Bool(true)) => Ok(reply),
        Some(Value::Object(reply)) => match newer {
            Some(op) if reply.get("error").and_then(Value::as_str).is_some_and(|e| e.starts_with("op: one of")) => {
                Err(LedgerError::new(format!("ledgerd is older than 0.3.0 and has no {op} op")))
            }
            _ => Err(LedgerError { reply }),
        },
        _ => Err(LedgerError::new("malformed reply")),
    }
}

/// Ops a ledgerd before 0.3.0 does not know: its refusal is turned into one that says so.
const NEWER_OPS: [&str; 3] = ["leases", "lease_history", "ping"];
/// The exit status of acquire --exec when it never got the lease or lost it while the command ran (EX_TEMPFAIL).
pub const EXEC_NO_LEASE: i32 = 75;
/// How long a command that lost its lease gets between SIGTERM and SIGKILL.
const EXEC_GRACE: Duration = Duration::from_secs(10);
pub const MAX_REQUEST_WAIT: f64 = 3600.0;

fn fields_wait(req: &Map) -> f64 {
    req.get("wait").and_then(Value::as_f64).filter(|w| w.is_finite()).unwrap_or(0.0)
}

static CANCEL_ON_SIGNALS: AtomicBool = AtomicBool::new(false);
static SIGNALLED: AtomicI32 = AtomicI32::new(0);

/// For a command-line client: a waiting acquire interrupted by SIGINT or SIGTERM cancels its place in the queue
/// (and gives back a lease granted just before), then the process dies of that signal as it would have.
pub fn cancel_on_signals() {
    CANCEL_ON_SIGNALS.store(true, Ordering::SeqCst);
}

extern "C" fn on_signal(signo: libc::c_int) {
    SIGNALLED.store(signo, Ordering::SeqCst);
}

/// SIGINT and SIGTERM noted in SIGNALLED while a waiting acquire runs; the previous handlers come back on drop.
struct Handlers([(libc::c_int, libc::sigaction); 2]);

impl Handlers {
    fn install() -> Handlers {
        let previous = [libc::SIGINT, libc::SIGTERM].map(|signo| unsafe {
            let mut action: libc::sigaction = std::mem::zeroed();
            action.sa_sigaction = on_signal as extern "C" fn(libc::c_int) as libc::sighandler_t;
            let mut old: libc::sigaction = std::mem::zeroed();
            libc::sigaction(signo, &action, &mut old);
            (signo, old)
        });
        Handlers(previous)
    }
}

impl Drop for Handlers {
    fn drop(&mut self) {
        for (signo, old) in &self.0 {
            unsafe { libc::sigaction(*signo, old, std::ptr::null_mut()) };
        }
    }
}

/// Withdraw the interrupted acquire sent with corr, give back a lease ledgerd granted it before the cancel arrived
/// (its reply comes first, on the same topic), then die of the signal. A ledgerd that predates cancel refuses it
/// as an unknown op, which changes nothing here.
fn cancel(conn: &Conn, replies: &Replies, reply_topic: &str, as_: Option<&Value>, corr: &[u8]) -> ! {
    let hex: String = corr.iter().map(|b| format!("{b:02x}")).collect();
    let body = dumps(&serde_json::json!({"op": "cancel", "as": as_, "correlation": hex}));
    let _ = request::send(conn, replies, reply_topic, "ledger", as_, body, 3.0);
    let granted = replies.take(corr).and_then(|p| crate::json::loads(&p));
    if let Some(lease_id) = granted.as_ref().and_then(|r| r.pointer("/lease/lease_id")).and_then(Value::as_str) {
        let body = dumps(&serde_json::json!({"op": "release", "as": as_, "lease_id": lease_id}));
        let _ = request::send(conn, replies, reply_topic, "ledger", as_, body, 3.0);
    }
    let signo = SIGNALLED.load(Ordering::SeqCst);
    unsafe {
        libc::signal(signo, libc::SIG_DFL);
        libc::raise(signo);
    }
    std::process::exit(128 + signo)
}

/// One ledgerd request as as_, over a connection of its own.
pub fn call(op: &str, as_: &str, fields: Map, timeout: Option<f64>) -> Result<Map, LedgerError> {
    call_value(op.into(), Some(as_.into()), fields, timeout)
}

/// call with op and as_ as the caller gave them; ledgerd refuses what is wrong with them, as it does for any client.
pub fn call_value(op: Value, as_: Option<Value>, fields: Map, timeout: Option<f64>) -> Result<Map, LedgerError> {
    let cfg = config::get().map_err(|e| LedgerError::new(e.0))?;
    let own = Arc::new(Own(Replies::default()));
    let opts = Options {
        client_id: String::new(),
        clean_start: true,
        expiry: 0,
        receive_max: None,
        manual_ack: false,
        reconnect: None,
    };
    let c = Conn::open(&cfg.host, cfg.port, opts, own.clone()).map_err(|e| match e {
        DialError::Unreachable(e) => LedgerError::new(format!(
            "broker unreachable at {}:{} ({})",
            cfg.host,
            cfg.port,
            crate::broker::os_text(&e)
        )),
        DialError::NotAccepted => LedgerError::new("broker did not accept the connection"),
    })?;
    let reply = call_over(&c, &own.0, &request::reply_topic("ledger", ""), op, as_, fields, timeout);
    c.close();
    reply
}

fn map(pairs: &[(&str, Value)]) -> Map {
    pairs.iter().map(|(k, v)| ((*k).to_owned(), v.clone())).collect()
}

fn text_of<'a>(m: &'a Value, pointer: &str) -> &'a str {
    m.pointer(pointer).and_then(Value::as_str).unwrap_or_default()
}

/// request, then wait up to wait seconds for the owner's answer by reading grants once a second. Ok is the granted
/// request (without "ok"); denied and still pending are errors, and a pending request stays pending.
pub fn request_wait(addr: &str, resource: &str, level: &str, wait: f64) -> Result<Map, LedgerError> {
    if !(0.0..=MAX_REQUEST_WAIT).contains(&wait) {
        return Err(LedgerError::new(format!("wait: number between 0 and {}", fmt_g(MAX_REQUEST_WAIT))));
    }
    call("init", addr, Map::new(), None)?;
    let fields = map(&[("resource", resource.into()), ("level", level.into())]);
    let mut row = Value::Object(call("request", addr, fields, None)?).get("request").cloned().unwrap_or_default();
    let id = text_of(&row, "/id").to_owned();
    let deadline = Instant::now() + Duration::from_secs_f64(wait);
    loop {
        match text_of(&row, "/status") {
            "granted" => return Ok(map(&[("request", row)])),
            "denied" => {
                let owner = text_of(&row, "/owner");
                return Err(LedgerError::new(format!(
                    "{owner} denied your {level} request for {resource} (request_id {id})"
                )));
            }
            _ => {}
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(LedgerError::new(format!("still pending after {} s (request_id {id})", fmt_g(wait))));
        }
        std::thread::sleep(left.min(Duration::from_secs(1)));
        let grants = Value::Object(call("grants", addr, Map::new(), None)?);
        let held = grants.get("held").and_then(Value::as_array).cloned().unwrap_or_default();
        row = held.into_iter().find(|r| text_of(r, "/id") == id).ok_or_else(|| {
            LedgerError::new(format!("request {id} is gone: the resource was removed or its owner left"))
        })?;
    }
}

static FORWARD: AtomicI32 = AtomicI32::new(0);

/// A signal sent to this process with kill(2) is passed on to the command; one the terminal sent to the whole
/// foreground process group (si_code > 0) already reached it, so it is only noted.
extern "C" fn on_forward(signo: libc::c_int, info: *mut libc::siginfo_t, _: *mut libc::c_void) {
    let sent = !info.is_null() && unsafe { (*info).si_code } <= 0;
    FORWARD.store(if sent { signo } else { -signo }, Ordering::SeqCst);
}

fn forward_signals() {
    for signo in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP] {
        unsafe {
            let mut action: libc::sigaction = std::mem::zeroed();
            action.sa_sigaction =
                on_forward as extern "C" fn(libc::c_int, *mut libc::siginfo_t, *mut libc::c_void) as libc::sighandler_t;
            action.sa_flags = libc::SA_SIGINFO;
            libc::sigaction(signo, &action, std::ptr::null_mut());
        }
    }
}

fn status_code(status: ExitStatus) -> i32 {
    use std::os::unix::process::ExitStatusExt;
    status.code().or_else(|| status.signal().map(|n| 128 + n)).unwrap_or(1)
}

/// acquire --exec: take the lease, run cmd with it (AGENT_BRIDGE_FENCE_TOKEN, AGENT_BRIDGE_LEASE_ID, _RESOURCE and
/// _LEVEL in its environment), renew it every ttl/3 while cmd runs, and release it when cmd exits. Returns cmd's
/// exit status (128+n if signal n killed it), 127 if cmd could not be started, or EXEC_NO_LEASE (75) if the lease
/// was refused or lost; losing it sends cmd SIGTERM, then SIGKILL after EXEC_GRACE. Messages go to stderr.
pub fn exec(addr: &str, resource: &str, level: &str, ttl: Option<f64>, wait: Option<f64>, cmd: &[String]) -> i32 {
    let fail = |text: String| {
        eprintln!("ledger: {}", text.split_whitespace().collect::<Vec<_>>().join(" "));
        EXEC_NO_LEASE
    };
    let Some((program, args)) = cmd.split_first() else { return fail("--exec needs a command".into()) };
    let ttl = ttl.unwrap_or(60.0);
    let mut fields = map(&[("resource", resource.into()), ("level", level.into()), ("ttl", ttl.into())]);
    if let Some(w) = wait {
        fields.insert("wait".into(), w.into());
    }
    // before the acquire, so a signal between the lease and the spawn is still seen
    forward_signals();
    let lease = match call("init", addr, Map::new(), None).and_then(|_| call("acquire", addr, fields, None)) {
        Ok(reply) => reply.get("lease").cloned().unwrap_or_default(),
        Err(e) => return fail(format!("{e}; {program} was not run")),
    };
    let lease_id = text_of(&lease, "/lease_id").to_owned();
    let release = || {
        let _ = call("release", addr, map(&[("lease_id", lease_id.clone().into())]), Some(5.0));
    };
    let got = FORWARD.load(Ordering::SeqCst);
    if got != 0 {
        release();
        return 128 + got.abs();
    }
    let mut command = Command::new(program);
    command.args(args).env("AGENT_BRIDGE_LEASE_ID", &lease_id).env("AGENT_BRIDGE_LEASE_RESOURCE", resource);
    command.env("AGENT_BRIDGE_LEASE_LEVEL", level).env_remove("AGENT_BRIDGE_FENCE_TOKEN");
    if let Some(token) = lease.get("token").and_then(Value::as_i64) {
        command.env("AGENT_BRIDGE_FENCE_TOKEN", token.to_string());
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(e) => {
            release();
            eprintln!("ledger: cannot run {program}: {e}");
            return 127;
        }
    };
    let pid = child.id() as libc::pid_t;
    let started = Instant::now();
    let mut expires = started + Duration::from_secs_f64(ttl);
    let mut renew_at = started + Duration::from_secs_f64(ttl / 3.0);
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                release();
                return status_code(status);
            }
            Ok(None) => {}
            Err(_) => break,
        }
        let got = FORWARD.swap(0, Ordering::SeqCst);
        if got > 0 {
            unsafe { libc::kill(pid, got) };
        }
        let now = Instant::now();
        if now >= renew_at {
            let left = expires.saturating_duration_since(now).as_secs_f64();
            let fields = map(&[("lease_id", lease_id.clone().into()), ("ttl", ttl.into())]);
            match call("renew", addr, fields, Some(left.clamp(1.0, 5.0))) {
                Ok(_) => {
                    let now = Instant::now();
                    expires = now + Duration::from_secs_f64(ttl);
                    renew_at = now + Duration::from_secs_f64(ttl / 3.0);
                }
                // gone, someone else's now, or not renewable before it runs out: the lock no longer protects cmd
                Err(e) if e.to_string().starts_with("no such lease") || e.to_string().starts_with("lease ") => {
                    return lost(&mut child, pid, &format!("{e}"), resource, program);
                }
                Err(e) if left <= 2.0 => return lost(&mut child, pid, &e.to_string(), resource, program),
                Err(_) => renew_at = now + Duration::from_secs(1),
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    release();
    EXEC_NO_LEASE
}

/// The lease is gone while cmd still runs: SIGTERM, then SIGKILL after EXEC_GRACE.
fn lost(child: &mut std::process::Child, pid: libc::pid_t, why: &str, resource: &str, program: &str) -> i32 {
    eprintln!("ledger: lost the lease on {resource} ({why}); stopping {program}");
    unsafe { libc::kill(pid, libc::SIGTERM) };
    let deadline = Instant::now() + EXEC_GRACE;
    while Instant::now() < deadline {
        if let Ok(Some(_)) = child.try_wait() {
            return EXEC_NO_LEASE;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let _ = child.kill();
    let _ = child.wait();
    EXEC_NO_LEASE
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    #[test]
    fn busy_names_the_holders() {
        let reply = |v: serde_json::Value| v.as_object().cloned().unwrap_or_default();
        let two = reply(json!({"ok": false, "error": "busy", "holders": [
            {"holder": "a-1", "level": "read", "expires_at": 100.2}, {"holder": "b-2", "level": "read", "expires_at": 130.0}]}));
        assert_eq!(super::busy_text(&two, 90.0), "busy, held by a-1 (read, 11 s left), b-2 (read, 40 s left)");
        assert_eq!(super::busy_text(&reply(json!({"ok": false, "error": "busy"})), 90.0), "busy");
    }

    #[test]
    fn g_format() {
        for (v, s) in
            [(10.0, "10"), (13.0, "13"), (10.5, "10.5"), (3.0, "3"), (10.123456789, "10.1235"), (0.25, "0.25")]
        {
            assert_eq!(super::fmt_g(v), s);
        }
    }
}
