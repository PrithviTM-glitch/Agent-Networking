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
    let newer = op.as_str().and_then(|o| NEWER_OPS.iter().find(|(name, _)| *name == o)).copied();
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
        Some(Value::Object(reply)) => {
            let error = reply.get("error").and_then(Value::as_str).unwrap_or_default();
            match newer {
                Some((op, since)) if error.starts_with("op: one of") => {
                    Err(LedgerError::new(format!("ledgerd is older than {since} and has no {op} op")))
                }
                _ => match newer_field(error) {
                    Some(text) => Err(LedgerError::new(text)),
                    None => Err(LedgerError { reply }),
                },
            }
        }
        _ => Err(LedgerError::new("malformed reply")),
    }
}

/// Ops an older ledgerd does not know, with the version that added them: its refusal is turned into one that says so.
const NEWER_OPS: [(&str, &str); 8] = [
    ("leases", "0.3.0"),
    ("lease_history", "0.3.0"),
    ("ping", "0.3.0"),
    ("claim", "0.3.1"),
    ("unclaim", "0.3.1"),
    ("claims", "0.3.1"),
    ("holders", "0.3.1"),
    ("worktrees", "0.3.2"),
];

/// An older ledgerd's "unknown fields for OP: ..." naming a field added later, said as what the caller lacks.
fn newer_field(error: &str) -> Option<String> {
    let fields = error.strip_prefix("unknown fields for ")?.split_once(": ")?.1;
    let has = |f: &str| fields.split(", ").any(|x| x == f);
    if has("path") || has("git_common_dir") {
        Some("ledgerd is older than 0.3.2 and has no worktree locks".into())
    } else if has("on_behalf") {
        Some("ledgerd is older than 0.3.2 and has no on_behalf".into())
    } else {
        None
    }
}
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
/// When provision::credentials_for gives as_ credentials (made by register under auth; admin's only when
/// AGENT_BRIDGE_CREDENTIALS names them), the call logs in as it, as call_with does, so it keeps working under enforce;
/// without them it is anonymous, as before 0.3.2.
pub fn call_value(op: Value, as_: Option<Value>, fields: Map, timeout: Option<f64>) -> Result<Map, LedgerError> {
    let addr =
        as_.as_ref().and_then(Value::as_str).filter(|a| crate::validation::is_ident(a, crate::validation::ID_MAX));
    let creds = match addr {
        Some(addr) => crate::provision::credentials_for(addr).map_err(|e| LedgerError::new(e.to_string()))?,
        None => None,
    };
    call_login(op, as_, fields, timeout, creds.as_ref())
}

/// call, logged in with creds when given: the broker then vouches for as_ (its username), and the reply comes on
/// ledger/resp/<as_>/<random>, which ledgerd 0.3.2 requires under auth enforce. With None it is anonymous, even when
/// as_ has a credentials file (call looks that up itself).
pub fn call_with(
    op: &str,
    as_: &str,
    fields: Map,
    timeout: Option<f64>,
    creds: Option<&config::Credentials>,
) -> Result<Map, LedgerError> {
    call_login(op.into(), Some(as_.into()), fields, timeout, creds)
}

fn call_login(
    op: Value,
    as_: Option<Value>,
    fields: Map,
    timeout: Option<f64>,
    creds: Option<&config::Credentials>,
) -> Result<Map, LedgerError> {
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
    let login = creds.map(|c| rumqttc::v5::mqttbytes::v5::Login::new(c.username.clone(), c.password.clone()));
    let c = Conn::open_as(&cfg.host, cfg.port, opts, login, own.clone()).map_err(|e| match e {
        DialError::Unreachable(e) => LedgerError::new(format!(
            "broker unreachable at {}:{} ({})",
            cfg.host,
            cfg.port,
            crate::broker::os_text(&e)
        )),
        DialError::NotAccepted => LedgerError::new(crate::broker::not_accepted(creds).to_string()),
    })?;
    let topic = match (creds, as_.as_ref().and_then(Value::as_str)) {
        (Some(_), Some(addr)) => format!("ledger/resp/{addr}/{}", random_hex(8)),
        _ => request::reply_topic("ledger", ""),
    };
    let reply = call_over(&c, &own.0, &topic, op, as_, fields, timeout);
    c.close();
    reply
}

fn map(pairs: &[(&str, Value)]) -> Map {
    pairs.iter().map(|(k, v)| ((*k).to_owned(), v.clone())).collect()
}

fn text_of<'a>(m: &'a Value, pointer: &str) -> &'a str {
    m.pointer(pointer).and_then(Value::as_str).unwrap_or_default()
}

/// The ledger resource of the worktree at realpath: admin/wt-<first 16 hex of its sha256>. The one definition, used by
/// ledgerd (which checks it), the worktree commands, the guard and the console.
pub fn worktree_resource(realpath: &str) -> String {
    use sha2::{Digest, Sha256};
    let hex: String = Sha256::digest(realpath.as_bytes()).iter().take(8).map(|b| format!("{b:02x}")).collect();
    format!("{}/wt-{hex}", crate::validation::ADMIN)
}

/// The acquire fields that make a worktree's resource on demand and file the caller's request (ledgerd 0.3.2).
pub fn worktree_fields(realpath: &str, common_dir: Option<&str>) -> Map {
    let mut fields = map(&[("path", realpath.into())]);
    if let Some(dir) = common_dir {
        fields.insert("git_common_dir".into(), dir.into());
    }
    fields
}

/// A write lease on the worktree at realpath for addr: init, then acquire with the worktree's fields. The reply
/// carries "created" when this call made the resource.
pub fn worktree_acquire(
    addr: &str,
    realpath: &str,
    common_dir: Option<&str>,
    ttl: Option<f64>,
    wait: Option<f64>,
) -> Result<Map, LedgerError> {
    call("init", addr, Map::new(), None)?;
    let mut fields = map(&[("resource", worktree_resource(realpath).into()), ("level", "write".into())]);
    fields.extend(worktree_fields(realpath, common_dir));
    if let Some(t) = ttl {
        fields.insert("ttl".into(), t.into());
    }
    if let Some(w) = wait {
        fields.insert("wait".into(), w.into());
    }
    call("acquire", addr, fields, None)
}

/// A lease on a worktree, from ledgerd's holders.
#[derive(Clone, Debug, PartialEq)]
pub struct Holder {
    pub holder: String,
    pub level: String,
    /// empty from a ledgerd older than 0.3.2
    pub lease_id: String,
    pub expires_at: f64,
}

/// Who holds the worktree at realpath, asked as addr within timeout seconds: Ok(None) when it is not registered (no
/// lock to enforce), Ok(Some(holders)) otherwise (empty when free). Err when ledgerd can't say: unreachable, too old
/// or broken, so a caller enforcing locks fails closed.
pub fn worktree_holders(addr: &str, realpath: &str, timeout: f64) -> Result<Option<Vec<Holder>>, LedgerError> {
    let resource = worktree_resource(realpath);
    let reply = match call("holders", addr, map(&[("resource", resource.clone().into())]), Some(timeout)) {
        Ok(reply) => reply,
        Err(e) if e.reply.get("error").and_then(Value::as_str) == Some(&format!("no such resource: {resource}")) => {
            return Ok(None);
        }
        Err(e) => return Err(e),
    };
    let holders = reply.get("holders").and_then(Value::as_array).ok_or_else(|| LedgerError::new("malformed reply"))?;
    Ok(Some(
        holders
            .iter()
            .map(|h| Holder {
                holder: text_of(h, "/holder").to_owned(),
                level: text_of(h, "/level").to_owned(),
                lease_id: text_of(h, "/lease_id").to_owned(),
                expires_at: h.get("expires_at").and_then(Value::as_f64).unwrap_or(0.0),
            })
            .collect(),
    ))
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
    exec_with(addr, resource, level, ttl, wait, Map::new(), cmd)
}

/// exec with more acquire fields, such as worktree_fields for a worktree's lease.
pub fn exec_with(
    addr: &str,
    resource: &str,
    level: &str,
    ttl: Option<f64>,
    wait: Option<f64>,
    extra: Map,
    cmd: &[String],
) -> i32 {
    let fail = |text: String| {
        eprintln!("ledger: {}", text.split_whitespace().collect::<Vec<_>>().join(" "));
        EXEC_NO_LEASE
    };
    let Some(program) = cmd.first() else { return fail("--exec needs a command".into()) };
    let ttl = ttl.unwrap_or(60.0);
    let mut fields = map(&[("resource", resource.into()), ("level", level.into()), ("ttl", ttl.into())]);
    if let Some(w) = wait {
        fields.insert("wait".into(), w.into());
    }
    fields.extend(extra);
    // before the acquire, so a signal between the lease and the spawn is still seen
    exec_signals();
    let lease = match call("init", addr, Map::new(), None).and_then(|_| call("acquire", addr, fields, None)) {
        Ok(reply) => reply.get("lease").and_then(Value::as_object).cloned().unwrap_or_default(),
        Err(e) => return fail(format!("{e}; {program} was not run")),
    };
    exec_held(addr, &lease, ttl, resource, level, cmd)
}

/// For a caller that takes the lease itself and then calls exec_held: from here on SIGINT, SIGTERM and SIGHUP are
/// noted instead of killing this process, so one that arrives between the lease and the command still releases it.
pub fn exec_signals() {
    forward_signals();
}

/// exec's second half, for a lease the caller already holds (lease is acquire's "lease" object, taken with ttl): run
/// cmd with it, renew it every ttl/3, pass signals on, release it when cmd exits, and return as exec does. Call
/// exec_signals before taking the lease.
pub fn exec_held(addr: &str, lease: &Map, ttl: f64, resource: &str, level: &str, cmd: &[String]) -> i32 {
    let Some((program, args)) = cmd.split_first() else {
        eprintln!("ledger: --exec needs a command");
        return EXEC_NO_LEASE;
    };
    forward_signals();
    let lease = Value::Object(lease.clone());
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
    fn worktree_ids_and_old_ledgerd_texts() {
        // printf %s /home/u/wt | sha256sum: pinned so every user of the id agrees
        let id = super::worktree_resource("/home/u/wt");
        assert_eq!(id, "admin/wt-8bb602a4c5b8e62f");
        assert_ne!(id, super::worktree_resource("/home/u/wt/"));
        assert_eq!(
            super::newer_field("unknown fields for acquire: path, git_common_dir").as_deref(),
            Some("ledgerd is older than 0.3.2 and has no worktree locks")
        );
        assert_eq!(
            super::newer_field("unknown fields for release: on_behalf").as_deref(),
            Some("ledgerd is older than 0.3.2 and has no on_behalf")
        );
        assert_eq!(super::newer_field("unknown fields for add: zz"), None);
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
