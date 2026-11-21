//! Follow one chat room live from the shell (agent-bridge watch ROOM): its last messages from historyd, then every new
//! one as it arrives, until Ctrl-C or SIGTERM.
//!
//! Read-only and nobody's agent: a connection of its own (random client id, clean start, reconnecting by itself), so it
//! never takes over an agent's session or its relay and never appears in presence. Rooms are not private, so no
//! membership is needed. It subscribes before asking historyd, so nothing falls in between, and skips a live message
//! that history already showed. Like recv, it leaves out posts the room's settings forbid (muted, banned, announce).
//! It logs in as provision::helper_credentials picks (auth warn or enforce), and a subscription the broker refuses
//! ends it with an error naming the topic, on the first connection or any reconnect, instead of showing nothing.

use crate::config;
use crate::history::client as history;
use crate::json::{Map, dumps_readable};
use crate::ledger::random_hex;
use crate::mqtt::{Acked, Conn, DialError, Handler, Message, Options, TIMEOUT, filter};
use crate::protocol::parse_message;
use crate::rooms::{parse_room, refusal};
use crate::text;
use crate::validation::{Result, check_ident, check_last, os_error};
use rumqttc::v5::mqttbytes::v5::Login;
use serde_json::Value;
use std::collections::{HashSet, VecDeque};
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

const RECONNECT_MAX: Duration = Duration::from_secs(30);
/// Messages kept for printing while the terminal is slow; beyond it the oldest go (a watcher is not a queue).
const QUEUE_MAX: usize = 10_000;

static STOP: AtomicBool = AtomicBool::new(false);

extern "C" fn on_stop(_: libc::c_int) {
    STOP.store(true, Ordering::SeqCst);
}

#[derive(Default)]
struct State {
    /// the room's settings record, for the moderation filter
    rec: Option<Map>,
    queue: VecDeque<Map>,
    subscribed: bool,
    /// the latest connection's SUBSCRIBE, and once its SUBACK arrived, that pkid to check
    pkid: Option<u16>,
    answered: Option<u16>,
}

struct Watcher {
    room: String,
    state: Mutex<State>,
    cv: Condvar,
}

impl Watcher {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Handler for Watcher {
    fn connected(&self, conn: &Conn, _session_present: bool) {
        // The settings record first: retained, it arrives before the SUBACK that marks this connection subscribed.
        let topics = vec![
            filter(&format!("rooms/{}", self.room), false, false),
            filter(&format!("room/{}", self.room), true, false),
            filter(&format!("room/{}/+", self.room), true, false),
        ];
        // Err only when this connection is already gone; the next one subscribes again
        if let Ok(pkid) = conn.subscribe(topics) {
            self.lock().pkid = Some(pkid);
        }
    }

    fn acked(&self, pkid: u16) {
        let mut st = self.lock();
        st.subscribed = true;
        if st.pkid == Some(pkid) {
            st.answered = Some(pkid);
        }
        self.cv.notify_all();
    }

    fn message(&self, _conn: &Conn, m: Message) {
        let Some(topic) = m.topic else { return };
        let mut st = self.lock();
        if topic.starts_with("rooms/") {
            st.rec = parse_room(&m.payload);
        } else if let Some(msg) = parse_message(&topic, &m.payload) {
            let from = msg.get("from").and_then(Value::as_str).unwrap_or("");
            if refusal(st.rec.as_ref(), from).is_some() {
                return;
            }
            if st.queue.len() >= QUEUE_MAX {
                st.queue.pop_front();
            }
            st.queue.push_back(msg);
            self.cv.notify_all();
        }
    }
}

fn key(m: &Map) -> String {
    let get = |k: &str| m.get(k).map(Value::to_string).unwrap_or_default();
    format!("{}\u{0}{}\u{0}{}", get("from"), get("ts"), get("body"))
}

/// Print room's last `last` messages, then each new one, as JSON lines or (text) as text::line, until SIGINT or
/// SIGTERM, which end it normally.
pub fn run(room: &str, last: i64, text: bool) -> Result<()> {
    let room = check_ident(room, "room")?.to_owned();
    check_last(last)?;
    let cfg = config::get()?;
    let watcher = Arc::new(Watcher { room: room.clone(), state: Mutex::new(State::default()), cv: Condvar::new() });
    let opts = Options {
        client_id: format!("watch-{}", random_hex(4)),
        clean_start: true,
        expiry: 0,
        receive_max: None,
        manual_ack: false,
        reconnect: Some(RECONNECT_MAX),
    };
    let creds = crate::provision::helper_credentials(None)?;
    let login = creds.as_ref().map(|c| Login::new(c.username.as_str(), c.password.as_str()));
    let opts = match creds {
        // a logged-in client's id is the broker's to assign (AUTH.md 4.3): its username may be bound to another id
        Some(_) => Options { client_id: String::new(), ..opts },
        None => opts,
    };
    let conn = Conn::open_as(&cfg.host, cfg.port, opts, login, watcher.clone()).map_err(|e| match e {
        DialError::Unreachable(e) => os_error(&e, None),
        DialError::NotAccepted => crate::broker::not_accepted(creds.as_ref()),
    })?;
    let (st, _) =
        watcher.cv.wait_timeout_while(watcher.lock(), TIMEOUT, |st| !st.subscribed).unwrap_or_else(|e| e.into_inner());
    if !st.subscribed {
        conn.close();
        return crate::validation::fail(format!(
            "broker unreachable at {}:{}, start it with: docker compose up -d",
            cfg.host, cfg.port
        ));
    }
    drop(st);
    if let Err(e) = refused(&watcher, &conn, &room) {
        conn.close();
        return Err(e);
    }

    let stop = nix::sys::signal::SigAction::new(
        nix::sys::signal::SigHandler::Handler(on_stop),
        nix::sys::signal::SaFlags::empty(),
        nix::sys::signal::SigSet::empty(),
    );
    STOP.store(false, Ordering::SeqCst);
    // SAFETY: on_stop only stores to an atomic, which is async-signal-safe; the old handlers are put back below.
    let old = unsafe {
        [
            nix::sys::signal::sigaction(nix::sys::signal::Signal::SIGINT, &stop),
            nix::sys::signal::sigaction(nix::sys::signal::Signal::SIGTERM, &stop),
        ]
    };
    let result = follow(&watcher, &conn, &room, last, text);
    for (sig, old) in [nix::sys::signal::Signal::SIGINT, nix::sys::signal::Signal::SIGTERM].into_iter().zip(old) {
        if let Ok(old) = old {
            // SAFETY: restores the handler that was installed before this call.
            #[allow(
                clippy::let_underscore_must_use,
                reason = "restoring the caller's handler is best effort on the way out"
            )]
            let _ = unsafe { nix::sys::signal::sigaction(sig, &old) };
        }
    }
    conn.close();
    result
}

/// An error when the latest SUBACK refused the room's topics (the broker's ACL), or failed them otherwise.
fn refused(watcher: &Watcher, conn: &Conn, room: &str) -> Result<()> {
    let Some(pkid) = watcher.lock().answered.take() else { return Ok(()) };
    match conn.wait_refused(pkid, Duration::ZERO) {
        Some(Acked::Refused) => crate::validation::fail(format!("subscription refused: room/{room}")),
        Some(Acked::No) => crate::validation::fail(format!("subscription failed: room/{room}")),
        _ => Ok(()),
    }
}

fn follow(watcher: &Watcher, conn: &Conn, room: &str, last: i64, text: bool) -> Result<()> {
    let out = std::io::stdout();
    let print = |m: &Map| -> bool {
        let line = if text { text::line(m) } else { dumps_readable(&Value::Object(m.clone())) };
        let mut out = out.lock();
        writeln!(out, "{line}").and_then(|()| out.flush()).is_ok()
    };
    // History is best effort: without historyd, watching starts from now.
    let mut seen = HashSet::new();
    match history::room(room, last, None) {
        Ok(past) => {
            for m in &past {
                seen.insert(key(m));
                if !print(m) {
                    return Ok(());
                }
            }
        }
        Err(e) => eprintln!("agent-bridge: no history ({e}); following #{room} from now"),
    }
    let mut last_seen = Instant::now();
    while !STOP.load(Ordering::SeqCst) {
        refused(watcher, conn, room)?; // a reconnect's subscription can be refused too (the ACL changed)
        let batch: Vec<Map> = {
            let st = watcher.lock();
            let (mut st, _) = watcher
                .cv
                .wait_timeout_while(st, Duration::from_millis(200), |st| st.queue.is_empty())
                .unwrap_or_else(|e| e.into_inner());
            st.queue.drain(..).collect()
        };
        for m in batch {
            // only messages history already printed can repeat, and only right after the start
            if !seen.is_empty() && seen.remove(&key(&m)) {
                continue;
            }
            if !print(&m) {
                return Ok(()); // stdout closed (a pipe ended): nothing left to watch for
            }
        }
        if !seen.is_empty() && last_seen.elapsed() > Duration::from_secs(10) {
            seen.clear();
            last_seen = Instant::now();
        }
    }
    Ok(())
}
