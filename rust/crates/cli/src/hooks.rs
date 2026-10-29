//! Claude Code hooks, `agent-bridge hook broadcast | relay-start`. Both print nothing on failure and always exit 0, so
//! they can never block a session.
//!
//! broadcast (UserPromptSubmit, SessionStart): shows this session the admin console's broadcasts it has not seen. It
//! asks historyd for them in one request with a 1 s timeout (a few ms while historyd is up, at once if the broker is
//! down) and shows nothing if that fails. It skips sessions with a live relay, which delivers broadcasts itself. State:
//! <data dir>/hook-state/<session id>, the timestamp of the newest broadcast already shown to that session; a session
//! the hook has never seen gets the last FIRST_S seconds.
//!
//! relay-start (SessionStart): starts this session's relay in the background and returns at once. SessionStart output
//! becomes context, so it prints nothing.

use crate::relay;
use agent_bridge::history::client::{BROADCASTS_TIMEOUT, call};
use agent_bridge::json::{Map, dumps, float, float_repr, now};
use agent_bridge::stamp::local;
use agent_bridge::validation::is_uuid;
use agent_bridge::{config, relay as relay_lock};
use serde_json::{Value, json};
use std::io::{Read, Write};
use std::os::unix::fs::DirBuilderExt;

const FIRST_S: f64 = 3600.0;
const SHOWN_MAX: usize = 20;
const EVENTS: [&str; 2] = ["UserPromptSubmit", "SessionStart"];

pub fn broadcast() {
    if let Some(text) = show_broadcasts() {
        let mut out = std::io::stdout().lock();
        let _ = writeln!(out, "{text}").and_then(|_| out.flush());
    }
}

/// The hook's output, or None when there is nothing to show or anything failed.
fn show_broadcasts() -> Option<String> {
    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input).ok()?;
    let hook: Value = serde_json::from_str(&input).ok()?;
    let session = hook.get("session_id")?.as_str().filter(|s| is_uuid(s))?.to_lowercase();
    let event = hook.get("hook_event_name")?.as_str().filter(|e| EVENTS.contains(e))?;
    if relay_lock::live(&session) {
        return None;
    }
    let state = config::data_path(&["hook-state", &session]).ok()?;
    let since = match std::fs::read_to_string(&state).map(|s| s.trim().parse::<f64>()) {
        Ok(Ok(since)) => since,
        _ => now() - FIRST_S,
    };
    // Python sends a NaN or infinity as such, which historyd refuses: nothing is shown.
    if !since.is_finite() {
        return None;
    }
    let mut fields = Map::new();
    fields.insert("since".into(), float(since));
    fields.insert("last".into(), SHOWN_MAX.into());
    let reply = call("broadcasts", None, fields, BROADCASTS_TIMEOUT).ok()?;
    let new: Vec<&Value> = reply.get("messages")?.as_array()?.iter().collect();
    if new.is_empty() {
        return None;
    }
    // Python's max(e["ts"]) raises on an entry without a numeric ts, so such a reply shows and records nothing.
    let stamps: Vec<f64> = new.iter().map(|e| e.get("ts")?.as_f64()).collect::<Option<_>>()?;
    let newest = stamps.iter().copied().fold(f64::MIN, f64::max);
    let dir = state.parent()?;
    std::fs::DirBuilder::new().recursive(true).mode(0o700).create(dir).ok()?;
    let tmp = dir.join(format!("{session}.tmp"));
    std::fs::write(&tmp, float_repr(newest)).ok()?;
    std::fs::rename(&tmp, &state).ok()?;
    let start = new.len().saturating_sub(SHOWN_MAX);
    let lines: Vec<String> = new[start..]
        .iter()
        .zip(&stamps[start..])
        .map(|(e, ts)| format!("[{}] {}", local(*ts as i64, ' '), e["body"].as_str().unwrap_or("")))
        .collect();
    let text = format!(
        "Broadcast from the user's agent-bridge admin console (the user's own admin page for the local agent bus, \
         sent to every Claude session on this machine). {} new since this session last checked:\n{}",
        new.len(),
        lines.join("\n")
    );
    Some(dumps(&json!({"hookSpecificOutput": {"hookEventName": event, "additionalContext": text}})))
}

pub fn relay_start() {
    if let Ok(devnull) = nix::fcntl::open("/dev/null", nix::fcntl::OFlag::O_RDWR, nix::sys::stat::Mode::empty()) {
        for fd in [0, 1, 2] {
            // SAFETY: dup2 onto the standard descriptors, which this process owns, before anything uses them.
            unsafe {
                libc::dup2(std::os::fd::AsRawFd::as_raw_fd(&devnull), fd);
            }
        }
    }
    relay::start();
}
