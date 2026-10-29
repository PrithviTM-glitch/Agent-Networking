//! A session relay as others see it: its lock file, <data dir>/relay/<session id>.lock, flocked exclusively by the
//! live relay and holding its pid, then the address it relays for. The console, the hooks and the CLI read it here;
//! the relay itself is the CLI's `relay run`.

use crate::validation::{ID_MAX, Result, check_session, is_ident, os_error};
use crate::{config, presence};
use serde_json::Value;
use std::fs::{File, TryLockError};
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// quiet room messages kept for the next post that wakes the session
pub const BACKLOG: usize = 50;

pub fn lock_path(session: &str) -> Result<PathBuf> {
    config::data_path(&["relay", &format!("{}.lock", check_session(Some(session))?.to_lowercase())])
}

/// True while a relay holds session's lock.
pub fn live(session: &str) -> bool {
    let Ok(f) = lock_path(session).and_then(|p| File::open(p).map_err(|e| os_error(&e, None))) else {
        return false;
    };
    matches!(f.try_lock_shared(), Err(TryLockError::WouldBlock))
}

/// The lock file's lines: the relay's pid, then the address it holds once registered.
fn lock_lines(session: &str) -> Vec<String> {
    let text = lock_path(session).ok().and_then(|p| std::fs::read_to_string(p).ok()).unwrap_or_default();
    text.split('\n').map(str::to_owned).collect()
}

/// The address the live relay of session holds, or None.
pub fn held(session: &str) -> Option<String> {
    let lines = lock_lines(session);
    lines.get(1).filter(|a| is_ident(a, ID_MAX) && live(session)).cloned()
}

/// SIGTERM session's relay and wait for its lock to free; false if it outlived timeout.
pub fn stop(session: &str, timeout: Duration) -> Result<bool> {
    let first = lock_lines(session).into_iter().next().unwrap_or_default();
    let first = first.trim();
    let Ok(pid) = (if first.is_empty() { Ok(0) } else { first.parse::<i32>() }) else {
        return Ok(true);
    };
    let deadline = Instant::now() + timeout;
    if pid > 0 && live(session) {
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), nix::sys::signal::Signal::SIGTERM)
            .map_err(|e| os_error(&e.into(), None))?;
    }
    while live(session) {
        if Instant::now() > deadline {
            return Ok(false);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(true)
}

/// The most recently seen live agent registered from this session (e.g. through the CLI), or None. Never a
/// subagent's agent: that one belongs to the subagent, not to the session.
pub fn session_agent(session: &str) -> Result<Option<String>> {
    Ok(presence::agents(None)?
        .into_iter()
        .find(|a| {
            a.get("session").and_then(Value::as_str).unwrap_or("").to_lowercase() == session.to_lowercase()
                && !a.contains_key("parent")
                && a.get("kind").is_none_or(|k| k != "subagent")
        })
        .and_then(|a| a["addr"].as_str().map(str::to_owned)))
}
