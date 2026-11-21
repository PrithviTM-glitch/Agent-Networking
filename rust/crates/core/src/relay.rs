//! A session relay as others see it: its lock file, <data dir>/relay/<session id>.lock, flocked exclusively by the
//! live relay and holding its pid, then the address it relays for, then (from 0.3.0) its version. The console, the hooks and the CLI read it here;
//! the relay itself is the CLI's `relay run`.

use crate::json::Map;
use crate::validation::{Error, ID_MAX, Result, check_ident, check_session, is_ident, os_error};
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

/// When a room's messages wake the session: "all" (default), "mentions" (only those that mention it, @all included,
/// or come from admin), "admin" (only admin's). The rest still reach the session with its next wake. "admin" is the
/// message's self-asserted sender, fine for deciding when to wake, never for trust.
pub const WAKE_MODES: [&str; 3] = ["all", "mentions", "admin"];

pub fn wake_path(session: &str) -> Result<PathBuf> {
    config::data_path(&["relay", &format!("{}.wake", check_session(Some(session))?.to_lowercase())])
}

/// This session's wake filters, room -> mode, rooms without one waking on everything.
pub fn wake_modes(session: &str) -> Result<Map> {
    let path = wake_path(session)?;
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Map::new()),
        Err(e) => return Err(os_error(&e, Some(&path))),
    };
    let rooms =
        serde_json::from_str::<Value>(&text).ok().and_then(|v| v.get("rooms").and_then(Value::as_object).cloned());
    // only well-formed entries count: a hand-edited file cannot make the relay misbehave
    Ok(rooms
        .unwrap_or_default()
        .into_iter()
        .filter(|(room, mode)| is_ident(room, ID_MAX) && mode.as_str().is_some_and(|m| WAKE_MODES[1..].contains(&m)))
        .collect())
}

/// Set room's wake mode for session ("all" removes the entry) and return the filters now in force. The file is
/// replaced whole (a temp file renamed over it), so the relay never reads half of it.
pub fn set_wake(session: &str, room: &str, mode: &str) -> Result<Map> {
    let room = check_ident(room, "room")?;
    if !WAKE_MODES.contains(&mode) {
        return Err(Error(format!("wake mode: one of {}, got {mode:?}", WAKE_MODES.join(", "))));
    }
    let mut rooms = wake_modes(session)?;
    match mode {
        "all" => drop(rooms.shift_remove(room)),
        _ => drop(rooms.insert(room.to_owned(), mode.into())),
    }
    let path = wake_path(session)?;
    let dir = path.parent().map(PathBuf::from).unwrap_or_default();
    std::fs::create_dir_all(&dir).map_err(|e| config::data_error(&e, &dir))?;
    let tmp = path.with_extension(format!("wake.{}", std::process::id()));
    let body = crate::json::dumps(&serde_json::json!({ "rooms": rooms }));
    std::fs::write(&tmp, body + "\n").and_then(|_| std::fs::rename(&tmp, &path)).map_err(|e| {
        #[allow(clippy::let_underscore_must_use, reason = "a temp file already gone is what we want")]
        let _ = std::fs::remove_file(&tmp);
        config::data_error(&e, &path)
    })?;
    Ok(rooms)
}

/// Whether a message wakes the session under modes: a room message by its room's mode, anything else always.
pub fn wakes(modes: &Map, msg: &Map) -> bool {
    let room = msg.get("topic").and_then(Value::as_str).and_then(|t| t.strip_prefix("room/"));
    let from_admin = msg.get("from").and_then(Value::as_str) == Some(crate::validation::ADMIN);
    let mentioned = msg.get("for_me") == Some(&Value::Bool(true));
    match room.and_then(|r| modes.get(r)).and_then(Value::as_str) {
        Some("mentions") => mentioned || from_admin,
        Some("admin") => from_admin,
        _ => msg.get("for_me") != Some(&Value::Bool(false)),
    }
}

/// The live relay's version, from its lock file; None if it is not live or predates 0.3.0.
pub fn version(session: &str) -> Option<String> {
    let lines = lock_lines(session);
    lines.get(2).map(|v| v.trim().to_owned()).filter(|v| !v.is_empty() && live(session))
}

/// The live relay's pid, or None.
pub fn pid(session: &str) -> Option<i32> {
    lock_lines(session).first().and_then(|l| l.trim().parse().ok()).filter(|p| *p > 0 && live(session))
}

/// When a process started, in epoch seconds: its start ticks after boot (/proc/PID/stat) plus the boot time.
pub fn started(pid: i32) -> Option<f64> {
    let ticks = crate::host::stat(pid.into())?.start as f64;
    let stat = std::fs::read_to_string("/proc/stat").ok()?;
    let boot: f64 = stat.lines().find_map(|l| l.strip_prefix("btime "))?.trim().parse().ok()?;
    // SAFETY: sysconf only reads a configuration value.
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    (hz > 0).then(|| boot + ticks / hz as f64)
}

/// The executable a process runs, as /proc/PID/exe names it: " (deleted)" at the end once the file was replaced.
pub fn binary(pid: i32) -> Option<String> {
    std::fs::read_link(format!("/proc/{pid}/exe")).ok().map(|p| p.to_string_lossy().into_owned())
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn msg(v: Value) -> Map {
        v.as_object().cloned().unwrap_or_default()
    }

    #[test]
    fn wake_modes_decide_per_room() {
        let modes = msg(json!({"mrs": "mentions", "noisy": "admin"}));
        let room = |r: &str, from: &str, for_me: Option<bool>| {
            let mut m = msg(json!({"topic": format!("room/{r}"), "from": from}));
            if let Some(f) = for_me {
                m.insert("for_me".into(), f.into());
            }
            wakes(&modes, &m)
        };
        // no filter: as before, everything but a mention of someone else
        assert!(room("ops", "bob-1", None) && room("ops", "bob-1", Some(true)) && !room("ops", "bob-1", Some(false)));
        assert!(!room("mrs", "bob-1", None), "mentions: a plain post waits");
        assert!(room("mrs", "bob-1", Some(true)) && room("mrs", "admin", None), "a mention or admin wakes");
        assert!(!room("noisy", "bob-1", Some(true)) && room("noisy", "admin", None), "admin: only admin wakes");
        for topic in ["inbox/me-1", "broadcast", "ci/x"] {
            assert!(wakes(&modes, &msg(json!({"topic": topic, "from": "bob-1"}))), "{topic} always wakes");
        }
    }
}
