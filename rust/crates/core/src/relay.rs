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

/// The live agent a relay for session follows, or None: see choose. A live relay's own address counts as current.
pub fn session_agent(session: &str) -> Result<Option<String>> {
    Ok(session_choice(session, held(session).as_deref())?.0)
}

/// session_agent with the relay's current address given, and whether the choice was a guess between equals.
pub fn session_choice(session: &str, current: Option<&str>) -> Result<(Option<String>, bool)> {
    let default = crate::identity::session_address(session);
    Ok(choose(&presence::agents(None)?, session, &default, current))
}

/// Which of the agents registered from session its relay follows. Never a subagent's agent (that one belongs to the
/// subagent). A named agent beats the session's default address, and one holding rooms or topics beats one holding
/// none. current stays chosen while it ties for best, so a relay never flaps between equals; otherwise the lowest
/// address wins, and the bool says it was a guess. Presence order ("seen") never decides: a relay keeps its own
/// record fresh.
pub fn choose(agents: &[Map], session: &str, default: &str, current: Option<&str>) -> (Option<String>, bool) {
    let rank = |a: &Map| {
        let addr = a.get("addr").and_then(Value::as_str).unwrap_or("");
        let named = addr != default;
        let busy = a.get("topics").and_then(Value::as_array).is_some_and(|t| !t.is_empty());
        (named, busy)
    };
    let mine: Vec<&Map> = agents
        .iter()
        .filter(|a| {
            a.get("session").and_then(Value::as_str).is_some_and(|s| s.eq_ignore_ascii_case(session))
                && !a.contains_key("parent")
                && a.get("kind").is_none_or(|k| k != "subagent")
                && a.get("addr").and_then(Value::as_str).is_some_and(|x| is_ident(x, ID_MAX))
        })
        .collect();
    let Some(best) = mine.iter().map(|a| rank(a)).max() else {
        return (None, false);
    };
    let mut top: Vec<&str> =
        mine.iter().filter(|a| rank(a) == best).filter_map(|a| a.get("addr").and_then(Value::as_str)).collect();
    top.sort_unstable();
    top.dedup();
    if let Some(c) = current.filter(|c| top.contains(c)) {
        return (Some(c.to_owned()), false);
    }
    (top.first().map(|a| (*a).to_owned()), top.len() > 1)
}

/// Ask session's live relay to look again at which agent it follows (SIGHUP), after an agent of that session was
/// registered or joined something. Best effort: false when there is no live relay to ask, or it is this process.
/// Only a relay that recorded a version that handles SIGHUP is signalled: its default action ends the process.
pub fn nudge(session: &str) -> bool {
    let Some(p) = pid(session).filter(|p| u32::try_from(*p).ok() != Some(std::process::id())) else {
        return false;
    };
    if !version(session).is_some_and(|v| handles_hup(&v)) {
        return false;
    }
    nix::sys::signal::kill(nix::unistd::Pid::from_raw(p), nix::sys::signal::Signal::SIGHUP).is_ok()
}

/// The first relay version that re-chooses its agent on SIGHUP instead of ending.
const HUP_SINCE: (u64, u64, u64) = (0, 3, 4);

fn handles_hup(version: &str) -> bool {
    let mut parts = version.trim().split(['.', '-', '+']).map(|p| p.parse::<u64>().ok());
    match (parts.next().flatten(), parts.next().flatten(), parts.next().flatten()) {
        (Some(a), Some(b), Some(c)) => (a, b, c) >= HUP_SINCE,
        _ => false,
    }
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

    fn agent(addr: &str, session: &str, topics: &[&str]) -> Map {
        msg(json!({"addr": addr, "session": session, "topics": topics}))
    }

    const S: &str = "6e2a9c41-5b7d-4f08-a3c6-9d1e2f4a7b58";
    const DEFAULT: &str = "proj-6e2a9c41";

    #[test]
    fn a_named_agent_beats_the_default_address_and_rooms_beat_none() {
        let only_default = [agent(DEFAULT, S, &[])];
        assert_eq!(choose(&only_default, S, DEFAULT, None), (Some(DEFAULT.into()), false));
        // the reported bug: the default is current, a named agent registered later must win
        let later = [agent(DEFAULT, S, &[]), agent("named-917fab", S, &[])];
        assert_eq!(choose(&later, S, DEFAULT, Some(DEFAULT)), (Some("named-917fab".into()), false));
        // a default address that joined rooms still loses to a named agent
        let busy_default = [agent(DEFAULT, S, &["room/a"]), agent("named-917fab", S, &[])];
        assert_eq!(choose(&busy_default, S, DEFAULT, Some(DEFAULT)), (Some("named-917fab".into()), false));
        let two = [agent("quiet-aaaaaa", S, &[]), agent("busy-bbbbbb", S, &["room/a"])];
        assert_eq!(choose(&two, S, DEFAULT, Some("quiet-aaaaaa")), (Some("busy-bbbbbb".into()), false));
    }

    #[test]
    fn equals_keep_the_current_choice_and_a_guess_is_flagged() {
        let tie = [agent("zed-aaaaaa", S, &["room/a"]), agent("amy-bbbbbb", S, &["room/b"])];
        assert_eq!(choose(&tie, S, DEFAULT, Some("zed-aaaaaa")), (Some("zed-aaaaaa".into()), false));
        // presence order never decides: either order gives the lowest address, flagged as a guess
        let mut flipped = tie.clone();
        flipped.reverse();
        for agents in [&tie, &flipped] {
            assert_eq!(choose(agents, S, DEFAULT, None), (Some("amy-bbbbbb".into()), true));
        }
    }

    #[test]
    fn other_sessions_and_subagents_never_count() {
        let mut sub = agent("sub-cccccc", S, &["room/a"]);
        sub.insert("kind".into(), "subagent".into());
        let mut child = agent("child-dddddd", S, &["room/a"]);
        child.insert("parent".into(), "named-917fab".into());
        let others = [agent("else-eeeeee", "11111111-2222-4333-8444-555555555555", &["room/a"]), sub, child];
        assert_eq!(choose(&others, S, DEFAULT, None), (None, false));
        let upper = [agent("named-917fab", &S.to_uppercase(), &[])];
        assert_eq!(choose(&upper, S, DEFAULT, None), (Some("named-917fab".into()), false));
    }

    #[test]
    fn only_relays_from_0_3_4_take_a_sighup() {
        for (v, want) in [
            ("0.3.4", true),
            ("0.3.10", true),
            ("0.4.0", true),
            ("1.0.0-rc1", true),
            ("0.3.3", false),
            ("0.2.9", false),
            ("", false),
            ("junk", false),
            ("0.3", false),
        ] {
            assert_eq!(handles_hup(v), want, "{v}");
        }
    }
}
