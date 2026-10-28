//! Who an agent is: the Claude Code session and process it runs under, and the addresses derived from them.

use crate::json::Map;
use crate::validation::{ADMIN, is_uuid};
use sha2::{Digest, Sha256};
use std::sync::OnceLock;

const ANCESTRY_MAX: usize = 64;
static ANCESTOR: OnceLock<i64> = OnceLock::new();
static ORIGIN: OnceLock<Map> = OnceLock::new();

pub struct Proc {
    pub comm: String,
    pub ppid: i64,
    pub start: u64,
}

/// comm, ppid and start time in clock ticks of a live process, or None.
pub fn read_proc(pid: i64) -> Option<Proc> {
    let stat = String::from_utf8(std::fs::read(format!("/proc/{pid}/stat")).ok()?).ok()?;
    // comm is parenthesised and may itself hold spaces or parens: only the last ")" ends it.
    let (open, close) = (stat.find('(')?, stat.rfind(')')?);
    let fields: Vec<&str> = stat.get(close + 2..)?.split_whitespace().collect();
    Some(Proc {
        comm: stat.get(open + 1..close)?.to_owned(),
        ppid: fields.get(1)?.parse().ok()?,
        start: fields.get(19)?.parse().ok()?,
    })
}

/// Where origin() starts walking up instead of this process's parent; the relay sets it, being detached from its
/// claude process. Only the first call counts, and only before origin() first runs.
pub fn set_ancestor(pid: i64) {
    let _ = ANCESTOR.set(pid);
}

/// The Claude Code session and process this agent runs under, published in presence for the admin console.
pub fn origin() -> &'static Map {
    ORIGIN.get_or_init(|| {
        let mut out = Map::new();
        if let Ok(sid) = std::env::var("CLAUDE_CODE_SESSION_ID")
            && is_uuid(&sid)
        {
            out.insert("session".into(), sid.into());
        }
        let mut pid = ANCESTOR.get().copied().filter(|p| *p != 0).unwrap_or(std::os::unix::process::parent_id() as i64);
        for _ in 0..ANCESTRY_MAX {
            let Some(p) = (pid >= 1).then(|| read_proc(pid)).flatten() else {
                break;
            };
            if p.comm == "claude" {
                out.insert("pid".into(), pid.into());
                out.insert("pid_start".into(), p.start.into());
                break;
            }
            pid = p.ppid;
        }
        out
    })
}

/// A Claude Code project's default agent name: its folder name as an identifier.
pub fn project_name() -> String {
    let path = std::env::var_os("CLAUDE_PROJECT_DIR")
        .filter(|v| !v.is_empty())
        .or_else(|| std::env::current_dir().ok().map(Into::into))
        .unwrap_or_default();
    let path = path.to_string_lossy();
    let base = path.rsplit('/').next().unwrap_or("");
    let (mut name, mut replacing) = (String::new(), false);
    for c in base.chars() {
        let keep = c.is_ascii_alphanumeric() || c == '_' || c == '-';
        if keep {
            name.push(c);
        } else if !replacing {
            name.push('-');
        }
        replacing = !keep;
    }
    let name: String = name.trim_matches('-').chars().take(40).collect();
    if name.is_empty() || name.to_lowercase() == ADMIN { "agent".into() } else { name }
}

pub fn short_hash(text: &str) -> String {
    Sha256::digest(text.as_bytes())[..4].iter().map(|b| format!("{b:02x}")).collect()
}

/// The fixed bus address of a Claude Code session, shared by its MCP server and its relay.
pub fn session_address(session: &str) -> String {
    format!("{}-{}", project_name(), short_hash(session))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_and_proc() {
        assert_eq!(short_hash("abc"), "ba7816bf");
        let me = read_proc(std::process::id() as i64).unwrap();
        assert!(me.ppid > 0 && me.start > 0);
        assert!(read_proc(-1).is_none());
    }
}
