//! Who an agent is: the session and process of the agent framework it runs under (see host.rs), and the addresses
//! derived from them.

use crate::host;
use crate::json::Map;
use crate::validation::ADMIN;
use sha2::{Digest, Sha256};
use std::sync::OnceLock;

static ANCESTOR: OnceLock<i64> = OnceLock::new();
static ORIGIN: OnceLock<Map> = OnceLock::new();

/// Where origin() starts walking up instead of this process's parent; the relay sets it, being detached from its
/// session's process. Only the first call counts, and only before origin() first runs.
pub fn set_ancestor(pid: i64) {
    let _ = ANCESTOR.set(pid);
}

/// The session and process this agent runs under, published in presence for the admin console.
pub fn origin() -> &'static Map {
    ORIGIN.get_or_init(|| {
        let h = host::current();
        let mut out = Map::new();
        if let Some(sid) = h.session() {
            out.insert("session".into(), sid.into());
        }
        if let Some((pid, start)) = h.process(ANCESTOR.get().copied()) {
            out.insert("pid".into(), pid.into());
            out.insert("pid_start".into(), start.into());
        }
        out
    })
}

/// A session's default agent name: its project folder's name as an identifier.
pub fn project_name() -> String {
    let path = host::installed()
        .into_iter()
        .find_map(|h| h.project_dir())
        .or_else(|| std::env::current_dir().ok().map(|d| d.to_string_lossy().into_owned()))
        .unwrap_or_default();
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

/// The fixed bus address of a session, shared by its MCP server and its relay.
pub fn session_address(session: &str) -> String {
    format!("{}-{}", project_name(), short_hash(session))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes() {
        assert_eq!(short_hash("abc"), "ba7816bf");
    }
}
