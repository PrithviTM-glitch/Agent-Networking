//! The agent framework a process runs under, behind one small interface, so the bridge itself knows none of them.
//!
//! A Host answers which session this process belongs to, which process is the session, and how to put a message into
//! the session. The generic host has no session, no process and no inbox, so an agent there reads messages with recv.
//! Each framework implements it in its own module (claude for Claude Code), and only that module knows the
//! framework's variables and sockets. current() picks the framework this process runs under.

use std::io;

pub struct Proc {
    pub comm: String,
    pub ppid: i64,
    pub start: u64,
}

/// comm, ppid and start time in clock ticks of a live process, or None.
pub fn stat(pid: i64) -> Option<Proc> {
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

pub trait Host: Sync {
    fn name(&self) -> &'static str;

    /// This process's session id (a UUID, as the framework gives it), or None.
    fn session(&self) -> Option<String> {
        None
    }

    /// (pid, start ticks) of the session process above this one (above start, if given), or None.
    fn process(&self, _start: Option<i64>) -> Option<(i64, u64)> {
        None
    }

    /// The session's project directory, when the framework says; agent names derive from it.
    fn project_dir(&self) -> Option<String> {
        None
    }

    fn detected(&self) -> bool {
        self.session().is_some() || self.process(None).is_some()
    }

    /// True if deliver() can reach this process's session.
    fn can_deliver(&self) -> bool {
        false
    }

    /// Put text into this process's session as a user message.
    fn deliver(&self, _text: &str) -> io::Result<()> {
        Err(io::Error::other(format!("{} host: no inbox to deliver to, read messages with recv", self.name())))
    }
}

/// No framework: nothing to deliver into.
pub struct Generic;

impl Host for Generic {
    fn name(&self) -> &'static str {
        "generic"
    }
}

pub static GENERIC: Generic = Generic;

/// Every framework this bridge has an adapter for.
pub fn installed() -> [&'static dyn Host; 1] {
    [&crate::claude::CLAUDE]
}

/// The framework this process runs under, else the generic host.
pub fn current() -> &'static dyn Host {
    installed().into_iter().find(|h| h.detected()).unwrap_or(&GENERIC)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generic_and_stat() {
        let g = &GENERIC;
        assert!(g.session().is_none() && g.process(None).is_none() && g.project_dir().is_none() && !g.detected());
        assert!(!g.can_deliver());
        assert_eq!(
            g.deliver("hi").unwrap_err().to_string(),
            "generic host: no inbox to deliver to, read messages with recv"
        );
        let me = stat(std::process::id() as i64).unwrap();
        assert!(me.ppid > 0 && me.start > 0);
        assert!(stat(-1).is_none());
    }
}
