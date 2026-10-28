//! Claude Code as a crate::host::Host.

use super::inbox;
use crate::host::{Host, stat};
use crate::validation::is_uuid;

pub const SESSION_ENV: &str = "CLAUDE_CODE_SESSION_ID";
const PROJECT_ENV: &str = "CLAUDE_PROJECT_DIR";
const ANCESTRY_MAX: usize = 64;
const PROCESS_NAME: &str = "claude";

pub struct Claude;

pub static CLAUDE: Claude = Claude;

impl Host for Claude {
    fn name(&self) -> &'static str {
        "claude-code"
    }

    fn session(&self) -> Option<String> {
        std::env::var(SESSION_ENV).ok().filter(|s| is_uuid(s))
    }

    /// The nearest ancestor named claude: the session's own process, whatever shells and tools sit between.
    fn process(&self, start: Option<i64>) -> Option<(i64, u64)> {
        let mut pid = start.filter(|p| *p != 0).unwrap_or(std::os::unix::process::parent_id() as i64);
        for _ in 0..ANCESTRY_MAX {
            let p = (pid >= 1).then(|| stat(pid)).flatten()?;
            if p.comm == PROCESS_NAME {
                return Some((pid, p.start));
            }
            pid = p.ppid;
        }
        None
    }

    fn project_dir(&self) -> Option<String> {
        std::env::var_os(PROJECT_ENV).filter(|d| !d.is_empty()).map(|d| d.to_string_lossy().into_owned())
    }

    fn can_deliver(&self) -> bool {
        inbox::available()
    }

    fn deliver(&self, text: &str) -> std::io::Result<()> {
        inbox::post(text)
    }
}
