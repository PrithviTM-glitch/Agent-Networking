//! Everything that knows Claude Code: its session variables and process, and the session inbox socket. The rest of
//! the bridge reaches it only through crate::host. Claude Code's sockets are internal and undocumented.

mod host;
pub mod inbox;

pub use host::{CLAUDE, SESSION_ENV};
