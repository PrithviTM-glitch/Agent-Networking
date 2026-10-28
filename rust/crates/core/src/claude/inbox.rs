//! The session inbox: a Unix socket Claude Code opens for each session, which takes one user message per connection
//! after an auth line. Its path and token are in the session's environment.

use crate::json::dumps;
use serde_json::json;
use std::io::{self, Write};
use std::os::unix::net::UnixStream;
use std::time::Duration;

const SOCKET: &str = "CLAUDE_CODE_MESSAGING_SOCKET";
const TOKEN: &str = "CLAUDE_CODE_MESSAGING_TOKEN";

pub fn available() -> bool {
    std::env::var_os(SOCKET).is_some_and(|s| !s.is_empty())
}

/// Write one user message to this session's inbox socket.
pub fn post(text: &str) -> io::Result<()> {
    let path = std::env::var_os(SOCKET).filter(|s| !s.is_empty()).ok_or_else(|| {
        io::Error::other(format!("{SOCKET} is not set: not inside a Claude Code session with an inbox"))
    })?;
    let mut s = UnixStream::connect(path)?;
    s.set_write_timeout(Some(Duration::from_secs(5)))?;
    let auth = json!({"type": "auth", "token": std::env::var(TOKEN).unwrap_or_default()});
    let user = json!({"type": "user", "message": {"role": "user", "content": text}});
    s.write_all(format!("{}\n{}\n", dumps(&auth), dumps(&user)).as_bytes())?;
    s.shutdown(std::net::Shutdown::Write)
}
