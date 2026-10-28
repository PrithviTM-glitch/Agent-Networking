//! Which framework a process runs under decides whether it can relay: the generic host has no inbox, Claude Code's
//! needs the session variable and the inbox socket.

use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

const RUST: &str = env!("CARGO_BIN_EXE_agent-bridge");

/// What `agent-bridge relay run 1 1` prints when started in the background of a shell that exits at once, so it is
/// reparented away from whatever runs these tests and has no claude ancestor.
fn detached_relay(dir: &Path, session: Option<&str>) -> String {
    let out = dir.join(if session.is_some() { "claude" } else { "generic" });
    let mut c = Command::new("sh");
    c.args(["-c", r#"{ "$EXE" relay run 1 1 > "$OUT.part" 2>&1; mv "$OUT.part" "$OUT"; } &"#])
        .env("EXE", RUST)
        .env("OUT", &out)
        .env("AGENT_BRIDGE_LOG", dir.join("history.jsonl"))
        .env_remove("CLAUDE_CODE_MESSAGING_SOCKET")
        .env_remove("CLAUDE_CODE_SESSION_ID");
    if let Some(s) = session {
        c.env("CLAUDE_CODE_SESSION_ID", s);
    }
    assert!(c.status().expect("sh runs").success());
    let deadline = Instant::now() + Duration::from_secs(10);
    while !out.exists() {
        assert!(Instant::now() < deadline, "the relay never finished");
        std::thread::sleep(Duration::from_millis(20));
    }
    std::fs::read_to_string(out).expect("its output")
}

#[test]
fn relay_needs_a_framework_with_an_inbox() {
    let dir = std::env::temp_dir().join(format!("agent-bridge-host-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("a temp dir");
    let generic = detached_relay(&dir, None);
    let claude = detached_relay(&dir, Some("0f8fad5b-d9cb-469f-a165-70867728950e"));
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(generic, "relay needs a session with an inbox (generic host): run it from inside the session\n");
    assert_eq!(claude, "relay needs a session with an inbox (claude-code host): run it from inside the session\n");
}
