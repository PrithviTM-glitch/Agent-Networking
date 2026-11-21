//! The command line without a broker: help texts, the hooks' silence and the relay's session errors.

#![allow(
    clippy::let_underscore_must_use,
    reason = "best-effort cleanup: killing, waiting for and removing what the test started"
)]

use std::io::Write;
use std::process::{Command, Output, Stdio};

const RUST: &str = env!("CARGO_BIN_EXE_agent-bridge");

fn run(args: &[&str], stdin: &[u8]) -> Output {
    let mut child = Command::new(RUST)
        .args(args)
        // nothing listens on port 1: no call reaches a broker
        .env("AGENT_BRIDGE_PORT", "1")
        .env_remove("CLAUDE_CODE_SESSION_ID")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the CLI runs");
    let _ = child.stdin.take().expect("its stdin").write_all(stdin);
    child.wait_with_output().expect("its output")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[test]
fn hooks_print_their_help_and_nothing_else() {
    for args in [&["hook", "-h"][..], &["hook", "broadcast", "--help"]] {
        let out = run(args, b"");
        assert_eq!(out.status.code(), Some(0), "{args:?}");
        assert!(text(&out.stdout).starts_with("a hook. Claude Code's broadcast"), "{args:?}: {}", text(&out.stdout));
    }
    // the guards fail closed: bad input refuses (guard 2, ref-guard 1) with a reason, never exits 0
    let out = run(&["hook", "guard"], b"not json");
    assert_eq!(out.status.code(), Some(2));
    assert!(text(&out.stderr).starts_with("blocked: "), "{}", text(&out.stderr));
    let out = run(&["hook", "ref-guard", "prepared"], b"one-word\n");
    assert_eq!(out.status.code(), Some(1));
    assert!(
        text(&out.stderr).starts_with("agent-bridge ref-guard: blocked: unreadable update line"),
        "{}",
        text(&out.stderr)
    );
    let out = run(&["hook", "ref-guard", "committed"], b"one-word\n");
    assert_eq!((out.status.code(), out.stderr.len()), (Some(0), 0), "only prepared is checked");
    for (args, stdin) in [(&["hook"][..], &b""[..]), (&["hook", "broadcast"], b"not json"), (&["hook", "nosuch"], b"")]
    {
        let out = run(args, stdin);
        assert_eq!((out.status.code(), out.stdout.len(), out.stderr.len()), (Some(0), 0, 0), "{args:?}");
    }
}

#[test]
fn relay_commands_outside_a_session_say_so() {
    for action in ["status", "stop"] {
        let out = run(&["relay", action], b"");
        assert_eq!(out.status.code(), Some(1));
        assert_eq!(
            text(&out.stderr),
            "no session: run it inside a Claude Code session (CLAUDE_CODE_SESSION_ID) or pass --session ID\n"
        );
    }
    let out = run(&["relay", "status", "--session", "nope"], b"");
    assert_eq!(text(&out.stderr), "bad session id 'nope': expected a UUID\n");
}

#[test]
fn help_says_what_prints_json() {
    let out = run(&["--help"], b"");
    assert!(
        text(&out.stdout).starts_with("agent-bridge: the bus from the shell. Commands that return data print JSON")
    );
    assert!(text(&out.stdout).contains("register prints just the address"));
    assert!(text(&out.stdout).contains("not empty or only whitespace for say and dm"));
    let say = text(&run(&["say", "--help"], b"").stdout);
    assert!(say.contains("any other @word stays plain text") && say.contains("@@word never mentions"), "{say}");
}

#[test]
fn relay_wake_filters_are_kept_per_session() {
    /// removed when the test ends, failed assertions included
    struct Gone(std::path::PathBuf);
    impl Drop for Gone {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let state = Gone(std::env::temp_dir().join(format!("agent-bridge-args-wake-{}", std::process::id())));
    let session = "2f1e0d9c-8b7a-4655-9443-322110fedcba";
    let wake = |args: &[&str]| {
        let out = Command::new(RUST)
            .args(["relay", "wake"])
            .args(args)
            .args(["--session", session])
            .env("AGENT_BRIDGE_PORT", "1")
            .env("XDG_STATE_HOME", &state.0)
            .env_remove("AGENT_BRIDGE_LOG")
            .env_remove("CLAUDE_CODE_SESSION_ID")
            .output()
            .expect("the CLI runs");
        (out.status.code(), text(&out.stdout), text(&out.stderr))
    };
    let rooms = |json: &str| format!("{{\"session\": \"{session}\", \"rooms\": {json}}}\n");
    assert_eq!(wake(&[]), (Some(0), rooms("{}"), String::new()));
    assert_eq!(wake(&["mrs", "mentions"]), (Some(0), rooms("{\"mrs\": \"mentions\"}"), String::new()));
    assert_eq!(wake(&["noisy", "admin"]).1, rooms("{\"mrs\": \"mentions\", \"noisy\": \"admin\"}"));
    assert_eq!(wake(&["mrs"]).1, rooms("{\"mrs\": \"mentions\"}"));
    assert_eq!(wake(&["ops"]).1, rooms("{\"ops\": \"all\"}"), "a room without a filter wakes on everything");
    assert_eq!(wake(&["mrs", "all"]).1, rooms("{\"noisy\": \"admin\"}"), "all removes the filter");
    let (code, _, err) = wake(&["mrs", "loud"]);
    assert_eq!((code, err.as_str()), (Some(1), "wake mode: one of all, mentions, admin, got \"loud\"\n"));
    assert_eq!(wake(&["a/b", "all"]).0, Some(1));
}
