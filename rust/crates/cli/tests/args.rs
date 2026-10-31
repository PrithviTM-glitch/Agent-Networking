//! The command line without a broker: help texts, the hooks' silence and the relay's session errors.

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
        assert!(text(&out.stdout).starts_with("a Claude Code hook"), "{args:?}: {}", text(&out.stdout));
    }
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
