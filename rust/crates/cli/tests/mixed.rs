//! The CLI on a disposable broker with the Rust ledgerd and historyd: a fixed scenario (DMs, rooms, mentions, history,
//! raw topics, one-line errors, ledger-raw) whose output must equal tests/golden/cli.txt line for line. The file is the
//! transcript the Python CLI printed for the same scenario before it was retired, so it pins the Rust CLI to that
//! behaviour. Needs Docker, so it is ignored by default: cargo test --workspace -- --ignored (which also builds the
//! services next to agent-bridge; AGENT_BRIDGE_LEDGERD and AGENT_BRIDGE_HISTORYD name others).

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const RUST: &str = env!("CARGO_BIN_EXE_agent-bridge");

struct World {
    broker: String,
    port: u16,
    dir: PathBuf,
    services: Vec<Child>,
}

impl Drop for World {
    fn drop(&mut self) {
        for mut s in self.services.drain(..) {
            let _ = s.kill();
            let _ = s.wait();
        }
        let _ = Command::new("docker").args(["rm", "-f", &self.broker]).output();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl World {
    fn cmd(&self, exe: &Path) -> Command {
        let mut c = Command::new(exe);
        c.env("AGENT_BRIDGE_HOST", "127.0.0.1")
            .env("AGENT_BRIDGE_PORT", self.port.to_string())
            .env("AGENT_BRIDGE_LOG", self.dir.join("history.jsonl"))
            .env("XDG_STATE_HOME", self.dir.join("state"))
            .env("LEDGER_DB", self.dir.join("ledger.db"))
            .env("HISTORY_DB", self.dir.join("history.db"));
        for name in
            ["CLAUDE_CODE_SESSION_ID", "CLAUDE_PROJECT_DIR", "CLAUDE_CODE_MESSAGING_SOCKET", "AGENT_BRIDGE_STALE_DAYS"]
        {
            c.env_remove(name);
        }
        c
    }

    fn run(&self, args: &[&str]) -> Output {
        self.cmd(Path::new(RUST)).args(args).output().expect("the CLI runs")
    }

    /// Each step as "$ agent-bridge <args>", its exit status, stdout and stderr, normalised.
    fn transcript(&self) -> String {
        let mut out = String::new();
        for args in steps() {
            let output = self.run(&args);
            out += &format!("$ agent-bridge {}\nexit {}\n", args.join(" "), output.status.code().unwrap_or(-1));
            out += &String::from_utf8_lossy(&output.stdout);
            out += &String::from_utf8_lossy(&output.stderr);
        }
        normal(&out)
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").and_then(|l| l.local_addr()).map(|a| a.port()).expect("a free port")
}

/// A service binary: the env override, else the one cargo built next to agent-bridge.
fn service(name: &str, env: &str) -> PathBuf {
    let path = std::env::var_os(env).map(PathBuf::from).unwrap_or_else(|| Path::new(RUST).with_file_name(name));
    assert!(path.exists(), "{} missing: cargo build -p {name}, or set {env}", path.display());
    path
}

fn start() -> World {
    // Unique per world: cargo runs this binary's tests in parallel, each with its own broker and temp dir.
    static WORLDS: AtomicUsize = AtomicUsize::new(0);
    let tag = format!("{:x}-{}", std::process::id(), WORLDS.fetch_add(1, Ordering::SeqCst));
    let dir = std::env::temp_dir().join(format!("agent-bridge-mixed-{tag}"));
    std::fs::create_dir_all(&dir).expect("a temp dir");
    let mut w =
        World { broker: format!("agent-bridge-rust-mixed-{tag}"), port: free_port(), dir, services: Vec::new() };
    let conf = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../config/mosquitto.conf");
    let started = Command::new("docker")
        .args(["run", "-d", "--name", &w.broker, "-p", &format!("127.0.0.1:{}:1883", w.port), "-v"])
        .arg(format!("{}:/mosquitto/config/mosquitto.conf:ro", conf.canonicalize().expect("the config").display()))
        .arg("eclipse-mosquitto:2.0.22")
        .output()
        .expect("docker runs");
    assert!(started.status.success(), "{}", String::from_utf8_lossy(&started.stderr));
    let deadline = Instant::now() + Duration::from_secs(15);
    while !w.run(&["agents"]).status.success() {
        assert!(Instant::now() < deadline, "the broker never came up");
        std::thread::sleep(Duration::from_millis(100));
    }
    for (name, env) in
        [("agent-bridge-ledgerd", "AGENT_BRIDGE_LEDGERD"), ("agent-bridge-historyd", "AGENT_BRIDGE_HISTORYD")]
    {
        let exe = service(name, env);
        w.services.push(w.cmd(&exe).stderr(Stdio::null()).spawn().expect("a service"));
    }
    // The CLI reads history through historyd: wait until it answers.
    while !w.run(&["history", "warmup"]).status.success() {
        assert!(Instant::now() < deadline, "historyd never answered");
        std::thread::sleep(Duration::from_millis(100));
    }
    w
}

/// Epoch seconds (ts, seen, created, expires ...) and 16-hex ledger ids, which differ from run to run.
fn normal(text: &str) -> String {
    let (b, mut out, mut i) = (text.as_bytes(), String::new(), 0);
    while i < b.len() {
        let digits = b[i..].iter().take_while(|c| c.is_ascii_digit()).count();
        let starts_word = i == 0 || !b[i - 1].is_ascii_alphanumeric();
        if starts_word && digits == 10 && b[i] == b'1' {
            let mut end = i + 10;
            if b.get(end) == Some(&b'.') {
                end += 1 + b[end + 1..].iter().take_while(|c| c.is_ascii_digit()).count();
            }
            out.push_str("<t>");
            i = end;
            continue;
        }
        let hex = b[i..].iter().take_while(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()).count();
        if i > 0 && b[i - 1] == b'"' && hex == 16 && b.get(i + 16) == Some(&b'"') {
            out.push_str("<id>");
            i += 16;
            continue;
        }
        let ch = text[i..].chars().next().expect("a char");
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// The scenario's commands.
fn steps() -> Vec<Vec<&'static str>> {
    let (a, b) = ("alpha-000001", "beta-000002");
    vec![
        vec!["register", "alpha", "--caps", "x", "--addr", a],
        vec!["register", "beta", "--addr", b],
        vec!["dm", a, b, "hi ✓"],
        vec!["recv", b],
        vec!["join", a, "room1"],
        vec!["join", b, "room1"],
        vec!["say", b, "room1", "@alpha-000001 hello"],
        vec!["recv", a],
        vec!["rooms"],
        vec!["room", "info", "room1"],
        vec!["history", "room1"],
        vec!["dms", b],
        vec!["dms", b, "--with", a],
        vec!["sub", a, "t/x"],
        vec!["pub", b, "t/x", "raw text"],
        vec!["recv", a],
        vec!["room", "set", b, "room1", "--mode", "announce"],
        vec!["say", b, "room1", "@nobody-x hi"],
        vec!["dm", b, "nobody-000000", "x"],
        vec!["register", "bad/name"],
        vec!["recv", "admin"],
        vec!["pub", b, "inbox/x", "x"],
        vec!["sub", b, "a/#/b"],
        vec!["history", "room1", "--last", "0"],
        vec!["dm", b, a, "bell \x07"],
        vec!["ledger-raw", "init", "--as", a],
        vec!["ledger-raw", "add", "--as", a, "--name", "r1", "--description", "first", "--visibility", "public"],
        vec!["ledger-raw", "init", "--as", b],
        vec!["ledger-raw", "public", "--as", b],
        vec!["ledger-raw", "request", "--as", b, "--resource", "alpha-000001/r1", "--level", "read"],
        vec!["ledger-raw", "acquire", "--as", b, "--resource", "alpha-000001/r1", "--level", "read", "--ttl", "5"],
        vec!["ledger-raw", "bogus", "--as", a],
        vec!["ledger-raw", "add", "--as", "nobody-1", "--name", "r2"],
        vec!["ledger-raw", "init", "--as", "bad/addr"],
        vec!["ledger-raw"],
        vec!["ledger-raw", "init", "--as", a, "--name"],
        vec!["ledger-raw", "init", "--timeout", "soon"],
        vec!["leave", b],
        vec!["leave", a],
    ]
}

/// The Rust CLI's transcript equals the frozen one line for line.
#[test]
#[ignore = "needs Docker"]
fn the_cli_matches_the_golden_transcript() {
    let want = std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/cli.txt"))
        .expect("tests/golden/cli.txt");
    let got = start().transcript();
    for (i, (g, w)) in got.lines().zip(want.lines()).enumerate() {
        assert_eq!(g, w, "line {}", i + 1);
    }
    assert_eq!(got.lines().count(), want.lines().count(), "line count");
}

/// A process killed when the test ends, failed assertions included.
struct Killed(Child);

impl Drop for Killed {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A relay notices its claude process ending even while the broker is gone: it waits for the broker in short steps
/// instead of inside a call that blocks for seconds, and its log says the broker is unreachable.
#[test]
#[ignore = "needs Docker"]
fn a_relay_ends_with_its_claude_while_the_broker_is_down() {
    let w = start();
    let session = "8f1c1b8e-2d64-4a51-9a3c-6f0e5b7d2a11";
    // a shell named claude, the relay's ancestor; it lives while its stdin is open
    let claude = w.dir.join("claude");
    std::fs::copy("/bin/sh", &claude).expect("a fake claude");
    let mut fake = w.cmd(&claude);
    fake.args(["-c", &format!("{RUST} relay start; read _")])
        .env("CLAUDE_CODE_SESSION_ID", session)
        .env("CLAUDE_CODE_MESSAGING_SOCKET", w.dir.join("inbox.sock"))
        .stdin(Stdio::piped());
    let fake = Killed(fake.spawn().expect("the fake claude runs"));
    let lock = w.dir.join("relay").join(format!("{session}.lock"));
    let deadline = Instant::now() + Duration::from_secs(10);
    let live = || {
        String::from_utf8_lossy(&w.run(&["relay", "status", "--session", session]).stdout).contains("\"live\": true")
    };
    let relay = loop {
        let pid = std::fs::read_to_string(&lock).ok().and_then(|l| l.lines().next()?.parse::<i32>().ok());
        if let Some(pid) = pid.filter(|_| live()) {
            break pid;
        }
        assert!(Instant::now() < deadline, "the relay never came up");
        std::thread::sleep(Duration::from_millis(50));
    };
    let _ = Command::new("docker").args(["rm", "-f", &w.broker]).output();
    std::thread::sleep(Duration::from_secs(3));
    let ended = Instant::now();
    drop(fake);
    while Path::new(&format!("/proc/{relay}")).exists() && ended.elapsed() < Duration::from_secs(5) {
        std::thread::sleep(Duration::from_millis(20));
    }
    let took = ended.elapsed();
    let log = std::fs::read_to_string(w.dir.join("relay").join("relay.log")).unwrap_or_default();
    assert!(took < Duration::from_secs(2), "the relay took {took:?} to notice its claude ended\n{log}");
    assert!(log.contains("broker unreachable, waiting for it"), "{log}");
}

#[test]
fn normalises_times_and_ids() {
    let text = r#"{"ts": 1793294701.3971186, "id": "0123456789abcdef", "token": 3, "n": 12345678901}"#;
    assert_eq!(normal(text), r#"{"ts": <t>, "id": "<id>", "token": 3, "n": 12345678901}"#);
}
