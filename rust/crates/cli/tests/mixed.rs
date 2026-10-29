//! A Python agent and a Rust agent on one disposable broker: DMs, room messages and mentions both ways, and the two
//! CLIs' outputs and errors compared line by line. Needs Docker and the repository's virtualenv (uv sync), so it is
//! ignored by default: cargo test -p agent-bridge -- --ignored

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const RUST: &str = env!("CARGO_BIN_EXE_agent-bridge");

struct World {
    broker: String,
    port: u16,
    dir: PathBuf,
    python: PathBuf,
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

    fn run(&self, exe: &Path, args: &[&str]) -> Output {
        self.cmd(exe).args(args).output().expect("the CLI runs")
    }

    fn ok(&self, exe: &Path, args: &[&str]) -> String {
        let out = self.run(exe, args);
        assert!(out.status.success(), "{exe:?} {args:?}: {}", String::from_utf8_lossy(&out.stderr));
        String::from_utf8(out.stdout).expect("UTF-8 output").trim_end().to_owned()
    }

    fn rust(&self, args: &[&str]) -> String {
        self.ok(Path::new(RUST), args)
    }

    fn py(&self, args: &[&str]) -> String {
        self.ok(&self.python.join("agent-bridge"), args)
    }

    fn json(text: &str) -> Vec<serde_json::Value> {
        text.lines().map(|l| serde_json::from_str(l).expect("one JSON object per line")).collect()
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").and_then(|l| l.local_addr()).map(|a| a.port()).expect("a free port")
}

fn start() -> World {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..").canonicalize().expect("the repository");
    // Unique per world: cargo runs this binary's tests in parallel, each with its own broker and temp dir.
    static WORLDS: AtomicUsize = AtomicUsize::new(0);
    let tag = format!("{:x}-{}", std::process::id(), WORLDS.fetch_add(1, Ordering::SeqCst));
    let dir = std::env::temp_dir().join(format!("agent-bridge-mixed-{tag}"));
    std::fs::create_dir_all(&dir).expect("a temp dir");
    let mut w = World {
        broker: format!("agent-bridge-rust-mixed-{tag}"),
        port: free_port(),
        dir,
        python: repo.join(".venv/bin"),
        services: Vec::new(),
    };
    let conf = repo.join("config/mosquitto.conf");
    let started = Command::new("docker")
        .args(["run", "-d", "--name", &w.broker, "-p", &format!("127.0.0.1:{}:1883", w.port), "-v"])
        .arg(format!("{}:/mosquitto/config/mosquitto.conf:ro", conf.display()))
        .arg("eclipse-mosquitto:2.0.22")
        .output()
        .expect("docker runs");
    assert!(started.status.success(), "{}", String::from_utf8_lossy(&started.stderr));
    let deadline = Instant::now() + Duration::from_secs(15);
    while !w.run(Path::new(RUST), &["agents"]).status.success() {
        assert!(Instant::now() < deadline, "the broker never came up");
        std::thread::sleep(Duration::from_millis(100));
    }
    for service in ["agent_bridge.ledger.service", "agent_bridge.history.service"] {
        let mut c = w.cmd(&w.python.join("python"));
        w.services.push(c.args(["-m", service]).stderr(Stdio::null()).spawn().expect("a service"));
    }
    // Both CLIs read history through historyd: wait until it answers.
    while !w.run(Path::new(RUST), &["history", "warmup"]).status.success() {
        assert!(Instant::now() < deadline, "historyd never answered");
        std::thread::sleep(Duration::from_millis(100));
    }
    w
}

#[test]
#[ignore = "needs Docker and the repository's virtualenv"]
fn python_and_rust_agents_talk() {
    let w = start();
    let (py, rs) =
        (w.py(&["register", "pyagent", "--caps", "python"]), w.rust(&["register", "rsagent", "--caps", "rust"]));
    let (py, rs) = (py.as_str(), rs.as_str());

    w.rust(&["dm", rs, py, "hello from rust"]);
    let got = World::json(&w.py(&["recv", py]));
    assert_eq!((got[0]["topic"].as_str(), got[0]["from"].as_str()), (Some(&*format!("inbox/{py}")), Some(rs)));
    assert_eq!(got[0]["body"], "hello from rust");

    w.py(&["dm", py, rs, "hello from python ✓"]);
    let got = World::json(&w.rust(&["recv", rs]));
    assert_eq!((got[0]["from"].as_str(), got[0]["body"].as_str()), (Some(py), Some("hello from python ✓")));

    w.py(&["join", py, "mixed"]);
    w.rust(&["join", rs, "mixed"]);
    w.rust(&["say", rs, "mixed", "@pyagent your turn"]);
    let got = World::json(&w.py(&["recv", py]));
    let said = got.iter().find(|m| m["body"] == "@pyagent your turn").expect("the mention arrives");
    assert_eq!((said["mentions"].clone(), said["for_me"].clone()), (serde_json::json!([py]), true.into()));
    assert!(got.iter().any(|m| m["body"] == "[joined #mixed] rsagent, caps: rust"));

    w.py(&["say", py, "mixed", &format!("@{rs} and @all")]);
    w.py(&["say", py, "mixed", "@pyagent only me"]);
    let got = World::json(&w.rust(&["recv", rs]));
    let bodies: Vec<_> = got.iter().map(|m| (m["body"].as_str().unwrap_or(""), m.get("for_me").cloned())).collect();
    assert!(bodies.contains(&("@pyagent only me", Some(false.into()))), "{bodies:?}");
    assert!(bodies.contains(&(&*format!("@{rs} and @all"), None)), "@all mentions nobody in particular");

    // Both CLIs read the same broker and log the same way: identical lines.
    for args in [
        vec!["rooms"],
        vec!["room", "info", "mixed"],
        vec!["history", "mixed"],
        vec!["dms", py],
        vec!["dms", rs, "--with", py],
    ] {
        assert_eq!(w.rust(&args), w.py(&args), "{args:?}");
    }
    // And the same one-line errors with a non-zero exit.
    let bad = format!("@nobody-{} hi", "x");
    for args in [
        vec!["say", rs, "mixed", &bad],
        vec!["dm", rs, "nobody-000000", "x"],
        vec!["register", "bad/name"],
        vec!["recv", "admin"],
        vec!["pub", rs, "inbox/x", "x"],
        vec!["sub", rs, "a/#/b"],
        vec!["history", "mixed", "--last", "0"],
        vec!["dm", rs, py, "bell \x07"],
    ] {
        let (r, p) = (w.run(Path::new(RUST), &args), w.run(&w.python.join("agent-bridge"), &args));
        assert!(!r.status.success() && !p.status.success(), "{args:?}");
        assert_eq!(String::from_utf8_lossy(&r.stderr), String::from_utf8_lossy(&p.stderr), "{args:?}");
    }

    w.rust(&["leave", rs]);
    w.py(&["leave", py]);
    assert_eq!(w.rust(&["agents"]), "");
}

/// Which CLI runs a scenario: the Python one (agent-bridge, agent-bridge-ledger) or the Rust one.
#[derive(Clone, Copy)]
enum Cli {
    Python,
    Rust,
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

/// The scenario's commands; "ledger-raw" steps run agent-bridge-ledger on the Python side.
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

impl World {
    /// Each step as "$ agent-bridge <args>", its exit status, stdout and stderr, normalised.
    fn transcript(&self, cli: Cli) -> String {
        let mut out = String::new();
        for args in steps() {
            let output = match (cli, args.first()) {
                (Cli::Python, Some(&"ledger-raw")) => self.run(&self.python.join("agent-bridge-ledger"), &args[1..]),
                (Cli::Python, _) => self.run(&self.python.join("agent-bridge"), &args),
                (Cli::Rust, _) => self.run(Path::new(RUST), &args),
            };
            out += &format!("$ agent-bridge {}\nexit {}\n", args.join(" "), output.status.code().unwrap_or(-1));
            out += &String::from_utf8_lossy(&output.stdout);
            out += &String::from_utf8_lossy(&output.stderr);
        }
        normal(&out)
    }
}

/// The Python CLI's transcript, frozen in tests/golden/cli.txt (rewritten with AGENT_BRIDGE_WRITE_GOLDEN=1 while
/// the Python CLI exists), and the Rust CLI's, which must equal it line for line.
#[test]
#[ignore = "needs Docker and the repository's virtualenv"]
fn the_rust_cli_matches_the_python_transcript() {
    let golden = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/cli.txt");
    let python = start().transcript(Cli::Python);
    if std::env::var_os("AGENT_BRIDGE_WRITE_GOLDEN").is_some() {
        std::fs::create_dir_all(golden.parent().expect("a directory")).expect("the golden directory");
        std::fs::write(&golden, &python).expect("the golden file");
    }
    let want = std::fs::read_to_string(&golden).expect("tests/golden/cli.txt");
    for (cli, got) in [("python", python), ("rust", start().transcript(Cli::Rust))] {
        for (i, (g, w)) in got.lines().zip(want.lines()).enumerate() {
            assert_eq!(g, w, "{cli} CLI, line {}", i + 1);
        }
        assert_eq!(got.lines().count(), want.lines().count(), "{cli} CLI: line count");
    }
}

#[test]
fn normalises_times_and_ids() {
    let text = r#"{"ts": 1793294701.3971186, "id": "0123456789abcdef", "token": 3, "n": 12345678901}"#;
    assert_eq!(normal(text), r#"{"ts": <t>, "id": "<id>", "token": 3, "n": 12345678901}"#);
}
