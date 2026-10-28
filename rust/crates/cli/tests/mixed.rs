//! A Python agent and a Rust agent on one disposable broker: DMs, room messages and mentions both ways, and the two
//! CLIs' outputs and errors compared line by line. Needs Docker and the repository's virtualenv (uv sync), so it is
//! ignored by default: cargo test -p agent-bridge -- --ignored

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const RUST: &str = env!("CARGO_BIN_EXE_agent-bridge");

struct World {
    broker: String,
    port: u16,
    dir: PathBuf,
    python: PathBuf,
    ledgerd: Option<Child>,
}

impl Drop for World {
    fn drop(&mut self) {
        if let Some(mut l) = self.ledgerd.take() {
            let _ = l.kill();
            let _ = l.wait();
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
            .env("LEDGER_DB", self.dir.join("ledger.db"));
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
    let tag = format!("{:x}", std::process::id() ^ Instant::now().elapsed().subsec_nanos());
    let dir = std::env::temp_dir().join(format!("agent-bridge-mixed-{tag}"));
    std::fs::create_dir_all(&dir).expect("a temp dir");
    let mut w = World {
        broker: format!("agent-bridge-rust-mixed-{tag}"),
        port: free_port(),
        dir,
        python: repo.join(".venv/bin"),
        ledgerd: None,
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
    let mut ledgerd = w.cmd(&w.python.join("python"));
    w.ledgerd =
        Some(ledgerd.args(["-m", "agent_bridge.ledger.service"]).stderr(Stdio::null()).spawn().expect("ledgerd"));
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
