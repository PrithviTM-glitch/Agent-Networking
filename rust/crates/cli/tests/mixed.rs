//! The CLI on a disposable broker with the Rust ledgerd and historyd: a fixed scenario (DMs, rooms, mentions, history,
//! raw topics, one-line errors, ledger-raw) whose output must equal tests/golden/cli.txt line for line. The file is the
//! transcript the Python CLI printed for the same scenario before it was retired, so it pins the Rust CLI to that
//! behaviour. Needs Docker, so it is ignored by default: cargo test --workspace -- --ignored (which also builds the
//! services next to agent-bridge; AGENT_BRIDGE_LEDGERD and AGENT_BRIDGE_HISTORYD name others).

#![allow(
    clippy::let_underscore_must_use,
    reason = "best-effort cleanup: killing, waiting for and removing what the test started"
)]

use std::io::Write;
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
        // a quoted id first: one that happens to start with ten digits must not be taken for a time
        let hex = b[i..].iter().take_while(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()).count();
        if i > 0 && b[i - 1] == b'"' && hex == 16 && b.get(i + 16) == Some(&b'"') {
            out.push_str("<id>");
            i += 16;
            continue;
        }
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
    // to review a deliberate change: AGENT_BRIDGE_GOLDEN_OUT=/tmp/cli.txt, then diff it against the golden
    if let Some(path) = std::env::var_os("AGENT_BRIDGE_GOLDEN_OUT") {
        std::fs::write(path, &got).expect("the transcript is written");
    }
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

/// A shell named claude, the relay's ancestor, running `relay start` for session; it lives while its stdin is open.
/// Returns it and the relay's pid once the relay is live.
fn relayed(w: &World, session: &str) -> (Killed, i32) {
    relayed_by(w, session, Path::new(RUST))
}

/// relayed, with exe as the agent-bridge that starts the relay. The fake claude then runs each line written to its
/// stdin as a shell command, so a command can run inside the session (as its child).
fn relayed_by(w: &World, session: &str, exe: &Path) -> (Killed, i32) {
    relayed_with(w, session, exe, &[])
}

/// Puts a copy of the executable src at dst with a `cp` of its own, so this process never holds a write fd to an
/// executable. Another test thread that forks while fs::copy has dst open would carry that fd until its child execs,
/// and an exec of dst meanwhile fails with ETXTBSY ("Text file busy", rust-lang/rust#114554). The relay's re-exec,
/// the fake claude's `relay start` and git's hooks exec these files out of this test's hands, so the fd must never
/// exist here, not only be retried around.
fn install(src: &Path, dst: &Path) {
    let out = Command::new("cp").arg(src).arg(dst).output().expect("cp runs");
    assert!(out.status.success(), "cp {} {}: {}", src.display(), dst.display(), String::from_utf8_lossy(&out.stderr));
}

/// c spawned, retrying while its executable is busy (ETXTBSY) for up to 5 s, then failing with what it was.
fn spawn_busy_retry(c: &mut Command, what: &str) -> Child {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match c.spawn() {
            Ok(child) => return child,
            Err(e) if e.kind() == std::io::ErrorKind::ExecutableFileBusy && Instant::now() < deadline => {
                std::thread::yield_now();
            }
            Err(e) => panic!("{what}: {e}"),
        }
    }
}

/// relayed_by, with extra environment for the fake claude and so for its relay.
fn relayed_with(w: &World, session: &str, exe: &Path, env: &[(&str, &str)]) -> (Killed, i32) {
    let claude = w.dir.join("claude");
    if !claude.exists() {
        install(Path::new("/bin/sh"), &claude);
    }
    let mut fake = w.cmd(&claude);
    fake.args(["-c", &format!("{} relay start; while read -r c; do eval \"$c\"; done", exe.display())])
        .env("CLAUDE_CODE_SESSION_ID", session)
        .env("CLAUDE_CODE_MESSAGING_SOCKET", w.dir.join("inbox.sock"))
        .envs(env.iter().copied())
        .stdin(Stdio::piped());
    let fake = Killed(spawn_busy_retry(&mut fake, "the fake claude runs"));
    let lock = w.dir.join("relay").join(format!("{session}.lock"));
    let deadline = Instant::now() + Duration::from_secs(10);
    let live = || {
        String::from_utf8_lossy(&w.run(&["relay", "status", "--session", session]).stdout).contains("\"live\": true")
    };
    loop {
        let text = std::fs::read_to_string(&lock).unwrap_or_default();
        let pid = text.lines().next().and_then(|l| l.parse::<i32>().ok());
        // the second line, its address, is written once it has registered
        if let Some(pid) = pid.filter(|_| text.lines().nth(1).is_some() && live()) {
            return (fake, pid);
        }
        assert!(Instant::now() < deadline, "the relay never came up");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn relay_log(w: &World) -> String {
    std::fs::read_to_string(w.dir.join("relay").join("relay.log")).unwrap_or_default()
}

/// A relay notices its claude process ending even while the broker is gone: it waits for the broker in short steps
/// instead of inside a call that blocks for seconds, and its log says the broker is unreachable.
#[test]
#[ignore = "needs Docker"]
fn a_relay_ends_with_its_claude_while_the_broker_is_down() {
    let w = start();
    let (fake, relay) = relayed(&w, "8f1c1b8e-2d64-4a51-9a3c-6f0e5b7d2a11");
    let _ = Command::new("docker").args(["rm", "-f", &w.broker]).output();
    // its claude ends only once the relay is waiting for the broker, however slow the machine is
    log_until(&w, "the broker outage", |log| log.contains("broker unreachable, waiting for it"));
    let ended = Instant::now();
    drop(fake);
    while Path::new(&format!("/proc/{relay}")).exists() && ended.elapsed() < Duration::from_secs(5) {
        std::thread::sleep(Duration::from_millis(20));
    }
    let took = ended.elapsed();
    assert!(took < Duration::from_secs(2), "the relay took {took:?} to notice its claude ended\n{}", relay_log(&w));
}

/// While a session's relay is live, say, dm, pub and describe from that session go over a connection of their own and never
/// take the relay's session over; join still does (it changes the session's subscriptions), and the relay takes it
/// back without logging an outage. The broker's log counts the takeovers.
#[test]
#[ignore = "needs Docker"]
fn posting_from_the_shell_leaves_the_relay_its_session() {
    let w = start();
    let session = "0d3f6a52-91b7-4c2e-8a45-3e7b9c1d2f60";
    let (_fake, _) = relayed(&w, session);
    let addr = std::fs::read_to_string(w.dir.join("relay").join(format!("{session}.lock")))
        .ok()
        .and_then(|t| t.lines().nth(1).map(str::to_owned))
        .expect("the relay's address");
    let peer = String::from_utf8_lossy(&w.run(&["register", "peer"]).stdout).trim().to_owned();
    let takeovers = || {
        let logs = Command::new("docker").args(["logs", &w.broker]).output().expect("docker logs");
        let text = String::from_utf8_lossy(&logs.stderr).into_owned() + &String::from_utf8_lossy(&logs.stdout);
        text.lines().filter(|l| l.contains(&format!("Client {addr} already connected"))).count()
    };
    let in_session = |args: &[&str]| {
        let out = w.cmd(Path::new(RUST)).args(args).env("CLAUDE_CODE_SESSION_ID", session).output().expect("the CLI");
        assert!(out.status.success(), "{args:?}: {}", String::from_utf8_lossy(&out.stderr));
    };
    in_session(&["join", &addr, "room1"]);
    in_session(&["join", &peer, "room1"]);
    let deadline = Instant::now() + Duration::from_secs(10);
    while takeovers() == 0 {
        assert!(Instant::now() < deadline, "join never took the relay's session over: the count is broken");
        std::thread::sleep(Duration::from_millis(100));
    }
    // the relay takes its session back about a second after a takeover
    std::thread::sleep(Duration::from_secs(2));
    let before = takeovers();
    in_session(&["say", &addr, "room1", "hello from the shell"]);
    in_session(&["dm", &addr, &peer, "a dm from the shell"]);
    in_session(&["pub", &addr, "topic/a", "a raw post"]);
    in_session(&["describe", &addr, "--about", "posts from the shell"]);
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(takeovers(), before, "a post or describe took the relay's session over");
    let agents = String::from_utf8_lossy(&w.run(&["agents"]).stdout).into_owned();
    assert!(agents.contains("\"about\": \"posts from the shell\""), "{agents}");
    let said = String::from_utf8_lossy(&w.run(&["history", "room1"]).stdout).into_owned();
    assert!(said.contains("hello from the shell"), "{said}");
    let got = String::from_utf8_lossy(&w.run(&["recv", &peer]).stdout).into_owned();
    assert!(got.contains("a dm from the shell") && got.contains(&format!("\"from\": \"{addr}\"")), "{got}");
    let log = relay_log(&w);
    assert!(!log.contains("broker unreachable"), "a takeover was logged as an outage\n{log}");
    assert!(log.lines().all(|l| l.contains(&format!(" {addr}: ")) || l.contains(" session ")), "{log}");
}

fn json_lines(out: &[u8]) -> Vec<serde_json::Value> {
    String::from_utf8_lossy(out).lines().map(|l| serde_json::from_str(l).expect("a JSON line")).collect()
}

/// relay status names the relay's pid, binary and version; doctor flags a relay on a replaced binary and a hook whose
/// program is gone, and relay restart, run inside the session, moves the relay to the current binary.
#[test]
#[ignore = "needs Docker"]
fn doctor_and_relay_restart_see_what_a_relay_runs() {
    let w = start();
    let session = "5b2e8f0c-7d4a-4e61-b3c9-0a1d2e3f4a5b";
    // an "old install": a copy of the binary, which the upgrade then deletes
    let old = w.dir.join("old").join("agent-bridge");
    std::fs::create_dir_all(old.parent().expect("a dir")).expect("a dir");
    install(Path::new(RUST), &old);
    let (mut fake, first) = relayed_by(&w, session, &old);
    let status = json_lines(&w.run(&["relay", "status", "--session", session]).stdout).remove(0);
    assert_eq!(status["pid"], first);
    assert_eq!(status["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(status["binary"], old.display().to_string());
    assert!(status["uptime_s"].as_i64().is_some_and(|u| u >= 0) && status["started"].as_f64().is_some_and(|t| t > 0.0));
    std::fs::remove_file(&old).expect("the old binary goes");

    let home = w.dir.join("home");
    std::fs::create_dir_all(home.join(".claude")).expect("a home");
    let hooks = |commands: &[&str]| {
        let groups: Vec<_> =
            commands.iter().map(|c| serde_json::json!({"hooks": [{"type": "command", "command": c}]})).collect();
        let settings = serde_json::json!({"hooks": {"SessionStart": groups}});
        std::fs::write(home.join(".claude/settings.json"), settings.to_string()).expect("settings");
    };
    let doctor = || {
        let out = w.cmd(Path::new(RUST)).args(["doctor", "--json"]).env("HOME", &home).output().expect("doctor runs");
        let checks: std::collections::HashMap<String, serde_json::Value> = json_lines(&out.stdout)
            .into_iter()
            .map(|c| (c["check"].as_str().unwrap_or_default().to_owned(), c))
            .collect();
        (out.status.code(), checks)
    };
    hooks(&[&format!("{RUST} hook relay-start"), "/nowhere/agent-bridge hook broadcast"]);
    let (code, checks) = doctor();
    assert_eq!(code, Some(1), "a missing hook program fails");
    assert_eq!(checks["hooks"]["status"], "fail");
    assert_eq!(checks["hooks"]["detail"], "missing: /nowhere/agent-bridge");
    for name in ["broker", "ledgerd", "historyd", "state_dir", "cli_lock"] {
        assert_eq!(checks[name]["status"], "ok", "{name}: {}", checks[name]);
    }
    let v = env!("CARGO_PKG_VERSION");
    assert_eq!((&checks["ledgerd"]["version"], &checks["historyd"]["version"]), (&v.into(), &v.into()));
    assert_eq!(checks["relays"]["status"], "warn");
    assert_eq!(checks["relays"]["stale"][0]["session"], session);
    assert_eq!(checks["relays"]["stale"][0]["why"], "its binary was replaced");

    let stdin = fake.0.stdin.as_mut().expect("the fake claude's stdin");
    let restarted = w.dir.join("restart.out");
    writeln!(stdin, "{RUST} relay restart > {} 2>&1", restarted.display()).expect("a command");
    let deadline = Instant::now() + Duration::from_secs(15);
    while !std::fs::read_to_string(&restarted).is_ok_and(|t| t.ends_with('\n')) {
        assert!(Instant::now() < deadline, "relay restart never answered");
        std::thread::sleep(Duration::from_millis(50));
    }
    let row = json_lines(&std::fs::read(&restarted).expect("its output")).remove(0);
    assert_eq!(row["live"], true, "{row}");
    assert_ne!(row["pid"], first);
    assert_eq!(row["binary"], std::fs::canonicalize(RUST).expect("the binary").display().to_string());
    hooks(&[&format!("{RUST} hook relay-start")]);
    let (code, checks) = doctor();
    assert_eq!(code, Some(0), "{checks:?}");
    assert_eq!(checks["relays"]["status"], "ok", "{}", checks["relays"]);
    assert_eq!(checks["relays"]["detail"], "1 current");
}

/// A relay whose binary an upgrade replaced restarts itself on the new one once idle: same pid, its lock held
/// throughout, never on a binary that changed in the last 30 s, and its log says why it waits.
#[test]
#[ignore = "needs Docker"]
fn a_relay_restarts_itself_on_an_upgraded_binary() {
    let w = start();
    let session = "c3d4e5f6-0718-4293-a4b5-c6d7e8f90a1b";
    let bin = w.dir.join("tool").join("agent-bridge");
    std::fs::create_dir_all(bin.parent().expect("a dir")).expect("a dir");
    install(Path::new(RUST), &bin);
    let (_fake, pid) = relayed_with(&w, session, &bin, &[("AGENT_BRIDGE_RELAY_UPGRADE_CHECK", "1")]);
    let status = || json_lines(&w.run(&["relay", "status", "--session", session]).stdout).remove(0);
    // the upgrade: a new file at the same path, as uv tool install --force leaves it
    std::fs::remove_file(&bin).expect("the old binary goes");
    install(Path::new(RUST), &bin);
    let deadline = Instant::now() + Duration::from_secs(10);
    while !relay_log(&w).contains("changed less than 30 s ago") {
        assert!(Instant::now() < deadline, "the relay never noticed the fresh binary\n{}", relay_log(&w));
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(status()["pid"], pid, "it waits while the install may still be writing");
    let settled = Command::new("touch").args(["-d", "-1 minute"]).arg(&bin).status().expect("touch runs");
    assert!(settled.success());
    let deadline = Instant::now() + Duration::from_secs(15);
    // /proc/PID/exe flips at the exec itself, before the new image has logged or rewritten its lock lines
    while status()["binary"] != bin.display().to_string() {
        assert_eq!(status()["live"], true, "the lock is held throughout\n{}", relay_log(&w));
        assert!(Instant::now() < deadline, "the relay never restarted\n{}", relay_log(&w));
        std::thread::sleep(Duration::from_millis(100));
    }
    let kept = format!("session {session}: kept its lock across the restart");
    let log = log_until(&w, "the new image's start lines", |log| {
        log.matches("relaying for claude pid").count() == 2 && log.contains(&kept)
    });
    assert!(log.contains(&format!("binary replaced, restarting on {}", bin.display())), "{log}");
    // its address and version (lock lines 2 and 3) come back once the new image has registered
    let deadline = Instant::now() + Duration::from_secs(10);
    let row = loop {
        let row = status();
        assert_eq!(row["live"], true, "the lock is held throughout\n{}", relay_log(&w));
        if !row["version"].is_null() {
            break row;
        }
        assert!(Instant::now() < deadline, "the new image never wrote its version\n{}", relay_log(&w));
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(row["pid"], pid, "exec keeps the process");
    assert_eq!(row["version"], env!("CARGO_PKG_VERSION"));
}

/// The relay log once done(log) holds, polled up to 15 s: the relay writes it from its own process, after whatever
/// state change the test saw.
fn log_until(w: &World, what: &str, done: impl Fn(&str) -> bool) -> String {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let log = relay_log(w);
        if done(&log) {
            return log;
        }
        assert!(Instant::now() < deadline, "the relay log never showed {what}\n{log}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// The session's inbox socket, as a list of what the relay posted to it.
fn inbox(w: &World) -> std::sync::Arc<std::sync::Mutex<Vec<String>>> {
    let posts = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let listener = std::os::unix::net::UnixListener::bind(w.dir.join("inbox.sock")).expect("an inbox socket");
    let kept = posts.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut text = String::new();
            let _ = std::io::Read::read_to_string(&mut &stream, &mut text);
            kept.lock().unwrap_or_else(|e| e.into_inner()).push(text);
        }
    });
    posts
}

/// relay wake ROOM mentions: a plain post in that room waits for the next wake instead of waking the session, and
/// comes along with the post that mentions it.
#[test]
#[ignore = "needs Docker"]
fn a_wake_filter_holds_plain_posts_until_a_mention() {
    let w = start();
    let posts = inbox(&w);
    let session = "4a5b6c7d-8e9f-40a1-b2c3-d4e5f6a7b8c9";
    let (_fake, _) = relayed(&w, session);
    let addr = std::fs::read_to_string(w.dir.join("relay").join(format!("{session}.lock")))
        .ok()
        .and_then(|t| t.lines().nth(1).map(str::to_owned))
        .expect("the relay's address");
    // the filter first, so not even the join notices below can wake the session, however late they arrive
    assert!(w.run(&["relay", "wake", "lane", "mentions", "--session", session]).status.success());
    let peer = String::from_utf8_lossy(&w.run(&["register", "peer"]).stdout).trim().to_owned();
    for who in [&addr, &peer] {
        assert!(w.run(&["join", who, "lane"]).status.success());
    }
    // the relay takes its session back about a second after the join took it over
    std::thread::sleep(Duration::from_secs(3));
    posts.lock().unwrap_or_else(|e| e.into_inner()).clear();
    assert!(w.run(&["say", &peer, "lane", "just chatter"]).status.success());
    std::thread::sleep(Duration::from_secs(3));
    let seen = posts.lock().unwrap_or_else(|e| e.into_inner()).clone();
    assert!(seen.is_empty(), "a plain post woke the session: {seen:?}");
    assert!(w.run(&["say", &peer, "lane", &format!("@{addr} over to you")]).status.success());
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let seen = posts.lock().unwrap_or_else(|e| e.into_inner()).concat();
        if seen.contains("over to you") {
            assert!(seen.contains("just chatter") && seen.contains("while you were away"), "{seen}");
            // the relay path's id is the one every other path derives (_native.message_id binds this function)
            // each post is an auth line, then a user line whose content is the frame, one JSON line per message
            let frames: Vec<String> = seen
                .lines()
                .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
                .filter_map(|m| m["message"]["content"].as_str().map(str::to_owned))
                .collect();
            let line = frames
                .iter()
                .flat_map(|f| f.lines())
                .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
                .find(|m| m["body"].as_str().is_some_and(|b| b.contains("over to you")))
                .expect("the delivered line");
            let want = agent_bridge::protocol::message_id(
                line["topic"].as_str().expect("topic"),
                line["from"].as_str().expect("from"),
                line["ts"].as_f64().expect("ts"),
            );
            assert_eq!(line["id"], want.as_str(), "{line}");
            break;
        }
        assert!(Instant::now() < deadline, "the mention never woke the session\n{}", relay_log(&w));
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// An agent registered from inside a session after its relay started becomes the one the relay follows, so a post in
/// a room that agent joined wakes the session.
#[test]
#[ignore = "needs Docker"]
fn a_relay_follows_an_agent_registered_after_it_started() {
    let w = start();
    let posts = inbox(&w);
    let session = "6e2a9c41-5b7d-4f08-a3c6-9d1e2f4a7b58";
    let (_fake, _) = relayed(&w, session);
    let lock = w.dir.join("relay").join(format!("{session}.lock"));
    let held = || std::fs::read_to_string(&lock).ok().and_then(|t| t.lines().nth(1).map(str::to_owned));
    let first = held().expect("the relay's first address");
    let in_session = |args: &[&str]| {
        let out = w.cmd(Path::new(RUST)).args(args).env("CLAUDE_CODE_SESSION_ID", session).output().expect("the CLI");
        assert!(out.status.success(), "{args:?}: {}", String::from_utf8_lossy(&out.stderr));
        String::from_utf8_lossy(&out.stdout).trim().to_owned()
    };
    let named = in_session(&["register", "named"]);
    assert_ne!(named, first, "register reused the relay's default address");
    in_session(&["join", &named, "lane"]);
    let deadline = Instant::now() + Duration::from_secs(15);
    while held().as_deref() != Some(named.as_str()) {
        assert!(Instant::now() < deadline, "the relay still holds {:?}, not {named}\n{}", held(), relay_log(&w));
        std::thread::sleep(Duration::from_millis(100));
    }
    let peer = String::from_utf8_lossy(&w.run(&["register", "peer"]).stdout).trim().to_owned();
    assert!(w.run(&["join", &peer, "lane"]).status.success());
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        assert!(w.run(&["say", &peer, "lane", &format!("@{named} are you there")]).status.success());
        let seen = posts.lock().unwrap_or_else(|e| e.into_inner()).concat();
        if seen.contains("are you there") {
            break;
        }
        assert!(Instant::now() < deadline, "the post never woke the session\n{}", relay_log(&w));
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// The 0.3.1 commands from the shell: a pin on a room, a reply by id that history keeps, a mention of someone who left,
/// and claims, held, refused, listed and released.
#[test]
#[ignore = "needs Docker"]
fn pins_replies_and_claims_from_the_shell() {
    let w = start();
    let ok = |args: &[&str]| {
        let out = w.run(args);
        assert!(out.status.success(), "{args:?}: {}", String::from_utf8_lossy(&out.stderr));
        out
    };
    let a = String::from_utf8_lossy(&ok(&["register", "alpha"]).stdout).trim().to_owned();
    let row = json_lines(&ok(&["register", "beta", "--json"]).stdout).remove(0);
    let b = row["addr"].as_str().expect("register --json names the address").to_owned();
    assert_eq!(row["name"], "beta");
    for who in [&a, &b] {
        ok(&["join", who, "lane"]);
    }
    // a pin needs a moderator or admin: alpha is neither, until the room's record (as the console writes it) names it
    assert!(!w.run(&["pin", &a, "lane", "the format"]).status.success());
    let record = format!(
        r#"{{"mode": "open", "moderators": ["{a}"], "rules": "", "muted": [], "banned": [], "by": "admin", "ts": 1.0}}"#
    );
    let named = Command::new("docker")
        .args(["exec", &w.broker, "mosquitto_pub", "-q", "1", "-r", "-t", "rooms/lane", "-m", &record])
        .status()
        .expect("docker exec runs");
    assert!(named.success());
    let deadline = Instant::now() + Duration::from_secs(5);
    while !w.run(&["pin", &a, "lane", "the format"]).status.success() {
        assert!(Instant::now() < deadline, "alpha never became a moderator");
        std::thread::sleep(Duration::from_millis(100));
    }
    let pin = json_lines(&ok(&["pin", &a, "lane", "the format"]).stdout).remove(0);
    assert_eq!((pin["text"].as_str(), pin["by"].as_str()), (Some("the format"), Some(a.as_str())));
    assert_eq!(json_lines(&ok(&["room", "info", "lane"]).stdout)[0]["pin"]["text"], "the format");
    ok(&["pin", &a, "lane", "--clear"]);
    assert_eq!(json_lines(&ok(&["room", "info", "lane"]).stdout)[0]["pin"], serde_json::Value::Null);

    ok(&["say", &a, "lane", "a question"]);
    let asked = json_lines(&ok(&["recv", &b]).stdout);
    let id = asked.iter().find(|m| m["body"] == "a question").and_then(|m| m["id"].as_str()).expect("an id").to_owned();
    ok(&["say", &b, "lane", "the answer", "--reply-to", &id]);
    let history = json_lines(&ok(&["history", "lane"]).stdout);
    let answer = history.iter().find(|m| m["body"] == "the answer").expect("the reply in history");
    assert_eq!(answer["reply_to"], id.as_str());
    let refused = w.run(&["say", &b, "lane", "x", "--reply-to", "nope"]);
    assert!(!refused.status.success() && String::from_utf8_lossy(&refused.stderr).contains("bad reply_to"));
    ok(&["part", &a, "lane"]);
    let said = ok(&["say", &b, "lane", &format!("@{a} still there?")]);
    let notice = String::from_utf8_lossy(&said.stderr).into_owned();
    assert!(notice.contains(&format!("@{a} is not in #lane (it left or never joined)")), "{notice}");

    let claim = json_lines(&ok(&["claim", &b, "lane", "BUG-1", "--note", "on it"]).stdout).remove(0);
    assert_eq!((claim["claim"]["holder"].as_str(), claim["claim"]["note"].as_str()), (Some(b.as_str()), Some("on it")));
    let taken = w.run(&["claim", &a, "lane", "BUG-1"]);
    assert_eq!(taken.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&taken.stderr).contains(&format!("claimed by {b}")));
    let listed = json_lines(&ok(&["claims", &a, "lane"]).stdout).remove(0);
    assert_eq!(listed["claims"][0]["key"], "BUG-1");
    ok(&["unclaim", &b, "lane", "BUG-1"]);
    assert_eq!(json_lines(&ok(&["claims", &a, "lane"]).stdout)[0]["claims"], serde_json::json!([]));
    ok(&["add", &b, "db", "--visibility", "public", "--max-level", "write"]);
    let held = json_lines(&ok(&["holders", &a, &format!("{b}/db")]).stdout).remove(0);
    assert_eq!(held["holders"], serde_json::json!([]));
}

/// A failed exec (here the exec target is forced to a missing file) leaves the relay running on its old image: still
/// live, same pid, still delivering, and it does not retry at once.
#[test]
#[ignore = "needs Docker"]
fn a_relay_survives_a_failed_restart() {
    let w = start();
    let posts = inbox(&w);
    let session = "d4e5f6a7-b8c9-40d1-82e3-f4a5b6c7d8e9";
    let bin = w.dir.join("tool").join("agent-bridge");
    std::fs::create_dir_all(bin.parent().expect("a dir")).expect("a dir");
    install(Path::new(RUST), &bin);
    let env = [("AGENT_BRIDGE_RELAY_UPGRADE_CHECK", "1"), ("AGENT_BRIDGE_RELAY_EXEC_AS", "/nowhere/agent-bridge")];
    let (_fake, pid) = relayed_with(&w, session, &bin, &env);
    let addr = std::fs::read_to_string(w.dir.join("relay").join(format!("{session}.lock")))
        .ok()
        .and_then(|t| t.lines().nth(1).map(str::to_owned))
        .expect("the relay's address");
    std::fs::remove_file(&bin).expect("the old binary goes");
    install(Path::new(RUST), &bin);
    assert!(Command::new("touch").args(["-d", "-1 minute"]).arg(&bin).status().expect("touch").success());
    let deadline = Instant::now() + Duration::from_secs(15);
    while !relay_log(&w).contains("still running on the old image") {
        assert!(Instant::now() < deadline, "the exec never failed\n{}", relay_log(&w));
        std::thread::sleep(Duration::from_millis(100));
    }
    let row = json_lines(&w.run(&["relay", "status", "--session", session]).stdout).remove(0);
    assert_eq!((&row["live"], &row["pid"]), (&serde_json::json!(true), &serde_json::json!(pid)));
    let peer = String::from_utf8_lossy(&w.run(&["register", "peer"]).stdout).trim().to_owned();
    assert!(w.run(&["dm", &peer, &addr, "still there?"]).status.success());
    let deadline = Instant::now() + Duration::from_secs(10);
    while !posts.lock().unwrap_or_else(|e| e.into_inner()).concat().contains("still there?") {
        assert!(Instant::now() < deadline, "nothing delivered after the failed exec\n{}", relay_log(&w));
        std::thread::sleep(Duration::from_millis(100));
    }
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(relay_log(&w).matches("exec failed").count(), 1, "no retry within 10 minutes\n{}", relay_log(&w));
}

#[test]
fn normalises_times_and_ids() {
    let text = r#"{"ts": 1793294701.3971186, "id": "0123456789abcdef", "token": 3, "n": 12345678901}"#;
    assert_eq!(normal(text), r#"{"ts": <t>, "id": "<id>", "token": 3, "n": 12345678901}"#);
    // an id that starts with a time-shaped run of digits is still an id, and a quoted time is still a time
    let text = r#"{"id": "1234567890abcdef", "ts": 1794957030.0274324, "seen": "1794957030"}"#;
    assert_eq!(normal(text), r#"{"id": "<id>", "ts": <t>, "seen": "<t>"}"#);
}

/// A scratch repository with a linked worktree "lane" on branch lane: (main, lane), both real paths.
fn scratch_repo(dir: &Path) -> (PathBuf, PathBuf) {
    let (main, lane) = (dir.join("repo"), dir.join("lane"));
    let git = |args: &[&str]| {
        let out = Command::new("git")
            .args(args)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .expect("git runs");
        assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
    };
    let (m, l) = (main.to_str().expect("UTF-8"), lane.to_str().expect("UTF-8"));
    git(&["init", "-q", "-b", "main", m]);
    git(&["-C", m, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "--allow-empty", "-m", "i"]);
    git(&["-C", m, "worktree", "add", "-q", "-b", "lane", l]);
    (main.canonicalize().expect("main"), lane.canonicalize().expect("lane"))
}

/// worktree acquire takes the lease and git-locks the worktree with the pinned reason; another agent is refused
/// naming the holder; the ref-guard refuses a commit there from another session, allows the holder's and warns
/// outside a session; release saves uncommitted work under refs/agent-bridge/released/<id> and unlocks; --exec holds
/// the lock while its command runs; a relay renews a session's lease past its ttl and releases it when the session
/// ends.
#[test]
#[ignore = "needs Docker"]
fn worktree_locks_from_the_shell_and_the_relay() {
    let w = start();
    let (main, lane) = scratch_repo(&w.dir);
    let lane_s = lane.to_str().expect("UTF-8").to_owned();
    let locked = main.join(".git/worktrees/lane/locked");
    let (holder, other) = ("8a0e3c11-6a1f-4c55-9d1e-2b3c4d5e6f70", "8a0e3c11-6a1f-4c55-9d1e-2b3c4d5e6f71");
    let as_session = |session: Option<&str>, args: &[&str]| {
        let mut c = w.cmd(Path::new(RUST));
        if let Some(s) = session {
            c.env("CLAUDE_CODE_SESSION_ID", s);
        }
        c.args(args).output().expect("the CLI")
    };
    let a = String::from_utf8_lossy(&as_session(Some(holder), &["register", "wt-a"]).stdout).trim().to_owned();
    let b = String::from_utf8_lossy(&w.run(&["register", "wt-b"]).stdout).trim().to_owned();

    let got = as_session(Some(holder), &["worktree", "acquire", &a, &format!("{lane_s}/."), "--ttl", "120"]);
    assert!(got.status.success(), "{}", String::from_utf8_lossy(&got.stderr));
    let got = &json_lines(&got.stdout)[0];
    let id = got["lease_id"].as_str().expect("a lease id").to_owned();
    assert_eq!((got["path"].as_str(), got["git_lock"].as_bool()), (Some(lane_s.as_str()), Some(true)), "{got}");
    assert_eq!(got["renewed_by"], "none", "no relay in this session yet");
    let reason = format!("agent-bridge lease {id} held by {a}");
    assert_eq!(std::fs::read_to_string(&locked).expect("git's lock").trim(), reason);
    let status = &json_lines(&w.run(&["worktree", "status", &lane_s]).stdout)[0];
    assert_eq!((status["holder"].as_str(), status["stale"].as_bool()), (Some(a.as_str()), Some(false)), "{status}");
    let busy = w.run(&["worktree", "acquire", &b, &lane_s]);
    assert!(!busy.status.success() && String::from_utf8_lossy(&busy.stderr).contains(&a), "names the holder");

    // the ref guard, as git's reference-transaction hook of this repository
    let hook = main.join(".git/hooks/reference-transaction");
    // written beside it and installed by cp, so no write fd to the hook git execs is ever open here
    let draft = w.dir.join("reference-transaction.draft");
    std::fs::write(&draft, format!("#!/bin/sh\nexec {RUST} hook ref-guard \"$@\"\n")).expect("the hook");
    std::fs::set_permissions(&draft, std::os::unix::fs::PermissionsExt::from_mode(0o755)).expect("chmod");
    install(&draft, &hook);
    let commit = |session: Option<&str>, dir: &Path| {
        let mut c = w.cmd(Path::new("git"));
        c.args(["-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "--allow-empty", "-m", "x"])
            .current_dir(dir)
            .env("GIT_CONFIG_GLOBAL", "/dev/null");
        if let Some(s) = session {
            c.env("CLAUDE_CODE_SESSION_ID", s);
        }
        c.output().expect("git runs")
    };
    let refused = commit(Some(other), &lane);
    let text = String::from_utf8_lossy(&refused.stderr).into_owned();
    assert!(!refused.status.success(), "another session committed in the leased worktree");
    let want = format!("blocked: the update of refs/heads/lane is inside {lane_s}, leased by {a} until ");
    assert!(
        text.contains(&want)
            && text.contains(&format!(
                "(lease {id}). To work there: agent-bridge worktree acquire <your address> {lane_s}"
            )),
        "{text}"
    );
    assert!(commit(Some(holder), &lane).status.success(), "the holder's session commits");
    assert!(commit(Some(other), &main).status.success(), "the main branch is not leased");
    let outside = commit(None, &lane);
    assert!(outside.status.success(), "outside a session it is allowed");
    assert!(String::from_utf8_lossy(&outside.stderr).contains("allowed outside an agent session, but blocked:"));

    // release saves what is uncommitted and unlocks
    std::fs::write(lane.join("work.txt"), "unsaved\n").expect("a change");
    let released = &json_lines(&w.run(&["worktree", "release", &a, &lane_s]).stdout)[0];
    let saved = format!("refs/agent-bridge/released/{id}");
    assert_eq!((released["snapshot"].as_str(), released["unlocked"].as_bool()), (Some(saved.as_str()), Some(true)));
    assert!(!locked.exists(), "release removes the git lock");
    let show = Command::new("git").args(["-C", main.to_str().unwrap(), "show", &format!("{saved}:work.txt")]).output();
    assert_eq!(String::from_utf8_lossy(&show.expect("git").stdout), "unsaved\n");
    assert!(lane.join("work.txt").exists(), "the files stay as they were");
    let free = &json_lines(&w.run(&["worktree", "status", &lane_s]).stdout)[0];
    assert!(free.get("holder").is_none() && free["lock"].is_null(), "{free}");

    // --exec: the lock is there while the command runs, and gone after
    let check = format!("test -f {}", locked.display());
    let exec = w.run(&["worktree", "acquire", &b, &lane_s, "--exec", "sh", "-c", &check]);
    assert!(exec.status.success(), "the lock was not held during --exec: {}", String::from_utf8_lossy(&exec.stderr));
    assert!(!locked.exists(), "--exec unlocks after its command");

    // a session's relay renews the lease past its ttl, and releases and unlocks it when the session ends
    let (fake, relay) = relayed(&w, holder);
    let got = as_session(Some(holder), &["worktree", "acquire", &a, &lane_s, "--ttl", "3"]);
    let got = &json_lines(&got.stdout)[0];
    assert_eq!(got["renewed_by"], "relay", "{got}");
    let id = got["lease_id"].as_str().expect("a lease id").to_owned();
    std::thread::sleep(Duration::from_secs(5));
    let status = &json_lines(&w.run(&["worktree", "status", &lane_s]).stdout)[0];
    assert_eq!(
        status["lease_id"].as_str(),
        Some(id.as_str()),
        "the relay renewed it past its 3 s ttl\n{}",
        relay_log(&w)
    );
    std::fs::write(lane.join("more.txt"), "at the end\n").expect("a change");
    drop(fake);
    let log = log_until(&w, "the session-end release", |l| l.contains(&format!("released worktree {lane_s}")));
    assert!(log.contains(&format!("changes saved as refs/agent-bridge/released/{id}")), "{log}");
    while Path::new(&format!("/proc/{relay}")).exists() {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(!locked.exists(), "the session's end unlocks it");
    let free = &json_lines(&w.run(&["worktree", "status", &lane_s]).stdout)[0];
    assert!(free.get("holder").is_none(), "{free}");
}
