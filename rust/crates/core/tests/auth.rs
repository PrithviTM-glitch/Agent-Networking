//! Auth on a disposable broker started through config/broker-entrypoint.sh (compose.auth.yaml's broker): warn writes
//! the admin and service credentials and lets anonymous clients in; provisioning makes an agent's client and role,
//! whose literal topics refuse another agent's sender level, inbox and reply topics; the unix socket works; enforce
//! refuses anonymous clients and keeps credentialed ones. Needs Docker: cargo test -p agent-bridge-core -- --ignored.

#![allow(
    clippy::let_underscore_must_use,
    reason = "best-effort cleanup: killing, waiting for and removing what the test started"
)]
#![expect(clippy::disallowed_methods, reason = "the test checks the credential files directly")]

#[path = "common/bins.rs"]
mod bins;

use agent_bridge::broker::{self, Ignore};
use agent_bridge::bus::Bus;
use agent_bridge::config::{self, Auth, Credentials};
use agent_bridge::validation::is_refused;
use agent_bridge::{presence, provision, rooms};
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct World {
    name: String,
    volume: String,
    dir: PathBuf,
    port: u16,
}

impl Drop for World {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.name]).output();
        let _ = Command::new("docker").args(["volume", "rm", "-f", &self.volume]).output();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl World {
    fn cfg(&self) -> PathBuf {
        self.dir.join("xdg/agent-bridge")
    }

    fn run_dir(&self) -> PathBuf {
        self.dir.join("state/agent-bridge/broker")
    }

    /// (Re)start the broker in mode, on the same data volume and directories, and wait until it is ready.
    fn up(&self, mode: &str) {
        // a graceful stop, as compose's recreate does, so mosquitto saves its retained records first
        let _ = Command::new("docker").args(["stop", &self.name]).output();
        let _ = Command::new("docker").args(["rm", "-f", &self.name]).output();
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..").canonicalize().expect("the repo");
        let started = Command::new("docker")
            .args(["run", "-d", "--name", &self.name, "-e", &format!("AGENT_BRIDGE_AUTH={mode}")])
            .args(["-p", &format!("127.0.0.1:{}:1883", self.port)])
            .args(["-v", &format!("{}/config/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro", repo.display())])
            .args(["-v", &format!("{}/config/broker-entrypoint.sh:/agent-bridge/entrypoint.sh:ro", repo.display())])
            .args(["-v", &format!("{}:/mosquitto/data", self.volume)])
            .args(["-v", &format!("{}:/agent-bridge-config", self.cfg().display())])
            .args(["-v", &format!("{}:/run/agent-bridge", self.run_dir().display())])
            .args(["--entrypoint", "/bin/sh", "eclipse-mosquitto:2.0.22", "/agent-bridge/entrypoint.sh"])
            .output()
            .expect("docker runs");
        assert!(started.status.success(), "{}", String::from_utf8_lossy(&started.stderr));
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let ready =
                Command::new("docker").args(["exec", &self.name, "test", "-f", "/tmp/agent-bridge-ready"]).output();
            if ready.is_ok_and(|o| o.status.success()) {
                return;
            }
            let logs = Command::new("docker").args(["logs", &self.name]).output().expect("docker logs");
            assert!(Instant::now() < deadline, "not ready: {}", String::from_utf8_lossy(&logs.stderr));
            std::thread::sleep(Duration::from_millis(200));
        }
    }
}

/// A child process killed when the test ends, failed assertions included.
struct Killed(std::process::Child);

impl Killed {
    fn wait_with_output_stderr(&mut self) -> String {
        let mut text = String::new();
        if let Some(mut err) = self.0.stderr.take() {
            let _ = std::io::Read::read_to_string(&mut err, &mut text);
        }
        text
    }
}

impl Drop for Killed {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Both tests here set the process environment: one at a time.
static ENV: Mutex<()> = Mutex::new(());

fn world() -> World {
    let port = std::net::TcpListener::bind("127.0.0.1:0").and_then(|l| l.local_addr()).expect("a free port").port();
    let name = format!("agent-bridge-core-auth-{}", std::process::id());
    let dir = std::env::temp_dir().join(&name);
    let w = World { volume: format!("{name}-data"), name, dir, port };
    for d in [w.cfg(), w.run_dir()] {
        std::fs::DirBuilder::new().recursive(true).mode(0o700).create(&d).expect("a dir");
    }
    // SAFETY: set once, before any thread of this test reads the environment.
    unsafe {
        std::env::set_var("AGENT_BRIDGE_PORT", port.to_string());
        std::env::set_var("XDG_CONFIG_HOME", w.dir.join("xdg"));
        std::env::set_var("XDG_STATE_HOME", w.dir.join("state"));
        std::env::set_var("AGENT_BRIDGE_LOG", w.dir.join("history.jsonl"));
        for name in ["CLAUDE_CODE_SESSION_ID", "AGENT_BRIDGE_AUTH", "AGENT_BRIDGE_CREDENTIALS", "AGENT_BRIDGE_SOCKET"] {
            std::env::remove_var(name);
        }
    }
    w
}

fn mode(path: &Path) -> u32 {
    std::fs::metadata(path).expect("the file").permissions().mode() & 0o777
}

fn publish(creds: Option<&Credentials>, topic: &str) -> agent_bridge::Result<()> {
    let c = broker::one_shot_with("", creds, Arc::new(Ignore))?;
    broker::publish(&c, topic, "{}", false, None)
}

fn subscribe(creds: Option<&Credentials>, topic: &str) -> agent_bridge::Result<()> {
    let c = broker::one_shot_with("", creds, Arc::new(Ignore))?;
    broker::subscribe(&c, &[topic], true, false)
}

#[test]
#[ignore]
fn warn_provision_refusals_socket_then_enforce() {
    let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
    let w = world();
    // an agent from before auth: registered under off, it has no credentials file
    w.up("off");
    assert!(!broker::runs_auth().expect("reachable"), "no dynsec plugin under off");
    // agents from before auth, registered under off: a register-made one and a session's own (8 hex, its relay's
    // and MCP server's), with no credentials files
    let (old, session_addr) = ("old-c0ffee", "oldproj-0a1b2c3d");
    for addr in [old, session_addr, "hand-picked"] {
        Bus::register("old", &[], false, Some(addr), false, true).expect("registers under off").close();
        assert!(!w.cfg().join(format!("agents/{addr}.json")).exists(), "nothing to provision with under off");
    }
    w.up("warn");
    assert!(broker::runs_auth().expect("reachable"), "warn runs the plugin");
    for f in ["admin.json", "services/ledgerd.json", "services/historyd.json"] {
        assert_eq!(mode(&w.cfg().join(f)), 0o600, "{f}");
    }
    assert_eq!(mode(&w.cfg().join("services")), 0o700);
    assert_eq!(config::auth_mode(), Ok(Auth::Warn), "read from the auth-mode file the entrypoint wrote");
    assert!(config::credentials("ledgerd").expect("readable").is_none(), "services live under services/");

    // anonymous 0.3.1 clients keep full rights in warn
    publish(None, "room/r").expect("anonymous posts in warn");
    subscribe(None, "inbox/bo-b2c3d4").expect("anonymous reads in warn");

    // provisioning: a client and literal role per address, and a private file recording the session
    let session = "0b9d6a3e-5f39-4c43-9d0a-2b1f6c1e9a01";
    let al = provision::ensure("al-a1b2c3", Some(session)).expect("provisions").expect("auth is on");
    let file = w.cfg().join("agents/al-a1b2c3.json");
    assert_eq!((mode(&file), mode(&w.cfg().join("agents"))), (0o600, 0o700));
    assert_eq!(provision::bound_session("al-a1b2c3").as_deref(), Some(session));
    assert_eq!(provision::ensure("al-a1b2c3", None).expect("again"), Some(al.clone()), "a second call reuses the file");
    assert!(al.client_id.starts_with("c-") && al.client_id.len() == 34, "a secret id, never the address");

    let al = Some(&al);
    for topic in [
        "room/r/al-a1b2c3",
        "room/r",
        "inbox/bo-b2c3d4/al-a1b2c3",
        "agents/al-a1b2c3",
        "ledger/req/al-a1b2c3",
        "ci/build",
    ] {
        publish(al, topic).unwrap_or_else(|e| panic!("{topic}: {e}"));
    }
    for topic in [
        "room/r/bo-b2c3d4",
        "inbox/al-a1b2c3/bo-b2c3d4",
        "agents/bo-b2c3d4",
        "ledger/req/bo-b2c3d4",
        "history/req/bo-b2c3d4",
        "broadcast",
    ] {
        let e = publish(al, topic).expect_err(topic);
        assert_eq!(e.0, format!("not authorized to publish on {topic}"));
        assert!(is_refused(&e));
    }
    for topic in ["inbox/al-a1b2c3", "inbox/al-a1b2c3/+", "room/r/+", "agents/+", "ledger/resp/al-a1b2c3/+", "ci/#"] {
        subscribe(al, topic).unwrap_or_else(|e| panic!("{topic}: {e}"));
    }
    for topic in ["inbox/bo-b2c3d4", "inbox/+", "inbox/#", "ledger/resp/bo-b2c3d4/+", "history/resp/#"] {
        let e = subscribe(al, topic).expect_err(topic);
        assert_eq!(e.0, format!("not authorized to subscribe to {topic}"));
        assert!(is_refused(&e));
    }
    // create only: an address whose client exists is never re-provisioned, so its password survives
    let saved = std::fs::read(&file).expect("the file");
    std::fs::remove_file(&file).expect("lose the file");
    assert_eq!(
        provision::ensure("al-a1b2c3", Some(session)).expect_err("exists elsewhere").0,
        "al-a1b2c3 already has credentials elsewhere; ask admin to reissue"
    );
    std::fs::write(&file, saved).expect("put it back");
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    publish(al, "room/r/al-a1b2c3").expect("the old password still logs in");
    assert!(provision::ensure("admin", None).expect_err("admin").0.contains("reserved for the admin console"));
    for addr in ["ledgerd", "historyd", "hand-picked"] {
        assert_eq!(provision::ensure(addr, None), Ok(None), "{addr} stays anonymous, never an error");
    }
    // agents from before auth get credentials when their relay starts again (Bus::register, as the relay does) or
    // their Bus reattaches (the MCP server's attach); a hand-picked address carries on anonymously
    Bus::register("old", &[], false, Some(old), false, true).expect("re-registers under warn").close();
    let migrated = config::credentials(old).expect("readable").expect("provisioned on its relay's restart");
    Bus::attach(session_addr).expect("reattaches under warn").close();
    let session_creds = config::credentials(session_addr).expect("readable").expect("provisioned on reattach");
    Bus::register("hand", &[], false, Some("hand-picked"), false, true).expect("anonymous in warn").close();
    assert!(config::credentials("hand-picked").expect("readable").is_none());
    let wrong = Credentials { password: "wrong".into(), ..al.cloned().expect("creds") };
    assert_eq!(
        publish(Some(&wrong), "ci/x").expect_err("a bad password").0,
        "broker did not accept the login of al-a1b2c3"
    );

    // the own-session rule
    assert_eq!(provision::check_own("al-a1b2c3"), Ok(()), "outside a session");
    unsafe { std::env::set_var("CLAUDE_CODE_SESSION_ID", "7c1e2d4f-0000-4000-8000-000000000001") };
    assert_eq!(
        provision::check_own("al-a1b2c3").expect_err("another session").0,
        "agent-bridge: al-a1b2c3 is not your agent (bound to another session)"
    );
    unsafe { std::env::set_var("CLAUDE_CODE_SESSION_ID", session.to_uppercase()) };
    assert_eq!(provision::check_own("al-a1b2c3"), Ok(()), "its own session, in any case");
    assert_eq!(provision::check_own("no-file-9"), Ok(()), "no credentials, no binding");
    unsafe { std::env::remove_var("CLAUDE_CODE_SESSION_ID") };

    // the unix socket, owned by this user, takes precedence over host and port
    let sock = w.run_dir().join("mqtt.sock");
    let meta = std::fs::metadata(&sock).expect("the socket");
    assert_eq!(std::os::unix::fs::MetadataExt::uid(&meta), nix::unistd::getuid().as_raw());
    unsafe { std::env::set_var("AGENT_BRIDGE_SOCKET", &sock) };
    assert_eq!(config::socket(), Some(sock.clone()));
    publish(al, "room/r/al-a1b2c3").expect("over the socket");
    unsafe { std::env::remove_var("AGENT_BRIDGE_SOCKET") };
    assert_eq!(config::socket(), None, "AGENT_BRIDGE_PORT picks TCP");

    // enforce: anonymous refused, credentials unchanged, the mode file follows
    w.up("enforce");
    assert_eq!(config::auth_mode(), Ok(Auth::Enforce));
    assert_eq!(publish(None, "room/r").expect_err("anonymous").0, "this broker requires credentials (auth enforce)");
    publish(al, "room/r/al-a1b2c3").expect("credentials still work");
    assert!(broker::runs_auth().expect("reachable"), "enforce turns anonymous away");
    publish(Some(&migrated), &format!("room/r/{old}")).expect("the migrated agent works under enforce");
    Bus::register("old", &[], false, Some(old), false, true).expect("and its relay starts under enforce").close();
    publish(Some(&session_creds), &format!("room/r/{session_addr}")).expect("the session agent works under enforce");
    Bus::attach(session_addr).expect("and reattaches under enforce").close();
    let hand = Bus::register("hand", &[], false, Some("hand-picked"), false, true).map(|b| b.close());
    assert_eq!(hand.expect_err("anonymous under enforce").0, "this broker requires credentials (auth enforce)");
    // helper reads log in: as the agent bound to this session, as admin outside any session (the console, a
    // terminal), and not at all from a session with no agent of its own
    unsafe { std::env::set_var("CLAUDE_CODE_SESSION_ID", session) };
    presence::agents(None).expect("agents, as al-a1b2c3");
    rooms::listing().expect("rooms, as al-a1b2c3");
    rooms::info("r").expect("room info, as al-a1b2c3");
    assert!(presence::find("al-a1b2c3").is_ok(), "the presence lookup register makes");
    let _ = agent_bridge::guard::me(session);
    // never admin by default: no session id and no AGENT_BRIDGE_CREDENTIALS is anonymous, refused
    let refused = "this broker requires credentials (auth enforce)";
    unsafe { std::env::remove_var("CLAUDE_CODE_SESSION_ID") };
    assert_eq!(presence::agents(None).expect_err("outside a session").0, refused);
    unsafe { std::env::set_var("CLAUDE_CODE_SESSION_ID", "7c1e2d4f-0000-4000-8000-000000000002") };
    assert_eq!(presence::agents(None).expect_err("a session with no agent").0, refused);
    unsafe { std::env::remove_var("CLAUDE_CODE_SESSION_ID") };
    // the invariant at every entry point: naming admin without AGENT_BRIDGE_CREDENTIALS is anonymous (refused under
    // enforce), and another session's agent is "not your agent"
    let not_yours = "agent-bridge: al-a1b2c3 is not your agent (bound to another session)";
    let entry_points = |addr: &str| -> Vec<(&str, String)> {
        let text = |r: Result<(), String>| r.err().unwrap_or_else(|| "succeeded".into());
        vec![
            (
                "ledger",
                text(
                    agent_bridge::ledger::call("ping", addr, Default::default(), Some(3.0))
                        .map(drop)
                        .map_err(|e| e.to_string()),
                ),
            ),
            (
                "history",
                text(
                    agent_bridge::history::client::call("ping", Some(addr), Default::default(), 3.0)
                        .map(drop)
                        .map_err(|e| e.to_string()),
                ),
            ),
            ("retained", text(broker::retained_as(&["agents/+"], Some(addr)).map(drop).map_err(|e| e.0))),
            ("one-shot", text(broker::one_shot_as("", Some(addr), Arc::new(Ignore)).map(drop).map_err(|e| e.0))),
            ("bus", text(agent_bridge::bus::Bus::attach(addr).map(drop).map_err(|e| e.0))),
        ]
    };
    let mut wrong: Vec<String> = entry_points("admin")
        .into_iter()
        .filter(|(_, got)| !got.contains(refused))
        .map(|(what, got)| format!("{what} as admin with no variable: {got}"))
        .collect();
    unsafe { std::env::set_var("CLAUDE_CODE_SESSION_ID", "7c1e2d4f-0000-4000-8000-000000000002") };
    wrong.extend(
        entry_points("al-a1b2c3")
            .into_iter()
            .filter(|(_, got)| !got.contains(not_yours))
            .map(|(what, got)| format!("{what} as another session's agent: {got}")),
    );
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    assert_eq!(provision::ensure("al-a1b2c3", None).expect_err("bound elsewhere").0, not_yours);
    assert_eq!(provision::credentials_for("admin"), Ok(None), "admin.json on disk is not enough");
    unsafe { std::env::remove_var("CLAUDE_CODE_SESSION_ID") };
    // the console and agent-bridge-admin set it deliberately
    unsafe { std::env::set_var("AGENT_BRIDGE_CREDENTIALS", w.cfg().join("admin.json")) };
    presence::agents(None).expect("agents, as admin");
    rooms::info("r").expect("room info, as admin");
    assert!(provision::credentials_for("admin").expect("readable").is_some(), "named by the variable");
    unsafe { std::env::remove_var("AGENT_BRIDGE_CREDENTIALS") };
    let ledgerd: Credentials = {
        unsafe { std::env::set_var("AGENT_BRIDGE_CREDENTIALS", w.cfg().join("services/ledgerd.json")) };
        config::credentials("ledgerd").expect("readable").expect("the service file")
    };
    subscribe(Some(&ledgerd), "ledger/req/+").expect("ledgerd reads requests");
    publish(Some(&ledgerd), "ledger/resp/al-a1b2c3/x").expect("ledgerd replies");
    assert!(is_refused(&publish(Some(&ledgerd), "room/r").expect_err("ledgerd does not post")));
    unsafe { std::env::remove_var("AGENT_BRIDGE_CREDENTIALS") };

    // a refused retained read is an error, never an empty answer
    let refused_read = broker::retained_as(&["inbox/bo-b2c3d4"], Some("al-a1b2c3")).expect_err("another's inbox");
    assert_eq!(refused_read.0, "not authorized to subscribe to inbox/bo-b2c3d4");

    // historyd logs in with its service file and serves under enforce (0.3.2 dialled anonymously and never connected)
    let historyd = Command::new(bins::service("agent-bridge-historyd", "AGENT_BRIDGE_HISTORYD"))
        .env("AGENT_BRIDGE_CREDENTIALS", w.cfg().join("services/historyd.json"))
        .env("HISTORY_DB", w.dir.join("history.db"))
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("historyd starts");
    let mut historyd = Killed(historyd);
    let deadline = Instant::now() + Duration::from_secs(20);
    while agent_bridge::history::client::call("ping", Some("al-a1b2c3"), Default::default(), 1.0).is_err() {
        if Instant::now() > deadline {
            let _ = historyd.0.kill();
            let out = historyd.wait_with_output_stderr();
            panic!("historyd never answered under enforce: {out}");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    drop(historyd);

    // back to off: today's broker, the mode file gone
    w.up("off");
    assert_eq!(config::auth_mode(), Ok(Auth::Off));
    publish(None, "room/r").expect("anonymous again");
    assert!(!broker::runs_auth().expect("reachable"));
    // a stale auth-mode file (plain compose.yaml right after warn) is what doctor warns about
    std::fs::write(w.cfg().join("auth-mode"), "warn\n").expect("a stale file");
    assert_eq!(config::auth_mode(), Ok(Auth::Warn));
    assert!(!broker::runs_auth().expect("reachable"), "the broker says otherwise");
    publish(al, "room/r/al-a1b2c3").expect("credentials are still accepted without the plugin");
}

/// credentials_for over every combination of the real inputs: this process's session, what AGENT_BRIDGE_CREDENTIALS
/// points at, whether admin.json exists, and whether the agent has a file and which session it binds it to. Admin's
/// credentials iff the variable points at an existing admin file; never from admin.json found on disk.
#[test]
fn credentials_for_over_the_real_environment_and_files() {
    let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
    let dir = std::env::temp_dir().join(format!("agent-bridge-creds-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = dir.join("agent-bridge");
    for d in [cfg.join("agents"), cfg.join("services")] {
        std::fs::DirBuilder::new().recursive(true).mode(0o700).create(&d).expect("a dir");
    }
    let write = |path: &Path, body: String| {
        std::fs::write(path, body).expect("write");
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    };
    let file = |user: &str, session: Option<&str>| {
        let bound = session.map_or(String::new(), |s| format!(r#", "session": "{s}""#));
        format!(r#"{{"username": "{user}", "password": "{user}-pw", "client_id": "c"{bound}}}"#)
    };
    let (agent, s1, s2) = ("al-a1b2c3", "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222");
    let (admin_json, ledgerd_json) = (cfg.join("admin.json"), cfg.join("services/ledgerd.json"));
    let agent_json = cfg.join(format!("agents/{agent}.json"));
    write(&ledgerd_json, file("ledgerd", None));
    let set = |name: &str, value: Option<&str>| match value {
        // SAFETY: under ENV, and no other thread of this test reads the environment
        Some(v) => unsafe { std::env::set_var(name, v) },
        None => unsafe { std::env::remove_var(name) },
    };
    set("XDG_CONFIG_HOME", Some(dir.to_str().expect("UTF-8")));
    let mut cases = 0;
    for session in [None, Some(s1), Some(s2)] {
        for variable in [None, Some(&admin_json), Some(&ledgerd_json)] {
            for admin_exists in [false, true] {
                for agent_file in [None, Some(None), Some(Some(s1))] {
                    set("CLAUDE_CODE_SESSION_ID", session);
                    set("AGENT_BRIDGE_CREDENTIALS", variable.map(|p| p.to_str().expect("UTF-8")));
                    let _ = std::fs::remove_file(&admin_json);
                    if admin_exists {
                        write(&admin_json, file("admin", None));
                    }
                    let _ = std::fs::remove_file(&agent_json);
                    if let Some(bound) = agent_file {
                        write(&agent_json, file(agent, bound));
                    }
                    let case = format!(
                        "session {session:?}, variable {variable:?}, admin.json {admin_exists}, agent file {agent_file:?}"
                    );
                    let admin = provision::credentials_for("admin").expect("never an error for admin");
                    let names_admin = variable == Some(&admin_json) && admin_exists;
                    assert_eq!(admin.map(|c| c.username), names_admin.then(|| "admin".to_owned()), "admin: {case}");
                    let got = provision::credentials_for(agent);
                    let elsewhere = matches!((agent_file, session), (Some(Some(b)), Some(m)) if b != m);
                    match got {
                        Err(e) => {
                            assert!(elsewhere, "{case}: {e}");
                            assert_eq!(
                                e.0,
                                format!("agent-bridge: {agent} is not your agent (bound to another session)")
                            );
                        }
                        Ok(c) => {
                            assert!(!elsewhere, "{case}");
                            assert_eq!(c.map(|c| c.username), agent_file.map(|_| agent.to_owned()), "{case}");
                        }
                    }
                    cases += 1;
                }
            }
        }
    }
    assert_eq!(cases, 3 * 3 * 2 * 3);
    for name in ["CLAUDE_CODE_SESSION_ID", "AGENT_BRIDGE_CREDENTIALS", "XDG_CONFIG_HOME"] {
        set(name, None);
    }
    let _ = std::fs::remove_dir_all(&dir);
}
