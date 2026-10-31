//! The command line: only no arguments starts the service, and --health answers from a running historyd. The broker
//! test needs Docker, so it is ignored by default: cargo test -p agent-bridge-historyd -- --ignored

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const HISTORYD: &str = env!("CARGO_BIN_EXE_agent-bridge-historyd");

/// A container removed when the test ends, failed assertions included.
struct Removed(String);

impl Drop for Removed {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.0]).output();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").and_then(|l| l.local_addr()).map(|a| a.port()).expect("a free port")
}

/// A temp dir removed when the test ends, failed assertions included.
struct TempDir(PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

impl std::ops::Deref for TempDir {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.0
    }
}

/// A running service killed when the test ends, failed assertions included: a Child is not killed on drop.
struct Killed(Option<Child>);

impl Killed {
    fn take(&mut self) -> Child {
        self.0.take().expect("still ours")
    }
}

impl Drop for Killed {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn temp_dir(what: &str) -> TempDir {
    let dir = std::env::temp_dir().join(format!("agent-bridge-historyd-{what}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("a temp dir");
    TempDir(dir)
}

fn historyd(port: u16, dir: &Path, args: &[&str]) -> Command {
    let mut c = Command::new(HISTORYD);
    c.args(args)
        .env("AGENT_BRIDGE_HOST", "127.0.0.1")
        .env("AGENT_BRIDGE_PORT", port.to_string())
        .env("HISTORY_DB", dir.join("history.db"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    c
}

/// The process's output, or a failure if it is still running after timeout, which here means it started serving.
fn finishes(mut child: Child, timeout: Duration) -> Output {
    let deadline = Instant::now() + timeout;
    while child.try_wait().expect("waitable").is_none() {
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("still running after {timeout:?}: it started a service");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    child.wait_with_output().expect("its output")
}

fn run(port: u16, dir: &Path, args: &[&str], timeout: Duration) -> Output {
    finishes(historyd(port, dir, args).spawn().expect("historyd runs"), timeout)
}

#[test]
fn arguments_never_start_the_service() {
    let (port, dir) = (free_port(), temp_dir("args"));
    for help in ["--help", "-h"] {
        let out = run(port, &dir, &[help], Duration::from_secs(3));
        assert_eq!(out.status.code(), Some(0));
        assert!(String::from_utf8_lossy(&out.stdout).starts_with("usage: agent-bridge-historyd [-h] [--health]\n"));
    }
    // argparse's (allow_abbrev=False) usage line, error line and status, as the Python historyd prints them
    for (bad, extra) in
        [(&["--bogus"][..], "--bogus"), (&["--health", "now"], "now"), (&["serve"], "serve"), (&["--hea"], "--hea")]
    {
        let out = run(port, &dir, bad, Duration::from_secs(3));
        assert_eq!(out.status.code(), Some(2), "{bad:?}");
        let err = String::from_utf8_lossy(&out.stderr);
        let want = format!(
            "usage: agent-bridge-historyd [-h] [--health]\nagent-bridge-historyd: error: unrecognized arguments: {extra}\n"
        );
        assert_eq!(err, want);
    }
    // Nothing listens on port: the probe fails at once instead of retrying like the service would.
    let out = run(port, &dir, &["--health"], Duration::from_secs(8));
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).starts_with("agent-bridge-historyd: unhealthy: broker unreachable"));
    assert!(!dir.join("history.db").exists(), "no argument opens the history");
}

#[test]
#[ignore = "needs Docker"]
fn health_answers_and_a_second_historyd_takes_over() {
    let (port, dir) = (free_port(), temp_dir("health"));
    let name = format!("agent-bridge-historyd-health-{}", std::process::id());
    let conf = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../config/mosquitto.conf");
    let started = Command::new("docker")
        .args(["run", "-d", "--name", &name, "-p", &format!("127.0.0.1:{port}:1883"), "-v"])
        .arg(format!("{}:/mosquitto/config/mosquitto.conf:ro", conf.canonicalize().expect("the config").display()))
        .arg("eclipse-mosquitto:2.0.22")
        .output()
        .expect("docker runs");
    assert!(started.status.success(), "{}", String::from_utf8_lossy(&started.stderr));
    let _broker = Removed(name);
    let mut first = Killed(Some(historyd(port, &dir, &[]).spawn().expect("historyd runs")));
    let deadline = Instant::now() + Duration::from_secs(15);
    let healthy = loop {
        let ok = run(port, &dir, &["--health"], Duration::from_secs(12)).status.code() == Some(0);
        if ok || Instant::now() > deadline {
            break ok;
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    // The broker ends the first one's session with e0 01 8e (reason 142, no properties): it steps aside.
    let second = Killed(Some(historyd(port, &dir, &[]).stderr(Stdio::null()).spawn().expect("historyd runs")));
    let stepped_aside = finishes(first.take(), Duration::from_secs(10));
    drop(second);
    let after = run(port, &dir, &["--health"], Duration::from_secs(12));
    assert!(healthy, "a running historyd answers the probe");
    assert_eq!(stepped_aside.status.code(), Some(1));
    let err = String::from_utf8_lossy(&stepped_aside.stderr);
    assert!(err.ends_with("another client connected as historyd, exiting\n"), "{err}");
    assert_eq!(after.status.code(), Some(1), "no historyd, no answer");
    assert!(String::from_utf8_lossy(&after.stderr).contains("no reply from historyd"));
}
