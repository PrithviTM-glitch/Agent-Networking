//! A disposable broker for the Docker tests of this crate.

use agent_bridge::presence;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

pub struct Broker(String);

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.0]).output();
    }
}

/// A disposable mosquitto on a free port, with this test binary's environment pointed at it and a temp state dir.
pub fn broker(what: &str) -> Broker {
    let port = std::net::TcpListener::bind("127.0.0.1:0").and_then(|l| l.local_addr()).expect("a free port").port();
    let name = format!("agent-bridge-core-{what}-{}", std::process::id());
    let conf = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../config/mosquitto.conf");
    let started = Command::new("docker")
        .args(["run", "-d", "--name", &name, "-p", &format!("127.0.0.1:{port}:1883"), "-v"])
        .arg(format!("{}:/mosquitto/config/mosquitto.conf:ro", conf.canonicalize().expect("the config").display()))
        .arg("eclipse-mosquitto:2.0.22")
        .output()
        .expect("docker runs");
    assert!(started.status.success(), "{}", String::from_utf8_lossy(&started.stderr));
    let dir = std::env::temp_dir().join(&name);
    std::fs::create_dir_all(&dir).expect("a temp dir");
    // SAFETY: set once, before any thread of this test reads the environment.
    unsafe {
        std::env::set_var("AGENT_BRIDGE_PORT", port.to_string());
        std::env::set_var("AGENT_BRIDGE_LOG", dir.join("history.jsonl"));
        std::env::set_var("XDG_STATE_HOME", dir.join("state"));
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    while presence::agents(None).is_err() {
        assert!(Instant::now() < deadline, "the broker never came up");
        std::thread::sleep(Duration::from_millis(100));
    }
    Broker(name)
}
