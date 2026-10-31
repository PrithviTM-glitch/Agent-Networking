//! What a relay's lock file tells others: live while flocked, the address it holds, and stop by SIGTERM. A shell
//! stands in for the relay, holding the lock through `flock` as a relay does.

use agent_bridge::relay::{held, live, lock_path, stop};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

const SESSION: &str = "0F8FAD5B-D9CB-469F-A165-70867728950E";

/// A process that holds the lock exclusively and writes "<its pid>\n<addr>\n", like a relay once registered.
fn fake_relay(addr: &str) -> Child {
    let path = lock_path(SESSION).expect("a lock path");
    std::fs::create_dir_all(path.parent().expect("a dir")).expect("the relay dir");
    let script = r#"exec 9>>"$LOCK"; flock -x 9; printf '%s\n%s\n' $$ "$ADDR" > "$LOCK"; exec sleep 30"#;
    let child = Command::new("sh").args(["-c", script]).env("LOCK", &path).env("ADDR", addr).spawn().expect("sh");
    let deadline = Instant::now() + Duration::from_secs(5);
    while held(SESSION).is_none() {
        assert!(Instant::now() < deadline, "the fake relay never took the lock");
        std::thread::sleep(Duration::from_millis(20));
    }
    child
}

/// A temp dir removed when the test ends, failed assertions included.
struct TempDir(std::path::PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn live_held_and_stop() {
    let tmp = TempDir(std::env::temp_dir().join(format!("agent-bridge-relay-lock-{}", std::process::id())));
    let dir = &tmp.0;
    // SAFETY: set before anything in this test binary reads the environment.
    unsafe { std::env::set_var("AGENT_BRIDGE_LOG", dir.join("history.jsonl")) };
    assert!(lock_path(SESSION).unwrap().ends_with("relay/0f8fad5b-d9cb-469f-a165-70867728950e.lock"));
    assert!(!live(SESSION) && held(SESSION).is_none(), "no lock file, no relay");
    assert!(stop(SESSION, Duration::from_secs(1)).unwrap(), "stopping nothing succeeds");

    let mut relay = fake_relay("proj-1a2b3c4d");
    assert!(live(SESSION));
    assert_eq!(held(SESSION).as_deref(), Some("proj-1a2b3c4d"));
    assert!(stop(SESSION, Duration::from_secs(5)).unwrap(), "SIGTERM frees the lock");
    let _ = relay.wait();
    assert!(!live(SESSION) && held(SESSION).is_none(), "a stale lock file is not a live relay");
    assert!(lock_path("not-a-uuid").is_err());
}
