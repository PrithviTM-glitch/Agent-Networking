//! A worktree lease stays enforced when ledgerd goes away. The main worktree can't be git-locked, so the guard's
//! offline fallback (the last locks it saw, plus the git lock reasons) knows of a lease on it only if acquire recorded
//! it there. And release forgets it again, so a released worktree isn't held offline forever. Needs Docker and the
//! ledgerd binary (cargo build -p agent-bridge-ledgerd, or AGENT_BRIDGE_LEDGERD): cargo test -p agent-bridge-core
//! --test worktree_offline -- --ignored.

#![allow(
    clippy::let_underscore_must_use,
    reason = "best-effort cleanup: killing, waiting for and removing what the test started"
)]

#[path = "common/bins.rs"]
mod bins;
mod common;

use agent_bridge::bus::Bus;
use agent_bridge::{guard, ledger, worktree};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// A host ledgerd on the test broker, killed when dropped.
struct Ledgerd(Child);

impl Drop for Ledgerd {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn ledgerd_exe() -> PathBuf {
    bins::service("agent-bridge-ledgerd", "AGENT_BRIDGE_LEDGERD")
}

fn start_ledgerd(db: &Path) -> Ledgerd {
    let child = Command::new(ledgerd_exe()).env("LEDGER_DB", db).stderr(Stdio::null()).spawn().expect("ledgerd starts");
    let deadline = Instant::now() + Duration::from_secs(15);
    while ledger::call("ping", "probe-0", Default::default(), Some(1.0)).is_err() {
        assert!(Instant::now() < deadline, "ledgerd never answered");
        std::thread::sleep(Duration::from_millis(100));
    }
    Ledgerd(child)
}

fn git(dir: &Path, args: &[&str]) {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .expect("git runs");
    assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
}

/// What the guard answers when another session writes a file in dir.
fn foreign_write(dir: &Path, session: &str) -> guard::Outcome {
    let event = json!({"session_id": session, "cwd": dir, "tool_name": "Write",
                       "tool_input": {"file_path": dir.join("notes.txt"), "content": "x"}});
    guard::run(&event.to_string())
}

#[test]
#[ignore = "needs Docker and the ledgerd binary"]
fn a_main_worktree_lease_holds_with_ledgerd_down() {
    let broker = common::broker("wt-offline");
    let dir = std::env::temp_dir().join(format!("agent-bridge-wt-offline-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("a temp dir");
    // SAFETY: set before any thread of this test reads the environment
    unsafe { std::env::set_var("XDG_CONFIG_HOME", dir.join("config")) };
    let db = dir.join("ledger.db");
    let repo = dir.join("repo");
    std::fs::create_dir_all(&repo).expect("the repo dir");
    git(&repo, &["init", "-q", "-b", "main"]);
    git(&repo, &["-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "--allow-empty", "-m", "i"]);
    let repo = repo.canonicalize().expect("the real path");
    let (holder_session, other) = ("5c3a9d10-1e2f-4a3b-8c4d-5e6f7a8b9c01", "5c3a9d10-1e2f-4a3b-8c4d-5e6f7a8b9c02");

    let ledgerd = start_ledgerd(&db);
    unsafe { std::env::set_var("CLAUDE_CODE_SESSION_ID", holder_session) };
    let holder = Bus::register("holder", &[], true, None, false, true).expect("registers");
    let addr = holder.addr.clone();
    let got = worktree::acquire(&addr, repo.to_str().expect("UTF-8"), Some(120.0), None).expect("acquires");
    assert_eq!(got["git_lock"], false, "the main worktree can't be git-locked: {got:?}");
    unsafe { std::env::remove_var("CLAUDE_CODE_SESSION_ID") };

    // ledgerd goes away before any guard lookup has seen the lease
    drop(ledgerd);
    let refused = foreign_write(&repo, other);
    assert_eq!(refused.code, 2, "another session wrote into a leased worktree while ledgerd was down: {refused:?}");
    assert!(refused.stderr.contains(&format!("leased by {addr}")), "{}", refused.stderr);
    assert!(refused.stderr.contains("ledgerd unreachable"), "{}", refused.stderr);
    assert_eq!(foreign_write(&repo, holder_session).code, 0, "the holder's own session still writes");

    // release forgets it: offline, the released worktree is free again
    let ledgerd = start_ledgerd(&db);
    worktree::release(&addr, repo.to_str().expect("UTF-8")).expect("releases");
    drop(ledgerd);
    let free = foreign_write(&repo, other);
    assert_eq!(free.code, 0, "a released worktree stayed held offline: {free:?}");

    // a lease the guard can't record is given back, never held unseen
    let ledgerd = start_ledgerd(&db);
    let locks = agent_bridge::config::data_path(&["guard-locks.json"]).expect("the data dir");
    let _ = std::fs::remove_file(&locks);
    std::fs::create_dir_all(locks.join("in-the-way")).expect("a directory where the file goes");
    let failed = worktree::acquire(&addr, repo.to_str().expect("UTF-8"), Some(120.0), None).expect_err("unrecorded");
    assert!(failed.0.starts_with("cannot record lease ") && failed.0.ends_with("; released it"), "{failed}");
    let status = worktree::status(repo.to_str().expect("UTF-8")).expect("status");
    assert!(status.get("holder").is_none(), "the unrecorded lease was released: {status:?}");
    std::fs::remove_dir_all(&locks).expect("tidy");
    drop(ledgerd);

    holder.close();
    drop(broker);
    let _ = std::fs::remove_dir_all(&dir);
}
