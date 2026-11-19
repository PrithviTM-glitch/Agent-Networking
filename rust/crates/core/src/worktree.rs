//! Worktree locks: a write lease on a git worktree (ledgerd's admin/wt-<hash> resource, made on demand), mirrored in
//! a `git worktree lock` whose reason names the lease, so the hooks can still enforce it while ledgerd is down. The
//! session's relay renews the lease and releases it when the session ends; release first saves uncommitted changes
//! under refs/agent-bridge/released/<lease id>.

use crate::json::Map;
use crate::ledger::{self, LedgerError};
use crate::validation::{Error, ID_MAX, Result, check_ident, check_session, is_ident};
use crate::{config, host, relay};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

/// The lease's ttl without --ttl. With a live relay the lease lasts the session anyway.
pub const DEFAULT_TTL: f64 = 600.0;
/// The ref prefix release saves uncommitted changes under.
pub const RELEASED: &str = "refs/agent-bridge/released/";
const LEDGER_TIMEOUT: f64 = 5.0;

/// The `git worktree lock` reason of a lease. It carries no expiry, so renewals never rewrite it.
pub fn lock_reason(lease_id: &str, addr: &str) -> String {
    format!("agent-bridge lease {lease_id} held by {addr}")
}

/// (lease id, holder) of an agent-bridge lock reason, also the older "... lease <id> by <addr> until ..." form.
pub fn parse_reason(reason: &str) -> Option<(String, String)> {
    let words: Vec<&str> = reason.split_whitespace().collect();
    if words.len() < 5 || words[0] != "agent-bridge" || words[1] != "lease" {
        return None;
    }
    let holder = words.iter().position(|w| *w == "by").and_then(|i| words.get(i + 1))?;
    Some((words[2].to_owned(), (*holder).to_owned()))
}

/// A worktree as git reports it.
pub struct Tree {
    pub root: PathBuf,
    pub git_dir: PathBuf,
    pub common_dir: PathBuf,
}

impl Tree {
    /// The main worktree has the repository's own git dir; git can't lock it.
    pub fn is_main(&self) -> bool {
        self.git_dir == self.common_dir
    }

    fn root_str(&self) -> Result<&str> {
        self.root.to_str().ok_or_else(|| Error(format!("{} is not a UTF-8 path", self.root.display())))
    }

    /// The reason of the worktree's git lock, if it has one.
    pub fn lock(&self) -> Option<String> {
        std::fs::read_to_string(self.git_dir.join("locked")).ok().map(|r| r.trim().to_owned())
    }
}

/// git with the repository taken from cwd only, never from a GIT_DIR a hook or caller set.
fn git(cwd: &Path, args: &[&str]) -> Command {
    let mut c = Command::new("git");
    c.arg("-C").arg(cwd).args(args).stdin(Stdio::null());
    for var in ["GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR", "GIT_OBJECT_DIRECTORY"] {
        c.env_remove(var);
    }
    c
}

fn run(mut c: Command) -> Result<Output> {
    c.output().map_err(|e| Error(format!("cannot run git: {e}")))
}

fn failed(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).trim().trim_start_matches("fatal: ").to_owned()
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).trim_end_matches('\n').to_owned()
}

/// The worktree path is in: its root (symlinks resolved), git dir and common dir.
pub fn tree(path: &str) -> Result<Tree> {
    let dir = Path::new(path);
    let real = std::fs::canonicalize(dir).map_err(|e| Error(format!("{path}: {e}")))?;
    let start = if real.is_dir() { real } else { real.parent().map(Path::to_path_buf).unwrap_or(real) };
    let out = run(git(&start, &["rev-parse", "--show-toplevel", "--absolute-git-dir", "--git-common-dir"]))?;
    if !out.status.success() {
        return Err(Error(format!("{path} is not inside a git worktree")));
    }
    let text = stdout(&out);
    let lines: Vec<&str> = text.lines().collect();
    let [root, git_dir, common] = lines[..] else { return Err(Error(format!("{path}: git rev-parse said {text:?}"))) };
    let real = |p: &str| {
        let p = Path::new(p);
        let p = if p.is_absolute() { p.to_path_buf() } else { start.join(p) };
        std::fs::canonicalize(&p).unwrap_or(p)
    };
    Ok(Tree { root: real(root), git_dir: real(git_dir), common_dir: real(common) })
}

fn ledger_err(e: LedgerError) -> Error {
    Error(e.to_string())
}

/// Lock the worktree in git for lease_id, replacing an older agent-bridge lock; a warning when it can't.
fn git_lock(t: &Tree, lease_id: &str, addr: &str) -> std::result::Result<String, String> {
    let reason = lock_reason(lease_id, addr);
    if t.is_main() {
        return Err("the main worktree can't be git-locked; the lease and the hooks still hold it".into());
    }
    match t.lock() {
        Some(r) if r == reason => return Ok(reason),
        Some(r) if parse_reason(&r).is_some() => {
            let _ = run(git(&t.root, &["worktree", "unlock", &t.root.to_string_lossy()]));
        }
        Some(r) => return Err(format!("git lock left as it is: already locked ({r})")),
        None => {}
    }
    let root = t.root.to_string_lossy().into_owned();
    let out = run(git(&t.root, &["worktree", "lock", "--reason", &reason, &root])).map_err(|e| e.0)?;
    if !out.status.success() {
        return Err(format!("git worktree lock failed: {}", failed(&out)));
    }
    Ok(reason)
}

/// Unlock the worktree in git if its lock is lease_id's; true if it did.
fn git_unlock(t: &Tree, lease_id: &str) -> bool {
    let ours = t.lock().and_then(|r| parse_reason(&r)).is_some_and(|(id, _)| id == lease_id);
    let root = t.root.to_string_lossy().into_owned();
    ours && run(git(&t.root, &["worktree", "unlock", &root])).is_ok_and(|o| o.status.success())
}

/// Save the worktree's uncommitted changes, untracked files included, as a commit on top of HEAD under
/// refs/agent-bridge/released/<lease_id>, through a scratch index so the real index and files stay as they are. None
/// when nothing is uncommitted.
pub fn snapshot(t: &Tree, lease_id: &str) -> Result<Option<String>> {
    check_ident(lease_id, "lease_id")?;
    let status = run(git(&t.root, &["status", "--porcelain", "--untracked-files=all"]))?;
    if !status.status.success() {
        return Err(Error(format!("git status failed: {}", failed(&status))));
    }
    if status.stdout.is_empty() {
        return Ok(None);
    }
    let index = t.git_dir.join(format!("agent-bridge-snapshot-{lease_id}.index"));
    let real_index = t.git_dir.join("index");
    if real_index.exists() {
        std::fs::copy(&real_index, &index).map_err(|e| Error(format!("cannot copy the index: {e}")))?;
    }
    let with_index = |args: &[&str]| {
        let mut c = git(&t.root, args);
        c.env("GIT_INDEX_FILE", &index);
        run(c)
    };
    let result = (|| {
        let add = with_index(&["add", "-A", "."])?;
        if !add.status.success() {
            return Err(Error(format!("git add failed: {}", failed(&add))));
        }
        let tree = with_index(&["write-tree"])?;
        if !tree.status.success() {
            return Err(Error(format!("git write-tree failed: {}", failed(&tree))));
        }
        let head = run(git(&t.root, &["rev-parse", "-q", "--verify", "HEAD"]))?;
        let message = format!("agent-bridge: uncommitted changes at the release of lease {lease_id}");
        let tree = stdout(&tree);
        let head = stdout(&head);
        // a signing setup could prompt or fail here, and the snapshot is the bridge's, not the user's commit
        let mut args = vec!["-c", "commit.gpgSign=false", "commit-tree", tree.as_str(), "-m", message.as_str()];
        if !head.is_empty() {
            args.extend(["-p", head.as_str()]);
        }
        let mut c = git(&t.root, &args);
        // commit-tree needs an identity; a user without one still gets the snapshot
        if !run(git(&t.root, &["var", "GIT_COMMITTER_IDENT"]))?.status.success() {
            for (k, v) in [
                ("GIT_AUTHOR_NAME", "agent-bridge"),
                ("GIT_AUTHOR_EMAIL", "agent-bridge@localhost"),
                ("GIT_COMMITTER_NAME", "agent-bridge"),
                ("GIT_COMMITTER_EMAIL", "agent-bridge@localhost"),
            ] {
                c.env(k, v);
            }
        }
        let commit = run(c)?;
        if !commit.status.success() {
            return Err(Error(format!("git commit-tree failed: {}", failed(&commit))));
        }
        let commit = stdout(&commit);
        let name = format!("{RELEASED}{lease_id}");
        let update = run(git(&t.root, &["update-ref", &name, &commit]))?;
        if !update.status.success() {
            return Err(Error(format!("git update-ref failed: {}", failed(&update))));
        }
        Ok(Some(name))
    })();
    let _ = std::fs::remove_file(&index);
    result
}

/// A lease the session's relay renews and, when the session ends, releases.
#[derive(Clone, Debug, PartialEq)]
pub struct Lease {
    pub addr: String,
    pub path: String,
    pub lease_id: String,
    pub ttl: f64,
}

/// <data dir>/relay/<session>.leases: one JSON line per lease the relay keeps.
pub fn leases_path(session: &str) -> Result<PathBuf> {
    config::data_path(&["relay", &format!("{}.leases", check_session(Some(session))?.to_lowercase())])
}

pub fn registered(session: &str) -> Vec<Lease> {
    let text = leases_path(session).ok().and_then(|p| std::fs::read_to_string(p).ok()).unwrap_or_default();
    text.lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter_map(|v| {
            Some(Lease {
                addr: v.get("addr")?.as_str()?.to_owned(),
                path: v.get("path")?.as_str()?.to_owned(),
                lease_id: v.get("lease_id")?.as_str()?.to_owned(),
                ttl: v.get("ttl")?.as_f64()?,
            })
        })
        .collect()
}

fn save(session: &str, leases: &[Lease]) -> Result<()> {
    let path = leases_path(session)?;
    if leases.is_empty() {
        let _ = std::fs::remove_file(&path);
        return Ok(());
    }
    let text: String = leases
        .iter()
        .map(|l| format!("{}\n", json!({"addr": l.addr, "path": l.path, "lease_id": l.lease_id, "ttl": l.ttl})))
        .collect();
    let tmp = path.with_extension("leases.tmp");
    std::fs::write(&tmp, text).and_then(|_| std::fs::rename(&tmp, &path)).map_err(|e| config::data_error(&e, &path))
}

fn register(session: &str, lease: Lease) -> Result<()> {
    config::data_dir()?;
    let mut all = registered(session);
    all.retain(|l| l.lease_id != lease.lease_id && l.path != lease.path);
    all.push(lease);
    save(session, &all)
}

fn unregister(session: &str, lease_id: &str) {
    let mut all = registered(session);
    let before = all.len();
    all.retain(|l| l.lease_id != lease_id);
    if all.len() != before {
        let _ = save(session, &all);
    }
}

/// This process's session, when its relay is live to renew what it registers.
fn relayed_session() -> Option<String> {
    host::current().session().filter(|s| relay::live(s))
}

fn obj(v: Value) -> Map {
    v.as_object().cloned().unwrap_or_default()
}

/// A write lease on the worktree path is in, for addr: ledgerd's lease (made on demand), the git lock, and the
/// relay's renewal when this session has a live relay. ttl defaults to DEFAULT_TTL; wait as for acquire.
pub fn acquire(addr: &str, path: &str, ttl: Option<f64>, wait: Option<f64>) -> Result<Map> {
    acquire_with(addr, path, ttl, wait, true)
}

/// acquire; with relay false the session's relay never renews or releases it (acquire --exec does both itself).
pub fn acquire_with(addr: &str, path: &str, ttl: Option<f64>, wait: Option<f64>, relay: bool) -> Result<Map> {
    check_ident(addr, "address")?;
    let t = tree(path)?;
    let root = t.root_str()?;
    let common = t.common_dir.to_str();
    let ttl = ttl.unwrap_or(DEFAULT_TTL);
    let mut reply = ledger::worktree_acquire(addr, root, common, Some(ttl), wait).map_err(ledger_err)?;
    reply.remove("ok");
    let lease_id = reply.get("lease").and_then(|l| l.get("lease_id")).and_then(Value::as_str).unwrap_or_default();
    let lease_id = lease_id.to_owned();
    let mut warnings = Vec::new();
    let lock = git_lock(&t, &lease_id, addr).map_err(|w| warnings.push(w)).ok();
    reply.insert("resource".into(), ledger::worktree_resource(root).into());
    reply.insert("lease_id".into(), lease_id.clone().into());
    reply.insert("git_lock".into(), lock.is_some().into());
    reply.insert("lock".into(), lock.into());
    let renewed = match relayed_session().filter(|_| relay) {
        Some(session) => {
            let lease = Lease { addr: addr.into(), path: root.into(), lease_id: lease_id.clone(), ttl };
            match register(&session, lease) {
                Ok(()) => "relay",
                Err(e) => {
                    warnings.push(format!("the relay will not renew it: {e}"));
                    "none"
                }
            }
        }
        None if !relay => "exec",
        None => {
            warnings.push(format!(
                "no relay renews it in this session: it lapses in {} s unless renewed (agent-bridge renew)",
                ledger::fmt_g(ttl)
            ));
            "none"
        }
    };
    reply.insert("path".into(), root.into());
    reply.insert("renewed_by".into(), renewed.into());
    if !warnings.is_empty() {
        reply.insert("warnings".into(), warnings.into());
    }
    Ok(reply)
}

/// The live lease addr holds on the worktree at root.
fn held_by(addr: &str, root: &str) -> Result<String> {
    let holders = ledger::worktree_holders(addr, root, LEDGER_TIMEOUT).map_err(ledger_err)?;
    let Some(holders) = holders else { return Err(Error(format!("{root} has no worktree lease"))) };
    if let Some(h) = holders.iter().find(|h| h.holder == addr && !h.lease_id.is_empty()) {
        return Ok(h.lease_id.clone());
    }
    let others: Vec<String> = holders.iter().map(|h| format!("{} (lease {})", h.holder, h.lease_id)).collect();
    Err(Error(match others.is_empty() {
        true => format!("{addr} holds no lease on {root}"),
        false => format!("{addr} holds no lease on {root}; held by {}", others.join(", ")),
    }))
}

/// Release lease_id on the worktree at path: snapshot uncommitted changes, unlock git, release in ledgerd.
fn release_lease(addr: &str, t: &Tree, lease_id: &str) -> Result<Map> {
    let mut warnings = Vec::new();
    let saved = snapshot(t, lease_id).unwrap_or_else(|e| {
        warnings.push(format!("uncommitted changes not saved: {e}"));
        None
    });
    let unlocked = git_unlock(t, lease_id);
    let fields = obj(json!({"lease_id": lease_id}));
    let released = ledger::call("release", addr, fields, Some(LEDGER_TIMEOUT));
    if let Err(e) = released {
        // already gone (expired, revoked): the git lock and snapshot are still dealt with
        if !e.to_string().starts_with("no such lease") {
            return Err(ledger_err(e));
        }
        warnings.push(e.to_string());
    }
    let mut reply = obj(json!({
        "released": lease_id,
        "path": t.root.to_string_lossy(),
        "snapshot": saved,
        "unlocked": unlocked,
    }));
    if !warnings.is_empty() {
        reply.insert("warnings".into(), warnings.into());
    }
    Ok(reply)
}

/// After acquire --exec: save uncommitted changes and remove the git lock of lease_id, which exec_held released.
pub fn finish(addr: &str, path: &str, lease_id: &str) -> Result<Map> {
    release_lease(addr, &tree(path)?, lease_id)
}

/// Release addr's lease on the worktree path is in: uncommitted changes are saved under
/// refs/agent-bridge/released/<lease id>, the git lock is removed, and the relay stops renewing it.
pub fn release(addr: &str, path: &str) -> Result<Map> {
    check_ident(addr, "address")?;
    let t = tree(path)?;
    let lease_id = held_by(addr, t.root_str()?)?;
    let reply = release_lease(addr, &t, &lease_id)?;
    if let Some(session) = host::current().session() {
        unregister(&session, &lease_id);
    }
    Ok(reply)
}

/// Who holds the worktree path is in, from ledgerd ("holder", "lease_id", "expires_at" while leased), and its git
/// lock ("lock", the reason). "stale" marks an agent-bridge git lock with no live lease behind it; with ledgerd
/// unreachable, "ledger_error" says why and only the git lock is shown.
pub fn status(path: &str) -> Result<Map> {
    let t = tree(path)?;
    let root = t.root_str()?;
    let mut reply = obj(json!({
        "path": root,
        "resource": ledger::worktree_resource(root),
        "git_common_dir": t.common_dir.to_string_lossy(),
        "main": t.is_main(),
    }));
    let git = t.lock();
    let ours = git.as_deref().and_then(parse_reason);
    reply.insert("lock".into(), git.clone().into());
    match ledger::worktree_holders("worktree-status", root, LEDGER_TIMEOUT) {
        Ok(holders) => {
            let holders = holders.unwrap_or_default();
            let live = |id: &str| holders.iter().any(|h| h.lease_id == id);
            reply.insert("stale".into(), ours.as_ref().is_some_and(|(id, _)| !live(id)).into());
            // worktree leases are write leases: one holder at most
            if let Some(h) = holders.first() {
                reply.insert("holder".into(), h.holder.clone().into());
                reply.insert("lease_id".into(), h.lease_id.clone().into());
                reply.insert("expires_at".into(), crate::json::float(h.expires_at));
            }
        }
        Err(e) => {
            reply.insert("ledger_error".into(), e.to_string().into());
        }
    }
    Ok(reply)
}

/// The relay's side: renews each registered lease every ttl/3 and drops one ledgerd no longer has.
#[derive(Default)]
pub struct Renewer {
    due: std::collections::HashMap<String, Instant>,
}

impl Renewer {
    /// Renew what is due now; returns a line to log for each lease dropped or failing.
    pub fn tick(&mut self, session: &str) -> Vec<String> {
        let mut said = Vec::new();
        let now = Instant::now();
        let leases = registered(session);
        self.due.retain(|id, _| leases.iter().any(|l| &l.lease_id == id));
        for l in leases {
            let third = Duration::from_secs_f64((l.ttl / 3.0).max(1.0));
            let due = *self.due.entry(l.lease_id.clone()).or_insert(now + third);
            if now < due {
                continue;
            }
            let fields = obj(json!({"lease_id": l.lease_id, "ttl": l.ttl}));
            match ledger::call("renew", &l.addr, fields, Some(LEDGER_TIMEOUT)) {
                Ok(_) => {
                    self.due.insert(l.lease_id.clone(), now + third);
                }
                Err(e) if e.to_string().starts_with("no such lease") || e.to_string().starts_with("lease ") => {
                    unregister(session, &l.lease_id);
                    self.due.remove(&l.lease_id);
                    said.push(format!("worktree lease {} on {} is gone ({e}), no longer renewed", l.lease_id, l.path));
                }
                Err(e) => {
                    self.due.insert(l.lease_id.clone(), now + Duration::from_secs(5));
                    said.push(format!("cannot renew worktree lease {} on {}: {e}", l.lease_id, l.path));
                }
            }
        }
        said
    }
}

/// At the end of the session: release every registered lease (snapshot, unlock, release). Returns a line to log
/// for each.
pub fn release_session(session: &str) -> Vec<String> {
    let mut said = Vec::new();
    for l in registered(session) {
        let line = match tree(&l.path).and_then(|t| release_lease(&l.addr, &t, &l.lease_id)) {
            Ok(r) => match r.get("snapshot").and_then(Value::as_str) {
                Some(saved) => format!("released worktree {} (lease {}), changes saved as {saved}", l.path, l.lease_id),
                None => format!("released worktree {} (lease {})", l.path, l.lease_id),
            },
            Err(e) => format!("cannot release worktree {} (lease {}): {e}", l.path, l.lease_id),
        };
        said.push(line);
    }
    if let Ok(path) = leases_path(session) {
        let _ = std::fs::remove_file(path);
    }
    said
}

/// The address a refusal tells this session to acquire as: its agent, when one is known.
pub fn session_agent() -> Option<String> {
    let session = host::current().session()?;
    relay::held(&session).or_else(|| relay::session_agent(&session).ok().flatten()).filter(|a| is_ident(a, ID_MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_lock_reason_is_pinned() {
        // W-1: the console and the hooks parse this exact text
        assert_eq!(lock_reason("3f2a", "al-1"), "agent-bridge lease 3f2a held by al-1");
        assert_eq!(parse_reason("agent-bridge lease 3f2a held by al-1\n"), Some(("3f2a".into(), "al-1".into())));
        assert_eq!(parse_reason("agent-bridge lease 3f2a by al-1 until 12:00"), Some(("3f2a".into(), "al-1".into())));
        assert_eq!(parse_reason("on a USB stick"), None);
    }

    fn sh(dir: &Path, args: &[&str]) -> String {
        let out = git(dir, args)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .expect("git runs");
        assert!(out.status.success(), "git {args:?}: {}", failed(&out));
        stdout(&out)
    }

    /// A linked worktree is git-locked with the lease's reason, an older agent-bridge lock is replaced, a human's is
    /// left alone, and release saves tracked and untracked changes without touching the index or the files.
    #[test]
    fn git_lock_snapshot_and_unlock_in_a_scratch_repo() {
        let base = std::env::temp_dir().join(format!("agent-bridge-wt-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let (main, linked) = (base.join("main"), base.join("linked"));
        std::fs::create_dir_all(&main).expect("dirs");
        sh(&main, &["init", "-q", "-b", "main"]);
        sh(&main, &["config", "user.email", "t@example.com"]);
        sh(&main, &["config", "user.name", "t"]);
        std::fs::write(main.join("f"), "one\n").expect("write");
        sh(&main, &["add", "f"]);
        sh(&main, &["commit", "-q", "-m", "one"]);
        sh(&main, &["worktree", "add", "-q", "-b", "side", linked.to_str().unwrap()]);

        let m = tree(main.to_str().unwrap()).expect("main");
        assert!(m.is_main());
        assert!(git_lock(&m, "aa11", "al-1").unwrap_err().starts_with("the main worktree can't be git-locked"));
        let t = tree(linked.join("f").to_str().unwrap()).expect("a file names its worktree");
        assert!(!t.is_main());
        assert_eq!(t.root, std::fs::canonicalize(&linked).unwrap());
        assert_eq!(git_lock(&t, "aa11", "al-1"), Ok("agent-bridge lease aa11 held by al-1".into()));
        assert_eq!(t.lock().as_deref(), Some("agent-bridge lease aa11 held by al-1"));
        assert_eq!(git_lock(&t, "bb22", "bo-2"), Ok("agent-bridge lease bb22 held by bo-2".into()), "replaces ours");
        assert!(!git_unlock(&t, "aa11"), "another lease's lock stays");

        assert_eq!(snapshot(&t, "bb22"), Ok(None), "a clean tree saves nothing");
        std::fs::write(linked.join("f"), "two\n").expect("write");
        std::fs::write(linked.join("new"), "untracked\n").expect("write");
        let before = sh(&linked, &["status", "--porcelain"]);
        let saved = snapshot(&t, "bb22").expect("saves").expect("a ref");
        assert_eq!(saved, "refs/agent-bridge/released/bb22");
        assert_eq!(sh(&linked, &["status", "--porcelain"]), before, "index and files untouched");
        assert_eq!(sh(&main, &["show", &format!("{saved}:f")]), "two");
        assert_eq!(sh(&main, &["show", &format!("{saved}:new")]), "untracked");
        assert_eq!(sh(&main, &["rev-parse", &format!("{saved}^")]), sh(&main, &["rev-parse", "side"]));

        assert!(git_unlock(&t, "bb22"));
        assert_eq!(t.lock(), None);
        sh(&main, &["worktree", "lock", "--reason", "on a USB stick", linked.to_str().unwrap()]);
        assert_eq!(git_lock(&t, "cc33", "al-1"), Err("git lock left as it is: already locked (on a USB stick)".into()));
        assert!(!git_unlock(&t, "cc33"), "a human's lock is never removed");
        let _ = std::fs::remove_dir_all(&base);
    }
}
