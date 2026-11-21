//! The prototype's measured cases (ACCESS.md 5.6), on a scratch repository with git's own layout: a main worktree
//! `repo/` and a linked worktree `lane/` (leased by other-1), a free dir with a symlink and a hard link into the lane.

use super::*;
use std::fs;

struct Fixture {
    base: PathBuf,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        #[allow(clippy::let_underscore_must_use, reason = "cleanup: a leftover scratch dir is harmless")]
        let _ = fs::remove_dir_all(&self.base);
    }
}

fn fixture(name: &str) -> Fixture {
    let base = std::env::temp_dir().join(format!("guard-{name}-{}", std::process::id()));
    #[allow(clippy::let_underscore_must_use, reason = "a leftover from an earlier run, usually none")]
    let _ = fs::remove_dir_all(&base);
    let git = base.join("repo/.git");
    fs::create_dir_all(git.join("worktrees/lane")).unwrap();
    fs::create_dir_all(git.join("refs/heads")).unwrap();
    fs::create_dir_all(git.join("hooks")).unwrap();
    fs::write(git.join("HEAD"), "ref: refs/heads/main\n").unwrap();
    fs::create_dir_all(base.join("lane/src")).unwrap();
    fs::create_dir_all(base.join("free")).unwrap();
    let base = fs::canonicalize(&base).unwrap();
    let wt = base.join("repo/.git/worktrees/lane");
    fs::write(base.join("lane/.git"), format!("gitdir: {}\n", wt.display())).unwrap();
    fs::write(wt.join("gitdir"), format!("{}\n", base.join("lane/.git").display())).unwrap();
    fs::write(wt.join("commondir"), "../..\n").unwrap();
    fs::write(wt.join("HEAD"), "ref: refs/heads/lane\n").unwrap();
    fs::write(base.join("lane/src/a.txt"), "hello\n").unwrap();
    fs::write(base.join("free/b.txt"), "x\n").unwrap();
    std::os::unix::fs::symlink("../lane", base.join("free/via-link")).unwrap();
    fs::hard_link(base.join("lane/src/a.txt"), base.join("free/hard.txt")).unwrap();
    Fixture { base }
}

struct Mock {
    held: Vec<Lock>,
    me: Vec<String>,
    note: Option<String>,
}

impl Lookup for Mock {
    fn me(&self) -> Vec<String> {
        self.me.clone()
    }
    fn locks(&self, roots: &[PathBuf]) -> Locks {
        let _ = roots;
        Locks { held: self.held.clone(), note: self.note.clone(), stale: vec![] }
    }
}

fn lane_lock(f: &Fixture, holder: &str) -> Lock {
    let root = f.base.join("lane");
    Lock {
        resource: worktree_resource(&root),
        root,
        holder: holder.into(),
        lease_id: "a1b2c3d4e5f60718".into(),
        expires_at: Some(now() + 600.0),
        source: Source::Ledger,
    }
}

fn mock(f: &Fixture, holder: &str) -> Mock {
    Mock { held: vec![lane_lock(f, holder)], me: vec!["me-1".into()], note: None }
}

fn call(tool: &str, input: Value, cwd: &Path) -> ToolCall {
    ToolCall { tool: tool.into(), input: input.as_object().cloned().unwrap(), cwd: cwd.to_path_buf() }
}

fn bash(cmd: &str, cwd: &Path) -> ToolCall {
    call("Bash", json!({"command": cmd}), cwd)
}

fn denied(d: &Decision) -> bool {
    matches!(d, Decision::Deny(_))
}

#[test]
fn file_tools_by_resolved_path() {
    let f = fixture("files");
    let (lane, free) = (f.base.join("lane"), f.base.join("free"));
    let m = mock(&f, "other-1");
    let cases: Vec<(ToolCall, bool)> = vec![
        (call("Edit", json!({"file_path": lane.join("src/a.txt")}), &free), true),
        (call("Write", json!({"file_path": lane.join("src/new.txt")}), &free), true),
        (call("Write", json!({"file_path": free.join("via-link/src/a.txt")}), &free), true),
        (call("Edit", json!({"file_path": free.join("../lane/src/a.txt")}), &free), true),
        (call("Write", json!({"file_path": "../lane/x"}), &free), true),
        (call("NotebookEdit", json!({"notebook_path": lane.join("n.ipynb")}), &free), true),
        (call("mcp__filesystem__write_file", json!({"path": lane.join("m.txt")}), &free), true),
        (call("Edit", json!({"file_path": free.join("hard.txt")}), &free), true),
        (call("Write", json!({"file_path": free.join("c.txt")}), &free), false),
        (call("Read", json!({"file_path": lane.join("src/a.txt")}), &free), false),
    ];
    for (c, deny) in cases {
        let d = decide(&c, &m);
        assert_eq!(denied(&d), deny, "{} {:?} -> {d:?}", c.tool, c.input);
    }
}

#[test]
fn shell_writes() {
    let f = fixture("shell");
    let (lane, free) = (f.base.join("lane"), f.base.join("free"));
    let m = mock(&f, "other-1");
    let l = lane.display();
    let blocked_from_free = [
        format!("rm -rf {l}/src"),
        "rm via-link/src/a.txt".into(),
        format!("cd {l} && rm src/a.txt"),
        format!("echo x > {l}/f"),
        "echo x >> via-link/src/a.txt".into(),
        format!("sed -i s/h/j/ {l}/src/a.txt"),
        format!("cp b.txt {l}/"),
        format!("mv {l}/src /tmp/stolen"),
        format!("cat b.txt | tee {l}/t"),
        format!("git -C {l} commit -am wip"),
        format!("find {l} -name '*.tmp' -delete"),
        format!("rm {l}/src/*"),
        format!("ln -s {l}/src/a.txt l2 && echo x > l2"),
        format!("GIT_DIR={l}/.git GIT_WORK_TREE={l} git commit -am x"),
        "echo x > hard.txt".into(),
        format!("bash -c 'rm {l}/src/a.txt'"),
        format!("echo $(rm {l}/src/a.txt)"),
        format!("timeout 5 nohup touch {l}/x"),
    ];
    for cmd in &blocked_from_free {
        assert!(denied(&decide(&bash(cmd, &free), &m)), "should be blocked: {cmd}");
    }
    for cmd in ["git commit -am wip", "git branch -D feature", "make build", "cargo test", "python3 -m pytest"] {
        assert!(denied(&decide(&bash(cmd, &lane), &m)), "inside the lane: {cmd}");
    }
    let repo = f.base.join("repo");
    for cmd in [format!("D={l}; rm -rf $D/src"), "rm -rf $X".into(), "ls | xargs rm".into(), "cd $X && make".into()] {
        assert!(denied(&decide(&bash(&cmd, &repo), &m)), "unresolvable, in the locked lane's repository: {cmd}");
    }
    for cmd in [format!("rm -rf {l}/s*/a.*"), "rm via-*/src/a.txt".into(), "rm [v]ia-link/src/*".into()] {
        assert!(denied(&decide(&bash(&cmd, &free), &m)), "a glob naming the lane: {cmd}");
    }
    for cmd in [
        format!("ls -la {l}/src && cat {l}/src/a.txt"),
        format!("grep -rn hello {l}"),
        "cp b.txt c.txt && rm c.txt".into(),
        "cat > /tmp/x <<'E'\nrm -rf src\nE".into(),
        "rm *.tmp b.t?t".into(),
    ] {
        assert_eq!(decide(&bash(&cmd, &free), &m), Decision::Allow, "{cmd}");
    }
    for cmd in ["git status && git log --oneline -3 && git diff", "ls", "agent-bridge worktree status ."] {
        assert_eq!(decide(&bash(cmd, &lane), &m), Decision::Allow, "read inside the lane: {cmd}");
    }
}

#[test]
fn the_shared_git_files_and_hook_overrides() {
    let f = fixture("refs");
    let (repo, free) = (f.base.join("repo"), f.base.join("free"));
    let m = mock(&f, "other-1");
    let git = repo.join(".git");
    for path in ["refs/heads/lane", "refs/heads/lane.lock", "packed-refs", "hooks/reference-transaction", "config"] {
        let c = call("Write", json!({"file_path": git.join(path)}), &free);
        assert!(denied(&decide(&c, &m)), "{path}");
    }
    let c = call("Write", json!({"file_path": git.join("refs/heads/other")}), &free);
    assert_eq!(decide(&c, &m), Decision::Allow, "another branch's ref is not the lane's");
    for cmd in [
        "git -c core.hooksPath=/dev/null commit -am x",
        "GIT_CONFIG_PARAMETERS=\"'core.hooksPath'='/dev/null'\" git commit -am x",
        "git config core.hooksPath /tmp/none",
    ] {
        assert!(denied(&decide(&bash(cmd, &repo), &m)), "{cmd}");
    }
    assert_eq!(decide(&bash("git config --get core.hooksPath", &repo), &m), Decision::Allow);
    assert_eq!(decide(&bash("git commit -am x", &repo), &m), Decision::Allow, "the main worktree isn't leased");
}

#[test]
fn unrelated_work_is_never_wedged() {
    let f = fixture("unrelated");
    let free = f.base.join("free");
    let m = mock(&f, "other-1");
    for cmd in [
        "rm $X",
        "rm *.tmp",
        "cp $SRC/* .",
        "find . -name '*.o' | xargs rm",
        "cd \"$OUT\" && make",
        "git -c core.hooksPath=/dev/null status",
        "git config core.hooksPath .githooks",
    ] {
        assert_eq!(decide(&bash(cmd, &free), &m), Decision::Allow, "outside the locked repository: {cmd}");
    }
    let edit = call("Edit", json!({"file_path": free.join("b.txt")}), &free);
    assert_eq!(decide(&edit, &m), Decision::Allow);
    // a hard link elsewhere on the same device is still refused (the other name may be inside the lane)
    let hard = call("Edit", json!({"file_path": free.join("hard.txt")}), &free);
    assert!(denied(&decide(&hard, &m)));
    let repo = f.base.join("repo");
    let hooks = format!("git -C {} -c core.hooksPath=/dev/null commit -am x", repo.display());
    assert!(denied(&decide(&bash(&hooks, &free), &m)), "-C into the locked lane's repository");
    let hooks = format!("git --git-dir={}/.git -c core.hooksPath=/dev/null commit", repo.display());
    assert!(denied(&decide(&bash(&hooks, &free), &m)), "--git-dir into it");
}

#[test]
fn holders_and_free_worktrees() {
    let f = fixture("holders");
    let lane = f.base.join("lane");
    let edit = call("Edit", json!({"file_path": lane.join("src/a.txt")}), &lane);
    assert_eq!(decide(&edit, &mock(&f, "me-1")), Decision::Allow, "the holder works");
    let sub = Mock { held: vec![lane_lock(&f, "sub-2")], me: vec!["me-1".into(), "sub-2".into()], note: None };
    assert_eq!(decide(&edit, &sub), Decision::Allow, "a subagent's lease covers the session");
    let free = Mock { held: vec![], me: vec!["me-1".into()], note: None };
    assert_eq!(decide(&edit, &free), Decision::Allow, "nobody holds it: nothing to enforce");
    assert_eq!(decide(&bash("rm -rf $X", &f.base.join("free")), &free), Decision::Allow, "no lock, no doubt");
}

#[test]
fn the_refusal_reads_as_specified() {
    let f = fixture("text");
    let lane = f.base.join("lane");
    let Decision::Deny(text) = decide(&bash("rm src/a.txt", &lane), &mock(&f, "other-1")) else { panic!() };
    let l = lane.display();
    assert!(text.starts_with(&format!("blocked: `rm src/a.txt` is inside {l}, leased by other-1 until ")), "{text}");
    let Decision::Deny(run) = decide(&bash("make build", &lane), &mock(&f, "other-1")) else { panic!() };
    assert!(
        run.starts_with("blocked: `make build` (only read-only commands run in a worktree someone else holds)"),
        "{run}"
    );
    assert!(text.contains(&format!("is inside {l}, leased by other-1 until ")), "{text}");
    assert!(
        text.ends_with(&format!("(lease a1b2c3d4e5f60718). To work there: agent-bridge worktree acquire me-1 {l}")),
        "{text}"
    );
    let mut offline = mock(&f, "other-1");
    offline.held[0].expires_at = None;
    offline.note = Some("ledgerd unreachable; using locks as of 2026-11-18 16:00:00".into());
    offline.me.clear();
    let Decision::Deny(text) = decide(&bash("rm src/a.txt", &lane), &offline) else { panic!() };
    assert!(text.contains("leased by other-1 (lease"), "no expiry when it's unknown: {text}");
    assert!(text.contains("worktree acquire <your address>") && text.ends_with("as of 2026-11-18 16:00:00)"), "{text}");
}

#[test]
fn hook_output() {
    let deny = run("not json");
    assert_eq!(deny.code, 2, "unreadable input fails closed");
    let allow = run(r#"{"tool_name": "Read", "tool_input": {"file_path": "/etc/hostname"}, "cwd": "/"}"#);
    assert_eq!((allow.code, allow.stderr.as_str(), allow.stdout.as_str()), (0, "", ""));
    let ids = worktree_resource(Path::new("/home/u/wt"));
    assert!(ids.starts_with("admin/wt-") && ids.len() == "admin/wt-".len() + 16);
}

#[test]
fn the_last_known_file_never_loses_a_recorded_lease() {
    let f = fixture("known");
    let path = f.base.join("guard-locks.json");
    let lane = lane_lock(&f, "other-1");
    let ids = |p: &Path| recalled_in(p).1.into_iter().map(|l| l.lease_id).collect::<Vec<_>>();
    // a lookup queried before the grant writes after the acquire recorded it: the lease stays (WorktreeLock.tla)
    let asked_at = now() - 1.0;
    record_in(&path, &lane).unwrap();
    remember_in(&path, &[], asked_at).unwrap();
    assert_eq!(ids(&path), ["a1b2c3d4e5f60718"], "recorded after the query: kept");
    // a lookup queried after the record is authoritative: a lease ledgerd no longer lists is dropped
    remember_in(&path, &[], now() + 1.0).unwrap();
    assert!(ids(&path).is_empty(), "older than the query and not listed: gone");
    // record is add-only and replaces only the same worktree; forget drops only its lease
    let mut other = lane_lock(&f, "other-2");
    other.root = f.base.join("free");
    other.resource = worktree_resource(&other.root);
    other.lease_id = "ffffffffffffffff".into();
    record_in(&path, &lane).unwrap();
    record_in(&path, &other).unwrap();
    let mut again = lane.clone();
    again.lease_id = "0000000000000001".into();
    record_in(&path, &again).unwrap();
    assert_eq!(ids(&path), ["ffffffffffffffff", "0000000000000001"]);
    forget_in(&path, "ffffffffffffffff").unwrap();
    assert_eq!(ids(&path), ["0000000000000001"]);
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
    // concurrent records from many threads: none lost (read-modify-write under the flock)
    let handles: Vec<_> = (0..16)
        .map(|i| {
            let (path, mut l) = (path.clone(), lane.clone());
            std::thread::spawn(move || {
                l.root = PathBuf::from(format!("/wt/{i}"));
                l.lease_id = format!("{i:016x}");
                record_in(&path, &l).unwrap();
            })
        })
        .collect();
    handles.into_iter().for_each(|h| h.join().unwrap());
    assert_eq!(read_locks(&path).1.len(), 17);
}
