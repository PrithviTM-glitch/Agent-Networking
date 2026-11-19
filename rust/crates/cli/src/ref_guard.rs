//! `agent-bridge hook ref-guard STATE`: git's reference-transaction hook. In the "prepared" state it refuses a ref
//! update that would move a worktree another agent leases: that worktree's HEAD and per-worktree refs, and the branch
//! it has checked out, from whichever worktree of the repository the update comes. The lock lookup is the guard's
//! (guard::lock_of, me and refusal), so both hooks agree. Exit 1 aborts the transaction; outside an agent session the
//! update is allowed with a warning; an error of its own refuses (fails closed).

use agent_bridge::{guard, host};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

/// A worktree of the repository: its root and the branch it has checked out.
struct Worktree {
    root: PathBuf,
    branch: Option<String>,
}

/// git as the hook's git process set it up (GIT_DIR and all), read-only commands only.
fn git(args: &[&str]) -> Result<String, String> {
    let out = Command::new("git").args(args).output().map_err(|e| format!("cannot run git: {e}"))?;
    if !out.status.success() {
        return Err(format!("git {} failed: {}", args.join(" "), String::from_utf8_lossy(&out.stderr).trim()));
    }
    String::from_utf8(out.stdout).map_err(|_| format!("git {} printed non-UTF-8", args.join(" ")))
}

fn worktrees() -> Result<Vec<Worktree>, String> {
    let mut out = Vec::new();
    for block in git(&["worktree", "list", "--porcelain"])?.split("\n\n") {
        let mut lines = block.lines();
        let Some(root) = lines.next().and_then(|l| l.strip_prefix("worktree ")) else { continue };
        let branch = block.lines().find_map(|l| l.strip_prefix("branch ")).map(str::to_owned);
        let root = std::fs::canonicalize(root).unwrap_or_else(|_| PathBuf::from(root));
        out.push(Worktree { root, branch });
    }
    Ok(out)
}

/// The worktree this git process works in, None for a bare repository.
fn current() -> Option<PathBuf> {
    let top = git(&["rev-parse", "--show-toplevel"]).ok()?;
    let top = top.trim_end_matches('\n');
    (!top.is_empty()).then(|| std::fs::canonicalize(top).unwrap_or_else(|_| PathBuf::from(top)))
}

/// Refs that belong to one worktree rather than the repository.
fn per_worktree(name: &str) -> bool {
    name == "HEAD"
        || !name.starts_with("refs/")
        || ["refs/bisect/", "refs/worktree/", "refs/rewritten/"].iter().any(|p| name.starts_with(p))
}

/// The worktrees an update of ref name moves.
fn owners<'a>(name: &str, all: &'a [Worktree], here: Option<&Path>) -> Vec<&'a Path> {
    all.iter()
        .filter(|w| (per_worktree(name) && here == Some(w.root.as_path())) || w.branch.as_deref() == Some(name))
        .map(|w| w.root.as_path())
        .collect()
}

/// The refused updates' texts, or Err when the hook cannot decide.
fn check(input: &str, session: &str) -> Result<Vec<String>, String> {
    let names: Vec<&str> = input
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| l.splitn(3, ' ').nth(2).ok_or_else(|| format!("unreadable update line {l:?}")))
        .collect::<Result<_, _>>()?;
    if names.is_empty() {
        return Ok(Vec::new());
    }
    let all = worktrees()?;
    let here = current();
    let me = guard::me(session);
    let mut refused = Vec::new();
    let mut seen = Vec::new();
    for name in names {
        for root in owners(name, &all, here.as_deref()) {
            if seen.contains(&(name, root)) {
                continue;
            }
            seen.push((name, root));
            let (lock, note) = guard::lock_of(root, session);
            if let Some(lock) = lock.filter(|l| !me.contains(&l.holder)) {
                refused.push(guard::refusal(&format!("the update of {name}"), &lock, &me, note.as_deref()));
            }
        }
    }
    Ok(refused)
}

/// The hook: the exit status git sees.
pub fn run(args: &[String]) -> i32 {
    if args.first().map(String::as_str) != Some("prepared") {
        return 0;
    }
    let mut input = String::new();
    if std::io::stdin().read_to_string(&mut input).is_err() {
        eprintln!("agent-bridge ref-guard: blocked: could not read the ref updates");
        return 1;
    }
    let session = host::current().session().unwrap_or_default();
    match check(&input, &session) {
        Ok(refused) if refused.is_empty() => 0,
        Ok(refused) if session.is_empty() => {
            for text in refused {
                eprintln!("agent-bridge ref-guard: allowed outside an agent session, but {text}");
            }
            0
        }
        Ok(refused) => {
            for text in refused {
                eprintln!("agent-bridge ref-guard: {text}");
            }
            1
        }
        Err(e) => {
            eprintln!("agent-bridge ref-guard: blocked: {e}");
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn which_worktrees_a_ref_moves() {
        let all = [
            Worktree { root: "/r".into(), branch: Some("refs/heads/main".into()) },
            Worktree { root: "/w".into(), branch: Some("refs/heads/side".into()) },
            Worktree { root: "/d".into(), branch: None },
        ];
        let at = |name: &str, here: &str| owners(name, &all, Some(Path::new(here)));
        assert_eq!(at("HEAD", "/d"), [Path::new("/d")], "HEAD is the current worktree's");
        assert_eq!(at("refs/heads/side", "/r"), [Path::new("/w")], "a branch is its checkout's, from anywhere");
        assert_eq!(at("refs/bisect/bad", "/w"), [Path::new("/w")]);
        assert_eq!(at("ORIG_HEAD", "/w"), [Path::new("/w")]);
        assert!(at("refs/tags/v1", "/w").is_empty(), "shared refs no worktree has out are free");
        assert!(at("refs/agent-bridge/released/x", "/w").is_empty(), "release's snapshot ref is free");
        assert!(owners("HEAD", &all, None).is_empty(), "a bare repository has no current worktree");
    }
}
