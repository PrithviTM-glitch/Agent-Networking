//! The guard fixture the targets share: git's own layout in a scratch dir (a main worktree `repo/` and a linked
//! worktree `lane/` leased by other-1, a free dir with a symlink and a hard link into the lane), as guard/tests.rs.

#![allow(dead_code)]

use agent_bridge::guard::{Lock, Locks, Lookup, Source, worktree_resource};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

pub struct Fixture {
    pub base: PathBuf,
    pub lane: PathBuf,
    pub repo: PathBuf,
    pub free: PathBuf,
}

pub fn fixture() -> &'static Fixture {
    static F: OnceLock<Fixture> = OnceLock::new();
    F.get_or_init(|| {
        let base = std::env::temp_dir().join(format!("agent-bridge-fuzz-{}", std::process::id()));
        let _ = fs::remove_dir_all(&base);
        let git = base.join("repo/.git");
        for d in ["worktrees/lane", "refs/heads", "hooks"] {
            fs::create_dir_all(git.join(d)).unwrap();
        }
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
        fs::write(base.join("free/c.tmp"), "x\n").unwrap();
        std::os::unix::fs::symlink("../lane", base.join("free/via-link")).unwrap();
        fs::hard_link(base.join("lane/src/a.txt"), base.join("free/hard.txt")).unwrap();
        Fixture { lane: base.join("lane"), repo: base.join("repo"), free: base.join("free"), base }
    })
}

/// other-1 holds the lane; this session is me-1.
pub struct Mock;

impl Lookup for Mock {
    fn me(&self) -> Vec<String> {
        vec!["me-1".into()]
    }
    fn locks(&self, _roots: &[PathBuf]) -> Locks {
        let root = fixture().lane.clone();
        let lock = Lock {
            resource: worktree_resource(&root),
            root,
            holder: "other-1".into(),
            lease_id: "a1b2c3d4e5f60718".into(),
            expires_at: Some(4_000_000_000.0),
            source: Source::Ledger,
        };
        Locks { held: vec![lock], note: None, stale: vec![] }
    }
}

pub fn within(p: &Path, root: &Path) -> bool {
    p.starts_with(root)
}
