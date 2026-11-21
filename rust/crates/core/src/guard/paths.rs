//! Paths as the kernel would see them: `~` and relative paths made absolute, symlinks and `..` resolved (through the
//! longest prefix that exists, so a file not created yet still resolves), and the git worktree a path belongs to.

use std::fs;
use std::path::{Component, Path, PathBuf};

/// path resolved against cwd: `~` expanded, symlinks followed through the part that exists, `..` applied to the rest.
pub fn resolve(path: &str, cwd: &Path) -> PathBuf {
    let full = absolute(path, cwd);
    let comps: Vec<Component> = full.components().collect();
    // the longest prefix that exists, canonicalised; then the rest applied lexically
    for split in (1..=comps.len()).rev() {
        let head: PathBuf = comps[..split].iter().collect();
        if let Ok(real) = fs::canonicalize(&head) {
            let rest: PathBuf = comps[split..].iter().collect();
            return lexical(&real, &rest.to_string_lossy());
        }
    }
    full
}

/// rest applied to base without the filesystem: `.` dropped, `..` removes one component (never above `/`), the rest
/// appended. Pure, so it can be proved (Kani): with no `..` in rest, the result starts with base.
pub fn lexical(base: &Path, rest: &str) -> PathBuf {
    let mut out = base.to_path_buf();
    for c in Path::new(rest).components() {
        match c {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            Component::RootDir | Component::Prefix(_) => out = PathBuf::from("/"),
            Component::Normal(n) => out.push(n),
        }
    }
    out
}

/// path made absolute against cwd, `~` expanded; nothing resolved.
fn absolute(path: &str, cwd: &Path) -> PathBuf {
    let expanded = match path.strip_prefix('~') {
        Some(rest) if rest.is_empty() || rest.starts_with('/') => {
            PathBuf::from(std::env::var("HOME").unwrap_or_default()).join(rest.trim_start_matches('/'))
        }
        _ => PathBuf::from(path),
    };
    if expanded.is_absolute() { expanded } else { cwd.join(expanded) }
}

/// The paths a glob names from cwd, resolved, as the shell expands it: `*`, `?` and `[...]` within one component,
/// a leading `.` matched only literally. With no match the shell passes the pattern on, so its literal path is the
/// one judged. None past `limit` matches.
pub fn glob(pattern: &str, cwd: &Path, limit: usize) -> Option<Vec<PathBuf>> {
    let full = absolute(pattern, cwd);
    let mut found = vec![PathBuf::new()];
    for comp in full.components() {
        let name = comp.as_os_str().to_string_lossy();
        if !matches!(comp, Component::Normal(_)) || !name.contains(['*', '?', '[']) {
            found.iter_mut().for_each(|f| f.push(comp.as_os_str()));
            continue;
        }
        let pat: Vec<char> = name.chars().collect();
        let mut next = Vec::new();
        for base in &found {
            let Ok(entries) = fs::read_dir(base) else { continue };
            for e in entries.flatten() {
                let n: Vec<char> = e.file_name().to_string_lossy().chars().collect();
                if (n.first() != Some(&'.') || pat.first() == Some(&'.')) && fnmatch(&pat, &n) {
                    next.push(base.join(e.file_name()));
                    if next.len() > limit {
                        return None;
                    }
                }
            }
        }
        found = next;
    }
    if found.is_empty() {
        found.push(full);
    }
    found.sort();
    Some(found.iter().map(|p| resolve(&p.to_string_lossy(), Path::new("/"))).collect())
}

/// Shell pattern matching of one name, iterative (backtracking only to the last `*`), so no pattern is exponential.
fn fnmatch(p: &[char], n: &[char]) -> bool {
    let (mut pi, mut ni, mut star, mut mark) = (0, 0, None, 0);
    while ni < n.len() {
        if p.get(pi) == Some(&'*') {
            (star, mark, pi) = (Some(pi), ni, pi + 1);
            continue;
        }
        if let Some(step) = p.get(pi).and_then(|_| one(&p[pi..], n[ni])) {
            (pi, ni) = (pi + step, ni + 1);
            continue;
        }
        let Some(s) = star else { return false };
        mark += 1;
        (pi, ni) = (s + 1, mark);
    }
    p[pi.min(p.len())..].iter().all(|&c| c == '*')
}

/// Whether the pattern element at the head of p (`?`, a `[...]` class or one character) matches c, and its length.
fn one(p: &[char], c: char) -> Option<usize> {
    match p[0] {
        '?' => Some(1),
        '[' => {
            let neg = matches!(p.get(1), Some('!' | '^'));
            let mut i = if neg { 2 } else { 1 };
            let (start, mut hit) = (i, false);
            while i < p.len() && (p[i] != ']' || i == start) {
                if p.get(i + 1) == Some(&'-') && p.get(i + 2).is_some_and(|&e| e != ']') {
                    hit |= p[i] <= c && c <= p[i + 2];
                    i += 3;
                } else {
                    hit |= p[i] == c;
                    i += 1;
                }
            }
            if i >= p.len() {
                return (c == '[').then_some(1); // no closing `]`: a literal `[`
            }
            (hit != neg).then_some(i + 1)
        }
        x => (x == c).then_some(1),
    }
}

/// The worktree root path belongs to: the nearest ancestor holding a `.git` entry. A path inside a repository's own
/// git dir belongs to its worktree: `<root>/.git/worktrees/<name>/...` to that linked worktree, the rest to `<root>`.
pub fn worktree_root(path: &Path) -> Option<PathBuf> {
    let mut parts: Vec<&std::ffi::OsStr> = Vec::new();
    for anc in path.ancestors() {
        if anc.file_name().is_some_and(|n| n == ".git") && anc.is_dir() {
            // inside a git dir: a linked worktree's private dir maps to that worktree
            let rel: Vec<_> = parts.iter().rev().collect();
            if rel.len() >= 2 && rel[0] == &std::ffi::OsStr::new("worktrees") {
                let gitdir = anc.join("worktrees").join(rel[1]);
                if let Some(root) = linked_root(&gitdir) {
                    return Some(root);
                }
            }
            return anc.parent().map(Path::to_path_buf);
        }
        if anc.join(".git").exists() {
            return Some(anc.to_path_buf());
        }
        if let Some(n) = anc.file_name() {
            parts.push(n);
        }
    }
    None
}

/// The worktree root a linked worktree's private git dir (`<common>/worktrees/<name>`) belongs to, from its `gitdir`.
pub fn linked_root(gitdir: &Path) -> Option<PathBuf> {
    let dotgit = fs::read_to_string(gitdir.join("gitdir")).ok()?;
    Some(Path::new(dotgit.trim()).parent()?.to_path_buf())
}

/// A worktree's git dir: `<root>/.git` for the main worktree, the `gitdir:` target for a linked one.
pub fn git_dir(root: &Path) -> Option<PathBuf> {
    let dotgit = root.join(".git");
    if dotgit.is_dir() {
        return Some(dotgit);
    }
    let text = fs::read_to_string(&dotgit).ok()?;
    let target = text.trim().strip_prefix("gitdir:")?.trim();
    Some(resolve(target, root))
}

/// The repository's common dir (shared refs and objects) for a worktree.
pub fn common_dir(root: &Path) -> Option<PathBuf> {
    let gd = git_dir(root)?;
    match fs::read_to_string(gd.join("commondir")) {
        Ok(rel) => Some(resolve(rel.trim(), &gd)),
        Err(_) => Some(gd),
    }
}

/// The branch a worktree has checked out (`refs/heads/<name>`), or None when detached.
pub fn branch(root: &Path) -> Option<String> {
    let head = fs::read_to_string(git_dir(root)?.join("HEAD")).ok()?;
    head.trim().strip_prefix("ref: ").map(str::to_owned)
}

/// Every worktree of the repository whose common dir is `common`: the main one first, then the linked ones.
pub fn repo_worktrees(common: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if common.file_name().is_some_and(|n| n == ".git")
        && let Some(main) = common.parent()
    {
        out.push(main.to_path_buf());
    }
    if let Ok(entries) = fs::read_dir(common.join("worktrees")) {
        for e in entries.flatten() {
            if let Some(root) = linked_root(&e.path()) {
                out.push(root);
            }
        }
    }
    out
}

/// The git lock reasons of a repository's linked worktrees that name an agent-bridge lease: (root, lease id, holder).
/// Accepts "agent-bridge lease <id> held by <addr>" and the older "… lease <id> by <addr> until …".
pub fn git_locks(common: &Path) -> Vec<(PathBuf, String, String)> {
    let mut out = Vec::new();
    let Ok(entries) = fs::read_dir(common.join("worktrees")) else { return out };
    for e in entries.flatten() {
        let Ok(reason) = fs::read_to_string(e.path().join("locked")) else { continue };
        let words: Vec<&str> = reason.split_whitespace().collect();
        if words.len() >= 5 && words[0] == "agent-bridge" && words[1] == "lease" {
            let by = words.iter().position(|w| *w == "by").and_then(|i| words.get(i + 1));
            if let (Some(root), Some(holder)) = (linked_root(&e.path()), by) {
                out.push((root, words[2].to_owned(), (*holder).to_owned()));
            }
        }
    }
    out
}

/// More than one hard link: another name for this file may be inside a locked worktree.
pub fn multi_link(path: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    fs::metadata(path).is_ok_and(|m| m.is_file() && m.nlink() > 1)
}

/// Whether a (the nearest existing ancestor of) path is on the same filesystem as dir: only then can a hard link
/// join them.
pub fn same_device(path: &Path, dir: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    let dev = |p: &Path| p.ancestors().find_map(|a| fs::metadata(a).ok()).map(|m| m.dev());
    dev(path).is_some_and(|d| dev(dir) == Some(d))
}

pub fn within(path: &Path, root: &Path) -> bool {
    path == root || path.starts_with(root)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_like_the_kernel() {
        let d = std::env::temp_dir().join(format!("guard-paths-{}", std::process::id()));
        #[allow(clippy::let_underscore_must_use, reason = "a leftover from an earlier run, usually none")]
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(d.join("locked/src")).unwrap();
        fs::create_dir_all(d.join("free")).unwrap();
        std::os::unix::fs::symlink("../locked", d.join("free/via-link")).unwrap();
        let real = fs::canonicalize(&d).unwrap();
        let free = real.join("free");
        assert_eq!(resolve("via-link/src/new.txt", &free), real.join("locked/src/new.txt"), "symlink, new file");
        assert_eq!(resolve("../locked/./x", &free), real.join("locked/x"));
        assert_eq!(resolve(free.join("nope/../../locked").to_str().unwrap(), &free), real.join("locked"));
        assert_eq!(lexical(Path::new("/a/b"), "./c/../d"), Path::new("/a/b/d"));
        assert_eq!(lexical(Path::new("/a"), "../../.."), Path::new("/"), "never above the root");
        assert_eq!(lexical(Path::new("/a"), "/etc"), Path::new("/etc"), "an absolute rest starts over");
        fs::remove_dir_all(&d).unwrap();
    }

    #[test]
    fn globs_like_the_shell() {
        let d = std::env::temp_dir().join(format!("guard-glob-{}", std::process::id()));
        #[allow(clippy::let_underscore_must_use, reason = "a leftover from an earlier run, usually none")]
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(d.join("locked/src")).unwrap();
        fs::create_dir_all(d.join("free")).unwrap();
        for f in ["free/a.tmp", "free/b.tmp", "free/.h.tmp", "free/c.txt", "locked/src/x.rs"] {
            fs::write(d.join(f), "").unwrap();
        }
        std::os::unix::fs::symlink("../locked", d.join("free/via-link")).unwrap();
        let real = fs::canonicalize(&d).unwrap();
        let free = real.join("free");
        let names = |pat: &str| glob(pat, &free, 100).unwrap();
        assert_eq!(names("*.tmp"), [free.join("a.tmp"), free.join("b.tmp")], "no dot files");
        assert_eq!(names(".*.tmp"), [free.join(".h.tmp")]);
        assert_eq!(names("[ab].t?p"), [free.join("a.tmp"), free.join("b.tmp")]);
        assert_eq!(names("[!a]*.tmp"), [free.join("b.tmp")]);
        assert_eq!(names("via-*/src/*"), [real.join("locked/src/x.rs")], "through a symlink, resolved");
        assert_eq!(names("*.none"), [free.join("*.none")], "no match: the literal path");
        assert_eq!(glob("*", &free, 2), None, "past the limit");
        let long: Vec<char> = "*a*a*a*a*a*a*a*a*a*a*a*b".chars().collect();
        assert!(!fnmatch(&long, &['a'; 60]), "not exponential");
        fs::remove_dir_all(&d).unwrap();
    }
}
