//! The worktree guard (ACCESS.md 5.6 and 6): decides whether a Claude Code tool call may write inside a git worktree
//! that another agent holds a lease on. `agent-bridge hook guard` (a PreToolUse hook) calls [`run`]; the git
//! reference-transaction hook (`agent-bridge hook ref-guard`) uses [`lock_of`], [`me`] and [`refusal`], so the two
//! always agree.
//!
//! A worktree is locked only while an agent other than this session's holds its `admin/wt-<hash>` lease. Inside a
//! locked worktree, without the lease, the guard refuses:
//! - `Edit`, `Write`, `NotebookEdit` and the listed MCP write tools, by resolved path (symlinks, `..`, new files);
//! - shell writes: mutating commands' paths, redirections, in-place edits, `find -delete`/`-exec`, git writes aimed
//!   there (`git -C`, `--git-dir`, `--work-tree`, `GIT_DIR` …), and any program but a read-only allowlist when the
//!   working directory is inside (ACCESS.md decision 13);
//! - in that worktree's repository: edits of the branch's ref file, `packed-refs`, the hooks dir and the repository
//!   config; and, when the call runs in that repository (its working directory, `-C` or `--git-dir` target),
//!   `core.hooksPath` overrides and paths it can't resolve (`$VAR`, `$(...)`, `xargs` operands);
//! - writes to a file with more than one hard link on the same device as the locked worktree.
//!
//! Globs are expanded from the working directory and judged by the paths they name. Work in a repository with no
//! locked worktree, or outside any repository, is never refused for what the guard can't resolve.
//!
//! ledgerd is authoritative whenever reachable. When it isn't, the guard enforces the union of the last locks it saw
//! (`guard-locks.json`) and the git lock reasons, and says so. It is a text check in front of a shell: interpreters,
//! scripts and other harnesses get past it (labelled "hook"); the credential and the sandbox are the gates.

pub mod paths;
pub mod shell;

use crate::json::{Map, dumps, now};
use paths::{branch, common_dir, git_locks, multi_link, repo_worktrees, resolve, same_device, within, worktree_root};
use serde_json::{Value, json};
use shell::{Cmd, Word};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// What `hook guard` prints and exits with: 0 allows (stdout may carry a warning), 2 denies (stderr is the reason).
#[derive(Debug, PartialEq)]
pub struct Outcome {
    pub code: i32,
    pub stderr: String,
    pub stdout: String,
}

#[derive(Debug, PartialEq)]
pub enum Decision {
    Allow,
    Deny(String),
    Warn(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Source {
    Ledger,
    LastKnown,
    GitReason,
}

/// A worktree another agent may hold. `expires_at` is unknown when it came from a git lock reason.
#[derive(Debug, Clone, PartialEq)]
pub struct Lock {
    pub root: PathBuf,
    pub resource: String,
    pub holder: String,
    pub lease_id: String,
    pub expires_at: Option<f64>,
    pub source: Source,
}

/// The locks on some worktree roots, and what the caller should be told about how they were found.
#[derive(Debug, Default, Clone)]
pub struct Locks {
    pub held: Vec<Lock>,
    /// "ledgerd unreachable; using locks as of <t>", when it came from the fallback
    pub note: Option<String>,
    /// stale git locks (no live lease), for a warning
    pub stale: Vec<String>,
}

/// Where the guard learns who holds what, and who this session is. Live: ledgerd and presence; tests: fixed.
pub trait Lookup {
    /// This session's agent and its subagents' agents, the session's own first.
    fn me(&self) -> Vec<String>;
    /// Every held lock it knows of. `roots` are the worktrees the call touches (realpaths): the fallback reads their
    /// git lock reasons when ledgerd is unreachable, and they are checked for stale git locks.
    fn locks(&self, roots: &[PathBuf]) -> Locks;
}

pub struct ToolCall {
    pub tool: String,
    pub input: Map,
    pub cwd: PathBuf,
}

/// The resource id of a worktree (`admin/wt-<first 16 hex of sha256(realpath)>`): the one definition is the ledger's.
pub fn worktree_resource(realpath: &Path) -> String {
    crate::ledger::worktree_resource(&realpath.to_string_lossy())
}

/// File tools and the input field that names the file they write.
const FILE_TOOLS: [(&str, &str); 6] = [
    ("Edit", "file_path"),
    ("Write", "file_path"),
    ("NotebookEdit", "notebook_path"),
    ("mcp__filesystem__write_file", "path"),
    ("mcp__filesystem__edit_file", "path"),
    ("mcp__filesystem__create_directory", "path"),
];
/// Tools whose input carries a shell command.
const SHELL_TOOLS: [(&str, &str); 2] = [("Bash", "command"), ("mcp__terminal__run_in_terminal", "command")];

/// Programs that only read: the allowlist inside a locked worktree (ACCESS.md decision 13).
const READ_ONLY: [&str; 38] = [
    "ls",
    "cat",
    "head",
    "tail",
    "less",
    "more",
    "grep",
    "egrep",
    "fgrep",
    "rg",
    "ag",
    "wc",
    "stat",
    "file",
    "find",
    "tree",
    "du",
    "df",
    "diff",
    "cmp",
    "echo",
    "printf",
    "pwd",
    "true",
    "false",
    "test",
    "[",
    "which",
    "type",
    "whoami",
    "date",
    "basename",
    "dirname",
    "realpath",
    "readlink",
    "sort",
    "uniq",
    "agent-bridge",
];
/// git subcommands that never write.
const GIT_READ: [&str; 28] = [
    "status",
    "log",
    "diff",
    "show",
    "blame",
    "rev-parse",
    "ls-files",
    "ls-tree",
    "ls-remote",
    "grep",
    "describe",
    "shortlog",
    "cat-file",
    "rev-list",
    "help",
    "version",
    "var",
    "count-objects",
    "merge-base",
    "name-rev",
    "show-ref",
    "show-branch",
    "whatchanged",
    "cherry",
    "check-ignore",
    "check-attr",
    "for-each-ref",
    "annotate",
];
/// Mutating programs whose operands are paths (options taking a value listed per program).
const MUTATORS: [(&str, &[&str]); 19] = [
    ("rm", &[]),
    ("rmdir", &[]),
    ("unlink", &[]),
    ("shred", &["-n", "-s"]),
    ("touch", &["-d", "-t", "-r"]),
    ("mkdir", &["-m"]),
    ("truncate", &["-s", "-r"]),
    ("chmod", &["--reference"]),
    ("chown", &["--reference"]),
    ("chgrp", &["--reference"]),
    ("mv", &["-t", "-S"]),
    ("cp", &["-t", "-S"]),
    ("ln", &["-t", "-S"]),
    ("install", &["-m", "-o", "-g", "-t", "-S"]),
    ("rsync", &["-e", "--exclude", "--include", "--filter", "--rsh"]),
    ("tee", &[]),
    ("dd", &[]),
    ("patch", &["-p", "-o", "-d", "-i", "-r"]),
    ("git-mv", &[]),
];

/// One way a call could write; the worktree it lands in decides.
#[derive(Debug)]
enum Action {
    /// a file written or removed
    Write { what: String, path: PathBuf },
    /// a git command that writes, run against this directory
    Git { what: String, dir: PathBuf },
    /// a program other than the read-only allowlist, run with this working directory
    Run { what: String, cwd: PathBuf },
    /// a path or working directory it can't know (a variable, a substitution, text it couldn't lex), in the
    /// directory the call runs in (None: the call's own working directory)
    Unknown { what: String, scope: Option<PathBuf> },
    /// core.hooksPath set or overridden for a git command running in this directory
    Hooks { what: String, scope: Option<PathBuf> },
}

/// Decide one tool call. Pure apart from reading the filesystem (realpath, `.git` entries) and the lookup.
pub fn decide(call: &ToolCall, lookup: &dyn Lookup) -> Decision {
    let actions = actions_of(call);
    if actions.is_empty() {
        return Decision::Allow;
    }
    // every worktree the call could touch, and the other worktrees of their repositories
    let mut roots: BTreeSet<PathBuf> = BTreeSet::new();
    let add_repo = |p: &Path, roots: &mut BTreeSet<PathBuf>| {
        if let Some(root) = worktree_root(p) {
            if let Some(common) = common_dir(&root) {
                roots.extend(repo_worktrees(&common));
            }
            roots.insert(root);
        }
    };
    add_repo(&call.cwd, &mut roots);
    for a in &actions {
        match a {
            Action::Write { path, .. } => add_repo(path, &mut roots),
            Action::Git { dir, .. } | Action::Run { cwd: dir, .. } => add_repo(dir, &mut roots),
            Action::Unknown { scope, .. } | Action::Hooks { scope, .. } => {
                add_repo(scope.as_deref().unwrap_or(&call.cwd), &mut roots)
            }
        }
    }
    let roots: Vec<PathBuf> = roots.into_iter().collect();
    let locks = lookup.locks(&roots);
    if locks.held.is_empty() {
        return warn_or_allow(&locks);
    }
    let me = lookup.me();
    let others: Vec<&Lock> = locks.held.iter().filter(|l| !me.iter().any(|m| m == &l.holder)).collect();
    if others.is_empty() {
        return warn_or_allow(&locks);
    }
    for a in &actions {
        if let Some((what, lock)) = refused_by(a, &others, &call.cwd) {
            return Decision::Deny(refusal(&what, lock, &me, locks.note.as_deref()));
        }
    }
    warn_or_allow(&locks)
}

fn warn_or_allow(locks: &Locks) -> Decision {
    let mut notes: Vec<String> = locks.stale.clone();
    if let Some(n) = &locks.note {
        notes.push(n.clone());
    }
    if notes.is_empty() { Decision::Allow } else { Decision::Warn(format!("agent-bridge guard: {}", notes.join("; "))) }
}

/// The lock that refuses this action, with the wording of what was refused. `cwd` is the call's working directory.
fn refused_by<'a>(a: &Action, others: &[&'a Lock], cwd: &Path) -> Option<(String, &'a Lock)> {
    match a {
        Action::Write { what, path } => {
            if let Some(l) = others.iter().find(|l| within(path, &l.root)) {
                return Some((what.clone(), l));
            }
            if let Some(l) = protected_git_file(path, others) {
                return Some((what.clone(), l));
            }
            if !multi_link(path) {
                return None;
            }
            let l = others.iter().find(|l| same_device(path, &l.root))?;
            Some((format!("{what} (a file with more than one hard link)"), *l))
        }
        Action::Git { what, dir } => others.iter().find(|l| within(dir, &l.root)).map(|l| (what.clone(), *l)),
        Action::Run { what, cwd } => others
            .iter()
            .find(|l| within(cwd, &l.root))
            .map(|l| (format!("{what} (only read-only commands run in a worktree someone else holds)"), *l)),
        Action::Unknown { what, scope } | Action::Hooks { what, scope } => {
            lock_in_repo(scope.as_deref().unwrap_or(cwd), others).map(|l| (what.clone(), l))
        }
    }
}

/// A lock on a worktree of the repository `dir` is in (or on the worktree `dir` is in).
fn lock_in_repo<'a>(dir: &Path, others: &[&'a Lock]) -> Option<&'a Lock> {
    if let Some(l) = others.iter().find(|l| within(dir, &l.root)) {
        return Some(l);
    }
    let common = common_dir(&worktree_root(dir)?)?;
    others.iter().find(|l| common_dir(&l.root).as_ref() == Some(&common)).copied()
}

/// A repository file that changes a held worktree's history or guards: its branch's ref (and lock file),
/// packed-refs, the hooks dir and the repository config.
fn protected_git_file<'a>(path: &Path, others: &[&'a Lock]) -> Option<&'a Lock> {
    for l in others {
        let Some(common) = common_dir(&l.root) else { continue };
        let Ok(rel) = path.strip_prefix(&common) else { continue };
        let rel = rel.to_string_lossy();
        let rel = rel.strip_suffix(".lock").unwrap_or(&rel);
        if rel == "packed-refs" || rel == "config" || rel == "hooks" || rel.starts_with("hooks/") {
            return Some(l);
        }
        if branch(&l.root).is_some_and(|b| b == rel) {
            return Some(l);
        }
    }
    None
}

/// The one refusal text (ACCESS.md, W-4), shared with ref-guard and the MCP tools.
pub fn refusal(what: &str, lock: &Lock, me: &[String], note: Option<&str>) -> String {
    let until = lock.expires_at.map(|t| format!(" until {}", clock(t))).unwrap_or_default();
    let who = me.first().map_or("<your address>", String::as_str);
    let root = lock.root.display();
    let mut text = format!(
        "blocked: {what} is inside {root}, leased by {holder}{until} (lease {id}). To work there: agent-bridge \
         worktree acquire {who} {root}",
        holder = lock.holder,
        id = lock.lease_id,
    );
    if let Some(n) = note {
        text.push_str(&format!(" ({n})"));
    }
    text
}

fn clock(ts: f64) -> String {
    let at = crate::stamp::local(ts as i64, ' ');
    at.split_once(' ').map_or(at.clone(), |(_, t)| t.to_owned())
}

fn actions_of(call: &ToolCall) -> Vec<Action> {
    if let Some((_, field)) = FILE_TOOLS.iter().find(|(t, _)| *t == call.tool) {
        let Some(path) = call.input.get(*field).and_then(Value::as_str) else {
            return vec![Action::Unknown { what: format!("{} without a path", call.tool), scope: None }];
        };
        let what = format!("{} {path}", call.tool);
        return vec![Action::Write { path: resolve(path, &call.cwd), what }];
    }
    if call.tool == "mcp__filesystem__move_file" {
        return ["source", "destination"]
            .iter()
            .filter_map(|f| call.input.get(*f).and_then(Value::as_str))
            .map(|p| Action::Write { what: format!("{} {p}", call.tool), path: resolve(p, &call.cwd) })
            .collect();
    }
    if let Some((_, field)) = SHELL_TOOLS.iter().find(|(t, _)| *t == call.tool) {
        return match call.input.get(*field).and_then(Value::as_str) {
            Some(cmd) => shell_actions(cmd, &call.cwd),
            None => vec![],
        };
    }
    vec![]
}

fn shell_actions(command: &str, cwd: &Path) -> Vec<Action> {
    let cmds = match shell::commands(command) {
        Ok(c) => c,
        Err(e) => {
            return vec![Action::Unknown { what: format!("a command the guard could not read ({e})"), scope: None }];
        }
    };
    let mut out = Vec::new();
    let mut cur: Option<PathBuf> = Some(cwd.to_path_buf());
    for cmd in &cmds {
        command_actions(cmd, &mut cur, &mut out);
    }
    out
}

/// The most paths one glob may name before the guard treats it as unresolvable.
const GLOB_LIMIT: usize = 4096;

/// A word as the paths it names from the working directory (a glob expanded), or None when they can't be known.
fn paths_of(w: &Word, cur: &Option<PathBuf>) -> Option<Vec<PathBuf>> {
    if w.dynamic && !w.glob {
        return None;
    }
    let base = if w.text.starts_with('/') || w.text.starts_with('~') { Path::new("/") } else { cur.as_deref()? };
    if w.glob { paths::glob(&w.text, base, GLOB_LIMIT) } else { Some(vec![resolve(&w.text, base)]) }
}

/// A word as one directory (`cd`, `git -C`): a glob's first match.
fn path_of(w: &Word, cur: &Option<PathBuf>) -> Option<PathBuf> {
    paths_of(w, cur)?.into_iter().next()
}

fn write(what: String, w: &Word, cur: &Option<PathBuf>, out: &mut Vec<Action>) {
    match paths_of(w, cur) {
        Some(paths) => out.extend(paths.into_iter().map(|path| Action::Write { what: what.clone(), path })),
        None => out.push(Action::Unknown {
            what: format!("{what} (a path the guard can't resolve: `{}`)", w.text),
            scope: cur.clone(),
        }),
    }
}

fn command_actions(cmd: &Cmd, cur: &mut Option<PathBuf>, out: &mut Vec<Action>) {
    for (op, target) in &cmd.redirects {
        if op.contains('>') {
            write(format!("a redirection to {}", target.text), target, cur, out);
        }
    }
    let Some(prog) = cmd.program() else { return };
    let args = &cmd.words[1..];
    let line = shown(cmd);
    if prog == "cd" || prog == "pushd" {
        *cur = match args.iter().find(|a| !a.text.starts_with('-')) {
            Some(a) => path_of(a, cur),
            None => std::env::var("HOME").ok().map(PathBuf::from),
        };
        return;
    }
    if prog == "git" {
        return git_actions(cmd, cur, out);
    }
    if ["sh", "bash", "zsh", "dash", "ksh"].contains(&prog) && args.first().is_some_and(|w| w.dynamic) {
        out.push(Action::Unknown { what: format!("`{line}` (a script the guard can't read)"), scope: cur.clone() });
        return;
    }
    if let Some((_, with_value)) = MUTATORS.iter().find(|(p, _)| *p == prog) {
        let mut ops = operands(args, with_value);
        if ["chmod", "chown", "chgrp"].contains(&prog) && !args.iter().any(|a| a.text.starts_with("--reference")) {
            ops = ops.into_iter().skip(1).collect(); // the mode or owner
        }
        if prog == "dd" {
            ops = args
                .iter()
                .filter_map(|a| {
                    a.text.strip_prefix("of=").map(|p| Word { text: p.into(), dynamic: a.dynamic, glob: a.glob })
                })
                .collect();
        }
        if prog == "patch"
            && let Some(c) = cur.clone()
        {
            out.push(Action::Write { what: format!("`{line}`"), path: c });
        }
        for w in ops {
            write(format!("`{line}`"), &w, cur, out);
        }
        return;
    }
    let in_place = (prog == "sed" && args.iter().any(|a| a.text.starts_with("-i") || a.text == "--in-place"))
        || (prog == "perl" && args.iter().any(|a| a.text.starts_with("-i") || a.text.starts_with("-pi")));
    if in_place {
        let mut ops = operands(args, &["-e", "-f", "--expression", "--file"]);
        let scripted = args.iter().any(|a| ["-e", "-f", "--expression", "--file"].contains(&a.text.as_str()));
        if !scripted && !ops.is_empty() {
            ops.remove(0); // the sed or perl script
        }
        for w in ops {
            write(format!("`{line}`"), &w, cur, out);
        }
        return;
    }
    if prog == "find" {
        let writes = args.iter().any(|a| {
            ["-delete", "-exec", "-execdir", "-ok", "-okdir", "-fprint", "-fprintf", "-fls"].contains(&a.text.as_str())
        });
        let starts: Vec<&Word> =
            args.iter().take_while(|a| !a.text.starts_with('-') && a.text != "(" && a.text != "!").collect();
        if writes {
            if starts.is_empty()
                && let Some(c) = cur.clone()
            {
                out.push(Action::Write { what: format!("`{line}`"), path: c });
            }
            for w in starts {
                write(format!("`{line}`"), w, cur, out);
            }
        }
        return;
    }
    if READ_ONLY.contains(&prog) && !(prog == "sort" && args.iter().any(|a| a.text.starts_with("-o"))) {
        return;
    }
    match cur {
        Some(c) => out.push(Action::Run { what: format!("`{line}`"), cwd: c.clone() }),
        None => out
            .push(Action::Unknown { what: format!("`{line}` (in a directory the guard can't resolve)"), scope: None }),
    }
}

/// A command as the refusal shows it: at most 80 characters.
fn shown(cmd: &Cmd) -> String {
    let text: Vec<&str> = cmd.words.iter().map(|w| w.text.as_str()).collect();
    let text = text.join(" ");
    if text.chars().count() > 80 { format!("{}…", text.chars().take(79).collect::<String>()) } else { text }
}

/// The non-option arguments, skipping the values of the given options and stopping option parsing at "--".
fn operands(args: &[Word], with_value: &[&str]) -> Vec<Word> {
    let (mut out, mut i, mut opts) = (Vec::new(), 0, true);
    while i < args.len() {
        let t = &args[i].text;
        if opts && t == "--" {
            opts = false;
        } else if opts && t.starts_with('-') && t.len() > 1 {
            if with_value.contains(&t.as_str()) {
                i += 1;
            }
        } else {
            out.push(args[i].clone());
        }
        i += 1;
    }
    out
}

fn git_actions(cmd: &Cmd, cur: &mut Option<PathBuf>, out: &mut Vec<Action>) {
    let line = shown(cmd);
    let mut dir = cur.clone();
    let mut dynamic_dir = false;
    // the repository the command works on, for the core.hooksPath check: -C, then --git-dir or GIT_DIR
    let mut git_dir: Option<PathBuf> = None;
    let mut hooks: Option<String> = None;
    for (k, v) in &cmd.env {
        match k.as_str() {
            "GIT_DIR" | "GIT_WORK_TREE" | "GIT_INDEX_FILE" | "GIT_COMMON_DIR" => {
                let w = Word { text: v.clone(), dynamic: v.contains('$'), glob: false };
                if k == "GIT_DIR" {
                    git_dir = path_of(&w, cur);
                }
                write(format!("`{line}` with {k}={v}"), &w, cur, out);
            }
            "GIT_CONFIG_PARAMETERS" if v.to_lowercase().contains("core.hookspath") => {
                hooks = Some(format!("`{line}` overriding core.hooksPath"));
            }
            k if k.starts_with("GIT_CONFIG_KEY_") && v.eq_ignore_ascii_case("core.hooksPath") => {
                hooks = Some(format!("`{line}` overriding core.hooksPath"));
            }
            _ => {}
        }
    }
    let args = &cmd.words[1..];
    let mut i = 0;
    while i < args.len() {
        let t = args[i].text.as_str();
        match t {
            "-C" => {
                if let Some(w) = args.get(i + 1) {
                    match (&dir, w.dynamic) {
                        (_, true) => dynamic_dir = true,
                        (Some(d), false) => dir = Some(resolve(&w.text, d)),
                        (None, false) => dir = path_of(w, cur),
                    }
                }
                i += 2;
            }
            "-c" => {
                if args.get(i + 1).is_some_and(|w| w.text.to_lowercase().starts_with("core.hookspath")) {
                    hooks = Some(format!("`{line}` overriding core.hooksPath"));
                }
                i += 2;
            }
            _ if t.starts_with("--git-dir") || t.starts_with("--work-tree") => {
                let (name, value, step) = match t.split_once('=') {
                    Some((n, v)) => (n, Some(Word { text: v.into(), dynamic: args[i].dynamic, glob: args[i].glob }), 1),
                    None => (t, args.get(i + 1).cloned(), 2),
                };
                if let Some(v) = value {
                    if name == "--git-dir" {
                        git_dir = path_of(&v, &dir);
                    }
                    write(format!("`{line}` with {name}"), &v, cur, out);
                }
                i += step;
            }
            "--namespace" | "--exec-path" | "--super-prefix" | "--config-env" => i += 2,
            _ if t.starts_with('-') => i += 1,
            _ => break,
        }
    }
    let sub = args.get(i).map(|w| w.text.as_str()).unwrap_or("");
    let rest = args.get(i + 1..).unwrap_or(&[]);
    if sub == "config" && rest.iter().any(|w| w.text.eq_ignore_ascii_case("core.hooksPath")) {
        let reading = rest.iter().any(|w| ["--get", "--get-all", "--list", "-l"].contains(&w.text.as_str()));
        if !reading {
            hooks = Some(format!("`{line}` setting core.hooksPath"));
        }
    }
    if let Some(what) = hooks {
        out.push(Action::Hooks { what, scope: git_dir.or_else(|| dir.clone()) });
    }
    if !git_writes(sub, rest) {
        return;
    }
    if dynamic_dir {
        out.push(Action::Unknown { what: format!("`{line}` (a -C path the guard can't resolve)"), scope: cur.clone() });
        return;
    }
    match dir {
        Some(d) => out.push(Action::Git { what: format!("`{line}`"), dir: d }),
        None => out
            .push(Action::Unknown { what: format!("`{line}` (in a directory the guard can't resolve)"), scope: None }),
    }
}

/// Whether a git subcommand with these arguments writes (to the worktree, its index or its refs).
fn git_writes(sub: &str, rest: &[Word]) -> bool {
    let has = |opts: &[&str]| rest.iter().any(|w| opts.contains(&w.text.as_str()));
    let positional = rest.iter().any(|w| !w.text.starts_with('-'));
    match sub {
        "" => false,
        s if GIT_READ.contains(&s) => false,
        "branch" => {
            has(&["-d", "-D", "-m", "-M", "-c", "-C", "--delete", "--move", "--copy", "-f", "--force", "-u"])
                || has(&["--set-upstream-to", "--unset-upstream", "--edit-description"])
                || (positional && !has(&["--list", "-l", "--contains", "--merged", "--no-merged", "--points-at"]))
        }
        "tag" => !(rest.is_empty() || has(&["-l", "--list", "-v", "--verify"])),
        "remote" => !(rest.is_empty() || matches!(rest[0].text.as_str(), "-v" | "--verbose" | "show" | "get-url")),
        "config" => {
            !(has(&["--get", "--get-all", "--get-regexp", "--list", "-l"])
                || rest.iter().filter(|w| !w.text.starts_with('-')).count() <= 1)
        }
        "stash" => !matches!(rest.first().map(|w| w.text.as_str()), Some("list" | "show")),
        "worktree" => !matches!(rest.first().map(|w| w.text.as_str()), Some("list")),
        "reflog" => !matches!(rest.first().map(|w| w.text.as_str()), None | Some("show" | "list")),
        _ => true,
    }
}

// ---- the live lookup: ledgerd, presence, the last-known file, git lock reasons

/// The live lookup for one hook call, for `session` (the Claude Code session id; empty when there is none).
pub struct Live {
    pub session: String,
}

impl Lookup for Live {
    fn me(&self) -> Vec<String> {
        me(&self.session)
    }

    fn locks(&self, roots: &[PathBuf]) -> Locks {
        live_locks(roots, &self.session)
    }
}

/// This session's agent (the most recently seen first) and its subagents' agents, from presence.
pub fn me(session: &str) -> Vec<String> {
    if session.is_empty() {
        return Vec::new();
    }
    let Ok(agents) = crate::presence::agents(None) else { return Vec::new() };
    let s = session.to_lowercase();
    let field = |a: &Map, k: &str| a.get(k).and_then(Value::as_str).map(str::to_lowercase);
    let own = agents.iter().filter(|a| field(a, "session").as_deref() == Some(&s) && !a.contains_key("parent"));
    let subs = agents.iter().filter(|a| field(a, "parent").as_deref() == Some(&s));
    own.chain(subs).filter_map(|a| a.get("addr").and_then(Value::as_str).map(str::to_owned)).collect()
}

/// The held lock on one worktree root and the note to show with it (for ref-guard).
pub fn lock_of(root: &Path, session: &str) -> (Option<Lock>, Option<String>) {
    let locks = live_locks(&[root.to_path_buf()], session);
    (locks.held.into_iter().find(|l| l.root == root), locks.note)
}

const LEDGER_TIMEOUT: f64 = 2.0;

fn live_locks(roots: &[PathBuf], session: &str) -> Locks {
    let commons: BTreeSet<PathBuf> = roots.iter().filter_map(|r| common_dir(r)).collect();
    let reasons: Vec<(PathBuf, String, String)> = commons.iter().flat_map(|c| git_locks(c)).collect();
    // logged in as the session's agent when it has credentials (needed under auth enforce), anonymous otherwise
    let asker = me(session).into_iter().next();
    let creds = asker.as_deref().and_then(|a| crate::provision::credentials_for(a).ok().flatten());
    let asker = asker.unwrap_or_else(|| "guard".into());
    let asked_at = now();
    let asked = match &creds {
        Some(c) => crate::ledger::call_with("worktrees", &asker, Map::new(), Some(LEDGER_TIMEOUT), Some(c)),
        None => crate::ledger::call("worktrees", &asker, Map::new(), Some(LEDGER_TIMEOUT)),
    };
    match asked {
        Ok(reply) => {
            let held = ledger_locks(&reply);
            remember(&held, asked_at);
            let stale = reasons
                .iter()
                .filter(|(root, id, _)| !held.iter().any(|l| &l.root == root && &l.lease_id == id))
                .map(|(root, id, holder)| {
                    format!(
                        "{} is git-locked by {holder} (lease {id}), but ledgerd shows no such lease: stale, allowed",
                        root.display()
                    )
                })
                .collect();
            Locks { held, note: None, stale }
        }
        Err(_) => {
            // the union of what we last saw and the git lock reasons: it can only add locks (W-2)
            let (as_of, mut held) = recalled();
            for (root, id, holder) in reasons {
                if !held.iter().any(|l| l.root == root) {
                    let resource = worktree_resource(&root);
                    held.push(Lock {
                        root,
                        resource,
                        holder,
                        lease_id: id,
                        expires_at: None,
                        source: Source::GitReason,
                    });
                }
            }
            let since = as_of.map_or("no earlier lookup".to_owned(), |t| crate::stamp::local(t as i64, ' '));
            Locks { held, note: Some(format!("ledgerd unreachable; using locks as of {since}")), stale: vec![] }
        }
    }
}

/// The ledgerd `worktrees` reply as locks, keeping only those whose id matches the sha256 of their path.
fn ledger_locks(reply: &Map) -> Vec<Lock> {
    let items = reply.get("worktrees").and_then(Value::as_array).cloned().unwrap_or_default();
    items
        .iter()
        .filter_map(|w| {
            let path = PathBuf::from(w.get("path")?.as_str()?);
            let resource = w.get("resource")?.as_str()?.to_owned();
            (worktree_resource(&path) == resource).then_some(())?;
            Some(Lock {
                root: path,
                resource,
                holder: w.get("holder")?.as_str()?.to_owned(),
                lease_id: w.get("lease_id").and_then(Value::as_str).unwrap_or("?").to_owned(),
                expires_at: w.get("expires_at").and_then(Value::as_f64),
                source: Source::Ledger,
            })
        })
        .collect()
}

fn locks_file() -> Option<PathBuf> {
    crate::config::data_path(&["guard-locks.json"]).ok()
}

/// guard-locks.json as (as_of, entries): each entry a ledgerd `worktrees` item, plus "added" when an acquire
/// recorded it.
fn read_locks(path: &Path) -> (Option<f64>, Vec<Map>) {
    let Some(Value::Object(m)) = std::fs::read(path).ok().and_then(|b| crate::json::loads(&b)) else {
        return (None, vec![]);
    };
    let items = m.get("worktrees").and_then(Value::as_array).into_iter().flatten();
    (m.get("as_of").and_then(Value::as_f64), items.filter_map(|v| v.as_object().cloned()).collect())
}

fn entry(l: &Lock, added: Option<f64>) -> Map {
    let mut e = json!({"path": l.root.to_string_lossy(), "resource": l.resource, "holder": l.holder,
                       "lease_id": l.lease_id, "expires_at": l.expires_at});
    if let Some(t) = added {
        e["added"] = json!(t);
    }
    e.as_object().cloned().unwrap_or_default()
}

/// Change guard-locks.json under an exclusive flock on guard-locks.json.lock, so the acquires, releases and guard
/// lookups that update it never lose each other's entries; written atomically, 0600.
fn update_locks(
    path: &Path,
    change: impl FnOnce(Option<f64>, Vec<Map>) -> (Option<f64>, Vec<Map>),
) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let open = |p: &Path| std::fs::OpenOptions::new().write(true).create(true).truncate(false).mode(0o600).open(p);
    let lock = open(&path.with_extension("json.lock"))?;
    let _held = nix::fcntl::Flock::lock(lock, nix::fcntl::FlockArg::LockExclusive).map_err(|(_, e)| e)?;
    let (as_of, items) = read_locks(path);
    let (as_of, items) = change(as_of, items);
    let text = dumps(&json!({"as_of": as_of, "worktrees": items}));
    let tmp = path.with_extension("tmp");
    std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)
        .and_then(|mut f| f.write_all(text.as_bytes()))?;
    std::fs::rename(&tmp, path)
}

fn no_data_dir() -> std::io::Error {
    std::io::Error::other("no data dir for guard-locks.json")
}

/// Keep the last locks ledgerd reported, for when it is unreachable. ledgerd is authoritative for everything older
/// than the query (asked_at): an entry an acquire recorded after the query started is kept, since the reply may
/// predate its grant (specs/worktree/WorktreeLock.tla).
fn remember(locks: &[Lock], asked_at: f64) {
    if let Some(path) = locks_file() {
        // best effort: this lookup's decision stands either way, and the next lookup writes the file again
        #[allow(clippy::let_underscore_must_use, reason = "a lookup that can't save its reply still decides")]
        let _ = remember_in(&path, locks, asked_at);
    }
}

fn remember_in(path: &Path, locks: &[Lock], asked_at: f64) -> std::io::Result<()> {
    update_locks(path, |_, items| {
        let mut out: Vec<Map> = locks.iter().map(|l| entry(l, None)).collect();
        for e in items {
            let newer = e.get("added").and_then(Value::as_f64).is_some_and(|t| t >= asked_at);
            let path = e.get("path").and_then(Value::as_str);
            if newer && !locks.iter().any(|l| Some(l.root.to_string_lossy().as_ref()) == path) {
                out.push(e);
            }
        }
        (Some(now()), out)
    })
}

/// Record a lease an acquire was just granted, so the guard enforces it even if ledgerd goes down before any lookup:
/// add-only (an entry for the same worktree is replaced, the others kept). An error means the lease is enforced only
/// while ledgerd answers (and by the git lock, on a linked worktree): the caller should say so.
pub fn record(lock: &Lock) -> std::io::Result<()> {
    record_in(&locks_file().ok_or_else(no_data_dir)?, lock)
}

fn record_in(path: &Path, lock: &Lock) -> std::io::Result<()> {
    update_locks(path, |as_of, mut items| {
        let path = lock.root.to_string_lossy();
        items.retain(|e| e.get("path").and_then(Value::as_str) != Some(path.as_ref()));
        items.push(entry(lock, Some(now())));
        (as_of.or(Some(now())), items)
    })
}

/// Drop a released lease's entry (after ledgerd released it, so it stays enforced offline until then). An error leaves
/// the entry: the worktree stays refused to others while ledgerd is down, until a lookup succeeds.
pub fn forget(lease_id: &str) -> std::io::Result<()> {
    forget_in(&locks_file().ok_or_else(no_data_dir)?, lease_id)
}

fn forget_in(path: &Path, lease_id: &str) -> std::io::Result<()> {
    update_locks(path, |as_of, mut items| {
        items.retain(|e| e.get("lease_id").and_then(Value::as_str) != Some(lease_id));
        (as_of, items)
    })
}

/// The last locks ledgerd reported, and when.
fn recalled() -> (Option<f64>, Vec<Lock>) {
    locks_file().map_or((None, vec![]), |p| recalled_in(&p))
}

fn recalled_in(path: &Path) -> (Option<f64>, Vec<Lock>) {
    let Some(text) = std::fs::read_to_string(path).ok() else { return (None, vec![]) };
    let Some(Value::Object(m)) = crate::json::loads(text.as_bytes()) else { return (None, vec![]) };
    let mut locks = ledger_locks(&m);
    for l in &mut locks {
        l.source = Source::LastKnown;
    }
    (m.get("as_of").and_then(Value::as_f64), locks)
}

/// `hook guard`: one PreToolUse event on stdin, decided with the live lookup.
pub fn run(stdin: &str) -> Outcome {
    let deny = |text: String| Outcome { code: 2, stderr: text, stdout: String::new() };
    let Some(Value::Object(event)) = crate::json::loads(stdin.as_bytes()) else {
        return deny("blocked: the guard could not read the hook's input".into());
    };
    let text = |k: &str| event.get(k).and_then(Value::as_str).unwrap_or("").to_owned();
    let cwd = PathBuf::from(text("cwd"));
    let call = ToolCall {
        tool: text("tool_name"),
        input: event.get("tool_input").and_then(Value::as_object).cloned().unwrap_or_default(),
        cwd: if cwd.is_absolute() { cwd } else { std::env::current_dir().unwrap_or_default() },
    };
    match decide(&call, &Live { session: text("session_id") }) {
        Decision::Allow => Outcome { code: 0, stderr: String::new(), stdout: String::new() },
        Decision::Deny(reason) => deny(reason),
        Decision::Warn(text) => {
            Outcome { code: 0, stderr: String::new(), stdout: dumps(&json!({"systemMessage": text})) }
        }
    }
}

#[cfg(test)]
mod tests;
