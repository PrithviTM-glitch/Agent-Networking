"""Worktrees for the console: which are leased (ledgerd's worktrees op), their git lock and branch, the live sessions
inside each (two sessions in one worktree are flagged with no lease at all), whether the path guard is installed, and
protecting a repo with agent-bridge's reference-transaction hook (the admin's click; it chains any existing hook and
removes only its own)."""

import glob
import hashlib
import json
import os
import shutil
import subprocess
import sys

from ..validation import ADMIN
from . import admin, sessions
from .processes import ERRORS, Refused

LOCK_PREFIX = "agent-bridge lease "  # the git lock reason: agent-bridge lease <id> held by <addr>
MARKER = "# agent-bridge ref-guard v1"
HOOK = "reference-transaction"
CHAINED = "reference-transaction.agent-bridge-chained"
GIT_TIMEOUT = 5
# Where Claude Code reads hooks from: managed settings (which a user's disableAllHooks can't turn off), then the user's.
SETTINGS = (
    "/etc/claude-code/managed-settings.json",
    "/etc/claude-code/managed-settings.d/*.json",
    "~/.claude/settings.json",
)


def wt_resource(path):
    """The ledger resource of the worktree at path, as the CLI, the guard and ref-guard derive it."""
    return "admin/wt-" + hashlib.sha256(os.path.realpath(path).encode()).hexdigest()[:16]


def root_of(path):
    """(worktree root, its git dir, the repo's common dir) for a path inside a git worktree, else None. Reads .git
    files only; no git process."""
    d = os.path.realpath(path)
    try:
        for _ in range(64):
            g = os.path.join(d, ".git")
            if os.path.isdir(g):
                return d, g, g
            if os.path.isfile(g):
                with open(g) as f:
                    first = f.readline(4096).strip()
                if not first.startswith("gitdir: "):
                    return None
                gitdir = os.path.realpath(os.path.join(d, first[8:]))
                common = gitdir
                if os.path.isfile(cd := os.path.join(gitdir, "commondir")):
                    with open(cd) as f:
                        common = os.path.realpath(os.path.join(gitdir, f.readline(4096).strip()))
                return d, gitdir, common
            if (parent := os.path.dirname(d)) == d:
                return None
            d = parent
    except OSError:
        return None
    return None


def lock_of(gitdir, common):
    """A linked worktree's git lock: None (unlocked, or the main worktree, which git can't lock), "" (locked, no
    reason) or the reason text."""
    if gitdir == common:
        return None
    try:
        with open(os.path.join(gitdir, "locked")) as f:
            return f.read(4096).strip()
    except FileNotFoundError:
        return None
    except OSError:
        return None


def parse_lock(reason):
    """(lease id, holder) from an agent-bridge lock reason, else None (someone else's lock, or none)."""
    if not reason or not reason.startswith(LOCK_PREFIX):
        return None
    parts = reason[len(LOCK_PREFIX) :].split()
    return (parts[0], parts[3]) if len(parts) >= 4 and parts[1:3] == ["held", "by"] else None


def _git(repo, *args):
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}  # GIT_DIR and friends would redirect it
    try:
        p = subprocess.run(
            ["git", "-C", repo, *args], env=env, capture_output=True, text=True, timeout=GIT_TIMEOUT, check=False
        )
    except (OSError, subprocess.TimeoutExpired) as e:
        raise Refused(f"git: {e}") from None
    if p.returncode != 0:
        raise Refused(
            f"not a git repository: {repo}" if "not a git repository" in p.stderr else f"git: {p.stderr.strip()}"
        )
    return p.stdout.splitlines()


def repo_paths(repo):
    """(common dir, effective hooks dir) of the repo at repo, honouring core.hooksPath (husky and the like set it)."""
    if not isinstance(repo, str) or not os.path.isabs(repo) or not os.path.isdir(repo):
        raise Refused("repo must be an absolute path to a directory")
    common, hooks = _git(repo, "rev-parse", "--path-format=absolute", "--git-common-dir", "--git-path", "hooks")
    return os.path.realpath(common), os.path.realpath(hooks)


def _ours(path):
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            return any(MARKER in f.readline(512) for _ in range(3))
    except OSError:
        return False


def protected(hooks):
    return _ours(os.path.join(hooks, HOOK))


def cli_path():
    """The agent-bridge CLI next to the console's own interpreter (the same install), else the one on PATH."""
    here = os.path.join(os.path.dirname(sys.executable), "agent-bridge")
    return here if os.access(here, os.X_OK) else shutil.which("agent-bridge")


def hook_script(cli):
    q = "'" + cli.replace("'", "'\\''") + "'"
    return f"""#!/bin/sh
{MARKER}: installed by the agent-bridge console's "Protect this repo"; its "Unprotect" removes it.
# Runs any earlier reference-transaction hook first, then agent-bridge's ref guard, on the same arguments and input;
# either one refusing aborts the ref update. If agent-bridge is ever missing, every ref update here fails: Unprotect
# in the console, or delete this file and rename {CHAINED} back.
input=$(mktemp) || exit 1
trap 'rm -f "$input"' EXIT
cat >"$input"
chained="$(dirname "$0")/{CHAINED}"
if [ -x "$chained" ]; then
    "$chained" "$@" <"$input" || exit $?
fi
{q} hook ref-guard "$@" <"$input"
"""


def protect(repo):
    common, hooks = repo_paths(repo)
    hook, chained = os.path.join(hooks, HOOK), os.path.join(hooks, CHAINED)
    if _ours(hook):
        return common, "already protected"
    if not (cli := cli_path()):
        raise Refused("agent-bridge is not installed where the console can find it")
    os.makedirs(hooks, exist_ok=True)
    note = ""
    if os.path.lexists(hook):
        if os.path.lexists(chained):
            raise Refused(f"{chained} already exists; resolve it by hand first")
        os.rename(hook, chained)
        note = ", chaining the existing hook"
    tmp = hook + ".agent-bridge-tmp"
    with open(tmp, "w") as f:
        f.write(hook_script(cli))
    os.chmod(tmp, 0o755)
    os.rename(tmp, hook)
    return common, f"protected ({hook}{note})"


def unprotect(repo):
    common, hooks = repo_paths(repo)
    hook, chained = os.path.join(hooks, HOOK), os.path.join(hooks, CHAINED)
    if not os.path.lexists(hook):
        raise Refused("this repo is not protected")
    if not _ours(hook):
        raise Refused(f"{hook} is not agent-bridge's; left as it is")
    os.remove(hook)
    restored = os.path.lexists(chained)
    if restored:
        os.rename(chained, hook)
    return common, "unprotected" + (", the earlier hook restored" if restored else "")


def guard():
    """Whether Claude Code runs agent-bridge's path guard: where it is configured, and whether a hook failure blocks
    (onFailure "block"; without it a timeout fails open). Read-only."""
    found = []
    for pattern in SETTINGS:
        for path in sorted(glob.glob(os.path.expanduser(pattern))):
            try:
                with open(path) as f:
                    cfg = json.load(f)
            except (OSError, ValueError):
                continue
            for entry in (cfg.get("hooks") or {}).get("PreToolUse") or []:
                for h in (entry.get("hooks") or []) if isinstance(entry, dict) else []:
                    cmd = isinstance(h, dict) and h.get("command")
                    if isinstance(cmd, str) and "agent-bridge" in cmd and "hook guard" in cmd:
                        found.append({"file": path, "blocks_on_failure": h.get("onFailure") == "block"})
    return {"installed": bool(found), "where": found}


def snapshot(_query=None):
    """The Worktrees view: leased worktrees with their lock, branch, sessions and drift; unleased worktrees that two or
    more live sessions share; the repos involved and whether each is protected; and the path guard's installation."""
    try:
        reply = admin.ledger_call("worktrees", ADMIN)
        leases, error = reply.get("worktrees", []), None
    except ERRORS as e:
        leases, error = [], str(e)
    live = [v for v in sessions.entities() if v.get("cwd") and (v.get("alive") is True or v.get("on_bus"))]
    inside = {}
    for v in live:
        if r := root_of(v["cwd"]):
            inside.setdefault(r[0], []).append(v)
    rows, seen = [], set()

    def row(path, lease):
        r = root_of(path) if os.path.isdir(path) else None
        root = r[0] if r else os.path.realpath(path)
        reason = lock_of(r[1], r[2]) if r else None
        parsed = parse_lock(reason)
        holder = lease and lease.get("holder")
        drift = []
        if lease:
            if lease.get("resource") != wt_resource(root):
                drift.append("the resource id doesn't match the path")
            if r and r[1] != r[2] and parsed is None:
                drift.append("no git lock")
            elif parsed and (parsed[1] != holder or parsed[0] != lease.get("lease_id")):
                drift.append(f"the git lock names {parsed[1]}")
        elif parsed:
            drift.append(f"stale git lock: {parsed[1]} holds no lease")
        here = inside.get(root, [])
        seen.add(root)
        return {
            "path": root,
            "exists": r is not None,
            "repo": r[2] if r else None,
            "branch": sessions.git_branch(root) if r else None,
            "resource": wt_resource(root),
            "holder": holder,
            "lease_id": lease and lease.get("lease_id"),
            "expires_at": lease and lease.get("expires_at"),
            "lock": reason,
            "drift": drift,
            "sessions": [{"key": v["key"], "addr": v["addr"], "friendly": v["friendly"]} for v in here],
            "others": [v["key"] for v in here if lease and v.get("addr") != holder],
            "shared": len(here) >= 2,
        }

    for lease in leases:
        if isinstance(lease.get("path"), str):
            rows.append(row(lease["path"], lease))
    for root, here in inside.items():
        if root not in seen and len(here) >= 2:
            rows.append(row(root, None))
    repos = {}
    for r in rows:
        if r["repo"] and r["repo"] not in repos:
            try:
                common, hooks = repo_paths(r["path"])
                repos[common] = {"repo": common, "worktree": r["path"], "protected": protected(hooks), "hooks": hooks}
            except Refused as e:
                repos[r["repo"]] = {"repo": r["repo"], "worktree": r["path"], "protected": False, "error": str(e)}
    for r in rows:
        r["protected"] = bool(repos.get(r["repo"], {}).get("protected"))
    home = os.path.expanduser("~")
    return {"worktrees": rows, "repos": list(repos.values()), "guard": guard(), "error": error, "home": home}
