"""The worktree guard end to end (ACCESS.md 5.6): `agent-bridge worktree acquire` in one agent session, then
`agent-bridge hook guard` (Claude Code's PreToolUse hook) deciding other sessions' tool calls, against ledgerd on a
disposable broker. A scratch git repository with a linked worktree; nothing real is touched."""

import json
import os
import re
import subprocess
import uuid

import pytest
from conftest import BIN

BRIDGE = str(BIN / "agent-bridge")
GIT_ENV = {"GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1"}


def run(*args, env=None, ok=True, stdin=None):
    p = subprocess.run(
        [BRIDGE, *args],
        env={**os.environ, **(env or {})},
        input=stdin,
        capture_output=True,
        text=True,
        errors="replace",
    )
    assert (p.returncode == 0) == ok, (args, p.returncode, p.stdout, p.stderr)
    return p


def git(*args, cwd):
    subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@t", *args],
        cwd=cwd,
        env={**os.environ, **GIT_ENV},
        check=True,
        capture_output=True,
    )


@pytest.fixture(scope="module")
def s(ledgerd, historyd, tmp_path_factory):
    base = tmp_path_factory.mktemp("guard").resolve()
    repo, lane = base / "repo", base / "lane"
    repo.mkdir()
    git("init", "-q", "-b", "main", cwd=repo)
    (repo / "README").write_text("hi\n")
    git("add", ".", cwd=repo)
    git("commit", "-qm", "init", cwd=repo)
    git("worktree", "add", "-q", "-b", "lane", str(lane), cwd=repo)
    (lane / "src").mkdir()
    (lane / "src" / "a.txt").write_text("hello\n")
    sa, sb = str(uuid.uuid4()), str(uuid.uuid4())
    a = run("register", "holder", env={"CLAUDE_CODE_SESSION_ID": sa}).stdout.strip()
    b = run("register", "other", env={"CLAUDE_CODE_SESSION_ID": sb}).stdout.strip()
    got = json.loads(run("worktree", "acquire", a, str(lane), env={"CLAUDE_CODE_SESSION_ID": sa}).stdout)
    yield type("S", (), {"repo": repo, "lane": lane, "a": a, "b": b, "sa": sa, "sb": sb, "acquired": got})
    subprocess.run(
        [BRIDGE, "worktree", "release", a, str(lane)],
        env={**os.environ, "CLAUDE_CODE_SESSION_ID": sa},
        capture_output=True,
    )
    for x in (a, b):
        run("leave", x)


def hook(session, tool, tool_input, cwd, env=None):
    event = {"session_id": session, "hook_event_name": "PreToolUse", "tool_name": tool, "tool_input": tool_input}
    event["cwd"] = str(cwd)
    p = subprocess.run(
        [BRIDGE, "hook", "guard"],
        input=json.dumps(event),
        env={**os.environ, **(env or {})},
        capture_output=True,
        text=True,
    )
    assert p.returncode in (0, 2), (p.returncode, p.stderr)
    return p


def test_the_holder_works_and_others_are_refused(s):
    lane, edit = s.lane, {"file_path": str(s.lane / "src" / "a.txt")}
    assert hook(s.sa, "Edit", edit, lane).returncode == 0, "the holder's own session"
    refused = hook(s.sb, "Edit", edit, s.repo)
    assert refused.returncode == 2, refused
    lease = s.acquired.get("lease", {}).get("lease_id", "")
    want = (
        rf"blocked: Edit {re.escape(edit['file_path'])} is inside {re.escape(str(lane))}, leased by {s.a} "
        rf"until \d\d:\d\d:\d\d \(lease {lease}\)\. "
        rf"To work there: agent-bridge worktree acquire {s.b} {re.escape(str(lane))}"
    )
    assert re.fullmatch(want, refused.stderr.strip()), refused.stderr
    for cmd in ("git commit -am wip", "rm src/a.txt", "make build", f"cd {lane} && echo x >> src/a.txt"):
        assert hook(s.sb, "Bash", {"command": cmd}, lane).returncode == 2, cmd
    for cmd in ("git status", "git log --oneline -1", "cat src/a.txt", "ls -la"):
        assert hook(s.sb, "Bash", {"command": cmd}, lane).returncode == 0, cmd
    write_elsewhere = {"file_path": str(s.repo / "notes.txt")}
    assert hook(s.sb, "Write", write_elsewhere, s.repo).returncode == 0, "the main worktree isn't leased"
    assert hook(s.sb, "Read", {"file_path": edit["file_path"]}, s.repo).returncode == 0
    assert hook(s.sb, "Bash", {"command": "git -c core.hooksPath=/dev/null commit -am x"}, s.repo).returncode == 2


def test_unrelated_work_is_not_wedged(s, tmp_path):
    """A lock in one repository refuses nothing elsewhere: commands the guard can't resolve and core.hooksPath
    overrides are judged only in a repository with a locked worktree, and globs by the paths they name."""
    other = tmp_path / "other"
    other.mkdir()
    git("init", "-q", "-b", "main", cwd=other)
    (other / "a.tmp").write_text("x\n")
    for cwd in (other, tmp_path):
        for cmd in ("rm $X", "rm *.tmp", "git -c core.hooksPath=/dev/null status", 'cd "$OUT" && make'):
            p = hook(s.sb, "Bash", {"command": cmd}, cwd)
            assert p.returncode == 0, (cwd, cmd, p.stderr)
    for cmd in ("rm $X", "git -c core.hooksPath=/dev/null status"):
        assert hook(s.sb, "Bash", {"command": cmd}, s.repo).returncode == 2, f"in the locked lane's repository: {cmd}"
    assert hook(s.sb, "Bash", {"command": f"rm {s.lane}/src/*"}, other).returncode == 2, "a glob naming the lane"


def test_offline_it_enforces_the_last_known_and_the_git_locks(s):
    """With the broker unreachable, the last locks ledgerd reported and the git lock reasons still hold, and the
    refusal says how they were found."""
    edit = {"file_path": str(s.lane / "src" / "a.txt")}
    assert hook(s.sb, "Edit", edit, s.repo).returncode == 2  # leaves guard-locks.json behind
    dead = {"AGENT_BRIDGE_PORT": "1", "AGENT_BRIDGE_HOST": "127.0.0.1"}
    refused = hook(s.sb, "Edit", edit, s.repo, env=dead)
    assert refused.returncode == 2, refused
    assert f"leased by {s.a}" in refused.stderr and "(ledgerd unreachable; using locks as of" in refused.stderr
    assert "worktree acquire <your address>" in refused.stderr, "offline it can't look the session's agent up"


def test_bad_input_fails_closed():
    p = subprocess.run([BRIDGE, "hook", "guard"], input="not json", capture_output=True, text=True)
    assert p.returncode == 2 and p.stderr.strip(), p
