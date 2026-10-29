"""The agent-framework adapter: the generic host, and Claude Code's session detection, inbox socket, session registry,
transcripts and kind detection, each against fakes (a registry dir, an inbox socket, a fake claude process)."""

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid

import pytest
from conftest import wait_for

from agent_bridge import _native, host
from agent_bridge.claude import inbox, registry, transcripts
from agent_bridge.claude.host import CLAUDE, DESKTOP_DIR, SESSION_ENV

SID = str(uuid.uuid4())


@pytest.fixture
def tmp():
    d = tempfile.mkdtemp(prefix="test_host-")
    yield d
    shutil.rmtree(d, ignore_errors=True)


def test_generic_host():
    g = host.GENERIC
    assert (
        g.session() is None
        and g.session_key() is None
        and g.process() is None
        and g.project_dir() is None
        and not g.detected()
    )
    assert not g.can_deliver() and g.sessions() == {} and g.transcript(SID) is None and g.kind("cli") is None
    assert not g.alive(os.getpid(), 1) and g.resume_argv(SID, "x") is None and g.output_lines("raw") == ["raw"]
    with pytest.raises(OSError, match="generic host: no inbox"):
        g.deliver("hi")
    assert host.by_process("claude") is CLAUDE and host.by_process("bash") is None and host.installed() == [CLAUDE]


def test_not_under_a_framework(tmp):
    """A process with no session variable and no claude above it (reparented to init) gets the generic host and an
    empty origin, so it publishes no session or pid."""
    out = os.path.join(tmp, "out.json")
    code = (
        "import json, sys, time; time.sleep(0.3)\n"
        "from agent_bridge import _native, host\n"
        f"json.dump([host.current().name, _native.identity_origin()], open({out!r}, 'w'))\n"
    )
    env = {k: v for k, v in os.environ.items() if k != SESSION_ENV}
    subprocess.run(["sh", "-c", f'"{sys.executable}" -c "$CODE" &'], env={**env, "CODE": code}, check=True)
    assert wait_for(lambda: os.path.exists(out) and os.path.getsize(out), 15)
    with open(out) as f:
        assert json.load(f) == ["generic", {}]


def test_claude_session_and_process(tmp):
    assert CLAUDE.session({SESSION_ENV: SID}) == SID
    assert CLAUDE.session({SESSION_ENV: SID.upper()}) == SID.upper(), "kept as given, like the presence field"
    assert CLAUDE.session({SESSION_ENV: "not-a-uuid"}) is None and CLAUDE.session({}) is None
    assert CLAUDE.session_key({SESSION_ENV: "not-a-uuid"}) == "not-a-uuid", "the MCP server's address key is raw"
    assert CLAUDE.session_key({SESSION_ENV: ""}) is None and CLAUDE.session_key({}) is None
    assert CLAUDE.session_of_env({SESSION_ENV: SID}) == SID
    fake = os.path.join(tmp, "claude")
    shutil.copy("/bin/sh", fake)
    out = os.path.join(tmp, "process.json")
    child = f"from agent_bridge.claude.host import CLAUDE; import json; json.dump(CLAUDE.process(), open({out!r}, 'w'))"
    p = subprocess.Popen([fake, "-c", f'"{sys.executable}" -c "{child}"; sleep 600 & wait'], start_new_session=True)
    try:
        assert wait_for(lambda: os.path.exists(out) and os.path.getsize(out), 15)
        with open(out) as f:
            pid, start = json.load(f)
        assert pid == p.pid and start == host.stat(p.pid)["start"]
        assert CLAUDE.alive(pid, start) and not CLAUDE.alive(pid, start + 1) and not CLAUDE.alive(os.getpid(), start)
    finally:
        os.killpg(p.pid, signal.SIGKILL)
        p.wait()
    assert not CLAUDE.alive(pid, start), "gone"


def test_inbox(tmp, monkeypatch):
    path = os.path.join(tmp, "inbox.sock")
    got = []
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(path)
    server.listen(1)

    def accept():
        conn, _ = server.accept()
        with conn:
            data = b""
            while chunk := conn.recv(65536):
                data += chunk
        got.append(data)

    threading.Thread(target=accept, daemon=True).start()
    monkeypatch.delenv(inbox.SOCKET, raising=False)
    assert not CLAUDE.can_deliver()
    with pytest.raises(OSError, match=inbox.SOCKET):
        CLAUDE.deliver("hi")
    monkeypatch.setenv(inbox.SOCKET, path)
    monkeypatch.setenv(inbox.TOKEN, "t0ken")
    assert CLAUDE.can_deliver()
    CLAUDE.deliver("hello\nsession")
    assert wait_for(lambda: got)
    lines = [json.loads(x) for x in got[0].decode().splitlines()]
    assert lines == [
        {"type": "auth", "token": "t0ken"},
        {"type": "user", "message": {"role": "user", "content": "hello\nsession"}},
    ]
    server.close()
    monkeypatch.setenv(inbox.SOCKET, os.path.join(tmp, "gone.sock"))
    with pytest.raises(OSError):
        CLAUDE.deliver("nobody listens")


def test_registry(tmp, monkeypatch):
    monkeypatch.setattr(registry, "REGISTRY", tmp)
    now = time.time() * 1000
    good = {
        "sessionId": SID.upper(),
        "pid": 4242,
        "procStart": "777",
        "cwd": "/srv/x",
        "entrypoint": "cli",
        "kind": "interactive",
        "status": "busy",
        "version": "2.1.299",
        "name": "Task\x07 name",
        "nameSource": "user",
        "hostSessionId": "local_1",
        "startedAt": now,
        "updatedAt": now,
        "future": {"x": [1]},
    }
    older = {**good, "pid": 4243, "startedAt": now - 1000}
    files = {
        "4242.json": json.dumps(good),
        "4243.json": json.dumps(older),
        "1.json": "{not json",
        "2.json": json.dumps({"sessionId": "nope"}),
        "notes.json": json.dumps(good),
        "3.json": json.dumps({"sessionId": str(uuid.uuid4()), "pid": True, "procStart": [1], "cwd": "rel"}),
    }
    for name, text in files.items():
        with open(os.path.join(tmp, name), "w") as f:
            f.write(text)
    os.mkfifo(os.path.join(tmp, "4242.0123abcd.key"))  # opening it would block: proves it is never opened
    seen = []

    def alive(pid, start):
        seen.append((pid, start))
        return pid == 4243

    result = []
    reader = threading.Thread(target=lambda: result.append(registry.read(alive)), daemon=True)
    reader.start()
    reader.join(5)
    assert result, "registry.read blocked: it opened the .key FIFO"
    regs = result[0]
    assert len(regs) == 2 and SID in regs, regs
    e = regs[SID]
    assert e["pid"] == 4243 and e["alive"], "the live one of two files for a session wins"
    assert e["pid_start"] == 777 and e["cwd"] == "/srv/x" and e["entrypoint"] == "cli" and e["mode"] == "interactive"
    assert e["name"] == "Task name" and e["name_source"] == "user" and e["desktop_id"] == "local_1"
    assert e["started"] == pytest.approx((now - 1000) / 1000) and e["status"] == "busy" and e["version"] == "2.1.299"
    bad = next(v for k, v in regs.items() if k != SID)
    assert bad["pid"] is None and bad["cwd"] is None and not bad["alive"], bad
    assert (4242, 777) in seen and (4243, 777) in seen
    monkeypatch.setattr(registry, "REGISTRY", os.path.join(tmp, "missing"))
    assert registry.read(alive) == {}


def test_transcripts(tmp, monkeypatch):
    monkeypatch.setattr(transcripts, "PROJECTS", tmp)
    d = os.path.join(tmp, "-srv-x")
    os.makedirs(d)
    t = CLAUDE.transcript(SID)
    t.refresh()
    assert t.title is None and t.prompt is None and list(t.recent) == []

    def turn(kind, content, **extra):
        return {
            "type": kind,
            "sessionId": SID,
            "cwd": "/srv/x",
            "gitBranch": "main",
            "entrypoint": "claude-desktop",
            "timestamp": "2026-10-28T10:00:00.000Z",
            "message": {"role": kind, "content": content},
            **extra,
        }

    lines = [
        {"type": "custom-title", "customTitle": "Fix it", "sessionId": SID},
        turn("user", "<system-reminder>skip</system-reminder>Fix the flaky login test please"),
        turn("user", "meta", isMeta=True),
        turn("assistant", [{"type": "text", "text": "On it."}, {"type": "tool_use", "name": "Read", "input": {}}]),
        turn("assistant", [{"type": "text", "text": "sidechain"}], isSidechain=True),
        "not json",
    ]
    with open(os.path.join(d, f"{SID}.jsonl"), "w") as f:
        f.writelines((x if isinstance(x, str) else json.dumps(x, separators=(",", ":"))) + "\n" for x in lines)
    t.__init__(SID)  # forget the earlier miss, which is retried only every FIND_S
    t.refresh()
    assert t.title == "Fix it" and t.prompt == "Fix the flaky login test please" and t.cwd == "/srv/x"
    assert t.branch == "main" and t.entrypoint == "claude-desktop"
    assert [(x["role"], x["text"]) for x in t.recent] == [
        ("user", "Fix the flaky login test please"),
        ("assistant", "On it."),
        ("tool", "Read"),
    ]
    with open(os.path.join(d, f"{SID}.jsonl"), "a") as f:
        f.write(json.dumps({"type": "custom-title", "customTitle": "Renamed", "sessionId": SID}) + "\n")
    t.refresh()
    assert t.title == "Renamed", "read incrementally"


def test_kind_and_resume():
    assert CLAUDE.kind(entrypoint="claude-desktop") == "desktop" and CLAUDE.kind(entrypoint="cli") == "cli"
    assert CLAUDE.kind(argv0=os.path.join(DESKTOP_DIR, "claude-code", "2.1", "claude")) == "desktop"
    assert CLAUDE.kind(argv0="/usr/local/bin/claude") == "cli" and CLAUDE.kind() is None
    assert CLAUDE.resume_argv(SID, "/bin/claude") == ["/bin/claude", "--resume", SID]
    assert CLAUDE.prompt_argv(SID, "c") == ["c", "-p", "--resume", SID, "--output-format", "stream-json", "--verbose"]
    assistant = {
        "type": "assistant",
        "message": {"content": [{"type": "text", "text": "hi"}, {"type": "tool_use", "name": "Bash"}]},
    }
    assert CLAUDE.output_lines(json.dumps(assistant)) == ["hi", "[tool] Bash"]
    assert CLAUDE.output_lines('{"type":"result","result":"done"}') == ["[result] done"]
    assert CLAUDE.output_lines("plain <b>") == ["plain <b>"] and CLAUDE.output_lines('{"type":"other"}') == []
    assert CLAUDE.usage_metrics["cost"] == ("claude_code_cost_usage_USD_total", False)


def test_identity(monkeypatch, tmp):
    monkeypatch.setenv("CLAUDE_PROJECT_DIR", "/srv/My Project!")
    assert _native.identity_project_name() == "My-Project"
    assert _native.identity_session_address(SID) == f"My-Project-{_native.identity_short_hash(SID)}"
    monkeypatch.setenv("CLAUDE_PROJECT_DIR", "/srv/Admin")
    assert _native.identity_project_name() == "agent", "admin is reserved"
    monkeypatch.delenv("CLAUDE_PROJECT_DIR")
    os.makedirs(plain := os.path.join(tmp, "plain"))
    monkeypatch.chdir(plain)
    assert _native.identity_project_name() == "plain", "without a project dir, the working directory names it"
    assert _native.identity_read_proc(os.getpid())["ppid"] == os.getppid()
