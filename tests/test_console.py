"""The admin console end to end: a console on a free port with a fake HOME (token, transcripts, session registry)
and fake claude / gnome-terminal binaries, against a broker with ledgerd. Covers the token, Origin and Host checks,
messaging as admin, the event stream, friendly names, death detection, signals, restarts, ledger actions, the audit
log and the broadcast hook. The tests are one scenario in file order."""

import fcntl
import http.client
import json
import os
import shutil
import signal
import stat
import subprocess
import tempfile
import threading
import time
import uuid

import pytest
from conftest import BIN, free_port

from agent_bridge import config
from agent_bridge.ledger import client as ledger

HOOK = str(BIN / "agent-bridge-broadcast-hook")
CONSOLE = str(BIN / "agent-bridge-console")
S1, S2, S3, S4, S5, S6, S7, S8, S9 = (str(uuid.uuid4()) for _ in range(9))
TMP = HOME = FAKE_BIN = DATA = LOG = PROJ = REG = WEB = ORIGIN = HOST = None
procs = []
state = {"token": None, "mon": None, "c": None}


def wait_for(pred, timeout=10):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if v := pred():
            return v
        time.sleep(0.05)
    return None


def req(method, path, body=None, token=..., origin=..., host=..., ctype="application/json", raw=None):
    origin = ORIGIN if origin is ... else origin
    host = HOST if host is ... else host
    c = http.client.HTTPConnection("127.0.0.1", WEB, timeout=30)
    h = {"Host": host}
    if token is not None:
        h["X-Admin-Token"] = state["token"] if token is ... else token
    if origin:
        h["Origin"] = origin
    data = raw if raw is not None else body if body is None else json.dumps(body).encode()
    if data is not None:
        h["Content-Type"] = ctype
    c.request(method, path, body=data, headers=h)
    r = c.getresponse()
    out = r.read()
    try:
        return r.status, json.loads(out)
    except ValueError:
        return r.status, out


def post(path, body, **kw):
    return req("POST", path, body, **kw)


def ok(path, body):
    code, j = post(path, body)
    assert code == 200 and j["ok"], (path, body, code, j)
    return j


def refused(path, body, want):
    code, j = post(path, body)
    assert code == 400 and want in j["error"], (path, body, code, j)
    return j


class Events:
    """The console's SSE stream, collected on a thread."""

    def __init__(self):
        self.items = []
        self.c = http.client.HTTPConnection("127.0.0.1", WEB, timeout=60)
        self.c.request("GET", "/api/stream", headers={"Host": HOST, "X-Admin-Token": state["token"]})
        self.r = self.c.getresponse()
        assert self.r.status == 200
        threading.Thread(target=self.run, daemon=True).start()

    def run(self):
        try:
            for line in self.r:
                if line.startswith(b"data: "):
                    self.items.append(json.loads(line[6:]))
        except (OSError, ValueError):
            pass

    def find(self, pred, timeout=10):
        return wait_for(lambda: next((e for e in self.items if pred(e)), None), timeout)


def presence(addr, **extra):
    rec = {
        "name": addr.rsplit("-", 1)[0],
        "caps": [],
        "topics": extra.pop("topics", []),
        "keep": False,
        "seen": time.time(),
        **extra,
    }
    state["c"].publish(f"agents/{addr}", json.dumps(rec), qos=1, retain=True).wait_for_publish(5)


def agent(addr):
    return next((a for a in req("GET", "/api/state")[1]["agents"] if a["addr"] == addr), None)


def start_fake(path):
    p = subprocess.Popen([path, "600"])
    procs.append(p)
    with open(f"/proc/{p.pid}/stat") as f:
        s = f.read()
    return p, int(s[s.rfind(")") + 2 :].split()[19])


def write_transcript(sid, lines):
    d = os.path.join(HOME, ".claude", "projects", "-tmp-proj")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, f"{sid}.jsonl"), "a") as f:
        for line in lines:
            f.write(json.dumps(line, separators=(",", ":")) + "\n")


def turn(sid, kind, content, **extra):
    return {
        "type": kind,
        "sessionId": sid,
        "cwd": PROJ,
        "gitBranch": "feature/login-fix",
        "entrypoint": "cli",
        "timestamp": "2026-10-27T10:00:00.000Z",
        "message": {"role": kind, "content": content},
        **extra,
    }


ASSISTANT = json.dumps(
    {"type": "assistant", "message": {"content": [{"type": "text", "text": "hello from resume"},
                                                  {"type": "tool_use", "name": "Bash"}]}},
    separators=(",", ":"),
)  # fmt: skip


def fixtures():
    os.makedirs(FAKE_BIN)
    os.makedirs(PROJ)
    shutil.copy("/bin/sleep", os.path.join(TMP, "claude"))
    desk = os.path.join(HOME, ".config", "Claude", "claude-code", "2.1.0", "abc")
    os.makedirs(desk)
    shutil.copy("/bin/sleep", os.path.join(desk, "claude"))
    scripts = {
        "claude": f"""#!/bin/sh
printf '%s\\n' "$PWD" "$@" > {TMP}/claude-args
cat > {TMP}/claude-stdin
echo '{{"type":"system","subtype":"init"}}'
echo '{ASSISTANT}'
echo 'not json <b>'
echo '{{"type":"result","result":"done"}}'
""",
        "gnome-terminal": f"#!/bin/sh\nprintf '%s\\n' \"$@\" > {TMP}/terminal-args\n",
    }
    for name, text in scripts.items():
        with open(os.path.join(FAKE_BIN, name), "w") as f:
            f.write(text)
        os.chmod(os.path.join(FAKE_BIN, name), 0o755)
    write_transcript(
        S1,
        [
            {"type": "custom-title", "customTitle": "Fix login", "sessionId": S1},
            turn(
                S1,
                "user",
                "<system-reminder>ignore me</system-reminder>Please fix the login redirect loop on staging today",
            ),
            turn(S1, "user", "meta", isMeta=True),
            turn(
                S1,
                "assistant",
                [{"type": "text", "text": "Looking at it."}, {"type": "tool_use", "name": "Read", "input": {}}],
            ),
            turn(S1, "assistant", [{"type": "text", "text": "subagent"}], isSidechain=True),
        ],
    )
    write_transcript(S2, [turn(S2, "user", [{"type": "text", "text": "Add a dark theme to the settings page please"}])])
    desk_turn = turn(S3, "user", "desktop task")
    desk_turn.update(entrypoint="claude-desktop", gitBranch="HEAD")
    write_transcript(S3, [desk_turn])


@pytest.fixture(scope="module", autouse=True)
def console(ledgerd, state_dir):
    global TMP, HOME, FAKE_BIN, DATA, LOG, PROJ, REG, WEB, ORIGIN, HOST
    TMP = tempfile.mkdtemp()
    HOME, FAKE_BIN = (os.path.join(TMP, d) for d in ("home", "bin"))
    DATA, LOG = state_dir, config.LOG
    PROJ, REG = os.path.join(TMP, "proj"), os.path.join(HOME, ".claude", "sessions")
    WEB = free_port()
    ORIGIN, HOST = f"http://127.0.0.1:{WEB}", f"127.0.0.1:{WEB}"
    fixtures()
    state["c"] = ledger.connect()
    env = {k: v for k, v in os.environ.items() if k not in ("XDG_CONFIG_HOME", "CLAUDE_CONFIG_DIR")}
    env.update(HOME=HOME, PATH=f"{FAKE_BIN}:{os.environ['PATH']}", MONITOR_PORT=str(WEB))
    with open(os.path.join(TMP, "monitor.err"), "w") as err:
        state["mon"] = mon = subprocess.Popen([CONSOLE], env=env, stdout=subprocess.PIPE, stderr=err, text=True)
    mon.stdout.readline()
    login = mon.stdout.readline().strip()
    assert login.startswith(f"login: http://127.0.0.1:{WEB}/#token="), login
    state["token"] = login.split("#token=")[1]
    assert wait_for(lambda: (a := agent("admin")) and a), "admin never joined the bus"
    yield
    for p in procs:
        p.kill()
    state["mon"].kill()
    state["mon"].wait()
    state["c"].disconnect()
    state["c"].loop_stop()
    shutil.rmtree(TMP, ignore_errors=True)


def test_security():
    path = os.path.join(HOME, ".config", "agent-bridge", "admin-token")
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600 and open(path).read().strip() == state["token"]
    assert stat.S_IMODE(os.stat(os.path.dirname(path)).st_mode) == 0o700
    code, page = req("GET", "/", token=None, origin=None)
    assert code == 200 and b"app.js" in page and state["token"].encode() not in page
    for path in ("/api/state", "/api/ledger", "/api/stream", "/api/agent?addr=admin"):
        assert req("GET", path, token=None)[0] == 401, path
        assert req("GET", path, token="x" * 43)[0] == 401, path
    for host in (f"evil.example:{WEB}", "127.0.0.1", f"localhost.evil:{WEB}", ""):
        assert req("GET", "/", host=host)[0] == 403, host
        assert post("/api/broadcast", {"text": "x"}, host=host)[0] == 403, host
    for origin in (None, "http://evil.example", f"http://127.0.0.1:{WEB}.evil", "null", f"https://127.0.0.1:{WEB}"):
        code, j = post("/api/broadcast", {"text": "nope"}, origin=origin)
        assert code == 403 and "cross-origin" in j["error"], (origin, code, j)
    assert post("/api/broadcast", {"text": "nope"}, token=None)[0] == 401
    assert post("/api/broadcast", {"text": "nope"}, token="wrong")[0] == 401
    assert req("OPTIONS", "/api/broadcast")[0] == 405
    assert post("/api/broadcast", None, raw=b"text=x", ctype="application/x-www-form-urlencoded")[0] == 400
    assert post("/api/broadcast", None, raw=b"[1]")[0] == 400
    assert post("/api/broadcast", None, raw=b"{bad")[0] == 400
    assert post("/api/nope", {"x": 1})[0] == 404
    code, _ = req("GET", "/", token=None, origin=None)
    c = http.client.HTTPConnection("127.0.0.1", WEB)
    c.request("GET", "/", headers={"Host": HOST})
    r = c.getresponse()
    assert "script-src 'self'" in r.getheader("Content-Security-Policy") and r.getheader("X-Frame-Options") == "DENY"
    log = [json.loads(x) for x in open(os.path.join(DATA, "admin-audit.jsonl"))]
    assert any(e["result"] == "refused: bad Origin" for e in log) and any(
        e["result"] == "refused: bad admin token" for e in log
    )
    assert not any("nope" in json.dumps(e) and e["result"] == "sent" for e in log)
    print("security ok")


def test_messaging():
    seen = []
    w = ledger.connect()
    w.on_message = lambda _c, _u, m: seen.append((m.topic, json.loads(m.payload)))
    for t in ("room/#", "inbox/#", "broadcast", "t/#"):
        w.subscribe(t, qos=1)
    presence("bob-1")
    assert wait_for(lambda: agent("bob-1"))
    ev = Events()
    ok("/api/send", {"kind": "room", "target": "ops", "text": "hello room"})
    ok("/api/send", {"kind": "dm", "target": "bob-1", "text": "hello bob\n<script>x</script>"})
    ok("/api/send", {"kind": "topic", "target": "t/x", "text": "hello topic"})
    ok("/api/broadcast", {"text": "everyone stop pushing to main"})
    got = wait_for(lambda: len(seen) >= 4 and seen)
    assert {(t, m["from"], m["body"]) for t, m in got} == {
        ("room/ops", "admin", "hello room"),
        ("inbox/bob-1", "admin", "hello bob\n<script>x</script>"),
        ("t/x", "admin", "hello topic"),
        ("broadcast", "admin", "everyone stop pushing to main"),
    }, got
    refused("/api/send", {"kind": "dm", "target": "nobody-1", "text": "x"}, "unknown address")
    refused("/api/send", {"kind": "room", "target": "bad room", "text": "x"}, "bad room")
    refused("/api/send", {"kind": "topic", "target": "agents/x", "text": "x"}, "reserved")
    refused("/api/send", {"kind": "topic", "target": "broadcast", "text": "x"}, "reserved")
    refused("/api/send", {"kind": "room", "target": "ops", "text": "bell\x07"}, "control character")
    refused("/api/send", {"kind": "room", "target": "ops", "text": 5}, "must be a string")
    refused("/api/send", {"kind": "shell", "target": "ops", "text": "x"}, "kind must be")
    refused("/api/send", {"kind": "room", "target": "ops", "text": "x" * 70000}, "limit")
    refused("/api/signal", {"addr": ["x"], "signal": "kill"}, "bad address")
    other = ledger.connect()
    other.publish(
        "room/ops", json.dumps({"from": "bob-1", "body": "from bob", "ts": time.time()}), qos=1
    ).wait_for_publish(5)
    assert ev.find(lambda e: e["kind"] == "msg" and e["msg"]["body"] == "from bob")
    assert ev.find(lambda e: e["kind"] == "msg" and e["msg"]["topic"] == "broadcast")
    st = req("GET", "/api/state")[1]
    bodies = {(m["topic"], m["body"]) for m in st["messages"]}
    assert ("broadcast", "everyone stop pushing to main") in bodies and ("room/ops", "hello room") in bodies
    for c in (w, other):
        c.disconnect()
    print("messaging ok")


def hook(session=S1, event="UserPromptSubmit"):
    r = subprocess.run(
        [HOOK],
        capture_output=True,
        text=True,
        input=json.dumps({"session_id": session, "hook_event_name": event, "prompt": "hi"}),
        timeout=10,
    )
    assert r.returncode == 0 and not r.stderr, r
    if not r.stdout:
        return None
    out = json.loads(r.stdout)["hookSpecificOutput"]
    assert out["hookEventName"] == event and "agent-bridge admin console" in out["additionalContext"]
    return out["additionalContext"]


def test_hooks():
    with open(LOG, "a") as f:
        f.write(json.dumps({"topic": "broadcast", "from": "bob-1", "body": "forged by bob", "ts": time.time()}) + "\n")
        f.write("{not json\n")
    first = hook()
    assert "everyone stop pushing to main" in first and "forged" not in first, first
    assert hook() is None
    ok("/api/broadcast", {"text": "second notice"})
    second = hook()
    assert "second notice" in second and "everyone stop" not in second, second
    assert hook() is None
    other = hook(session=S2, event="SessionStart")
    assert "everyone stop pushing to main" in other and "second notice" in other
    assert hook(session="not-a-uuid") is None
    assert hook(session=S3, event="Stop") is None
    t0 = time.monotonic()
    hook(session=str(uuid.uuid4()))
    assert time.monotonic() - t0 < 2
    bad = subprocess.run([HOOK], input="garbage", capture_output=True, text=True)
    assert bad.returncode == 0 and not bad.stdout and not bad.stderr
    print("hook ok")


def test_agents():
    cli, cli_start = start_fake(os.path.join(TMP, "claude"))
    desk, desk_start = start_fake(os.path.join(HOME, ".config", "Claude", "claude-code", "2.1.0", "abc", "claude"))
    other, other_start = start_fake(os.path.join(TMP, "claude"))
    plain = subprocess.Popen(["sleep", "600"])
    procs.append(plain)
    with open(f"/proc/{plain.pid}/stat") as f:
        s = f.read()
    plain_start = int(s[s.rfind(")") + 2 :].split()[19])
    ev = Events()
    presence("cli-1", session=S1, pid=cli.pid, pid_start=cli_start, topics=["room/ops", "t/#"])
    presence("desk-1", session=S3, pid=desk.pid, pid_start=desk_start)
    presence("other-1", session=S2, pid=other.pid, pid_start=other_start)
    presence("reuse-1", session=S2, pid=other.pid, pid_start=other_start + 1)
    presence("plain-1", pid=plain.pid, pid_start=plain_start)
    a = wait_for(lambda: (x := agent("cli-1")) and x["friendly"] != "cli-1" and x)
    assert a["friendly"] == "login-fix · Fix login" and a["alive"] is True and a["kind"] == "cli", a
    assert a["cwd"] == PROJ and a["session"] == S1 and a["rooms"] == ["ops"] and a["topics"] == ["t/#"], a
    assert a["prompt"] == "Please fix the login redirect loop on staging today", a
    assert wait_for(lambda: agent("other-1")["friendly"] == "login-fix · Add a dark theme to the")
    d = wait_for(lambda: (x := agent("desk-1")) and x["kind"] == "desktop" and x)
    assert d["alive"] is True and d["friendly"] == "desktop task", d
    assert agent("reuse-1")["alive"] is False and agent("plain-1")["alive"] is False
    assert agent("bob-1")["alive"] is None and agent("bob-1")["friendly"] == "bob-1"
    adm = agent("admin")
    assert adm["pid"] is None and adm["alive"] is None, adm
    write_transcript(
        S1,
        [
            {"type": "custom-title", "customTitle": "Renamed task", "sessionId": S1},
            turn(S1, "assistant", [{"type": "tool_use", "name": "Edit", "input": {}}]),
        ],
    )
    assert ev.find(
        lambda e: e["kind"] == "agents" and any(x["friendly"] == "login-fix · Renamed task" for x in e["agents"])
    )
    code, p = req("GET", "/api/agent?addr=cli-1")
    assert code == 200, p
    acts = [(x["role"], x["text"]) for x in p["activity"]]
    assert acts == [
        ("user", "Please fix the login redirect loop on staging today"),
        ("assistant", "Looking at it."),
        ("tool", "Read"),
        ("tool", "Edit"),
    ], acts
    assert [m["body"] for m in req("GET", "/api/agent?addr=bob-1")[1]["dms"]] == ["hello bob\n<script>x</script>"]
    assert req("GET", "/api/agent?addr=nobody-1")[0] == 400 and req("GET", "/api/agent?addr=%3Cx%3E")[0] == 400

    refused("/api/signal", {"addr": "reuse-1", "signal": "kill"}, "is dead")
    refused("/api/signal", {"addr": "plain-1", "signal": "kill"}, "is dead")
    refused("/api/signal", {"addr": "bob-1", "signal": "kill"}, "not a claude session")
    refused("/api/signal", {"addr": "cli-1", "signal": "hup"}, "signal must be")
    time.sleep(0.2)
    assert other.poll() is None and plain.poll() is None, "a refused signal reached a process"
    refused("/api/restart/terminal", {"addr": "cli-1"}, "is alive")
    refused("/api/restart/resume", {"addr": "cli-1", "text": "hi"}, "is alive")

    ok("/api/signal", {"addr": "cli-1", "signal": "terminate"})
    assert cli.wait(5) == -signal.SIGTERM
    assert ev.find(
        lambda e: e["kind"] == "agents" and any(x["addr"] == "cli-1" and x["alive"] is False for x in e["agents"]), 10
    )
    assert agent("cli-1") and agent("cli-1")["alive"] is False, "dead agents stay listed"
    refused("/api/signal", {"addr": "cli-1", "signal": "kill"}, "is dead")
    ok("/api/signal", {"addr": "desk-1", "signal": "kill"})
    assert desk.wait(5) == -signal.SIGKILL
    ok("/api/signal", {"addr": "other-1", "signal": "interrupt"})
    assert other.wait(5) == -signal.SIGINT
    assert wait_for(lambda: agent("desk-1")["alive"] is False)
    refused("/api/restart/terminal", {"addr": "desk-1"}, "reopen it in the Claude Desktop app")
    refused("/api/restart/resume", {"addr": "desk-1", "text": "hi"}, "reopen it in the Claude Desktop app")
    refused("/api/restart/terminal", {"addr": "plain-1"}, "unknown")
    refused("/api/restart/terminal", {"addr": "bob-1"}, "not a claude session")

    ok("/api/restart/terminal", {"addr": "cli-1"})
    args = wait_for(lambda: os.path.exists(f"{TMP}/terminal-args") and open(f"{TMP}/terminal-args").read().splitlines())
    assert args == [f"--working-directory={PROJ}", "--", os.path.join(FAKE_BIN, "claude"), "--resume", S1], args
    refused("/api/restart/resume", {"addr": "cli-1", "text": "  "}, "empty")
    j = ok("/api/restart/resume", {"addr": "cli-1", "text": "--dangerously-skip-permissions continue please"})
    jid = j["result"].rsplit(" ", 1)[1]
    assert ev.find(lambda e: e["kind"] == "job" and e["id"] == jid and e.get("state") == "exit 0")
    lines = [e["line"] for e in ev.items if e["kind"] == "job" and e["id"] == jid and "line" in e]
    assert lines == ["[session started]", "hello from resume", "[tool] Bash", "not json <b>", "[result] done"], lines
    argv = open(f"{TMP}/claude-args").read().splitlines()
    assert argv == [PROJ, "-p", "--resume", S1, "--output-format", "stream-json", "--verbose"], argv
    assert open(f"{TMP}/claude-stdin").read() == "--dangerously-skip-permissions continue please"
    assert [j for j in req("GET", "/api/state")[1]["jobs"] if j["id"] == jid][0]["state"] == "exit 0"
    plain.kill()
    print("agents ok")


def test_ledgers():
    c = state["c"]
    for who in ("own-1", "hold-1"):
        ledger.call("init", who, client=c)
        presence(who)
    ledger.call("add", "own-1", client=c, name="db", visibility="public", max_level="write")
    rq = ledger.call("request", "hold-1", client=c, resource="own-1/db", level="write")["request"]
    lease = ledger.call("acquire", "hold-1", client=c, resource="own-1/db", level="write", ttl=60)["lease"]
    d = req("GET", "/api/ledger")[1]
    assert any(x["lease_id"] == lease["lease_id"] and x["token"] == lease["token"] for x in d["leases"]), d
    assert wait_for(lambda: agent("own-1"))
    p = req("GET", "/api/agent?addr=own-1")[1]["ledger"]
    assert [r["id"] for r in p["resources"]] == ["own-1/db"] and p["leases"][0]["lease_id"] == lease["lease_id"], p
    assert p["given"][0]["id"] == rq["id"]
    h = req("GET", "/api/agent?addr=hold-1")[1]["ledger"]
    assert h["held"][0]["id"] == rq["id"] and h["leases"][0]["holder"] == "hold-1", h
    ok("/api/ledger/release", {"lease_id": lease["lease_id"]})
    assert not req("GET", "/api/ledger")[1]["leases"]
    refused("/api/ledger/release", {"lease_id": lease["lease_id"]}, "no such lease")
    refused("/api/ledger/release", {"lease_id": "x; drop"}, "16 hex")
    refused("/api/ledger/revoke", {"request_id": 7}, "16 hex")
    lease = ledger.call("acquire", "hold-1", client=c, resource="own-1/db", level="write", ttl=60)["lease"]
    ok("/api/ledger/revoke", {"request_id": rq["id"]})
    d = req("GET", "/api/ledger")[1]
    assert [r["status"] for r in d["requests"] if r["id"] == rq["id"]] == ["denied"] and not d["leases"], d
    refused("/api/ledger/revoke", {"request_id": "0" * 16}, "no such request")
    print("ledger ok")


def registry_file(file_pid, **fields):
    os.makedirs(REG, exist_ok=True)
    rec = {
        "cwd": PROJ,
        "startedAt": time.time() * 1000,
        "updatedAt": time.time() * 1000,
        "version": "2.1.299",
        "kind": "interactive",
        "entrypoint": "cli",
        "status": "idle",
        "nameSource": "user",
        "future": {"x": [1]},
        **fields,
    }
    with open(os.path.join(REG, f"{file_pid}.json"), "w") as f:
        json.dump(rec, f)


def session(sid):
    return next((a for a in req("GET", "/api/state")[1]["agents"] if a["session"] == sid), None)


def test_registry():
    p2, p3 = os.path.join(TMP, "proj2"), os.path.join(TMP, "proj3", "sub")
    os.makedirs(os.path.join(p2, ".git"))
    with open(os.path.join(p2, ".git", "HEAD"), "w") as f:
        f.write("ref: refs/heads/feature/registry-branch\n")
    os.makedirs(p3)
    os.makedirs(os.path.join(TMP, "gitdirs", "wt"))
    with open(os.path.join(TMP, "proj3", ".git"), "w") as f:
        f.write(f"gitdir: {TMP}/gitdirs/wt\n")
    with open(os.path.join(TMP, "gitdirs", "wt", "HEAD"), "w") as f:
        f.write("ref: refs/heads/wt-branch\n")
    live, live_start = start_fake(os.path.join(TMP, "claude"))
    joined, joined_start = start_fake(os.path.join(TMP, "claude"))
    other, other_start = start_fake(os.path.join(TMP, "claude"))
    gone, _ = start_fake(os.path.join(TMP, "claude"))
    gone.kill()
    gone.wait()
    write_transcript(S5, [turn(S5, "user", "Rotate the staging certificates before Friday please")])
    registry_file(
        live.pid,
        sessionId=S4,
        pid=live.pid,
        procStart=str(live_start),
        cwd=p2,
        name="Registry task",
        status="busy",
        hostSessionId="local_x",
    )
    registry_file(
        gone.pid, sessionId=S5, pid=gone.pid, procStart="12345", cwd=p3, name="proj3-1a", nameSource="derived"
    )
    registry_file(other.pid, sessionId=S6, pid=other.pid, procStart=str(other_start + 1), name="Mismatch")
    registry_file(joined.pid, sessionId=S7.upper(), pid=joined.pid, procStart=joined_start, name="Joined task")
    registry_file(4194301, sessionId=S8, pid=4194301, procStart="5", entrypoint="claude-desktop", name="Desk dead")
    registry_file(
        4194302,
        sessionId=S9,
        pid=4194302,
        procStart="5",
        name="Ancient",
        updatedAt=(time.time() - 4 * 86400) * 1000,
        startedAt=(time.time() - 5 * 86400) * 1000,
    )
    for name, text in (
        ("4194303.json", "{not json"),
        ("4194304.json", "[1, 2]"),
        ("notes.json", json.dumps({"sessionId": S9})),
        ("4194305.json", json.dumps({"sessionId": "nope", "pid": 1})),
        (
            "4194306.json",
            json.dumps(
                {
                    "sessionId": str(uuid.uuid4()),
                    "pid": True,
                    "procStart": [1],
                    "cwd": 7,
                    "name": {"x": 1},
                    "startedAt": "soon",
                }
            ),
        ),
        (f"{live.pid}.0123abcd.key", json.dumps({"sessionId": str(uuid.uuid4()), "name": "KEY FILE"})),
    ):
        with open(os.path.join(REG, name), "w") as f:
            f.write(text)
    presence("joined-1", session=S7)
    ev = Events()
    a = wait_for(lambda: (x := session(S4)) and x["friendly"] == "registry-branch · Registry task" and x)
    assert (
        a and a["key"] == S4 and not a["on_bus"] and a["addr"] is None and a["alive"] is True and a["kind"] == "cli"
    ), a
    assert a["status"] == "busy" and a["desktop_id"] == "local_x" and a["cwd"] == p2 and a["mode"] == "interactive", a
    assert a["branch"] == "feature/registry-branch" and a["relay"] is False, a
    d = wait_for(
        lambda: (
            (x := session(S5)) and x["friendly"] == "wt-branch · Rotate the staging certificates before Friday" and x
        )
    )
    assert d and d["alive"] is False and d["kind"] == "cli", d
    assert session(S6)["alive"] is False and session(S6)["friendly"] == "Mismatch"
    j = [x for x in req("GET", "/api/state")[1]["agents"] if x["session"] == S7]
    assert len(j) == 1 and j[0]["addr"] == "joined-1" and j[0]["alive"] is True and j[0]["pid"] == joined.pid, j
    assert j[0]["friendly"] == "Joined task" and j[0]["in_registry"], j
    assert session(S8)["kind"] == "desktop" and session(S8)["alive"] is False
    assert session(S9) is None, "a dead session idle past the stale period is hidden"
    names = json.dumps(req("GET", "/api/state")[1]["agents"])
    assert "KEY FILE" not in names
    code, p = req("GET", f"/api/agent?session={S5}")
    assert code == 200 and p["activity"][0]["text"].startswith("Rotate") and p["dms"] == [] and p["ledger"] is None, p
    assert req("GET", "/api/agent?session=not-a-uuid")[0] == 400

    refused("/api/signal", {"session": S6, "signal": "kill"}, "is dead")
    refused("/api/signal", {"session": "x", "signal": "kill"}, "session must be")
    refused("/api/restart/terminal", {"session": S4}, "is alive")
    refused("/api/restart/terminal", {"session": S8}, "reopen it in the Claude Desktop app")
    time.sleep(0.2)
    assert other.poll() is None
    ok("/api/restart/terminal", {"session": S5})
    args = wait_for(lambda: (x := open(f"{TMP}/terminal-args").read().splitlines())[-1:] == [S5] and x)
    assert args and args[0] == f"--working-directory={p3}", args
    ok("/api/signal", {"session": S4, "signal": "terminate"})
    assert live.wait(5) == -signal.SIGTERM
    assert ev.find(lambda e: e["kind"] == "agents" and any(x["key"] == S4 and x["alive"] is False for x in e["agents"]))

    os.makedirs(os.path.join(DATA, "relay"), exist_ok=True)
    with open(os.path.join(DATA, "relay", f"{S4}.lock"), "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        lock.write(str(os.getpid()))
        lock.flush()
        assert wait_for(lambda: session(S4)["relay"] is True)
        assert hook(session=S4) is None, "a session with a live relay gets broadcasts from the relay, not the hook"
        fcntl.flock(lock, fcntl.LOCK_UN)
    assert "second notice" in hook(session=S4)
    assert wait_for(lambda: session(S4)["relay"] is False)
    print("registry ok")


def test_audits():
    log = [json.loads(x) for x in open(os.path.join(DATA, "admin-audit.jsonl"))]
    shown = req("GET", "/api/state")[1]["audit"]
    assert shown == log[-len(shown) :]
    for e in log:
        assert set(e) == {"ts", "who", "peer", "action", "target", "result"}, e
    want = [
        ("send", "room:ops", "sent"),
        ("broadcast", "broadcast", "sent"),
        ("signal", "cli-1", "sent SIGTERM to pid"),
        ("signal", "reuse-1", "refused: reuse-1 is dead"),
        ("restart/terminal", "cli-1", "opened a terminal"),
        ("restart/resume", "cli-1", "resuming"),
        ("ledger/release", "own-1/db lease", "released as own-1"),
        ("ledger/revoke", "own-1/db write for hold-1", "denied as own-1 (was granted)"),
    ]
    for action, target, result in want:
        assert any(
            e["action"] == action
            and e["who"] == "admin"
            and (e["target"] or "").startswith(target)
            and e["result"].startswith(result)
            for e in log
        ), (action, target, result)
    print("audit ok")


def test_console_never_crashed():
    with open(os.path.join(TMP, "monitor.err")) as f:
        err = f.read()
    assert "Traceback" not in err, err
