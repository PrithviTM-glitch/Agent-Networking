"""The admin console end to end: a console on a free port with a fake HOME (token, transcripts, session registry)
and fake claude / gnome-terminal binaries, against a broker with ledgerd. Covers the token, Origin and Host checks,
messaging as admin, the event stream, friendly names, death detection, signals, restarts, ledger actions, the audit
log and the broadcast hook. The tests are one scenario in file order."""

import contextlib
import fcntl
import http.client
import http.server
import json
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import uuid

import pytest
from conftest import BIN, free_port

from agent_bridge import config, relay
from agent_bridge.ledger import client as ledger

HOOK = str(BIN / "agent-bridge-broadcast-hook")
CONSOLE = str(BIN / "agent-bridge-console")
S1, S2, S3, S4, S5, S6, S7, S8, S9 = (str(uuid.uuid4()) for _ in range(9))
RUN_ID = uuid.uuid4().hex[:8]
TMP = HOME = FAKE_BIN = DATA = LOG = PROJ = REG = WEB = ORIGIN = HOST = None
procs = []
loose = []  # pids of double-forked test processes, which are nobody's child
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
    """A fake claude: a copy of /bin/sh named claude, waiting on a sleep child. Not a copied sleep: coreutils that
    dispatch on argv[0] (uutils, as on Ubuntu 26.04) exit at once when the binary is called claude."""
    p = subprocess.Popen([path, "-c", "sleep 600 & wait"])
    procs.append(p)
    loose.extend(wait_for(lambda: children(p.pid)) or ())
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
    {
        "type": "assistant",
        "message": {"content": [{"type": "text", "text": "hello from resume"}, {"type": "tool_use", "name": "Bash"}]},
    },
    separators=(",", ":"),
)


USAGE = {
    "claude_code_cost_usage_USD_total": [({}, 12.5)],
    "claude_code_token_usage_tokens_total": [({"type": "input"}, 1200), ({"type": "output"}, 3400)],
    "claude_code_active_time_seconds_total": [({"type": "user"}, 600), ({"type": "cli"}, 1800)],
    "claude_code_lines_of_code_count_total": [({"type": "added"}, 120), ({"type": "removed"}, 7)],
    "claude_code_commit_count_total": [({}, 2)],
    "claude_code_pull_request_count_total": [({}, 1)],
}
fake = {"queries": []}


class FakePrometheus(http.server.BaseHTTPRequestHandler):
    """Answers /api/v1/query with USAGE for session S1, whatever the aggregation."""

    def do_GET(self):
        q = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)["query"][0]
        fake["queries"].append(q)
        metric = next((m for m in USAGE if m in q), None)
        rows = [{"metric": {"session_id": S1, **labels}, "value": [time.time(), str(v)]} for labels, v in USAGE[metric]]
        body = json.dumps({"status": "success", "data": {"resultType": "vector", "result": rows}}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


def fake_server(handler):
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def fixtures():
    os.makedirs(FAKE_BIN)
    os.makedirs(PROJ)
    shutil.copy("/bin/sh", os.path.join(TMP, "claude"))
    desk = os.path.join(HOME, ".config", "Claude", "claude-code", "2.1.0", "abc")
    os.makedirs(desk)
    shutil.copy("/bin/sh", os.path.join(desk, "claude"))
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
def console(ledgerd, historyd, state_dir):
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
    prom = fake_server(FakePrometheus)
    env.update(
        HOME=HOME,
        PATH=f"{FAKE_BIN}:{os.environ['PATH']}",
        MONITOR_PORT=str(WEB),
        AGENT_BRIDGE_PROMETHEUS_URL=f"http://127.0.0.1:{prom.server_port}",
        AGENT_BRIDGE_GRAFANA_URL="http://grafana.test:3000",
    )
    with open(os.path.join(TMP, "monitor.err"), "w") as err:
        state["mon"] = mon = subprocess.Popen([CONSOLE], env=env, stdout=subprocess.PIPE, stderr=err, text=True)
    mon.stdout.readline()
    login = mon.stdout.readline().strip()
    assert login.startswith(f"login: http://127.0.0.1:{WEB}/#token="), login
    state["token"] = login.split("#token=")[1]
    assert wait_for(lambda: (a := agent("admin")) and a), "admin never joined the bus"
    yield
    for p in procs:
        kill_tree(p.pid)
        p.kill()
    for pid in loose:
        with contextlib.suppress(OSError):
            os.kill(pid, signal.SIGKILL)
    state["mon"].kill()
    state["mon"].wait()
    prom.shutdown()
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
    # Service RPC traffic, like historyd's healthcheck, is never a message in the feed or the backlog.
    for topic, payload in (
        ("history/req", {"op": "ping"}),
        (f"history/resp/{RUN_ID}", {"ok": True}),
        (f"ledger/req/{RUN_ID}", {"op": "init", "as": RUN_ID}),
    ):
        other.publish(topic, json.dumps(payload), qos=1).wait_for_publish(5)
    other.publish(
        "room/ops", json.dumps({"from": "bob-1", "body": "from bob", "ts": time.time()}), qos=1
    ).wait_for_publish(5)
    assert ev.find(lambda e: e["kind"] == "msg" and e["msg"]["body"] == "from bob")
    services = [e for e in ev.items if e["kind"] == "msg" and e["msg"]["topic"].split("/")[0] in ("history", "ledger")]
    assert not services, services
    assert ev.find(lambda e: e["kind"] == "msg" and e["msg"]["topic"] == "broadcast")
    want = {("broadcast", "everyone stop pushing to main"), ("room/ops", "hello room")}
    # The backlog comes from historyd, which records what it sees on the bus a moment later.
    assert wait_for(lambda: want <= {(m["topic"], m["body"]) for m in req("GET", "/api/state")[1]["messages"]})
    bodies = {(m["topic"], m["body"]) for m in req("GET", "/api/state")[1]["messages"]}
    assert not [t for t, _ in bodies if t.split("/")[0] in ("history", "ledger")], bodies
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
    from agent_bridge.history import client as history

    # A raw client can publish on broadcast as anyone: historyd keeps it, but only admin's reach the hook.
    forged = json.dumps({"from": "bob-1", "body": "forged by bob", "ts": time.time()})
    state["c"].publish("broadcast", forged, qos=1).wait_for_publish(5)
    state["c"].publish("broadcast", "{not json", qos=1).wait_for_publish(5)
    assert wait_for(lambda: any(m["body"] == "forged by bob" for m in history.recent(last=100))), "historyd kept it"
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
    t0 = time.monotonic()
    down = subprocess.run(
        [HOOK],
        input=json.dumps({"session_id": str(uuid.uuid4()), "hook_event_name": "SessionStart"}),
        capture_output=True,
        text=True,
        env={**os.environ, "AGENT_BRIDGE_PORT": str(free_port())},
    )
    assert down.returncode == 0 and not down.stdout and not down.stderr, down
    assert time.monotonic() - t0 < 2, "with no broker the hook shows nothing and returns at once"
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


def dead(pid):
    """True once pid is gone or a zombie, for processes this test did not start itself."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().rsplit(")", 1)[1].split()[0] in ("Z", "X")
    except OSError:
        return True


def children(pid):
    """argv of each live child of pid, by child pid."""
    out = {}
    for name in os.listdir("/proc"):
        try:
            with open(f"/proc/{name}/stat") as f:
                if int(f.read().rsplit(")", 1)[1].split()[1]) != pid or dead(int(name)):
                    continue
            with open(f"/proc/{name}/cmdline", "rb") as f:
                out[int(name)] = [a.decode() for a in f.read().split(b"\0")[:-1]]
        except (OSError, ValueError, IndexError):
            pass
    return out


def fake_tool(name):
    """A fake console script: python sleeping 600 s under the given name, so argv reads like the real command."""
    path = os.path.join(TMP, "kbin", name)
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write("#!/usr/bin/env python3\nimport time\ntime.sleep(600)\n")
        os.chmod(path, 0o755)
    return path


def fake_session(agents, extra=""):
    """A fake claude process (a copy of /bin/sh named claude) running `agent-bridge recv <a>` for each agent plus any
    extra commands: (process, start ticks, recv pid by agent, every child's argv by pid)."""
    shclaude = os.path.join(TMP, "kbin", "shell", "claude")
    if not os.path.exists(shclaude):
        os.makedirs(os.path.dirname(shclaude), exist_ok=True)
        shutil.copy("/bin/sh", shclaude)
    ab = fake_tool("agent-bridge")
    script = " ".join(f"{ab} recv {a} --wait 600 &" for a in agents)
    claude = subprocess.Popen([shclaude, "-c", f"{script} {extra} while :; do sleep 1; done"])
    procs.append(claude)
    with open(f"/proc/{claude.pid}/stat") as f:
        start = int(f.read().rsplit(")", 1)[1].split()[19])
    want = len(agents) + extra.count(ab)
    kids = wait_for(lambda: len(c := {p: a for p, a in children(claude.pid).items() if a[1:2] == [ab]}) >= want and c)
    loose.extend(kids)
    by = {argv[3]: pid for pid, argv in kids.items() if len(argv) > 3 and argv[2] == "recv"}
    return claude, start, by, kids


def kill_tree(pid):
    """SIGKILL everything below pid, so no fake child outlives the test."""
    for child in children(pid):
        kill_tree(child)
        with contextlib.suppress(OSError):
            os.kill(child, signal.SIGKILL)


def test_room_kill():
    sk, sk2 = str(uuid.uuid4()), str(uuid.uuid4())
    # One fake claude session whose bus commands run as several agents, plus two decoys that must survive.
    agents = ("sub-k", "mark-k", "twin-k", "sub-2", "other-x")
    claude, start, by, kids = fake_session(agents, f"{fake_tool('agent-bridge')} dm other-x sub-k hi &")
    dm_pid = next(pid for pid, argv in kids.items() if argv[2:3] == ["dm"])
    registry_file(claude.pid, sessionId=sk, pid=claude.pid, procStart=str(start), name="Kill target")
    origin = {"pid": claude.pid, "pid_start": start}
    # The live case: sess-k and twin-k carry the same session and pid; only sess-k is held by the session's relay.
    presence("sess-k", session=sk, topics=["room/red"], **origin)
    presence("twin-k", session=sk, topics=["room/red"], **origin)
    presence("sub-k", topics=["room/red"], **origin)
    presence("mark-k", kind="subagent", parent=sk, topics=["room/red"], **origin)
    presence("ghost-k", pid=4194300, pid_start=5, topics=["room/red"])
    presence("claim-k", session=sk2, pid=4194300, pid_start=5, topics=["room/red"])
    presence("sub-2", topics=["room/red2"], **origin)
    presence("bystander-1", topics=["room/elsewhere"], **origin)
    for a in ("sess-k", "twin-k", "sub-k", "mark-k", "ghost-k", "claim-k", "sub-2", "bystander-1"):
        assert wait_for(lambda a=a: agent(a)), a
    os.makedirs(os.path.dirname(lock_path := relay.lock_path(sk)), exist_ok=True)
    lock = open(lock_path, "w")
    fcntl.flock(lock, fcntl.LOCK_EX)
    lock.write(f"{os.getpid()}\nsess-k\n")
    lock.flush()

    def preview(room, **q):
        code, j = req("GET", f"/api/room/kill?room={room}&" + "&".join(f"{k}={v}" for k, v in q.items()))
        assert code == 200, j
        return {t["addr"]: t for t in j["targets"]}

    others = "bystander-1, mark-k, sub-2, sub-k, twin-k"
    plain = preview("red")
    assert sorted(plain) == ["claim-k", "ghost-k", "mark-k", "sess-k", "sub-k", "twin-k"], plain
    assert plain["sess-k"]["class"] == "session" and plain["sess-k"]["reason"] == f"the primary agent of session {sk}"
    assert plain["sess-k"]["action"] == (
        f"skip: would end the WHOLE session {sk}; this also ends {others} (tick kill whole sessions)"
    )
    assert plain["twin-k"]["reason"] == f"shares session {sk} with its own agent sess-k", plain["twin-k"]
    assert plain["mark-k"]["reason"] == "marked as a subagent" and plain["sub-k"]["reason"] == "no session"
    for a in ("sub-k", "mark-k", "twin-k"):
        assert plain[a]["class"] == "subagent" and [p["pid"] for p in plain[a]["procs"]] == [by[a]], plain[a]
        assert plain[a]["action"] == (
            f"SIGTERM its bus processes {by[a]}, then wipe the agent; parent claude pid {claude.pid} keeps running"
        )
    assert plain["ghost-k"]["class"] == "unverifiable" and plain["ghost-k"]["action"].startswith("wipe the agent only")
    assert plain["claim-k"]["action"].startswith("skip: claims a whole session")
    whole = preview("red", kill_sessions="true")
    assert (
        whole["sess-k"]["action"]
        == f"SIGTERM claude pid {claude.pid}, ending the WHOLE session {sk}; this also ends {others}"
    )
    assert whole["claim-k"]["action"].startswith("wipe the agent only")
    assert preview("red", kill_sessions="true", signal="kill")["sess-k"]["action"].startswith("SIGKILL claude pid")
    fcntl.flock(lock, fcntl.LOCK_UN)
    nobody = preview("red", kill_sessions="true")
    assert nobody["sess-k"]["class"] == "subagent" and "none primary" in nobody["sess-k"]["reason"], nobody["sess-k"]
    assert not any("ending the WHOLE session" in t["action"] for t in nobody.values()), (
        "without a known primary agent no whole session may be killed"
    )
    fcntl.flock(lock, fcntl.LOCK_EX)

    assert req("GET", "/api/room/kill?room=red", token=None)[0] == 401
    assert req("GET", "/api/room/kill?room=bad%20room")[0] == 400
    assert req("GET", "/api/room/kill?room=red&signal=hup")[0] == 400
    body = {"room": "red", "confirm": "red"}
    assert post("/api/room/kill", body, origin=None)[0] == 403
    assert post("/api/room/kill", body, origin="http://evil.example")[0] == 403
    assert post("/api/room/kill", body, token="wrong")[0] == 401
    refused("/api/room/kill", {**body, "confirm": "RED"}, "type the room name")
    refused("/api/room/kill", {**body, "kill_sessions": "yes"}, "true or false")
    time.sleep(0.2)
    assert claude.poll() is None and not any(map(dead, kids)), "a refused kill touched a process"

    j = ok("/api/room/kill", body)
    got = {t["addr"]: t for t in j["targets"]}
    assert {a: (t["class"], t["action"]) for a, t in got.items()} == {
        a: (t["class"], t["action"]) for a, t in plain.items()
    }, "the kill must do what the preview showed"
    assert got["twin-k"]["result"] == f"subagent stopped: signalled {by['twin-k']}; wiped the agent", got
    assert got["ghost-k"]["result"] == "unverifiable: wiped the agent", got
    assert got["sess-k"]["result"].startswith("session: skip: would end the WHOLE session"), got
    assert wait_for(lambda: all(dead(by[a]) for a in ("sub-k", "mark-k", "twin-k")))
    time.sleep(0.3)
    assert claude.poll() is None, "killing the room's subagents must leave the session's claude running"
    assert not dead(by["sub-2"]) and not dead(by["other-x"]) and not dead(dm_pid), "only the room's agents stop"
    assert wait_for(lambda: not any(agent(a) for a in ("sub-k", "mark-k", "twin-k", "ghost-k")))
    assert agent("sess-k") and agent("claim-k") and agent("bystander-1")

    j = ok("/api/room/kill", {"room": "red2", "confirm": "red2"})
    assert j["targets"][0]["result"].startswith("subagent stopped: signalled"), j
    assert wait_for(lambda: dead(by["sub-2"]))
    time.sleep(0.3)
    assert claude.poll() is None, "killing a room with only a subagent leaves its parent claude running"

    j = ok("/api/room/kill", {"room": "red", "confirm": "red", "signal": "kill", "kill_sessions": True})
    assert {t["addr"]: t["result"] for t in j["targets"]} == {
        "claim-k": "unverifiable: wiped the agent",
        "sess-k": f"session killed: sent SIGKILL to claude pid {claude.pid}",
    }, j
    assert claude.wait(5) == -signal.SIGKILL
    lock.close()
    assert ok("/api/room/kill", {"room": "nobody-here", "confirm": "nobody-here"})["result"] == "terminate: no members"

    log = [json.loads(x) for x in open(os.path.join(DATA, "admin-audit.jsonl"))]
    kills = [(e["target"], e["result"]) for e in log if e["action"] == "room/kill"]
    for target, result in [
        ("red:sub-k", "subagent stopped: signalled"),
        ("red:mark-k", "subagent stopped: signalled"),
        ("red:twin-k", "subagent stopped: signalled"),
        ("red:ghost-k", "unverifiable: wiped the agent"),
        ("red:sess-k", "session: skip: would end the WHOLE session"),
        ("red:sess-k", "session killed: sent SIGKILL"),
        ("red2:sub-2", "subagent stopped"),
        ("room:red", "terminate: 1 session, 3 subagent, 2 unverifiable"),
        ("room:red", "kill, whole sessions too: 1 session, 1 unverifiable"),
        ("red", "refused: type the room name"),
        (None, "refused: bad Origin"),
    ]:
        assert any(t == target and r.startswith(result) for t, r in kills), (target, result, kills)


def test_admin_mentions():
    seen = []
    w = ledger.connect()
    w.on_message = lambda _c, _u, m: seen.append(json.loads(m.payload))
    w.subscribe("room/men", qos=1)
    presence("men-a1b2c3", topics=["room/men"])
    presence("other-d4e5f6", topics=["room/men"])
    assert wait_for(lambda: agent("men-a1b2c3") and agent("other-d4e5f6"))
    time.sleep(0.3)
    ok("/api/send", {"kind": "room", "target": "men", "text": "@men-a1b2c3 please look"})
    got = wait_for(lambda: [m for m in seen if m["body"] == "@men-a1b2c3 please look"])
    assert got and got[0]["from"] == "admin" and got[0]["mentions"] == ["men-a1b2c3"], got
    code, j = post("/api/send", {"kind": "room", "target": "men", "text": "@nobody-zzz9 hello"})
    assert code == 400, j
    shown = [m for m in req("GET", "/api/state")[1]["messages"] if m["body"] == "@men-a1b2c3 please look"]
    assert shown and shown[0]["mentions"] == ["men-a1b2c3"], shown
    w.disconnect()
    w.loop_stop()


def test_room_settings():
    presence("mod-a1b2c3", topics=["room/set"])
    presence("loud-a1b2c3", topics=["room/set"])
    assert wait_for(lambda: agent("mod-a1b2c3") and agent("loud-a1b2c3"))
    ev = Events()
    ok("/api/room/settings", {"room": "set", "mode": "announce", "rules": "be brief\nno spam"})
    ok("/api/room/moderator", {"room": "set", "action": "add", "addr": "mod-a1b2c3"})
    ok("/api/room/member", {"room": "set", "action": "mute", "addr": "loud-a1b2c3"})
    ok("/api/room/member", {"room": "set", "action": "kick", "addr": "loud-a1b2c3"})
    info = req("GET", "/api/room?room=set")[1]
    assert info["mode"] == "announce" and info["rules"] == "be brief\nno spam", info
    assert (
        info["moderators"] == ["mod-a1b2c3"] and info["muted"] == ["loud-a1b2c3"] and info["banned"] == ["loud-a1b2c3"]
    )
    assert ev.find(lambda e: e["kind"] == "room" and e["info"]["banned"] == ["loud-a1b2c3"])
    assert req("GET", "/api/state")[1]["rooms"]["set"]["moderators"] == ["mod-a1b2c3"]
    assert not any(m["topic"].startswith("rooms/") for m in req("GET", "/api/state")[1]["messages"])
    ok("/api/room/member", {"room": "set", "action": "unban", "addr": "loud-a1b2c3"})
    ok("/api/room/moderator", {"room": "set", "action": "remove", "addr": "mod-a1b2c3"})
    info = req("GET", "/api/room?room=set")[1]
    assert info["banned"] == [] and info["moderators"] == [], info
    refused("/api/room/member", {"room": "set", "action": "nuke", "addr": "loud-a1b2c3"}, "action must be")
    refused("/api/room/member", {"room": "set", "action": "ban", "addr": "admin"}, "admin")
    refused("/api/room/settings", {"room": "set", "mode": "silent"}, "mode must be")
    refused("/api/room/settings", {"room": "bad room", "mode": "open"}, "bad room")
    assert post("/api/room/member", {"room": "set", "action": "mute", "addr": "x"}, origin=None)[0] == 403
    assert req("GET", "/api/room?room=set", token=None)[0] == 401
    log = [json.loads(x) for x in open(os.path.join(DATA, "admin-audit.jsonl"))]
    done = {(e["action"], e["target"], e["result"]) for e in log}
    for want in [
        ("room/settings", "room:set", "mode announce, rules 16 chars"),
        ("room/moderator", "set:mod-a1b2c3", "moderator add"),
        ("room/member", "set:loud-a1b2c3", "muted"),
        ("room/member", "set:loud-a1b2c3", "kicked"),
        ("room/member", "set:loud-a1b2c3", "unbanned"),
    ]:
        assert want in done, (want, sorted(done))


def test_capacity():
    sc = str(uuid.uuid4())
    claude, start, _, _ = fake_session(("cap-x",), "python3 -c 'while True: pass' &")
    registry_file(claude.pid, sessionId=sc, pid=claude.pid, procStart=str(start), name="Busy session")

    def busy_row():
        c = req("GET", "/api/capacity")[1]
        row = next((r for r in c["sessions"] if r["pid"] == claude.pid), None)
        return row and row["cpu"] > 30 and (c, row)

    got = wait_for(busy_row, 20)
    assert got, req("GET", "/api/capacity")[1]
    c, row = got
    assert row["key"] == sc and row["friendly"] == "Busy session" and row["procs"] >= 3 and row["rss"] > 0, row
    spinner = next(r for r in row["tree"] if "while True" in r["cmd"])
    assert spinner["cpu"] > 30 and any(r["pid"] == claude.pid for r in row["tree"]), row["tree"]
    assert c["machine"]["cpus"] >= 1 and c["machine"]["mem_total"] > 0 and len(c["machine"]["load"]) == 3, c
    assert any(t["pid"] == spinner["pid"] and t["session"] == sc for t in c["top"]), c["top"]
    assert req("GET", "/api/capacity", token=None)[0] == 401
    kill_tree(claude.pid)


def double_fork(cmd, **env):
    """Start cmd so that its parent exits at once: it is then nobody's child, like a leftover. Returns pid, start."""
    out = subprocess.run(
        ["sh", "-c", f"{cmd} >/dev/null 2>&1 & echo $!"],
        env={**os.environ, **env},
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    pid = int(out)
    loose.append(pid)
    with open(f"/proc/{pid}/stat") as f:
        return pid, int(f.read().rsplit(")", 1)[1].split()[19])


def test_leaks():
    from agent_bridge.console import leaks

    sd = str(uuid.uuid4())
    gone, gone_start = start_fake(os.path.join(TMP, "claude"))
    gone.kill()
    gone.wait()
    registry_file(gone.pid, sessionId=sd, pid=gone.pid, procStart=str(gone_start), name="Dead session")
    orphan, orphan_start = double_fork(
        f"{fake_tool('agent-bridge')} recv orphan-x --wait 600", CLAUDE_CODE_SESSION_ID=sd
    )
    leftover, leftover_start = double_fork(f"{fake_tool('agent-bridge-ledgerd')}")
    name = f"agent-bridge-test-{uuid.uuid4().hex[:6]}"
    subprocess.run(["docker", "create", "--name", name, "eclipse-mosquitto:2.0.22"], check=True, capture_output=True)
    try:
        d = wait_for(lambda: (r := req("GET", "/api/leaks")[1]) and orphan in [p["pid"] for p in r["processes"]] and r)
        assert d, req("GET", "/api/leaks")[1]
        found = {p["pid"]: p for p in d["processes"]}
        assert found[orphan]["kind"] == "session" and sd in found[orphan]["reason"], found
        assert found[leftover]["kind"] == "test", found
        assert state["mon"].pid not in found and os.getpid() not in found, (
            "the console and the test run are no leftovers"
        )
        assert d["busy"].startswith("a pytest run is in progress"), d
        assert name in [c["name"] for c in d["containers"]]
        assert not {"agent-bridge-mqtt", "agent-bridge-ledgerd"} & {c["name"] for c in d["containers"]}

        refused("/api/leaks/container", {"name": name}, "not now: a pytest run is in progress")
        refused("/api/leaks/container", {"name": "agent-bridge-mqtt"}, "only agent-bridge-test-")
        refused("/api/leaks/process", {"pid": leftover, "start": leftover_start}, "not now: a pytest run")
        refused("/api/leaks/process", {"pid": os.getpid(), "start": 1}, "not an orphaned")
        refused("/api/leaks/process", {"pid": "1", "start": 1}, "positive integers")
        assert post("/api/leaks/process", {"pid": orphan, "start": orphan_start}, origin=None)[0] == 403
        assert dead(leftover) is False and dead(orphan) is False
        ok("/api/leaks/process", {"pid": orphan, "start": orphan_start})
        assert wait_for(lambda: dead(orphan))
        assert not dead(leftover), "a test leftover waits while a test run may use it"
        assert req("GET", "/api/leaks", token=None)[0] == 401

        # The removal itself, once no test run is active, checked in this process with the guard lifted.
        real = leaks.busy
        leaks.busy = lambda _procs: None
        try:
            assert leaks.remove_container(name) == f"removed container {name}"
        finally:
            leaks.busy = real
        assert name not in [c["name"] for c in leaks.containers()]
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
    log = [json.loads(x) for x in open(os.path.join(DATA, "admin-audit.jsonl"))]
    assert any(
        e["action"] == "leaks/process" and e["result"].startswith(f"sent SIGTERM to orphaned pid {orphan}") for e in log
    )
    assert any(e["action"] == "leaks/container" and e["result"].startswith("refused: not now") for e in log)


def test_targeted_kill():
    ss = str(uuid.uuid4())
    claude, start, by, _ = fake_session(("pick-a", "pick-b", "sess-s"))
    registry_file(claude.pid, sessionId=ss, pid=claude.pid, procStart=str(start), name="Targeted")
    origin = {"pid": claude.pid, "pid_start": start}
    presence("pick-a", topics=["room/pick"], **origin)
    presence("pick-b", topics=["room/pick"], **origin)
    presence("sess-s", session=ss, topics=["room/pick"], **origin)
    assert wait_for(lambda: agent("pick-a") and agent("pick-b") and agent("sess-s"))

    refused("/api/room/kill", {"room": "pick", "confirm": "pick", "only": ["nobody-1"]}, "not members of #pick")
    refused("/api/room/kill", {"room": "pick", "confirm": "pick", "only": "pick-a"}, "non-empty list")
    j = ok("/api/room/kill", {"room": "pick", "confirm": "pick", "only": ["pick-a"]})
    assert [t["addr"] for t in j["targets"]] == ["pick-a"] and j["result"].endswith("1 selected: 1 subagent"), j
    assert wait_for(lambda: dead(by["pick-a"]))
    assert not dead(by["pick-b"]) and agent("pick-b"), "an unselected member is left alone"

    code, p = req("GET", "/api/agent/stop?addr=pick-b")
    assert code == 200 and [t["class"] for t in p["targets"]] == ["subagent"], p
    assert req("GET", "/api/agent/stop?addr=nobody-1")[0] == 400
    refused("/api/agent/stop", {"addr": "pick-b"}, "type the agent's address")
    j = ok("/api/agent/stop", {"addr": "pick-b", "confirm": "pick-b"})
    assert j["result"] == f"subagent stopped: signalled {by['pick-b']}; wiped the agent", j
    assert wait_for(lambda: dead(by["pick-b"]) and not agent("pick-b"))
    j = ok("/api/agent/stop", {"addr": "sess-s", "confirm": "sess-s"})
    assert j["result"].startswith("session: skip: would end the WHOLE session"), j
    time.sleep(0.3)
    assert claude.poll() is None and not dead(by["sess-s"]), "stopping without whole sessions leaves the session"
    kill_tree(claude.pid)


def test_audit_filters():
    code, j = req("GET", "/api/audit?action=agent/stop")
    assert code == 200 and j["entries"] and {e["action"] for e in j["entries"]} == {"agent/stop"}, j
    assert {"room/kill", "agent/stop", "leaks/process", "send"} <= set(j["actions"])
    picked = req("GET", "/api/audit?target=pick-b")[1]["entries"]
    assert picked and all("pick-b" in e["target"] for e in picked), picked
    assert req("GET", f"/api/audit?since={time.time() + 100}")[1]["entries"] == []
    recent = req("GET", f"/api/audit?action=room/kill&since={time.time() - 600}")[1]["entries"]
    assert recent and all(e["action"] == "room/kill" for e in recent)
    assert req("GET", "/api/audit?since=yesterday")[0] == 400
    assert req("GET", "/api/audit", token=None)[0] == 401


def scraped(name):
    """The console's current /metrics samples for name, as label dicts with the value under "="."""
    return [labels | {"=": value} for labels, value in series(scrape()[2]).get(name, [])]


def test_usage():
    from agent_bridge.console import usage

    code, u = req("GET", "/api/usage")
    assert code == 200 and u["enabled"] and u["error"] is None and u["grafana"] == "http://grafana.test:3000", u
    x = u["sessions"][S1]
    assert x == {
        "cost": 12.5,
        "tokens": {"input": 1200, "output": 3400},
        "active": {"user": 600, "cli": 1800},
        "lines": {"added": 120, "removed": 7},
        "commits": 2,
        "prs": 1,
    }, x
    asked = len(fake["queries"])
    assert asked == 6 and all("max_over_time(" in q and "[30d]" in q for q in fake["queries"]), fake["queries"]
    assert req("GET", "/api/usage")[1]["sessions"][S1]["cost"] == 12.5 and len(fake["queries"]) == asked, "cached"
    assert req("GET", "/api/usage", token=None)[0] == 401

    # The room join for Grafana: a session agent in a room, and a subagent of the same session in it too.
    presence("join-1", session=S1, topics=["room/joined", "room/other"])
    presence("join-sub", kind="subagent", parent=S1, topics=["room/joined"])
    want = {("join-1", "joined", S1), ("join-1", "other", S1), ("join-sub", "joined", S1)}

    def memberships():
        return {(p["addr"], p.get("room"), p.get("session_id")) for p in scraped("agent_bridge_membership")}

    assert wait_for(lambda: want <= memberships(), 15), memberships()
    assert {p["="] for p in scraped("agent_bridge_membership")} == {1}
    named = [p for p in scraped("agent_bridge_session") if p["session_id"] == S1]
    assert len(named) == 1 and named[0]["addr"] == "join-1" and named[0]["name"], named
    agents = {p["addr"]: p for p in scraped("agent_bridge_agent")}
    assert agents["join-sub"]["kind"] == "subagent" and agents["join-sub"]["session_id"] == S1, agents["join-sub"]

    # Fail soft, and off when unset, checked in this process.
    real = usage.PROMETHEUS, usage.state["snap"]
    try:
        usage.PROMETHEUS, usage.state["snap"] = f"http://127.0.0.1:{free_port()}", None
        s = usage.snapshot()
        assert s["enabled"] and s["error"].startswith("Prometheus at") and s["sessions"] == {}, s
        usage.PROMETHEUS, usage.state["snap"] = None, None
        s = usage.snapshot()
        assert not s["enabled"] and "AGENT_BRIDGE_PROMETHEUS_URL" in s["hint"] and s["sessions"] == {}, s
    finally:
        usage.PROMETHEUS, usage.state["snap"] = real


SAMPLE = re.compile(r'([a-z_]+)(\{(?:[a-z_]+="(?:[^"\\]|\\.)*",?)*\})? (-?[0-9.e+-]+|NaN)')


def scrape(**kw):
    c = http.client.HTTPConnection("127.0.0.1", kw.pop("port", WEB), timeout=30)
    c.request("GET", "/metrics", headers={"Host": kw.pop("host", HOST)})
    r = c.getresponse()
    return r.status, r.getheader("Content-Type"), r.read().decode()


def series(text):
    """name -> list of (labels dict, value) from Prometheus text, checking every line's syntax."""
    out = {}
    for line in text.splitlines():
        if line.startswith("# HELP ") or line.startswith("# TYPE "):
            continue
        m = SAMPLE.fullmatch(line)
        assert m, f"bad exposition line: {line!r}"
        labels = dict(re.findall(r'([a-z_]+)="((?:[^"\\]|\\.)*)"', m[2] or ""))
        out.setdefault(m[1], []).append((labels, float(m[3])))
    return out


def test_metrics():
    code, ctype, text = scrape()
    assert code == 200 and ctype.startswith("text/plain; version=0.0.4"), (code, ctype)
    assert scrape(host="evil.example:1")[0] == 403, "the Host check covers /metrics too"
    assert "# TYPE agent_bridge_console_cpu_seconds_total counter" in text
    s = wait_for(lambda: (x := series(scrape()[2])) and "agent_bridge_broker_clients" in x and x, 20)
    assert s, "the broker's $SYS statistics never arrived"
    states = {lab["state"] for lab, _ in s["agent_bridge_broker_clients"]}
    assert {"connected", "total"} <= states and any(lab.get("version") for lab, _ in s["agent_bridge_broker_info"]), s
    assert any(lab == {"kind": "messages_received", "window": "1min"} for lab, _ in s["agent_bridge_broker_load"])
    assert s["agent_bridge_machine_cpus"][0][1] >= 1 and len(s["agent_bridge_machine_load"]) == 3
    assert s["agent_bridge_console_open_fds"][0][1] > 0 and s["agent_bridge_console_threads"][0][1] >= 3
    assert any(lab.get("addr") == "join-1" and lab.get("room") == "joined" for lab, _ in s["agent_bridge_membership"])
    assert any(lab["room"] == "set" for lab, _ in s["agent_bridge_room_members"])
    assert any(lab.get("action") == "send" for lab, _ in s["agent_bridge_audit_entries"])
    assert "agent_bridge_ledgers" in s and "agent_bridge_leftover_containers" in s, sorted(s)
    assert "agent_bridge_scrape_error" not in s, s.get("agent_bridge_scrape_error")
    sids = [lab["session_id"] for lab, _ in s["agent_bridge_session"]]
    assert len(sids) == len(set(sids)), "one agent_bridge_session series per session"
    assert "hello room" not in text and "please look" not in text, "no message text in metrics"
    assert not any(k.startswith("claude_code") for k in s), "Claude's own metrics are not re-exported"


def test_headless():
    port, broker_port = free_port(), free_port()
    env = {k: v for k, v in os.environ.items() if k not in ("XDG_CONFIG_HOME", "CLAUDE_CONFIG_DIR")}
    # No broker on its port: the console keeps retrying the bus while its HTTP side works, without ever touching
    # the test broker's admin identity.
    env.update(HOME=HOME, MONITOR_PORT=str(port), AGENT_BRIDGE_PORT=str(broker_port))
    with open(os.path.join(TMP, "headless.err"), "w") as err:
        p = subprocess.Popen([CONSOLE, "--headless"], env=env, stdout=subprocess.PIPE, stderr=err, text=True)
    procs.append(p)
    p.stdout.readline()
    line = p.stdout.readline()
    assert line.startswith(f"headless: API on http://127.0.0.1:{port}/api") and state["token"] not in line, line
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    for path in ("/", "/app.js", "/app.css"):
        c.request("GET", path, headers={"Host": f"127.0.0.1:{port}"})
        r = c.getresponse()
        r.read()
        assert r.status == 404, (path, r.status)
    c.request("GET", "/api/state", headers={"Host": f"127.0.0.1:{port}", "X-Admin-Token": state["token"]})
    r = c.getresponse()
    assert r.status == 200 and json.loads(r.read())["connected"] is False
    code, _, text = scrape(port=port, host=f"127.0.0.1:{port}")
    assert code == 200 and "agent_bridge_console_broker_connected 0" in text
    p.kill()
    p.wait()


def admin_cli(*args, home=None, port=None):
    env = {k: v for k, v in os.environ.items() if k not in ("XDG_CONFIG_HOME",)}
    env.update(HOME=home or HOME, MONITOR_PORT=str(port or WEB))
    r = subprocess.run(
        [sys.executable, "-m", "agent_bridge.console.cli", *args], env=env, capture_output=True, text=True, timeout=60
    )
    return (
        r.returncode,
        [json.loads(line) for line in r.stdout.splitlines()] if args[0] != "metrics" else r.stdout,
        r.stderr,
    )


def cli_ok(*args):
    code, out, err = admin_cli(*args)
    assert code == 0, (args, code, err)
    return out


def test_admin_cli():
    imports = subprocess.run(
        [sys.executable, "-c", "import sys, agent_bridge.console.cli; print(sorted(sys.modules))"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert "paho" not in imports and "agent_bridge.bus" not in imports, "the CLI must never open a bus of its own"

    [st] = cli_ok("status")
    assert st["connected"] is True and st["on_bus"] >= 3 and st["sessions"] >= st["on_bus"], st
    addrs = {a["addr"] for a in cli_ok("agents")}
    assert {"admin", "bob-1"} <= addrs, addrs
    seen = []
    w = ledger.connect()
    w.on_message = lambda _c, _u, m: seen.append((m.topic, json.loads(m.payload)))
    w.subscribe("room/cli-room", qos=1)
    w.subscribe("broadcast", qos=1)
    presence("cli-mod-a1b2c3", topics=["room/cli-room"])
    claude, start, by, _ = fake_session(("cli-sub",))
    presence("cli-sub", topics=["room/cli-room"], pid=claude.pid, pid_start=start)
    assert wait_for(lambda: agent("cli-mod-a1b2c3") and agent("cli-sub"))
    time.sleep(0.3)
    assert cli_ok("say", "cli-room", "hello from the cli") == [{"ok": True, "result": "sent"}]
    cli_ok("broadcast", "cli broadcast")
    assert wait_for(
        lambda: (
            {(t, m["body"]) for t, m in seen}
            >= {("room/cli-room", "hello from the cli"), ("broadcast", "cli broadcast")}
        )
    )
    assert all(m["from"] == "admin" for _, m in seen)
    cli_ok("room-set", "cli-room", "--mode", "announce", "--rules", "be brief")
    cli_ok("mod", "add", "cli-room", "cli-mod-a1b2c3")
    cli_ok("mute", "cli-room", "cli-sub")
    [info] = cli_ok("room", "cli-room")
    assert info["mode"] == "announce" and info["moderators"] == ["cli-mod-a1b2c3"] and info["muted"] == ["cli-sub"], (
        info
    )
    assert sorted(info["members"]) == ["cli-mod-a1b2c3", "cli-sub"], info
    cli_ok("unmute", "cli-room", "cli-sub")
    assert any(r["room"] == "cli-room" and r["mode"] == "announce" for r in cli_ok("rooms"))

    code, out, err = admin_cli("kill-room", "cli-room")
    assert code == 0 and "preview only: add --confirm cli-room" in err, err
    assert {t["preview"]["addr"]: t["preview"]["class"] for t in out} == {
        "cli-mod-a1b2c3": "unverifiable",
        "cli-sub": "subagent",
    }
    assert not dead(by["cli-sub"]), "a preview changes nothing"
    out = cli_ok("kill-room", "cli-room", "--confirm", "cli-room", "--only", "cli-sub")
    assert out[0]["target"]["addr"] == "cli-sub" and out[0]["target"]["result"].startswith(
        "subagent stopped: signalled"
    ), out
    assert out[-1]["result"].endswith("1 selected: 1 subagent"), out
    assert wait_for(lambda: dead(by["cli-sub"])) and claude.poll() is None
    code, out, err = admin_cli("stop-agent", "cli-mod-a1b2c3")
    assert code == 0 and out[0]["preview"]["class"] == "unverifiable" and "--confirm cli-mod-a1b2c3" in err
    out = cli_ok("stop-agent", "cli-mod-a1b2c3", "--confirm", "cli-mod-a1b2c3")
    assert out[-1]["result"] == "unverifiable: wiped the agent", out
    assert wait_for(lambda: not agent("cli-mod-a1b2c3"))

    kills = cli_ok("audit", "--action", "room/kill", "--since", "10m")
    assert (
        kills
        and all(e["action"] == "room/kill" for e in kills)
        and any(e["target"] == "cli-room:cli-sub" for e in kills)
    )
    assert cli_ok("audit", "--since", str(time.time() + 100)) == []
    [machine, *_] = cli_ok("capacity")
    assert machine["machine"]["cpus"] >= 1
    assert "busy" in cli_ok("leftovers")[0]
    code, text, _ = admin_cli("metrics")
    assert code == 0 and "agent_bridge_console_uptime_seconds" in text

    # One-line errors and a non-zero exit.
    code, out, err = admin_cli("say", "bad room", "x")
    assert code == 1 and out == [] and err.strip().startswith("bad room") and err.count("\n") == 1, err
    code, _, err = admin_cli("status", port=free_port())
    assert code == 1 and "no admin console on 127.0.0.1:" in err and "agent-bridge-console" in err, err
    code, _, err = admin_cli("status", home=tempfile.mkdtemp())
    assert code == 1 and "no admin token at" in err, err
    code, _, err = admin_cli("audit", "--since", "yesterday")
    assert code == 1 and "bad time" in err, err
    code, _, err = admin_cli("cleanup", "process", "12")
    assert code == 1 and "PID START" in err, err
    log = [json.loads(x) for x in open(os.path.join(DATA, "admin-audit.jsonl"))]
    assert any(e["action"] == "room/settings" and e["target"] == "room:cli-room" for e in log), (
        "CLI actions are audited"
    )
    w.disconnect()
    w.loop_stop()
    kill_tree(claude.pid)


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


def test_ledger_no_reply_wording(monkeypatch):
    """A release or deny whose reply never came may still have been applied, so the console says so; a read does not."""
    from agent_bridge.console import admin
    from agent_bridge.errors import LedgerError

    class Lost:
        def ledger(self, op, as_, **fields):
            raise LedgerError({"ok": False, "error": "no reply from ledgerd within 5s, is it running?"})

    monkeypatch.setitem(admin.held, "bus", Lost())
    for op, applied in (("release", True), ("deny", True), ("dump", False)):
        with pytest.raises(admin.Refused) as e:
            admin.ledger_call(op, "admin")
        assert str(e.value).startswith("ledger: no reply from ledgerd within 5s"), e.value
        assert ("it may still have been applied, check the ledger view" in str(e.value)) == applied, (op, e.value)


def test_native_only_imports():
    """The console and the MCP server run on the native core: importing them loads no paho and none of the Python bus,
    presence, rooms, ledger or history clients, so those can be deleted without touching either."""
    banned = (
        "paho",
        "agent_bridge.broker",
        "agent_bridge.bus",
        "agent_bridge.presence",
        "agent_bridge.rooms",
        "agent_bridge.ledger.client",
        "agent_bridge.history.client",
    )
    code = (
        "import sys, agent_bridge.console.server, agent_bridge.console.cli, agent_bridge.mcp_server\n"
        f"print(sorted(m for m in sys.modules if any(m == b or m.startswith(b + '.') for b in {banned!r})))"
    )
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert p.stdout.strip() == "[]", p.stdout


def test_console_never_crashed():
    with open(os.path.join(TMP, "monitor.err")) as f:
        err = f.read()
    assert "Traceback" not in err, err
