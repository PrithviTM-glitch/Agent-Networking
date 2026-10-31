"""Leftovers that cost capacity: test broker containers and orphaned agent-bridge processes, verified before cleanup.

A container is a leftover only by its exact test name. A process is orphaned only when it runs one of this package's
commands as this user, what started it is gone (it was reparented to init or a systemd user manager), and either its
Claude session is provably dead (the registry's pid and start time no longer match) or it is a test's process.
Nothing is removed while a test run may still be using it: any pytest process, or a held lease on a resource named
test-runner."""

import json
import os
import re
import shutil
import signal
import socket
import subprocess
import tempfile

from .. import host
from . import admin, processes, sessions
from .processes import Refused

TEST_CONTAINER = re.compile(r"agent-bridge-test-[0-9a-f]{6}")
KEEP = ("agent-bridge-mqtt", "agent-bridge-ledgerd", "agent-bridge-historyd")
TEST_LOCK = "test-runner"
ENTRY_POINTS = {
    "agent-bridge",
    "agent-bridge-ledger",
    "agent-bridge-ledgerd",
    "agent-bridge-historyd",
    "agent-bridge-mcp",
    "agent-bridge-console",
    "agent-bridge-admin",
    "agent-bridge-relay-hook",
    "agent-bridge-broadcast-hook",
}


def ours(argv):
    """True if argv runs one of this package's commands, directly or as `python -m agent_bridge...`."""
    head = argv[:2]
    if any(os.path.basename(a) in ENTRY_POINTS for a in head):
        return True
    return any(a == "-m" and argv[i + 1 : i + 2] and argv[i + 1].startswith("agent_bridge") for i, a in enumerate(argv))


def is_console(argv):
    return any(os.path.basename(a) == "agent-bridge-console" for a in argv[:2]) or "agent_bridge.console" in argv[1:3]


def is_pytest(argv):
    return any(os.path.basename(a) in ("pytest", "py.test") for a in argv[:2]) or argv[1:3] == ["-m", "pytest"]


def _environ(pid):
    try:
        with open(f"/proc/{pid}/environ", "rb") as f:
            pairs = (e.partition(b"=") for e in f.read().split(b"\0") if e)
            return {k.decode(errors="replace"): v.decode(errors="replace") for k, _, v in pairs}
    except OSError:
        return {}


def busy(procs):
    """Why a test run may be active, or None: a pytest process, or a held test-runner lease."""
    runs = sorted(pid for pid, i in procs.items() if is_pytest(i["argv"]))
    if runs:
        return f"a pytest run is in progress (pid {', '.join(map(str, runs))})"
    try:
        leases = admin.dump()["leases"]
    except Refused as e:
        return f"the test-runner lease cannot be checked ({e})"
    held = [x for x in leases if x["resource"].rsplit("/", 1)[-1] == TEST_LOCK]
    return f"{held[0]['holder']} holds the {held[0]['resource']} lease" if held else None


def containers():
    """Test broker containers left behind, running or not; never the live broker or ledgerd."""
    if not shutil.which("docker"):
        return []
    try:
        out = subprocess.run(
            ["docker", "ps", "-a", "--filter", "name=agent-bridge-test-", "--format", "{{json .}}"],
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout
    except (OSError, subprocess.TimeoutExpired):
        return []
    rows = []
    for line in out.splitlines():
        try:
            c = json.loads(line)
        except ValueError:
            continue
        name = c.get("Names", "")
        if TEST_CONTAINER.fullmatch(name) and name not in KEEP:
            port = re.search(r":(\d+)->1883/", c.get("Ports", ""))
            row = {"name": name, "status": c.get("Status", ""), "created": c.get("RunningFor", "")}
            rows.append({**row, "port": int(port[1]) if port else None})
    return rows


def _listening(port):
    """True if something accepts connections on 127.0.0.1:port right now."""
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.3):
            return True
    except OSError:
        return False


def orphans(procs, regs, test_ports=None):
    """Processes of ours that outlived what started them, each with the reason it counts as orphaned. A test-like
    process (temp history log) whose broker is up and is not a test broker belongs to a stack someone runs on purpose,
    so it is left out; one whose broker is gone, or is a leftover test broker, is listed."""
    mine, tmp, uid, names = processes.lineage(), tempfile.gettempdir() + os.sep, os.getuid(), processes.session_names()
    if test_ports is None:
        test_ports = frozenset(c["port"] for c in containers() if c["port"])
    out = []
    for pid, i in procs.items():
        # A console, this one or another, is never a leftover: it is what the admin is using.
        if i["uid"] != uid or pid in mine or i["comm"] in names or not ours(i["argv"]) or is_console(i["argv"]):
            continue
        parent = procs.get(i["ppid"])
        if i["ppid"] != 1 and not (parent and parent["comm"] == "systemd"):
            continue
        env = _environ(pid)
        session = next((s for h in host.installed() if (s := h.session_of_env(env))), "").lower()
        reg = regs.get(session)
        if reg and reg["pid"] and not processes.verified(reg["pid"], reg["pid_start"]):
            kind, why = "session", f"its session {session} ended (claude pid {reg['pid']} is gone)"
        elif not session and env.get("AGENT_BRIDGE_LOG", "").startswith(tmp):
            port = env.get("AGENT_BRIDGE_PORT", "")
            if port.isdigit() and int(port) not in test_ports and _listening(int(port)):
                continue
            kind, why = "test", "a test's process that outlived its run: its history log is in a temp directory"
        else:
            continue
        out.append(
            {
                "pid": pid,
                "start": i["start"],
                "cmd": " ".join(i["argv"])[:200],
                "rss": i["rss"],
                "kind": kind,
                "reason": why,
            }
        )
    return sorted(out, key=lambda o: o["pid"])


def report():
    procs, boxes = processes.table(), containers()
    ports = frozenset(c["port"] for c in boxes if c["port"])
    return {"busy": busy(procs), "containers": boxes, "processes": orphans(procs, sessions.read_registry(), ports)}


def remove_container(name):
    if not isinstance(name, str) or not TEST_CONTAINER.fullmatch(name) or name in KEEP:
        raise Refused("only agent-bridge-test-<6 hex> containers can be removed here")
    if why := busy(processes.table()):
        raise Refused(f"not now: {why}")
    if name not in {c["name"] for c in containers()}:
        raise Refused(f"no such test container: {name}")
    r = subprocess.run(["docker", "rm", "-f", name], capture_output=True, text=True, timeout=30)
    if r.returncode:
        raise Refused(f"docker rm failed: {' '.join(r.stderr.split())[:200]}")
    return f"removed container {name}"


def stop_process(pid, start):
    """SIGTERM an orphan, after classifying it again and pinning it with a pidfd."""
    procs = processes.table()
    found = next((o for o in orphans(procs, sessions.read_registry()) if (o["pid"], o["start"]) == (pid, start)), None)
    if not found:
        raise Refused(f"process {pid} is not an orphaned agent-bridge process")
    if found["kind"] == "test" and (why := busy(procs)):
        raise Refused(f"not now: {why}")

    def same(p):
        i = processes.info(p)
        return i and i["start"] == start and i["uid"] == os.getuid() and ours(i["argv"])

    processes.pidfd_signal(pid, same, signal.SIGTERM, "that orphaned agent-bridge process")
    return f"sent SIGTERM to orphaned pid {pid} ({found['kind']})"
