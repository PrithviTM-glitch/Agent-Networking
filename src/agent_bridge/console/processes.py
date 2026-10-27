"""Claude processes: identity checks against /proc, signals, and restarts of dead sessions."""

import json
import os
import shutil
import signal
import subprocess
import threading
import time
import uuid

from . import events

SIGNALS = {"interrupt": signal.SIGINT, "terminate": signal.SIGTERM, "kill": signal.SIGKILL}
JOB_LINES = 500
lock = threading.Lock()
jobs = {}


class Refused(Exception):
    """An admin action that is not allowed; the message is shown to the admin."""


def proc(pid):
    """(comm, start time in ticks, uid, argv[0]) of a process, or None once it is gone or a zombie."""
    try:
        with open(f"/proc/{pid}/stat", "rb") as f:
            stat = f.read()
        uid = os.stat(f"/proc/{pid}").st_uid
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            argv0 = f.read().split(b"\0")[0].decode(errors="replace")
        head, _, rest = stat.rpartition(b")")
        if rest.split()[0] in (b"Z", b"X"):
            return None
        return head.partition(b"(")[2].decode(errors="replace"), int(rest.split()[19]), uid, argv0
    except (OSError, ValueError, IndexError):
        return None


def verified(pid, start):
    """The process at pid if it is still the claude process that started at start, else None."""
    p = pid and start and proc(pid)
    return p if p and p[0] == "claude" and p[1] == start and p[2] == os.getuid() else None


def send_signal(pid, start, name):
    if name not in SIGNALS:
        raise Refused(f"signal must be one of {', '.join(SIGNALS)}")
    try:
        fd = os.pidfd_open(pid)
    except OSError:
        raise Refused(f"process {pid} is gone") from None
    try:
        # Checked after pidfd_open: if the pid was reused since, the check fails; if it passes, the fd is that process.
        if not verified(pid, start):
            raise Refused(f"process {pid} is no longer that claude session, nothing was signalled")
        signal.pidfd_send_signal(fd, SIGNALS[name])
    finally:
        os.close(fd)
    return f"sent SIG{SIGNALS[name].name[3:]} to pid {pid}"


def claude():
    if not (path := shutil.which("claude")):
        raise Refused("claude is not on the console's PATH")
    return path


def open_terminal(session, cwd):
    if not (term := shutil.which("gnome-terminal")):
        raise Refused("gnome-terminal is not installed")
    subprocess.Popen(
        [term, f"--working-directory={cwd}", "--", claude(), "--resume", session],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    return f"opened a terminal resuming {session}"


def resume(key, session, cwd, text):
    """Run one prompt in the dead session with claude -p, streaming its reply to the console as job events."""
    exe = claude()
    with lock:
        if any(j["session"] == session and j["state"] == "running" for j in jobs.values()):
            raise Refused("a resume is already running for this session")
        jid = uuid.uuid4().hex[:12]
        job = jobs[jid] = {
            "id": jid,
            "key": key,
            "session": session,
            "started": time.time(),
            "state": "running",
            "lines": [],
        }
    try:
        # The prompt goes on stdin, so text starting with "-" can never be read as a flag.
        p = subprocess.Popen(
            [exe, "-p", "--resume", session, "--output-format", "stream-json", "--verbose"],
            cwd=cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except OSError as e:
        job["state"] = "failed to start"
        raise Refused(f"could not start claude: {e}") from None
    threading.Thread(target=_run, args=(job, p, text), daemon=True).start()
    return f"resuming {session} as job {jid}"


def _lines(raw):
    """What a stream-json line shows in the console: text, tool names, the result."""
    try:
        d = json.loads(raw)
    except ValueError:
        return [raw]
    if not isinstance(d, dict):
        return [raw]
    if (
        d.get("type") == "assistant"
        and isinstance(d.get("message"), dict)
        and isinstance(d["message"].get("content"), list)
    ):
        return [
            b["text"] if b.get("type") == "text" else f"[tool] {b.get('name')}"
            for b in d["message"]["content"]
            if isinstance(b, dict) and b.get("type") in ("text", "tool_use")
        ]
    if d.get("type") == "result":
        return [f"[result] {d.get('result') if isinstance(d.get('result'), str) else d.get('subtype')}"]
    if d.get("type") == "system" and d.get("subtype") == "init":
        return ["[session started]"]
    return []


def _run(job, p, text):
    try:
        p.stdin.write(text.encode())
        p.stdin.close()
    except OSError:
        pass
    for raw in p.stdout:
        for line in _lines(raw.decode(errors="replace").rstrip("\n")):
            line = line[:4000]
            with lock:
                job["lines"] = (job["lines"] + [line])[-JOB_LINES:]
            events.push({"kind": "job", "id": job["id"], "key": job["key"], "line": line})
    job["state"] = f"exit {p.wait()}"
    events.push({"kind": "job", "id": job["id"], "key": job["key"], "state": job["state"]})


def job_list():
    with lock:
        return [{**j, "lines": j["lines"][-200:]} for j in jobs.values()]
