"""Claude processes: identity checks against /proc, signals, and restarts of dead sessions."""

import os
import shutil
import signal
import subprocess
import threading
import time
import uuid

from .. import host
from . import events

SIGNALS = {"interrupt": signal.SIGINT, "terminate": signal.SIGTERM, "kill": signal.SIGKILL}
PAGE = os.sysconf("SC_PAGE_SIZE")
JOB_LINES = 500
lock = threading.Lock()
jobs = {}


class Refused(Exception):
    """An admin action that is not allowed; the message is shown to the admin."""


def info(pid):
    """comm, ppid, start time and CPU time in clock ticks, resident memory in bytes, uid and argv of a process, or
    None once it is gone or a zombie."""
    try:
        with open(f"/proc/{pid}/stat", "rb") as f:
            stat = f.read()
        uid = os.stat(f"/proc/{pid}").st_uid
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            argv = [a.decode(errors="replace") for a in f.read().split(b"\0")[:-1]]
        head, _, rest = stat.rpartition(b")")
        fields = rest.split()
        if fields[0] in (b"Z", b"X"):
            return None
        comm = head.partition(b"(")[2].decode(errors="replace")
        return {
            "comm": comm,
            "ppid": int(fields[1]),
            "start": int(fields[19]),
            "cpu": int(fields[11]) + int(fields[12]),
            "rss": int(fields[21]) * PAGE,
            "uid": uid,
            "argv": argv,
        }
    except (OSError, ValueError, IndexError):
        return None


def proc(pid):
    """(comm, start time in ticks, uid, argv[0]) of a process, or None once it is gone or a zombie."""
    i = info(pid)
    return i and (i["comm"], i["start"], i["uid"], i["argv"][0] if i["argv"] else "")


def session_names():
    """The process names of every installed framework's sessions (claude for Claude Code)."""
    return {h.process_name for h in host.installed() if h.process_name}


def verified(pid, start):
    """The process at pid if it is still the session process that started at start, else None."""
    p = pid and start and proc(pid)
    return p if p and p[0] in session_names() and p[1] == start and p[2] == os.getuid() else None


def table():
    """pid -> info of every process this user can read."""
    out = {}
    for name in os.listdir("/proc"):
        if name.isdigit() and (i := info(int(name))):
            out[int(name)] = i
    return out


def descendants(root, procs):
    """pids below root in procs, nearest first."""
    children = {}
    for pid, i in procs.items():
        children.setdefault(i["ppid"], []).append(pid)
    out, todo = [], list(children.get(root, ()))
    while todo:
        pid = todo.pop(0)
        out.append(pid)
        todo.extend(children.get(pid, ()))
    return out


def lineage(pid=None):
    """pid (default: this process) and all its ancestors."""
    out, pid = set(), pid or os.getpid()
    while pid > 1 and pid not in out and (i := info(pid)):
        out.add(pid)
        pid = i["ppid"]
    return out


def pidfd_signal(pid, still_same, sig, what):
    """Signal pid only if still_same(pid) holds after a pidfd pins it, so a reused pid is never signalled."""
    try:
        fd = os.pidfd_open(pid)
    except OSError:
        raise Refused(f"process {pid} is gone") from None
    try:
        # Checked after pidfd_open: if the pid was reused since, the check fails; if it passes, the fd is that process.
        if not still_same(pid):
            raise Refused(f"process {pid} is no longer {what}, nothing was signalled")
        signal.pidfd_send_signal(fd, sig)
    finally:
        os.close(fd)


def send_signal(pid, start, name):
    if name not in SIGNALS:
        raise Refused(f"signal must be one of {', '.join(SIGNALS)}")
    pidfd_signal(pid, lambda p: verified(p, start), SIGNALS[name], "that claude session")
    return f"sent SIG{SIGNALS[name].name[3:]} to pid {pid}"


def executable(h):
    if not (path := h.executable()):
        raise Refused(f"{h.process_name or h.name} is not on the console's PATH")
    return path


def open_terminal(h, session, cwd):
    """Reopen a dead session of framework h in a terminal."""
    if not (term := shutil.which("gnome-terminal")):
        raise Refused("gnome-terminal is not installed")
    if not (argv := h.resume_argv(session, executable(h))):
        raise Refused(f"{h.name} sessions cannot be resumed")
    subprocess.Popen(
        [term, f"--working-directory={cwd}", "--", *argv],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    return f"opened a terminal resuming {session}"


def resume(h, key, session, cwd, text):
    """Run one prompt in the dead session of framework h, streaming its reply to the console as job events."""
    if not (argv := h.prompt_argv(session, executable(h))):
        raise Refused(f"{h.name} sessions cannot be resumed with a message")
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
            argv,
            cwd=cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except OSError as e:
        job["state"] = "failed to start"
        raise Refused(f"could not start {h.process_name or h.name}: {e}") from None
    threading.Thread(target=_run, args=(job, p, text, h.output_lines), daemon=True).start()
    return f"resuming {session} as job {jid}"


def _run(job, p, text, lines):
    try:
        p.stdin.write(text.encode())
        p.stdin.close()
    except OSError:
        pass
    for raw in p.stdout:
        for line in lines(raw.decode(errors="replace").rstrip("\n")):
            line = line[:4000]
            with lock:
                job["lines"] = (job["lines"] + [line])[-JOB_LINES:]
            events.push({"kind": "job", "id": job["id"], "key": job["key"], "line": line})
    job["state"] = f"exit {p.wait()}"
    events.push({"kind": "job", "id": job["id"], "key": job["key"], "state": job["state"]})


def job_list():
    with lock:
        return [{**j, "lines": j["lines"][-200:]} for j in jobs.values()]
