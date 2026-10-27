"""Every Claude session and bus agent, joined by session id: names, liveness, kind and activity.

Sources: Claude Code's session registry (CLAUDE_CONFIG_DIR or ~/.claude, sessions/<pid>.json), bus presence, the
session transcripts (projects/*/<session>.jsonl) and each working directory's .git/HEAD. The registry is internal
to Claude Code and may change between versions, so every field is optional and checked before use."""

import collections
import glob
import json
import mmap
import os
import re
import sys
import threading
import time

from .. import config
from ..protocol import ORIGIN
from ..relay import live as relay_live
from ..validation import ADMIN, UUID, check_ident, is_ident
from . import events
from .processes import Refused, verified

CLAUDE_DIR = os.environ.get("CLAUDE_CONFIG_DIR") or os.path.expanduser("~/.claude")
PROJECTS = os.path.join(CLAUDE_DIR, "projects")
REGISTRY = os.path.join(CLAUDE_DIR, "sessions")
DESKTOP_DIR = os.path.join(os.environ.get("XDG_CONFIG_HOME") or os.path.expanduser("~/.config"), "Claude") + os.sep
CHECK_S = 3
ACTIVITY = 30
HEAD_SCAN, TAIL_SCAN = 4 << 20, 4 << 20
REGISTRY_MAX = 64 * 1024
PID_MAX, TICKS_MAX = 4194304, 2**53
REGISTRY_FILE = re.compile(r"[0-9]{1,7}\.json")
TAGS = re.compile(r"<(system-reminder|command-[a-z-]+|local-command-[a-z-]+)>.*?</\1>", re.S)
CTRL = re.compile(r"[\x00-\x1f\x7f-\x9f]")

lock = threading.Lock()
presence = {}
transcripts = {}
kinds = {}
state = {"registry": None, "view": None}


def clean(text):
    return " ".join(CTRL.sub(" ", TAGS.sub(" ", text)).split())


# transcripts


def _texts(content):
    if isinstance(content, str):
        return [content]
    return [
        b["text"] for b in content if isinstance(b, dict) and b.get("type") == "text" and isinstance(b.get("text"), str)
    ]


class Transcript:
    """What the console shows from one session's transcript, read incrementally from where it left off."""

    def __init__(self, session):
        self.session, self.path, self.offset = session, None, 0
        self.title = self.prompt = self.cwd = self.branch = self.entrypoint = None
        self.recent = collections.deque(maxlen=ACTIVITY)

    def find(self):
        found = glob.glob(os.path.join(glob.escape(PROJECTS), "*", f"{self.session}.jsonl"))
        return max(found, key=os.path.getmtime) if found else None

    def refresh(self):
        if not self.path or not os.path.exists(self.path):
            self.__init__(self.session)
            if not (path := self.find()):
                return
            self.path = path
        try:
            with open(self.path, "rb") as f:
                size = os.fstat(f.fileno()).st_size
                if size < self.offset:
                    self.__init__(self.session)
                    return self.refresh()
                if self.offset == 0 and size:
                    self._first_read(f, size)
                f.seek(self.offset)
                data = f.read(size - self.offset)
        except OSError:
            return
        end = data.rfind(b"\n") + 1
        for line in data[:end].splitlines():
            self._feed(line)
        self.offset += end

    def _first_read(self, f, size):
        """Seed from the first prompt and the latest title, then let refresh read only the tail."""
        scanned = 0
        for line in f:
            scanned += len(line)
            if self.prompt or scanned > HEAD_SCAN:
                break
            if b'"type":"user"' in line:
                self._feed(line, activity=False)
        with mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ) as m:
            if (i := m.rfind(b'"customTitle":')) >= 0:
                end = m.find(b"\n", i)
                self._feed(m[m.rfind(b"\n", 0, i) + 1 : end if end >= 0 else size])
            if size > TAIL_SCAN:
                self.offset = m.find(b"\n", size - TAIL_SCAN) + 1 or size

    def _feed(self, line, activity=True):
        if b'"customTitle"' not in line and b'"type":"user"' not in line and b'"type":"assistant"' not in line:
            return
        try:
            d = json.loads(line)
        except ValueError:
            return
        if not isinstance(d, dict) or d.get("isSidechain"):
            return
        if d.get("type") == "custom-title" and isinstance(d.get("customTitle"), str):
            self.title = clean(d["customTitle"])[:120] or None
            return
        msg = d.get("message")
        if d.get("type") not in ("user", "assistant") or not isinstance(msg, dict):
            return
        for key, attr in (("cwd", "cwd"), ("gitBranch", "branch"), ("entrypoint", "entrypoint")):
            if isinstance(d.get(key), str):
                setattr(self, attr, d[key])
        content = msg.get("content")
        if not isinstance(content, (str, list)):
            return
        ts = d.get("timestamp") if isinstance(d.get("timestamp"), str) else None
        if d["type"] == "user" and not d.get("isMeta"):
            for t in map(clean, _texts(content)):
                if t and not t.startswith("[Request interrupted"):
                    self.prompt = self.prompt or t[:500]
                    activity and self.recent.append({"ts": ts, "role": "user", "text": t[:400]})
        elif d["type"] == "assistant" and activity:
            for b in content if isinstance(content, list) else []:
                if (
                    isinstance(b, dict)
                    and b.get("type") == "text"
                    and isinstance(b.get("text"), str)
                    and b["text"].strip()
                ):
                    self.recent.append({"ts": ts, "role": "assistant", "text": b["text"].strip()[:400]})
                elif isinstance(b, dict) and b.get("type") == "tool_use" and isinstance(b.get("name"), str):
                    self.recent.append({"ts": ts, "role": "tool", "text": b["name"][:80]})


def transcript(session):
    with lock:
        return transcripts.setdefault(session, Transcript(session))


def activity(session):
    t = transcripts.get(session)
    return list(t.recent) if t else []


# registry


def _count(v, cap):
    if isinstance(v, str) and v.isdigit() and len(v) < 20:
        v = int(v)
    return v if isinstance(v, int) and not isinstance(v, bool) and 0 < v <= cap else None


def _text(v, cap=200):
    return clean(v)[:cap] or None if isinstance(v, str) else None


def _ms(v):
    return v / 1000 if isinstance(v, (int, float)) and not isinstance(v, bool) and 0 < v < 1e13 else None


def _registry_entry(path):
    try:
        if os.path.getsize(path) > REGISTRY_MAX:
            return None
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
    except (OSError, ValueError, RecursionError):
        return None
    if not isinstance(d, dict) or not isinstance(d.get("sessionId"), str) or not UUID.fullmatch(d["sessionId"]):
        return None
    cwd = d.get("cwd")
    cwd = cwd if isinstance(cwd, str) and os.path.isabs(cwd) and len(cwd) < 4096 and not CTRL.search(cwd) else None
    pid, start = _count(d.get("pid"), PID_MAX), _count(d.get("procStart"), TICKS_MAX)
    return {
        "session": d["sessionId"].lower(),
        "pid": pid if start else None,
        "pid_start": start if pid else None,
        "cwd": cwd,
        "entrypoint": _text(d.get("entrypoint")),
        "mode": _text(d.get("kind")),
        "status": _text(d.get("status")),
        "version": _text(d.get("version")),
        "name": _text(d.get("name")),
        "name_source": _text(d.get("nameSource")),
        "desktop_id": _text(d.get("hostSessionId")),
        "started": _ms(d.get("startedAt")),
        "updated": _ms(d.get("updatedAt")),
    }


def read_registry():
    """session id -> registry entry; of several files for one session, the live one, else the newest."""
    out = {}
    try:
        names = os.listdir(REGISTRY)
    except OSError:
        names = []
    for name in names:
        # Only <pid>.json: the <pid>.<hash>.key files next to them are never opened.
        if not REGISTRY_FILE.fullmatch(name) or not (e := _registry_entry(os.path.join(REGISTRY, name))):
            continue
        e["alive"] = bool(e["pid"] and verified(e["pid"], e["pid_start"]))
        old = out.get(e["session"])
        if not old or (e["alive"], e["started"] or 0) > (old["alive"], old["started"] or 0):
            out[e["session"]] = e
    state["registry"] = out
    return out


def registry():
    return state["registry"] if state["registry"] is not None else read_registry()


# other per-session facts


def git_branch(cwd):
    """The branch checked out in cwd's repository from .git/HEAD, or None (no repo, detached HEAD)."""
    d = cwd
    try:
        for _ in range(64):
            git = os.path.join(d, ".git")
            if os.path.isfile(git):
                with open(git) as f:
                    first = f.readline(4096).strip()
                if not first.startswith("gitdir: "):
                    return None
                git = os.path.join(d, first[8:])
            if os.path.isdir(git):
                with open(os.path.join(git, "HEAD")) as f:
                    head = f.readline(4096).strip()
                return clean(head[16:])[:200] or None if head.startswith("ref: refs/heads/") else None
            if (parent := os.path.dirname(d)) == d:
                return None
            d = parent
    except OSError:
        return None
    return None


# presence


def set_presence(addr, rec):
    if rec and addr == ADMIN:
        # The console is no claude session, even when started from one's shell: never offer to signal that.
        rec = {k: v for k, v in rec.items() if k not in ORIGIN}
    with lock:
        if rec and is_ident(addr):
            presence[addr] = rec
        else:
            presence.pop(addr, None)


def clear_presence():
    with lock:
        presence.clear()


def live_presence():
    cutoff = time.time() - config.STALE_S
    with lock:
        return {a: r for a, r in presence.items() if r["keep"] or r["seen"] >= cutoff}


# views


def view(addr, rec, reg):
    """One session or agent as the page shows it. key is the address when on the bus, else the session id."""
    session = (reg and reg["session"]) or (rec and rec.get("session"))
    t = transcripts.get(session) if session else None
    pid, start = (
        (reg["pid"], reg["pid_start"])
        if reg and reg["pid"]
        else (rec.get("pid"), rec.get("pid_start"))
        if rec
        else (None, None)
    )
    p = verified(pid, start) if pid else None
    key = addr or session
    if p:
        kinds[key] = "desktop" if p[3].startswith(DESKTOP_DIR) else "cli"
    entry = (reg and reg["entrypoint"]) or (t and t.entrypoint)
    kind = ("desktop" if entry == "claude-desktop" else "cli") if entry else kinds.get(key)
    cwd = (reg and reg["cwd"]) or (t and t.cwd)
    branch = (cwd and git_branch(cwd)) or (t and t.branch if t and t.branch != "HEAD" else None)
    named = reg and reg["name_source"] != "derived" and reg["name"]
    task = named or (t and (t.title or (t.prompt and " ".join(t.prompt.split()[:6])[:48])))
    short = branch and branch.rsplit("/", 1)[-1][:24]
    fallback = addr or (reg and reg["name"]) or f"session {session[:8]}"
    seen = rec["seen"] if rec else (reg["updated"] or reg["started"] or 0)
    return {
        "key": key,
        "addr": addr,
        "session": session,
        "on_bus": rec is not None,
        "friendly": " · ".join(x for x in (short, task) if x) or fallback,
        "name": rec and rec["name"],
        "caps": rec["caps"] if rec else [],
        "keep": bool(rec and rec["keep"]),
        "rooms": [x[5:] for x in rec["topics"] if x.startswith("room/")] if rec else [],
        "topics": [x for x in rec["topics"] if not x.startswith("room/")] if rec else [],
        "seen": seen,
        "expires": None if not rec or rec["keep"] else rec["seen"] + config.STALE_S,
        "pid": pid,
        "pid_start": start,
        "alive": (p is not None) if pid else None,
        "kind": kind,
        "cwd": cwd,
        "branch": branch,
        "title": named or (t and t.title),
        "prompt": t and t.prompt and t.prompt[:300],
        "status": reg and reg["status"],
        "mode": reg and reg["mode"],
        "version": reg and reg["version"],
        "desktop_id": reg and reg["desktop_id"],
        "started": reg and reg["started"],
        "in_registry": reg is not None,
        "relay": relay_live(session) if session else None,
    }


def entities():
    pres, regs = live_presence(), registry()
    out, joined = [], set()
    for addr, rec in pres.items():
        reg = regs.get(rec["session"].lower()) if rec.get("session") else None
        joined.add(reg and reg["session"])
        out.append(view(addr, rec, reg))
    cutoff = time.time() - config.STALE_S
    for session, reg in regs.items():
        if session not in joined and (reg["alive"] or (reg["updated"] or reg["started"] or 0) >= cutoff):
            out.append(view(None, None, reg))
    return sorted(out, key=lambda v: -v["seen"])


def find(body):
    """The entity a request names, by {"addr": ...} or {"session": ...}."""
    if "session" in body:
        s = body["session"]
        if not isinstance(s, str) or not UUID.fullmatch(s):
            raise Refused("session must be a session id")
        found = next((v for v in entities() if v["session"] == s.lower()), None)
    else:
        addr = check_ident(body.get("addr"), "address")
        found = next((v for v in entities() if v["addr"] == addr), None)
    if not found:
        raise Refused("no such agent or session")
    return found


def push_agents():
    v = entities()
    with lock:
        same = state["view"] == (j := json.dumps(v))
        state["view"] = j
    if not same:
        events.push({"kind": "agents", "agents": v})


def watch():
    """Death detection, registry and transcript refresh every few seconds, so a dying session greys out live."""
    while True:
        try:
            read_registry()
            for v in entities():
                if v["session"]:
                    transcript(v["session"]).refresh()
            push_agents()
        except Exception as e:  # noqa: BLE001  the watcher must outlive any single bad record
            print(f"watch: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
        time.sleep(CHECK_S)
