"""Claude Code session transcripts: CLAUDE_CONFIG_DIR (or ~/.claude)/projects/*/<session>.jsonl, one JSON object per
line. Read incrementally and defensively: only the fields the console shows (title, first prompt, cwd, branch,
entrypoint, recent activity) are taken, each checked before use."""

import collections
import glob
import json
import math
import mmap
import os
import re
import time

CLAUDE_DIR = os.environ.get("CLAUDE_CONFIG_DIR") or os.path.expanduser("~/.claude")
PROJECTS = os.path.join(CLAUDE_DIR, "projects")
FIND_S = 30  # how often a session without a transcript yet is looked up again
ACTIVITY = 30
HEAD_SCAN, TAIL_SCAN = 4 << 20, 4 << 20
TAGS = re.compile(r"<(system-reminder|command-[a-z-]+|local-command-[a-z-]+)>.*?</\1>", re.S)
CTRL = re.compile(r"[\x00-\x1f\x7f-\x9f]")


def clean(text):
    return " ".join(CTRL.sub(" ", TAGS.sub(" ", text)).split())


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
        self.looked, self.seen = -math.inf, None
        self.title = self.prompt = self.cwd = self.branch = self.entrypoint = None
        self.recent = collections.deque(maxlen=ACTIVITY)

    def find(self):
        found = glob.glob(os.path.join(glob.escape(PROJECTS), "*", f"{self.session}.jsonl"))
        return max(found, key=os.path.getmtime) if found else None

    def refresh(self):
        if not self.path or not os.path.exists(self.path):
            if time.monotonic() - self.looked < FIND_S:
                return
            self.__init__(self.session)
            self.looked = time.monotonic()
            if not (path := self.find()):
                return
            self.path = path
        try:
            st = os.stat(self.path)
        except OSError:
            return
        if (st.st_size, st.st_mtime_ns) == self.seen:
            return
        self.seen = st.st_size, st.st_mtime_ns
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
