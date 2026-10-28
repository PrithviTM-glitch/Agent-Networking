"""Claude Code as an agent_bridge.host.Host."""

import json
import os
import shutil

from ..host import Host, stat
from ..validation import UUID
from . import inbox, registry
from .transcripts import Transcript

SESSION_ENV = "CLAUDE_CODE_SESSION_ID"
PROJECT_ENV = "CLAUDE_PROJECT_DIR"
ANCESTRY_MAX = 64
# Claude Desktop runs its bundled claude from here; a CLI install lives elsewhere.
DESKTOP_DIR = os.path.join(os.environ.get("XDG_CONFIG_HOME") or os.path.expanduser("~/.config"), "Claude") + os.sep


class Claude(Host):
    name = "claude-code"
    process_name = "claude"
    usage_metrics = {
        "cost": ("claude_code_cost_usage_USD_total", False),
        "tokens": ("claude_code_token_usage_tokens_total", True),
        "active": ("claude_code_active_time_seconds_total", True),
        "lines": ("claude_code_lines_of_code_count_total", True),
        "commits": ("claude_code_commit_count_total", False),
        "prs": ("claude_code_pull_request_count_total", False),
    }

    def session(self, env=None):
        sid = (os.environ if env is None else env).get(SESSION_ENV, "")
        return sid if isinstance(sid, str) and UUID.fullmatch(sid) else None

    def session_key(self, env=None):
        return (os.environ if env is None else env).get(SESSION_ENV) or None

    def process(self, start=None):
        """The nearest ancestor named claude: the session's own process, whatever shells and tools sit between."""
        pid = start or os.getppid()
        for _ in range(ANCESTRY_MAX):
            if pid < 1 or not (p := stat(pid)):
                return None
            if p["comm"] == self.process_name:
                return pid, p["start"]
            pid = p["ppid"]
        return None

    def project_dir(self):
        return os.environ.get(PROJECT_ENV) or None

    def can_deliver(self):
        return inbox.available()

    def deliver(self, text):
        inbox.post(text)

    def sessions(self):
        return registry.read(self.alive)

    def transcript(self, session):
        return Transcript(session)

    def kind(self, entrypoint=None, argv0=None):
        if entrypoint:
            return "desktop" if entrypoint == "claude-desktop" else "cli"
        if argv0:
            return "desktop" if argv0.startswith(DESKTOP_DIR) else "cli"
        return None

    def executable(self):
        return shutil.which("claude")

    def resume_argv(self, session, exe):
        return [exe, "--resume", session]

    def prompt_argv(self, session, exe):
        return [exe, "-p", "--resume", session, "--output-format", "stream-json", "--verbose"]

    def output_lines(self, raw):
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


CLAUDE = Claude()
