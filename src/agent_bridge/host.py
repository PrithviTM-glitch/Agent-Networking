"""The agent framework a process runs under, behind one small interface, so the bridge itself knows none of them.

A Host answers: which session this process belongs to, which process is the session (for liveness), how to put a
message into the session, and, machine-wide, which sessions exist and what they did. The base class is the generic
host: no session, no process, no inbox, so an agent there reads messages with recv. Each framework subclasses it in
its own package (agent_bridge.claude for Claude Code), and only that package knows the framework's files, variables
and formats. current() picks the framework this process runs under; installed() lists every one the machine may
have sessions of, for the admin console.

Only the standard library is imported here: the hooks and the console import this module at startup."""

import os

UID = os.getuid()


def stat(pid):
    """comm, state, ppid, start time in clock ticks and uid of a process, or None once it is gone."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            text = f.read()
        uid = os.stat(f"/proc/{pid}").st_uid
        # comm is parenthesised and may itself hold spaces or parens: only the last ")" ends it.
        fields = text[text.rfind(")") + 2 :].split()
        comm = text[text.find("(") + 1 : text.rfind(")")]
        return {"comm": comm, "state": fields[0], "ppid": int(fields[1]), "start": int(fields[19]), "uid": uid}
    except (OSError, ValueError, IndexError):
        return None


class Host:
    """No framework: nothing to deliver into, no sessions to list."""

    name = "generic"
    process_name = None  # comm of the framework's session process
    usage_metrics = {}  # console field -> (Prometheus metric, split by its type label)

    def session(self, env=None):
        """This process's session id (a UUID, as the framework gives it), or None."""
        return None

    def session_key(self, env=None):
        """The session identifier exactly as the framework gives it, unvalidated, or None. Only for deriving a
        stable address from it; presence and lock files use session()."""
        return None

    def process(self, start=None):
        """(pid, start ticks) of the session process above this one (above start, if given), or None."""
        return None

    def project_dir(self):
        """The session's project directory, when the framework says; agent names derive from it."""
        return None

    def detected(self):
        return self.session_key() is not None or self.process() is not None

    def alive(self, pid, start):
        """True while pid is still the session process that started at start, ours and not a zombie."""
        p = pid and start and self.process_name and stat(pid)
        ok = p and p["state"] not in ("Z", "X") and p["comm"] == self.process_name
        return bool(ok and p["start"] == start and p["uid"] == UID)

    def can_deliver(self):
        """True if deliver() can reach this process's session."""
        return False

    def deliver(self, text):
        """Put text into this process's session as a user message; OSError if it cannot."""
        raise OSError(f"{self.name} host: no inbox to deliver to, read messages with recv")

    def sessions(self):
        """session id -> {session, pid, pid_start, cwd, entrypoint, mode, status, version, name, name_source,
        desktop_id, started, updated, alive}, every value optional except session and alive."""
        return {}

    def transcript(self, session):
        """An object with refresh() and the attributes title, prompt, cwd, branch, entrypoint and recent (a list of
        {ts, role, text}), or None if the framework keeps none."""
        return None

    def kind(self, entrypoint=None, argv0=None):
        """How the session runs ("desktop" or "cli"), from what the framework reports or its process's argv[0]."""
        return None

    def session_of_env(self, env):
        """The session id in a process environment (a dict), for telling whose leftover a process is."""
        return self.session(env)

    def resume_argv(self, session, exe):
        """argv that reopens a dead session interactively, or None if the framework cannot."""
        return None

    def prompt_argv(self, session, exe):
        """argv that runs one prompt (given on stdin) in a dead session, streaming its output, or None."""
        return None

    def executable(self):
        """The framework's command on PATH, for the two above."""
        return None

    def output_lines(self, raw):
        """What one line of prompt_argv's output shows the admin: a list of strings."""
        return [raw]


GENERIC = Host()


def installed():
    """Every framework this bridge has an adapter for, whose sessions the admin console lists."""
    from .claude.host import CLAUDE

    return [CLAUDE]


def current():
    """The framework this process runs under, else the generic host. Not cached: tests and the relay change the
    environment it reads."""
    return next((h for h in installed() if h.detected()), GENERIC)


def by_process(comm):
    """The installed framework whose session processes are named comm, or None."""
    return next((h for h in installed() if h.process_name == comm), None)
