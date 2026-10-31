"""Shared fixtures: a disposable mosquitto in Docker on a free port, a state directory per module, and ledgerd.

One test run at a time per user: a run waits for the lock of any other run on this machine. Test containers carry
the user's uid as a label, and a run's temp directory its pid; every run removes both on exit, and the next run also
removes any a killed run left.
"""

import atexit
import fcntl
import os
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
CONF = ROOT / "config" / "mosquitto.conf"
IMAGE = "eclipse-mosquitto:2.0.22"
# The commands under test: by default the ones installed next to this Python (the wheel puts the native binaries and
# the shims in the same bin directory as its Python entry points). The overrides point the suite at other builds.
BIN = Path(os.environ.get("AGENT_BRIDGE_BIN") or Path(sys.executable).parent)
LEDGERD = shlex.split(os.environ.get("AGENT_BRIDGE_LEDGERD", "")) or [str(BIN / "agent-bridge-ledgerd")]
HISTORYD = shlex.split(os.environ.get("AGENT_BRIDGE_HISTORYD", "")) or [str(BIN / "agent-bridge-historyd")]


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for(pred, timeout=10, interval=0.05):
    """pred()'s first truthy value within timeout seconds, else None."""
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if value := pred():
            return value
        time.sleep(interval)
    return None


LABEL = f"agent-bridge-test.uid={os.getuid()}"


def _containers():
    found = subprocess.run(["docker", "ps", "-aq", "--filter", f"label={LABEL}"], capture_output=True, text=True)
    return found.stdout.split()


def _remove_containers():
    if shutil.which("docker") and (ids := _containers()):
        subprocess.run(["docker", "rm", "-f", *ids], capture_output=True)


TMP_PREFIX = "agent-bridge-test-"


def _alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:  # another user's process
        return True
    return True


def _sweep_stale_dirs(parent=None):
    """Remove this user's run directories whose run is gone: SIGKILL skips atexit, so the next run cleans up."""
    for entry in os.scandir(parent or tempfile.gettempdir()):
        m = re.fullmatch(rf"{TMP_PREFIX}(\d+)-\w+", entry.name)
        if (
            m
            and entry.is_dir(follow_symlinks=False)
            and entry.stat(follow_symlinks=False).st_uid == os.getuid()
            and not _alive(int(m[1]))
        ):
            shutil.rmtree(entry.path, ignore_errors=True)


def _one_run_at_a_time(config):
    """Hold this user's test lock for the life of the process, waiting while another run holds it."""
    path = os.path.join(
        os.environ.get("XDG_RUNTIME_DIR") or tempfile.gettempdir(), f"agent-bridge-tests-{os.getuid()}.lock"
    )
    lock = open(path, "a+")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock.seek(0)
        print(
            f"waiting for another agent-bridge test run (pid {lock.read().strip() or '?'}) to finish", file=sys.stderr
        )
        fcntl.flock(lock, fcntl.LOCK_EX)
    lock.truncate(0)
    lock.write(str(os.getpid()))
    lock.flush()
    config.agent_bridge_lock = lock


def _interrupted(signum, _frame):
    raise KeyboardInterrupt(f"signal {signum}")


def pytest_configure(config):
    _one_run_at_a_time(config)
    # Holding the lock, any container with our label, or run directory of a dead pid, is left over from a killed run.
    _remove_containers()
    _sweep_stale_dirs()
    atexit.register(_remove_containers)
    # pytest tears fixtures down on KeyboardInterrupt but not on these, so they become one.
    for signum in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, _interrupted)
    # agent_bridge reads its settings at import, so they are fixed here, before any test module imports it.
    tmp = tempfile.mkdtemp(prefix=f"{TMP_PREFIX}{os.getpid()}-")
    atexit.register(shutil.rmtree, tmp, ignore_errors=True)
    for name in (
        "CLAUDE_CODE_SESSION_ID",
        "CLAUDE_PROJECT_DIR",
        "CLAUDE_CODE_MESSAGING_SOCKET",
        "CLAUDE_CODE_MESSAGING_TOKEN",
        "AGENT_BRIDGE_STALE_DAYS",
        "LEDGER_DB",
        "HISTORY_DB",
    ):
        os.environ.pop(name, None)
    os.environ.update(
        AGENT_BRIDGE_HOST="127.0.0.1",
        AGENT_BRIDGE_PORT=str(free_port()),
        AGENT_BRIDGE_LOG=os.path.join(tmp, "history.jsonl"),
        XDG_STATE_HOME=os.path.join(tmp, "state"),
    )


# bridge-core's checklist of bus behaviours, by name, and the tests that pin each; the summary reports them, so the
# cutover gate shows which behaviour an implementation breaks rather than only which test failed.
BEHAVIOURS = {
    name: [*tests, f"tests/test_conformance.py::{conformance}"]
    for name, tests, conformance in (
        (
            "write-once origin",
            ["tests/test_bus.py::test_subagents_and_write_once_origin"],
            "test_origin_is_written_once",
        ),
        (
            "subagent lineage",
            ["tests/test_bus.py::test_subagents_and_write_once_origin", "tests/test_relay.py::test_adopts_cli_agent"],
            "test_subagent_keeps_its_lineage",
        ),
        (
            "whole-record refresh before republishing",
            ["tests/test_bus.py::test_presence_cache"],
            "test_a_join_keeps_what_another_process_changed",
        ),
        (
            "caches reset on reconnect",
            ["tests/test_bus.py::test_presence_cache", "tests/test_bus.py::test_broker_restart"],
            "test_presence_cache_resets_on_reconnect",
        ),
        (
            "for_me and the quiet backlog",
            [
                "tests/test_mentions.py::test_names_addresses_and_all_resolve",
                "tests/test_relay.py::test_mentions_wake_only_the_mentioned",
            ],
            "test_recv_marks_mentions_for_me",
        ),
        (
            "relay lock line 2 and held()",
            ["tests/test_relay.py::test_mentions_wake_only_the_mentioned"],
            "test_relay_lock_names_the_relayed_agent",
        ),
        (
            "moderation at recv time",
            [
                "tests/test_rooms.py::test_mute_stops_posts_and_spoofed_ones",
                "tests/test_mentions.py::test_moderation_still_applies",
            ],
            "test_moderation_applies_at_recv",
        ),
    )
}


def pytest_terminal_summary(terminalreporter):
    outcome = {}
    for status in ("passed", "failed", "error", "skipped", "xfailed", "xpassed"):
        for report in terminalreporter.stats.get(status, []):
            if getattr(report, "nodeid", None) and outcome.get(report.nodeid) not in ("failed", "error"):
                outcome[report.nodeid] = status
    rows = []
    for name, tests in BEHAVIOURS.items():
        seen = [outcome[t] for t in tests if t in outcome]
        if seen:
            bad = [t for t in tests if outcome.get(t) in ("failed", "error")]
            rows.append(f"{'FAILED' if bad else 'ok':6} {name}" + (f"  ({', '.join(bad)})" if bad else ""))
    if rows:
        terminalreporter.section("bus behaviours")
        for row in rows:
            terminalreporter.write_line(row)


class Broker:
    """The test session's mosquitto container on AGENT_BRIDGE_PORT; each test module gets a fresh one."""

    def __init__(self):
        self.name = f"agent-bridge-test-{uuid.uuid4().hex[:6]}"
        self.port = int(os.environ["AGENT_BRIDGE_PORT"])

    def start(self, conf=CONF):
        self.remove()
        subprocess.run(
            ["docker", "run", "-d", "--name", self.name, "--label", LABEL, "-p", f"127.0.0.1:{self.port}:1883",
             "-v", f"{conf}:/mosquitto/config/mosquitto.conf:ro", IMAGE],
            check=True,
            capture_output=True,
        )  # fmt: skip
        assert wait_for(self._accepts, 15), "the test broker never came up"

    def _accepts(self):
        from agent_bridge import _native
        from agent_bridge.errors import BridgeError

        try:
            _native.presence_agents()
        except BridgeError:
            return False
        return True

    def remove(self):
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)


@pytest.fixture(scope="session")
def broker():
    if not shutil.which("docker"):
        pytest.skip("docker is not installed")
    b = Broker()
    yield b
    b.remove()


@pytest.fixture(scope="module", autouse=True)
def fresh_broker(broker, request):
    """An empty broker for every module, as each module's scenario assumes; BROKER_CONF overrides the config."""
    broker.start(getattr(request.module, "BROKER_CONF", CONF))
    return broker


@pytest.fixture(scope="module", autouse=True)
def state_dir(request):
    """A fresh history log directory per module, for this process and the commands it starts."""
    from agent_bridge import config

    path = tempfile.mkdtemp(prefix=f"{request.module.__name__}-")
    log = in_temp(os.path.join(path, "history.jsonl"), "AGENT_BRIDGE_LOG")
    old = os.environ["AGENT_BRIDGE_LOG"], config.LOG
    os.environ["AGENT_BRIDGE_LOG"] = config.LOG = log
    yield path
    os.environ["AGENT_BRIDGE_LOG"], config.LOG = old
    shutil.rmtree(path, ignore_errors=True)


def in_temp(path, what):
    """path, which must lie in a temp directory, so no test ever opens the live state."""
    temp = os.path.realpath(tempfile.gettempdir())
    if os.path.commonpath([temp, os.path.realpath(path)]) != temp:
        raise AssertionError(f"{what} {path} is outside {temp}: tests never touch the live state directory")
    return path


def _service_env(var, db, extra):
    env = {**os.environ, var: db, **extra}
    in_temp(env[var], var)
    return env


def start_ledgerd(db, log, **env):
    """A host ledgerd on the test broker, answering requests before this returns."""
    from agent_bridge import _native
    from agent_bridge.errors import LedgerError

    with open(log, "a") as err:
        proc = subprocess.Popen(
            LEDGERD,
            stderr=err,
            env=_service_env("LEDGER_DB", db, env),
        )

    def answers():
        assert proc.poll() is None, open(log).read()
        try:  # dump needs no ledger, so the probe leaves no state behind
            return _native.ledger_call("dump", "probe-0", timeout=1)
        except LedgerError:
            return None

    assert wait_for(answers, 30), "ledgerd never answered"
    return proc


def start_historyd(db, log, **env):
    """A host historyd on the test broker, answering requests before this returns."""
    from agent_bridge import _native
    from agent_bridge.errors import HistoryError

    with open(log, "a") as err:
        proc = subprocess.Popen(HISTORYD, stderr=err, env=_service_env("HISTORY_DB", db, env))

    def answers():
        assert proc.poll() is None, open(log).read()
        try:
            return _native.history_call("ping", timeout=1)
        except HistoryError:
            return None

    assert wait_for(answers, 30), "historyd never answered"
    return proc


@pytest.fixture(scope="module")
def historyd(fresh_broker, state_dir):
    log = os.path.join(state_dir, "historyd.log")
    proc = start_historyd(os.path.join(state_dir, "history.db"), log)
    yield proc
    proc.kill()
    proc.wait()


@pytest.fixture(scope="module")
def ledgerd(fresh_broker, state_dir):
    log = os.path.join(state_dir, "ledgerd.log")
    proc = start_ledgerd(os.path.join(state_dir, "ledger.db"), log)
    yield proc
    proc.kill()
    proc.wait()
