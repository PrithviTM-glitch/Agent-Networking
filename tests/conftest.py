"""Shared fixtures: a disposable mosquitto in Docker on a free port, a state directory per module, and ledgerd.

One test run at a time per user: a run waits for the lock of any other run on this machine. Test containers carry
the user's uid as a label; every run removes them on exit, and the next run also removes any a killed run left.
"""

import atexit
import fcntl
import os
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
# Overrides for running the suite against another implementation, such as the Rust binaries.
BIN = Path(os.environ.get("AGENT_BRIDGE_BIN") or Path(sys.executable).parent)
LEDGERD = shlex.split(os.environ.get("AGENT_BRIDGE_LEDGERD", "")) or [
    sys.executable,
    "-m",
    "agent_bridge.ledger.service",
]
HISTORYD = shlex.split(os.environ.get("AGENT_BRIDGE_HISTORYD", "")) or [
    sys.executable,
    "-m",
    "agent_bridge.history.service",
]


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
    # Holding the lock, any container with our label is left over from a run that was killed.
    _remove_containers()
    atexit.register(_remove_containers)
    # pytest tears fixtures down on KeyboardInterrupt but not on these, so they become one.
    for signum in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, _interrupted)
    # agent_bridge reads its settings at import, so they are fixed here, before any test module imports it.
    tmp = tempfile.mkdtemp(prefix="agent-bridge-test-")
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
    "write-once origin": ["tests/test_bus.py::test_subagents_and_write_once_origin"],
    "subagent lineage": [
        "tests/test_bus.py::test_subagents_and_write_once_origin",
        "tests/test_relay.py::test_adopts_cli_agent",
    ],
    "whole-record refresh before republishing": ["tests/test_bus.py::test_presence_cache"],
    "caches reset on reconnect": ["tests/test_bus.py::test_presence_cache", "tests/test_bus.py::test_broker_restart"],
    "for_me and the quiet backlog": [
        "tests/test_mentions.py::test_names_addresses_and_all_resolve",
        "tests/test_relay.py::test_mentions_wake_only_the_mentioned",
    ],
    "relay lock line 2 and held()": ["tests/test_relay.py::test_mentions_wake_only_the_mentioned"],
    "moderation at recv time": [
        "tests/test_rooms.py::test_mute_stops_posts_and_spoofed_ones",
        "tests/test_mentions.py::test_moderation_still_applies",
    ],
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
        from agent_bridge.ledger import client

        try:
            c = client.connect()
        except client.LedgerError:
            return False
        c.disconnect()
        c.loop_stop()
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
    from agent_bridge.ledger import client

    with open(log, "a") as err:
        proc = subprocess.Popen(
            LEDGERD,
            stderr=err,
            env=_service_env("LEDGER_DB", db, env),
        )

    def answers():
        assert proc.poll() is None, open(log).read()
        try:
            return client.call("init", "probe-0", timeout=1)
        except client.LedgerError:
            return None

    assert wait_for(answers, 30), "ledgerd never answered"
    return proc


def start_historyd(db, log, **env):
    """A host historyd on the test broker, answering requests before this returns."""
    from agent_bridge.history import client

    with open(log, "a") as err:
        proc = subprocess.Popen(HISTORYD, stderr=err, env=_service_env("HISTORY_DB", db, env))

    def answers():
        assert proc.poll() is None, open(log).read()
        try:
            return client.call("ping", timeout=1)
        except client.HistoryError:
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
