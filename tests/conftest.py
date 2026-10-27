"""Shared fixtures: a disposable mosquitto in Docker on a free port, a state directory per module, and ledgerd."""

import os
import shutil
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
BIN = Path(sys.executable).parent


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


def pytest_configure(config):
    # agent_bridge reads its settings at import, so they are fixed here, before any test module imports it.
    tmp = tempfile.mkdtemp(prefix="agent-bridge-test-")
    for name in (
        "CLAUDE_CODE_SESSION_ID",
        "CLAUDE_PROJECT_DIR",
        "CLAUDE_CODE_MESSAGING_SOCKET",
        "CLAUDE_CODE_MESSAGING_TOKEN",
        "AGENT_BRIDGE_STALE_DAYS",
        "LEDGER_DB",
    ):
        os.environ.pop(name, None)
    os.environ.update(
        AGENT_BRIDGE_HOST="127.0.0.1",
        AGENT_BRIDGE_PORT=str(free_port()),
        AGENT_BRIDGE_LOG=os.path.join(tmp, "history.jsonl"),
        XDG_STATE_HOME=os.path.join(tmp, "state"),
    )


class Broker:
    """The test session's mosquitto container on AGENT_BRIDGE_PORT; each test module gets a fresh one."""

    def __init__(self):
        self.name = f"agent-bridge-test-{uuid.uuid4().hex[:6]}"
        self.port = int(os.environ["AGENT_BRIDGE_PORT"])

    def start(self, conf=CONF):
        self.remove()
        subprocess.run(
            ["docker", "run", "-d", "--name", self.name, "-p", f"127.0.0.1:{self.port}:1883", "-v",
             f"{conf}:/mosquitto/config/mosquitto.conf:ro", IMAGE],
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
    log = os.path.join(path, "history.jsonl")
    old = os.environ["AGENT_BRIDGE_LOG"], config.LOG
    os.environ["AGENT_BRIDGE_LOG"] = config.LOG = log
    yield path
    os.environ["AGENT_BRIDGE_LOG"], config.LOG = old
    shutil.rmtree(path, ignore_errors=True)


def start_ledgerd(db, log, **env):
    """A host ledgerd on the test broker, answering requests before this returns."""
    from agent_bridge.ledger import client

    with open(log, "a") as err:
        proc = subprocess.Popen(
            [sys.executable, "-m", "agent_bridge.ledger.service"],
            stderr=err,
            env={**os.environ, "LEDGER_DB": db, **env},
        )

    def answers():
        assert proc.poll() is None, open(log).read()
        try:
            return client.call("init", "probe-0", timeout=1)
        except client.LedgerError:
            return None

    assert wait_for(answers, 30), "ledgerd never answered"
    return proc


@pytest.fixture(scope="module")
def ledgerd(fresh_broker, state_dir):
    log = os.path.join(state_dir, "ledgerd.log")
    proc = start_ledgerd(os.path.join(state_dir, "ledger.db"), log)
    yield proc
    proc.kill()
    proc.wait()
