"""Auth as a table: every entry point x identity x mode against a disposable broker started through
config/broker-entrypoint.sh (compose.auth.yaml's broker) and a ledgerd run as compose runs it. Each cell is what
docs/AUTH.md says it should be; the test runs every cell, then reports every cell that differs at once.

Entry points (what each one exercises):
- ledger worktrees / ledger dump: _native.ledger_call, the path the CLI's ledger commands, the MCP ledger tools and
  the console's ledger views share (core::ledger::call_value);
- cli ledger-raw: the agent-bridge binary's own process and credential choice, asking for dump;
- cli say: the agent-bridge binary posting to a room as an agent;
- relay: the owner Bus a session's relay opens (Bus.register with the session's address), receiving a DM;
- mcp: agent-bridge-mcp over stdio in its own session: say, and the public ledger tool over its Bus;
- console: the console's own start (auth.use_admin_credentials) then its admin ledger read;
- ledgerd login / ledgerd --health: ledgerd's start and the container's health check, with AGENT_BRIDGE_CREDENTIALS
  as compose.auth.yaml sets it and unset.

Identities: admin (AGENT_BRIDGE_CREDENTIALS names admin.json, as the console and agent-bridge-admin set it), admin
with no variable (admin.json only on disk), own (an agent whose credentials bind it to this session), other (an agent
bound to another session, used from this one) and anonymous (an address that is never provisioned).

Modes run in order off, warn, enforce on one data volume, as a user switches. The three late 0.3.2 findings are cells
here: ledgerd --health under enforce, admin with no variable under enforce (the implicit admin fallback), and own's dump
under enforce (dump open to all). Needs Docker.
"""

import asyncio
import json
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

import pytest
from conftest import BIN, LABEL, LEDGERD, wait_for

from agent_bridge import _native
from agent_bridge.errors import BridgeError, LedgerError

REPO = Path(__file__).resolve().parent.parent
IMAGE = "eclipse-mosquitto:2.0.22"
CLI = str(BIN / "agent-bridge")
MCP = str(BIN / "agent-bridge-mcp")
RUN = uuid.uuid4().hex[:6]
ROOM = f"matrix{RUN}"
S1, S2, S3 = (str(uuid.uuid4()) for _ in range(3))  # this session, another one, the MCP server's
OWN, OTHER, ANON = f"own-{RUN}", f"other-{RUN}", f"hand-picked{RUN}"
MODES = ("off", "warn", "enforce")

OK = "ok"
NOT_YOURS = "is not your agent (bound to another session)"
NEEDS = "this broker requires credentials (auth enforce)"
ADMIN_ONLY = "dump: admin only"
LOGGED_IN = "ok: logged in as ledgerd"
ANONYMOUS = "ok: anonymous"
NO_FILE = "auth enforce: ledgerd needs its credentials file in AGENT_BRIDGE_CREDENTIALS"

# (entry point, identity) -> the expected outcome in off, warn and enforce, from docs/AUTH.md 4.2-4.8:
# - off is 0.3.1: anyone acts as any address, and no credentials exist yet, so "other" is just an address;
# - warn: credentials are used when present, anonymous clients keep full rights (the legacy group), and an agent
#   bound to another session is refused by the own-session rule;
# - enforce: anonymous clients are refused, dump is admin's alone, and admin is only ever explicit.
EXPECTED = {
    ("ledger worktrees", "admin"): (OK, OK, OK),
    ("ledger worktrees", "admin, no variable"): (OK, OK, NEEDS),
    ("ledger worktrees", "own"): (OK, OK, OK),
    ("ledger worktrees", "other"): (OK, NOT_YOURS, NOT_YOURS),
    ("ledger worktrees", "anonymous"): (OK, OK, NEEDS),
    ("ledger dump", "admin"): (OK, OK, OK),
    ("ledger dump", "admin, no variable"): (OK, OK, NEEDS),
    ("ledger dump", "own"): (OK, OK, ADMIN_ONLY),
    ("ledger dump", "other"): (OK, NOT_YOURS, NOT_YOURS),
    ("ledger dump", "anonymous"): (OK, OK, NEEDS),
    ("cli ledger-raw dump", "admin"): (OK, OK, OK),
    ("cli ledger-raw dump", "admin, no variable"): (OK, OK, NEEDS),
    ("cli ledger-raw dump", "own"): (OK, OK, ADMIN_ONLY),
    ("cli ledger-raw dump", "other"): (OK, NOT_YOURS, NOT_YOURS),
    ("cli ledger-raw dump", "anonymous"): (OK, OK, NEEDS),
    ("cli say", "own"): (OK, OK, OK),
    ("cli say", "other"): (OK, NOT_YOURS, NOT_YOURS),
    ("cli say", "anonymous"): (OK, OK, NEEDS),
    ("relay", "own"): (OK, OK, OK),
    ("relay", "other"): (OK, NOT_YOURS, NOT_YOURS),
    ("mcp say", "own"): (OK, OK, OK),
    ("mcp ledger public", "own"): (OK, OK, OK),
    ("console ledger dump", "admin"): (OK, OK, OK),
    # ledgerd's own credentials, as compose.auth.yaml sets AGENT_BRIDGE_CREDENTIALS and with it unset: the same file
    # and the same outcome as 0.3.2 (run this module with AGENT_BRIDGE_LEDGERD naming a 0.3.2 ledgerd to compare)
    ("ledgerd login", "ledgerd, variable set"): (ANONYMOUS, LOGGED_IN, LOGGED_IN),
    ("ledgerd login", "ledgerd, variable unset"): (ANONYMOUS, ANONYMOUS, NO_FILE),
    ("ledgerd --health", "ledgerd, variable set"): (OK, OK, OK),
    ("ledgerd --health", "ledgerd, variable unset"): (OK, OK, NEEDS),
}


class World:
    """The broker container and ledgerd, on the test run's port, with this module's own config dir."""

    def __init__(self, port, base):
        self.port, self.base = port, Path(base)
        self.name, self.volume = f"agent-bridge-matrix-{RUN}", f"agent-bridge-matrix-{RUN}-data"
        self.cfg = self.base / "config" / "agent-bridge"  # XDG_CONFIG_HOME/agent-bridge, mounted as the entrypoint's
        self.run = self.base / "run"
        for d in (self.cfg, self.run):
            d.mkdir(parents=True, mode=0o700)
        self.ledgerd = None
        self.log = self.base / "ledgerd.log"

    def up(self, mode):
        """(Re)start the broker in mode on the same volume, stopping it gracefully so retained records survive."""
        subprocess.run(["docker", "stop", self.name], capture_output=True)
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)
        subprocess.run(
            ["docker", "run", "-d", "--name", self.name, "--label", LABEL, "-e", f"AGENT_BRIDGE_AUTH={mode}",
             "-p", f"127.0.0.1:{self.port}:1883",
             "-v", f"{REPO}/config/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro",
             "-v", f"{REPO}/config/broker-entrypoint.sh:/agent-bridge/entrypoint.sh:ro",
             "-v", f"{self.volume}:/mosquitto/data", "-v", f"{self.cfg}:/agent-bridge-config",
             "-v", f"{self.run}:/run/agent-bridge",
             "--entrypoint", "/bin/sh", IMAGE, "/agent-bridge/entrypoint.sh"],
            check=True, capture_output=True,
        )  # fmt: skip

        def ready():
            p = subprocess.run(["docker", "exec", self.name, "test", "-f", "/tmp/agent-bridge-ready"])
            return p.returncode == 0

        assert wait_for(ready, 30), subprocess.run(["docker", "logs", self.name], capture_output=True, text=True)
        self.start_ledgerd(mode)

    def ledgerd_env(self, mode, variable=True):
        """ledgerd's environment as compose.auth.yaml sets it (AGENT_BRIDGE_AUTH, and AGENT_BRIDGE_CREDENTIALS naming
        services/ledgerd.json), or with the variable unset."""
        env = {**os.environ, "LEDGER_DB": str(self.base / "ledger.db"), "AGENT_BRIDGE_AUTH": mode}
        env.pop("CLAUDE_CODE_SESSION_ID", None)
        env.pop("AGENT_BRIDGE_CREDENTIALS", None)
        if variable:
            env["AGENT_BRIDGE_CREDENTIALS"] = str(self.cfg / "services" / "ledgerd.json")
        return env

    def answering(self):
        """ledgerd answers a request, asked as admin when admin's credentials exist (enforce refuses anonymous)."""
        admin = self.cfg / "admin.json"
        saved = os.environ.get("AGENT_BRIDGE_CREDENTIALS")
        if admin.exists():
            os.environ["AGENT_BRIDGE_CREDENTIALS"] = str(admin)
        try:
            return outcome(lambda: _native.ledger_call("worktrees", "admin", timeout=1)) == OK
        finally:
            os.environ.pop("AGENT_BRIDGE_CREDENTIALS", None)
            if saved is not None:
                os.environ["AGENT_BRIDGE_CREDENTIALS"] = saved

    def start_ledgerd(self, mode, variable=True):
        """ledgerd under mode; its outcome: LOGGED_IN or ANONYMOUS once it answers, else why it stopped."""
        self.stop_ledgerd()
        start = self.log.stat().st_size if self.log.exists() else 0
        with open(self.log, "a") as err:
            self.ledgerd = subprocess.Popen(LEDGERD, stderr=err, env=self.ledgerd_env(mode, variable))
        up = wait_for(lambda: self.ledgerd.poll() is not None or self.answering(), 30)
        said = self.log.read_text()[start:]
        if self.ledgerd.poll() is not None:
            self.ledgerd = None
            return said.strip().splitlines()[-1] if said.strip() else "exited"
        assert up, said
        return LOGGED_IN if "logging in as ledgerd" in said else ANONYMOUS

    def stop_ledgerd(self):
        if self.ledgerd:
            self.ledgerd.terminate()
            self.ledgerd.wait()
            self.ledgerd = None

    def remove(self):
        self.stop_ledgerd()
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)
        subprocess.run(["docker", "volume", "rm", "-f", self.volume], capture_output=True)


@pytest.fixture(scope="module")
def world(fresh_broker):
    # The native origin (identity.rs) is read once per process: read it now, session-less, on the run's plain broker,
    # or this module's registrations inside sessions would hand their session to every later module's Buses.
    os.environ.pop("CLAUDE_CODE_SESSION_ID", None)
    _native.Bus.register("matrix-origin").leave()
    fresh_broker.remove()  # the run's plain broker gives its port to this module's auth broker
    base = tempfile.mkdtemp(prefix="agent-bridge-matrix-")
    w = World(fresh_broker.port, base)
    saved = {k: os.environ.get(k) for k in ("XDG_CONFIG_HOME", "CLAUDE_CODE_SESSION_ID", "AGENT_BRIDGE_CREDENTIALS")}
    os.environ["XDG_CONFIG_HOME"] = str(w.base / "config")  # this module's credentials never reach other modules
    yield w
    w.remove()
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    shutil.rmtree(base, ignore_errors=True)


class As:
    """The process environment of an identity: its session and AGENT_BRIDGE_CREDENTIALS, restored on exit."""

    def __init__(self, world, identity, session=S1):
        self.env = {"CLAUDE_CODE_SESSION_ID": session, "AGENT_BRIDGE_CREDENTIALS": None}
        if identity == "admin" and (world.cfg / "admin.json").exists():
            self.env["AGENT_BRIDGE_CREDENTIALS"] = str(world.cfg / "admin.json")
        if identity in ("admin", "admin, no variable"):
            self.env["CLAUDE_CODE_SESSION_ID"] = None  # admin acts from outside any session
        self.addr = {"own": OWN, "other": OTHER, "anonymous": ANON}.get(identity, "admin")

    def __enter__(self):
        self.saved = {k: os.environ.get(k) for k in self.env}
        for k, v in self.env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        return self

    def __exit__(self, *_):
        for k, v in self.saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def environ(self):
        """The environment for a command run as this identity."""
        return dict(os.environ)


def outcome(f):
    """OK, or the error text f raised."""
    try:
        f()
    except (LedgerError, BridgeError) as e:
        return str(e)
    return OK


def run(cmd, env):
    p = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=60)
    return OK if p.returncode == 0 else (p.stderr.strip() or p.stdout.strip() or f"exit {p.returncode}")


class _Session:
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        self.saved = os.environ.get("CLAUDE_CODE_SESSION_ID"), os.environ.pop("AGENT_BRIDGE_CREDENTIALS", None)
        os.environ["CLAUDE_CODE_SESSION_ID"] = self.session

    def __exit__(self, *_):
        session, creds = self.saved
        os.environ.pop("CLAUDE_CODE_SESSION_ID")
        if session is not None:
            os.environ["CLAUDE_CODE_SESSION_ID"] = session
        if creds is not None:
            os.environ["AGENT_BRIDGE_CREDENTIALS"] = creds


def register(addr, session):
    """Register addr in session (an agent's relay does this at start): under warn it provisions the agent's
    credentials, bound to session."""
    with _Session(session):
        _native.Bus.register(addr.rsplit("-", 1)[0], keep=True, addr=addr).close()


def relay_receives(world, identity):
    """The owner Bus a relay opens for addr in this session, and a DM to it from another agent."""
    who = As(world, identity)
    with who:
        bus = _native.Bus.register(who.addr.rsplit("-", 1)[0], keep=True, addr=who.addr)  # raises when refused
    try:
        sender = OTHER if who.addr == OWN else OWN
        with _Session(S2 if sender == OTHER else S1):
            out = _native.Bus.attach(sender, owner=False)
            out.dm(who.addr, f"ping {RUN}")
            out.close()
        got = wait_for(lambda: [m for m in bus.recv(wait=0.5) if m.get("body") == f"ping {RUN}"], 10)
        assert got, f"{who.addr} never received the DM"
    finally:
        bus.close()


async def mcp_probes(mode):
    """agent-bridge-mcp in its own session S3: say, then the public ledger tool."""
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    env = {**os.environ, "CLAUDE_CODE_SESSION_ID": S3, "CLAUDE_PROJECT_DIR": f"/work/matrix{RUN}"}
    env.pop("AGENT_BRIDGE_CREDENTIALS", None)
    results = {}
    params = StdioServerParameters(command=MCP, args=[], env=env)
    with open(os.devnull, "w") as quiet:
        async with stdio_client(params, errlog=quiet) as (r, w), ClientSession(r, w) as s:
            await s.initialize()
            for key, tool, args in (("mcp say", "say", {"room": ROOM, "text": f"mcp {mode}"}),
                                    ("mcp ledger public", "public", {})):  # fmt: skip
                res = await s.call_tool(tool, args)
                text = res.content[0].text
                results[key] = text.strip() if res.isError else OK
    return results


def probe(world, mode):
    """Every cell's actual outcome in mode, keyed as EXPECTED."""
    got = {}
    for (entry, identity), _ in EXPECTED.items():
        if entry.startswith(("mcp", "ledgerd", "console", "relay")):
            continue
        who = As(world, identity)
        with who:
            if entry == "ledger worktrees":
                got[entry, identity] = outcome(lambda a=who.addr: _native.ledger_call("worktrees", a, timeout=4))
            elif entry == "ledger dump":
                got[entry, identity] = outcome(lambda a=who.addr: _native.ledger_call("dump", a, timeout=4))
            elif entry == "cli ledger-raw dump":
                got[entry, identity] = run(
                    [CLI, "ledger-raw", "--as", who.addr, "--timeout", "4", "dump"], who.environ()
                )
            elif entry == "cli say":
                got[entry, identity] = run([CLI, "say", who.addr, ROOM, f"cli {mode}"], who.environ())
    for identity in ("own", "other"):
        got["relay", identity] = outcome(lambda i=identity: relay_receives(world, i))
    got.update({(k, "own"): v for k, v in asyncio.run(mcp_probes(mode)).items()})
    console = (
        "from agent_bridge.console import auth\n"
        "auth.use_admin_credentials()\n"
        "from agent_bridge import _native\n"
        "_native.ledger_call('dump', 'admin', timeout=4)\n"
    )
    env = {k: v for k, v in os.environ.items() if k not in ("AGENT_BRIDGE_CREDENTIALS", "CLAUDE_CODE_SESSION_ID")}
    got["console ledger dump", "admin"] = run([sys.executable, "-I", "-c", console], env)
    for variable, who in ((True, "ledgerd, variable set"), (False, "ledgerd, variable unset")):
        got["ledgerd login", who] = world.start_ledgerd(mode, variable)
        got["ledgerd --health", who] = run([*LEDGERD, "--health"], world.ledgerd_env(mode, variable))
    world.start_ledgerd(mode)
    return got


def matches(want, have):
    return have == want if want in (OK, LOGGED_IN, ANONYMOUS) else (have != OK and want in have)


def test_entry_point_by_identity_by_mode(world):
    misses = []
    table = []
    for i, mode in enumerate(MODES):
        world.up(mode)
        if mode == "off":  # agents from before auth: no credentials yet
            for addr, session in ((OWN, S1), (OTHER, S2), (ANON, S1)):
                register(addr, session)
        if mode == "warn":  # their relays restart: register provisions own and other, bound to their own sessions
            for addr, session in ((OWN, S1), (OTHER, S2), (ANON, S1)):
                register(addr, session)
            agents = world.cfg / "agents"
            assert sorted(p.name for p in agents.iterdir() if p.name.startswith(("own", "other"))) == sorted(
                [f"{OTHER}.json", f"{OWN}.json"]
            ), "own and other are provisioned"
            assert not (agents / f"{ANON}.json").exists(), "a hand-picked address stays anonymous"
            assert json.loads((agents / f"{OWN}.json").read_text())["session"] == S1
        got = probe(world, mode)
        for key, want in EXPECTED.items():
            have = got[key]
            table.append(f"{mode:8} {key[0]:22} {key[1]:20} {'ok' if have == OK else have[:70]}")
            if not matches(want[i], have):
                misses.append(f"{mode} / {key[0]} / {key[1]}: expected {want[i]!r}, got {have!r}")
    print("\n".join(table))
    assert not misses, "cells that differ from docs/AUTH.md:\n" + "\n".join(misses)


def test_a_refused_subscription_is_logged(world):
    """ledgerd whose role can't read one of its request topics says so, instead of sitting connected and silent (the
    silent-refusal class). Admin takes ledger/req/+ out of role-ledgerd with mosquitto_ctrl, ledgerd restarts, and its
    log names the refusal; the ACL goes back afterwards."""
    world.up("enforce")
    admin = json.loads((world.cfg / "admin.json").read_text())
    ctl = ["docker", "exec", world.name, "mosquitto_ctrl", "-h", "localhost", "-p", "1883", "-u", "admin",
           "-P", admin["password"], "dynsec"]  # fmt: skip
    subprocess.run([*ctl, "removeRoleACL", "role-ledgerd", "subscribePattern", "ledger/req/+"], check=True)
    try:
        world.stop_ledgerd()
        start = world.log.stat().st_size
        with open(world.log, "a") as err:
            world.ledgerd = subprocess.Popen(LEDGERD, stderr=err, env=world.ledgerd_env("enforce"))
        said = wait_for(lambda: "subscription refused" in (s := world.log.read_text()[start:]) and s, 20)
        assert said, world.log.read_text()[start:]
        assert "ledger/req/+" in said and "check its role" in said, said
    finally:
        subprocess.run([*ctl, "addRoleACL", "role-ledgerd", "subscribePattern", "ledger/req/+", "allow"], check=True)
        world.start_ledgerd("enforce")


def test_a_refused_request_is_an_error(world):
    """The service request path (core request.rs) reports a broker refusal as an error naming the topic, not as a
    timeout: admin takes one row out of an agent's role, and a ledger call as that agent says what was refused."""
    world.up("enforce")
    admin = json.loads((world.cfg / "admin.json").read_text())
    ctl = ["docker", "exec", world.name, "mosquitto_ctrl", "-h", "localhost", "-p", "1883", "-u", "admin",
           "-P", admin["password"], "dynsec"]  # fmt: skip
    for kind, topic, want in (
        ("publishClientSend", f"ledger/req/{OWN}", f"not authorized to publish on ledger/req/{OWN}"),
        ("subscribePattern", f"ledger/resp/{OWN}/+", f"not authorized to subscribe to ledger/resp/{OWN}/"),
    ):
        subprocess.run([*ctl, "removeRoleACL", f"role-{OWN}", kind, topic], check=True)
        try:
            with As(world, "own"):
                got = outcome(lambda: _native.ledger_call("worktrees", OWN, timeout=30))
            assert want in got, (kind, got)
        finally:
            # back at provisioning's priority (provision.rs ALLOW = 10), above the role's reserved denies (5)
            subprocess.run([*ctl, "addRoleACL", f"role-{OWN}", kind, topic, "allow", "10"], check=True)
    with As(world, "own"):
        assert outcome(lambda: _native.ledger_call("worktrees", OWN, timeout=5)) == OK, "the rows are back"
