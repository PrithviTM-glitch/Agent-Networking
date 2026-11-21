"""A subscription the broker's ACL refuses is reported, never silently empty: watch exits non-zero with "subscription
refused: <topic>", and the Observer (the console's view of the bus) says it is degraded. A disposable mosquitto of its
own with the dynamic-security plugin, which lets anonymous clients read everything but room/secret, "#" and $SYS."""

import json
import os
import shutil
import signal
import subprocess
import tempfile
import uuid

import pytest
from conftest import BIN, LABEL, free_port, wait_for
from waits import holds_for

IMAGE = "eclipse-mosquitto:2.0.22"
BRIDGE = str(BIN / "agent-bridge")
# The dynamic-security plugin, as under AGENT_BRIDGE_AUTH: it refuses a subscription in the SUBACK (an acl_file grants
# every SUBACK and only filters delivery, so it can't show this). Anonymous clients get the "anon" group's role:
# everything the commands need, but not room/secret, "#", $SYS or historyd's replies.
CONF = """listener 1883
allow_anonymous true
plugin /usr/lib/mosquitto_dynamic_security.so
plugin_opt_config_file /mosquitto/config/dynsec.json
"""
READ = ["agents/#", "rooms/#", "room/open", "room/open/+", "inbox/#", "ledger/#"]  # not history/#
DYNSEC = {
    "defaultACLAccess": {
        "publishClientSend": False,
        "publishClientReceive": True,
        "subscribe": False,
        "unsubscribe": True,
    },
    "clients": [],
    "groups": [{"groupname": "anon", "roles": [{"rolename": "reader"}], "clients": []}],
    "roles": [
        {
            "rolename": "reader",
            "acls": [{"acltype": "subscribePattern", "topic": t, "allow": True} for t in READ]
            + [{"acltype": "publishClientSend", "topic": "#", "allow": True}],
        }
    ],
    "anonymousGroup": "anon",
}


@pytest.fixture(scope="module")
def acl_broker():
    if not shutil.which("docker"):
        pytest.skip("docker is not installed")
    conf = tempfile.mkdtemp(prefix="acl-broker-")
    with open(os.path.join(conf, "mosquitto.conf"), "w") as f:
        f.write(CONF)
    with open(os.path.join(conf, "dynsec.json"), "w") as f:
        json.dump(DYNSEC, f)
    os.chmod(conf, 0o777)  # the container's mosquitto user may rewrite dynsec.json
    for name in ("mosquitto.conf", "dynsec.json"):
        os.chmod(os.path.join(conf, name), 0o666)
    port, name = free_port(), f"agent-bridge-test-acl-{uuid.uuid4().hex[:6]}"
    subprocess.run(
        ["docker", "run", "-d", "--name", name, "--label", LABEL, "-p", f"127.0.0.1:{port}:1883",
         "-v", f"{conf}:/mosquitto/config", IMAGE],
        check=True,
        capture_output=True,
    )  # fmt: skip
    env = {**os.environ, "AGENT_BRIDGE_HOST": "127.0.0.1", "AGENT_BRIDGE_PORT": str(port)}
    up = lambda: subprocess.run([BRIDGE, "agents"], env=env, capture_output=True).returncode == 0  # noqa: E731
    try:
        assert wait_for(up, 15), "the ACL broker never came up"
        yield env
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
        shutil.rmtree(conf, ignore_errors=True)


def watch(env, room):
    return subprocess.Popen([BRIDGE, "watch", room], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def test_watch_says_when_the_broker_refuses_the_room(acl_broker):
    w = watch(acl_broker, "secret")
    try:
        code = w.wait(15)
    except subprocess.TimeoutExpired:
        w.send_signal(signal.SIGINT)
        code = w.wait(10)
        pytest.fail(f"watch kept running on a refused room, showing nothing (exit {code} on Ctrl-C)")
    err = w.stderr.read()
    assert code != 0, err
    assert "subscription refused: room/secret" in err, err


def test_watch_still_follows_a_room_it_may_read(acl_broker):
    w = watch(acl_broker, "open")
    try:
        holds_for(lambda: w.poll() is None, 3, "watch still following room/open")
    finally:
        w.send_signal(signal.SIGINT)
        assert w.wait(10) == 0


def test_a_history_request_names_the_refused_reply_topic(acl_broker):
    """Refused, a history call says so at once with the topic, not "no reply from historyd" after its timeout."""
    p = subprocess.run([BRIDGE, "history", "open"], env=acl_broker, capture_output=True, text=True, timeout=60)
    assert p.returncode != 0, p
    assert "not authorized to subscribe to history/resp/" in p.stderr, p.stderr


def test_the_observer_reports_a_refused_subscription(acl_broker, monkeypatch):
    from agent_bridge import _native

    for k, v in acl_broker.items():
        monkeypatch.setenv(k, v)
    obs = _native.Observer()
    try:
        # "#" and $SYS/# are refused to anonymous clients here: the observer would see nothing, and says so
        assert wait_for(lambda: obs.degraded is not None, 10), "the observer never reported the refusal"
        assert obs.degraded == "subscription refused: #, $SYS/#", obs.degraded
    finally:
        obs.close()
