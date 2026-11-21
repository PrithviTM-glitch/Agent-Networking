"""Replay behaviours of the ledger model against a running ledgerd over raw MQTT, and check every step agrees.

The behaviours are random walks of LedgerReplay.cfg, the untimed part of Ledger.tla: no tick ever passes, so no lease
expires and no wait ends, and everything else is in play: grants, busy, the FIFO queue, release and the pump, refusals
("you already hold", "no such lease"), QoS 1 redelivery (the same bytes again, which ledgerd must replay, not run) and,
when the caller can restart ledgerd, restarts between steps. After every step a holders request both waits for ledgerd
to have handled what came before (it serves one request at a time) and reads the resource: its holders, their fencing
tokens and the waiters must be the model's, in order.

Timed behaviour (expiry, wait deadlines, the dedupe window) is not replayed: ledgerd's clock is the wall clock and
DEDUPE_S is a constant 300 s, so a timed replay would need either a test clock in ledgerd or minutes per trace. TLC
checks that part on the spec; mirror.py's walks of Ledger.cfg are what a timed replay would start from.

Used by tests/test_ledger.py (test_the_model_replays_against_ledgerd). Real seconds stand in for the model's constants
only where they must outlast the replay: ttl 60 s, wait 30 s, a request's lifetime 60 s.
"""

import json
import random
import threading
import uuid
from pathlib import Path

import mirror
import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

HERE = Path(__file__).resolve().parent
CFG = HERE / "LedgerReplay.cfg"
TTL, WAIT, LIFE = 60, 30, 60


class Ledgerd:
    """A raw MQTT client of ledgerd: every request and reply is visible, as the broker carries them."""

    def __init__(self, host, port):
        self.replies, self.cv = {}, threading.Condition()
        self.topic = f"ledger/resp/replay{uuid.uuid4().hex[:8]}"
        self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
        up = threading.Event()
        self.c.on_connect = lambda *_: up.set()
        self.c.on_message = self._message
        self.c.connect(host, port)
        self.c.loop_start()
        assert up.wait(10), "broker unreachable"
        done = threading.Event()
        self.c.on_subscribe = lambda *_: done.set()
        self.c.subscribe(self.topic, qos=1)
        assert done.wait(10)

    def _message(self, _c, _u, m):
        corr = getattr(m.properties, "CorrelationData", None)
        with self.cv:
            self.replies.setdefault(corr, []).append(json.loads(m.payload))
            self.cv.notify_all()

    def send(self, who, body, corr):
        """Publish one request exactly as a client would; a redelivery is the same call again."""
        props = Properties(PacketTypes.PUBLISH)
        props.ResponseTopic, props.CorrelationData, props.MessageExpiryInterval = self.topic, corr, LIFE
        sent = self.c.publish(f"ledger/req/{who}", json.dumps(body, sort_keys=True), qos=1, properties=props)
        sent.wait_for_publish(5)

    def call(self, who, body, timeout=10):
        corr = uuid.uuid4().bytes
        self.send(who, body, corr)
        return self.wait(corr, 1, timeout)[0]

    def wait(self, corr, n, timeout=10):
        with self.cv:
            ok = self.cv.wait_for(lambda: len(self.replies.get(corr, [])) >= n, timeout)
            assert ok, f"no reply {n} to {corr!r}: {self.replies.get(corr)}"
            return self.replies[corr]

    def close(self):
        self.c.loop_stop()
        self.c.disconnect()


def walks(n, seed):
    """n random behaviours of LedgerReplay.cfg, each with the model's state after every step."""
    model = mirror.Model(mirror.load_cfg(CFG)[0])
    rng = random.Random(seed)
    for _ in range(n):
        s, steps = model.init(), []
        for _ in range(60):
            nxt = model.successors(s)
            if not nxt:
                break
            action, args, s = rng.choice(nxt)
            steps.append((action, args, s))
        yield steps


def replay(host, port, n=30, seed=1, restart=None, restart_every=0):
    """Replay n walks; restart(), when given, restarts ledgerd before every restart_every-th delivery.
    Returns how many steps and deliveries ran. Raises AssertionError at the first step where ledgerd and the model
    disagree, naming the walk and step."""
    led = Ledgerd(host, port)
    run = uuid.uuid4().hex[:6]
    totals = {"walks": 0, "steps": 0, "deliveries": 0, "redeliveries": 0, "restarts": 0}
    try:
        owner = f"rpo-{run}"
        led.call(owner, {"op": "init"})
        for w, steps in enumerate(walks(n, seed)):
            res = f"r{w}"
            rid = f"{owner}/{res}"
            assert led.call(owner, {"op": "add", "name": res, "visibility": "public", "max_level": "write"})["ok"]
            agent = {c: f"rp{c}-{run}{w}" for c in mirror.load_cfg(CFG)[0]["Clients"]}
            for a in agent.values():
                led.call(a, {"op": "init"})
                asked = led.call(a, {"op": "request", "resource": rid, "level": "write"})
                assert asked["request"]["status"] == "granted", asked
            _walk(led, w, steps, agent, rid, restart, restart_every, totals)
            totals["walks"] += 1
    finally:
        led.close()
    return totals


def _walk(led, w, steps, agent, rid, restart, restart_every, totals):
    corr = {}  # model request id -> CorrelationData
    body = {}  # model request id -> the request's JSON, sent identically on every delivery
    first = {}  # model request id -> the replies seen to its first delivery
    real_lease = {}  # model lease id (its acquire's request id) -> ledgerd's lease_id
    offset = None  # ledgerd's fencing token minus the model's: the counter is global, the model's starts at 0
    for i, (action, args, s) in enumerate(steps):
        where = f"walk {w} step {i} {action} {args}"
        totals["steps"] += 1
        reqs = s[2]
        if action in ("SendAcquire", "SendRelease"):
            client, op, _, lease = reqs[args["id"] - 1]
            corr[args["id"]] = uuid.uuid4().bytes
            if op == "acquire":
                body[args["id"]] = {"op": "acquire", "resource": rid, "level": "write", "ttl": TTL, "wait": WAIT}
            else:
                body[args["id"]] = {"op": "release", "lease_id": real_lease[lease]}
        elif action == "Deliver":
            mid = args["id"]
            client = reqs[mid - 1][0]
            if restart and restart_every and totals["deliveries"] % restart_every == restart_every - 1:
                restart()
                totals["restarts"] += 1
            seen = len(led.replies.get(corr[mid], []))
            led.send(agent[client], body[mid], corr[mid])
            totals["deliveries"] += 1
            _barrier(led, rid)
            got = led.replies.get(corr[mid], [])[seen:]
            if args["replayed"]:
                totals["redeliveries"] += 1
                # the stored answer again, or nothing for a request that queued a waiter
                want = first[mid][:1]
                assert [_strip(r) for r in got] == [_strip(r) for r in want], (where, got, want)
            else:
                first[mid] = got
            for lease in s[5]:
                if lease[0] not in real_lease:
                    grant = [r for r in led.replies.get(corr[lease[0]], []) if r.get("lease")]
                    assert grant, (where, "the model granted a lease ledgerd did not", lease)
                    real = grant[-1]["lease"]
                    real_lease[lease[0]] = real["lease_id"]
                    offset = real["token"] - lease[2] if offset is None else offset
                    assert real["token"] == lease[2] + offset, (where, "fencing", real, lease)
                    assert real["holder"] == agent[lease[1]], (where, real, lease)
        # every step: ledgerd's view of the resource is the model's
        h = led.call("rp-observer", {"op": "holders", "resource": rid})
        holders = [(x["holder"], x["lease_id"]) for x in h["holders"]]
        model = [(agent[le[1]], real_lease.get(le[0])) for le in sorted(s[5])]
        assert holders == model, (where, "holders", holders, model)
        waiting = [x["holder"] for x in h["waiting"]]
        assert waiting == [agent[x[1]] for x in s[6]], (where, "waiting", waiting, s[6])


def _barrier(led, rid):
    """ledgerd has handled everything sent before this returns: it serves one request at a time, in order."""
    led.call("rp-observer", {"op": "holders", "resource": rid})


def _strip(reply):
    """A reply without the parts that are never the same twice (none today: ledgerd replays the stored body)."""
    return reply
