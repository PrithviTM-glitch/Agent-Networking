"""An executable mirror of Ledger.tla: the same state, actions and invariants, explored breadth first.

TLC is the checker of record. Until it is approved for CI, this mirror runs the same models (the four .cfg files)
with the standard library only, so the spec's claims are tested now: Ledger.cfg holds every invariant, and each
broken variant fails the invariant its first line names. Every action below names the Ledger.tla action it
mirrors; a change to one is a change to both.

Usage: python3 mirror.py [CFG ...]   (default: every .cfg here). Exit 0 when each cfg does what it says.
Also: --trace CFG prints the shortest violating trace; --random N --seed S --cfg CFG prints N random behaviours as
JSON lines (the input of replay.py).
"""

import json
import random
import re
import sys
import time
from collections import deque
from pathlib import Path

HERE = Path(__file__).resolve().parent
INVARIANTS = ("TypeOK", "AtMostOneWriteLease", "FencingIncreases", "AtMostOnce", "LeaseOnlyToListener")


def load_cfg(path):
    """The CONSTANTS of a .cfg file, and its expectation: the invariant named after "MUST FAIL", or None."""
    text = Path(path).read_text()
    first = text.splitlines()[0]
    must = re.search(r"MUST FAIL (\w+)", first)
    consts = {}
    for name, value in re.findall(r"^\s+(\w+) = (.+)$", text, re.M):
        value = value.strip()
        if value in ("TRUE", "FALSE"):
            consts[name] = value == "TRUE"
        elif value.startswith("{"):
            consts[name] = tuple(v.strip() for v in value.strip("{}").split(","))
        else:
            consts[name] = int(value)
    return consts, must.group(1) if must else None


class Model:
    """State: (now, broker, reqs, sent_by, replies, leases, waiters, fence, executed, tokens, grants), all hashable.
    broker: frozenset of (id, deliveries); reqs: tuple of (client, op, sent, lease), id = index + 1;
    replies and executed: tuples of sorted (id, value); leases: frozenset of (id, holder, token, expires);
    waiters: tuple of (req, holder, deadline); grants: frozenset of (req, at)."""

    def __init__(self, k):
        self.k = k
        self.clients = k["Clients"]

    def init(self):
        return (0, frozenset(), (), tuple(0 for _ in self.clients), (), frozenset(), (), 0, (), (), frozenset())

    # helpers, as in Ledger.tla
    def listening(self, s, rid):
        return s[0] < s[2][rid - 1][2] + self.k["Life"]

    @staticmethod
    def holds(s, c):
        return any(le[1] == c for le in s[5])

    def knows(self, s, lease):
        sent = s[2][lease[0] - 1][2]
        return any(g[0] == lease[0] and g[1] < sent + self.k["Life"] for g in s[10])

    def tick_due(self, s):
        now = s[0]
        return any(le[3] <= now for le in s[5]) or any(w[2] <= now for w in s[6])

    def pump(self, s, ls, ws):
        """Pump(ls, ws): the parts (leases, waiters, fence, tokens, grants) after it."""
        now, fence, tokens, grants = s[0], s[7], s[9], s[10]
        if not ls and ws:
            w = ws[0]
            return (
                frozenset({(w[0], w[1], fence + 1, now + self.k["TTL"])}),
                ws[1:],
                fence + 1,
                tokens + (fence + 1,),
                grants | {(w[0], now)},
            )
        return frozenset(ls), tuple(ws), fence, tokens, grants

    def acquire(self, s, rid):
        k, now = self.k, s[0]
        client, _, sent, _ = s[2][rid - 1]
        remaining = sent + k["Life"] - now
        w = min(k["Wait"], remaining - k["Margin"]) if k["CapWaitAtExpiry"] else k["Wait"]
        leases, waiters, fence, tokens, grants = s[5], s[6], s[7], s[9], s[10]
        if self.holds(s, client):
            return leases, waiters, fence, tokens, grants
        if not leases and not waiters:
            return (
                frozenset({(rid, client, fence + 1, now + k["TTL"])}),
                waiters,
                fence + 1,
                tokens + (fence + 1,),
                grants | {(rid, now)},
            )
        if w <= 0:
            return leases, waiters, fence, tokens, grants
        return leases, waiters + ((rid, client, now + w),), fence, tokens, grants

    def release(self, s, rid):
        client, _, _, lease = s[2][rid - 1]
        if any(le[0] == lease and le[1] == client for le in s[5]):
            return self.pump(s, [le for le in s[5] if le[0] != lease], s[6])
        return s[5], s[6], s[7], s[9], s[10]

    def successors(self, s):
        """(action name, args, next state) for every enabled action, as Next."""
        k = self.k
        now, broker, reqs, sent_by, replies, leases, waiters, fence, executed, tokens, grants = s
        out = []
        for i, c in enumerate(self.clients):
            if sent_by[i] >= k["MaxReqs"]:
                continue
            nid = len(reqs) + 1
            bumped = sent_by[:i] + (sent_by[i] + 1,) + sent_by[i + 1 :]
            # SendAcquire(c)
            mine = [j + 1 for j, r in enumerate(reqs) if r[0] == c and r[1] == "acquire"]
            if not self.holds(s, c) and not any(self.listening(s, j) for j in mine):
                out.append(
                    (
                        "SendAcquire",
                        {"client": c, "id": nid},
                        (
                            now,
                            broker | {(nid, 0)},
                            reqs + ((c, "acquire", now, 0),),
                            bumped,
                            replies,
                            leases,
                            waiters,
                            fence,
                            executed,
                            tokens,
                            grants,
                        ),
                    )
                )
            # SendRelease(c)
            for le in sorted(leases):
                if le[1] == c and self.knows(s, le):
                    out.append(
                        (
                            "SendRelease",
                            {"client": c, "id": nid, "lease": le[0]},
                            (
                                now,
                                broker | {(nid, 0)},
                                reqs + ((c, "release", now, le[0]),),
                                bumped,
                                replies,
                                leases,
                                waiters,
                                fence,
                                executed,
                                tokens,
                                grants,
                            ),
                        )
                    )
        stored = dict(replies)
        for m in sorted(broker):
            rid, deliveries = m
            # Deliver(m)
            if deliveries < k["MaxDeliveries"] and self.listening(s, rid) and not self.tick_due(s):
                b2 = (broker - {m}) | {(rid, deliveries + 1)}
                remaining = reqs[rid - 1][2] + k["Life"] - now
                keep = min(max(k["Dedupe"], remaining), k["MaxKeep"]) if k["KeepForLifetime"] else k["Dedupe"]
                if rid in stored and now <= stored[rid]:
                    out.append(
                        (
                            "Deliver",
                            {"id": rid, "replayed": True},
                            (now, b2, reqs, sent_by, replies, leases, waiters, fence, executed, tokens, grants),
                        )
                    )
                else:
                    r2 = dict(stored)
                    r2[rid] = now + keep
                    e2 = dict(executed)
                    e2[rid] = e2.get(rid, 0) + 1
                    op = self.acquire(s, rid) if reqs[rid - 1][1] == "acquire" else self.release(s, rid)
                    l2, w2, f2, t2, g2 = op
                    out.append(
                        (
                            "Deliver",
                            {"id": rid, "replayed": False},
                            (
                                now,
                                b2,
                                reqs,
                                sent_by,
                                tuple(sorted(r2.items())),
                                l2,
                                w2,
                                f2,
                                tuple(sorted(e2.items())),
                                t2,
                                g2,
                            ),
                        )
                    )
            # Ack(m)
            if deliveries >= 1:
                out.append(("Ack", {"id": rid}, (now, broker - {m}, *s[2:])))
            # Expire(m)
            if not self.listening(s, rid):
                out.append(("Expire", {"id": rid}, (now, broker - {m}, *s[2:])))
        # TickLeases
        if any(le[3] <= now for le in leases):
            l2, w2, f2, t2, g2 = self.pump(s, [le for le in leases if le[3] > now], waiters)
            out.append(("TickLeases", {}, (now, broker, reqs, sent_by, replies, l2, w2, f2, executed, t2, g2)))
        # TickWaiters
        if not any(le[3] <= now for le in leases) and any(w[2] <= now for w in waiters):
            l2, w2, f2, t2, g2 = self.pump(s, leases, tuple(w for w in waiters if w[2] > now))
            out.append(("TickWaiters", {}, (now, broker, reqs, sent_by, replies, l2, w2, f2, executed, t2, g2)))
        # Advance
        if now < k["MaxTime"] and not self.tick_due(s):
            out.append(("Advance", {}, (now + 1, *s[1:])))
        # Restart
        if not k["FencePersisted"]:
            out.append(("Restart", {}, (*s[:7], 0, *s[8:])))
        return out

    def violated(self, s):
        """The first invariant s breaks, in INVARIANTS order, or None."""
        now, _, reqs, _, _, leases, waiters, _, executed, tokens, grants = s
        if not (0 <= now <= self.k["MaxTime"]):
            return "TypeOK"
        if len(leases) > 1:
            return "AtMostOneWriteLease"
        if any(tokens[i] >= tokens[i + 1] for i in range(len(tokens) - 1)):
            return "FencingIncreases"
        if any(n > 1 for _, n in executed):
            return "AtMostOnce"
        if any(at >= reqs[req - 1][2] + self.k["Life"] for req, at in grants):
            return "LeaseOnlyToListener"
        return None


def check(cfg):
    """Breadth first over every reachable state: (distinct states, the violated invariant or None, its trace)."""
    model = Model(load_cfg(cfg)[0])
    start = model.init()
    parent = {start: None}
    queue = deque([start])
    while queue:
        s = queue.popleft()
        bad = model.violated(s)
        if bad:
            steps = []
            while parent[s] is not None:
                s, action, args = parent[s]
                steps.append((action, args))
            return len(parent), bad, steps[::-1]
        for action, args, nxt in model.successors(s):
            if nxt not in parent:
                parent[nxt] = (s, action, args)
                queue.append(nxt)
    return len(parent), None, []


def walks(cfg, n, seed):
    """n random behaviours of the model, as action lists: the input of replay.py."""
    model = Model(load_cfg(cfg)[0])
    rng = random.Random(seed)
    for i in range(n):
        s, steps = model.init(), []
        for _ in range(200):
            nxt = model.successors(s)
            if not nxt:
                break
            action, args, s = rng.choice(nxt)
            steps.append({"action": action, **args, "now": s[0]})
        yield {"walk": i, "seed": seed, "steps": steps}


def main(argv):
    if argv[:1] == ["--random"]:
        n, seed, cfg = int(argv[1]), int(argv[argv.index("--seed") + 1]), argv[argv.index("--cfg") + 1]
        for w in walks(cfg, n, seed):
            print(json.dumps(w))
        return 0
    show = argv[:1] == ["--trace"]
    cfgs = argv[1:] if show else argv
    cfgs = cfgs or sorted(str(p) for p in HERE.glob("*.cfg"))
    ok = True
    for cfg in cfgs:
        _, must = load_cfg(cfg)
        t = time.monotonic()
        states, bad, steps = check(cfg)
        secs = time.monotonic() - t
        good = bad == must
        ok &= good
        what = f"violates {bad} in {len(steps)} steps" if bad else "holds every invariant"
        want = f"must fail {must}" if must else "must hold"
        print(f"{'ok  ' if good else 'FAIL'} {Path(cfg).name}: {what} ({states} states, {secs:.1f} s; {want})")
        if bad and show:
            for action, args in steps:
                print(f"       {action} {args}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
