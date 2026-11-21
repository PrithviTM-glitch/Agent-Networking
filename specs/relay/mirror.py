"""An executable mirror of RelayDelivery.tla: the same state, actions and invariants, explored breadth first.

TLC is the checker of record. Until it is approved for CI, this mirror runs the same models (the .cfg files here)
with the standard library only, so the spec's claims are tested now: MC.cfg holds every invariant, and each
MUST FAIL cfg fails the invariant its first line names. Every action below names the RelayDelivery.tla action it
mirrors; a change to one is a change to both.

Usage: python3 mirror.py [CFG ...]   (default: every .cfg here). Exit 0 when each cfg does what it says.
Also: --trace CFG [INVARIANT] prints the shortest behaviour that violates INVARIANT (default: the cfg's MUST FAIL one).
"""

import re
import sys
import time
from collections import deque
from pathlib import Path

HERE = Path(__file__).resolve().parent
INVARIANTS = ("TypeOK", "NoLoss", "AckAfterInbox")
FIELDS = (
    "published",
    "queued",
    "sent",
    "buf",
    "pending",
    "inbox",
    "acked",
    "backlog",
    "owner",
    "gen",
    "brokerUp",
    "alive",
)


def load_cfg(path):
    """The CONSTANTS of a .cfg file, and its expectation: the invariant named after "MUST FAIL", or None."""
    text = Path(path).read_text()
    must = re.search(r"MUST FAIL (\w+)", text.splitlines()[0])
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


def initial():
    e = frozenset()
    return (e, e, e, e, e, e, e, e, "relay", 1, True, True)


def successors(s, c):
    """(action name, next state) for every enabled action, as RelayDelivery.tla's Next."""
    published, queued, sent, buf, pending, inbox, acked, backlog, owner, gen, up, alive = s
    msgs, max_gen, ack_first, quiet = c["Msgs"], c["MaxGen"], c["AckBeforeInbox"], c["QuietFilter"]
    receiving = alive and up and owner == "relay"

    def mk(**kw):
        d = dict(zip(FIELDS, s, strict=True))
        d.update(kw)
        return tuple(d[f] for f in FIELDS)

    for m in msgs:
        if up and m not in published:  # Publish(m)
            yield f"Publish({m})", mk(published=published | {m}, queued=queued | {m})
        if receiving and m in queued and (m, gen) not in sent:  # Send(m)
            yield f"Send({m})", mk(sent=sent | {(m, gen)}, buf=buf | {(m, gen)})

    def ack(m, g):
        """The broker accepts an ack only on the current generation (mqtt.rs:332-336)."""
        return (queued - {m}, acked | {m}) if g == gen and g != 0 else (queued, acked)

    for e in buf:
        m, g = e
        if not ack_first and receiving:  # Deliver(e): the frame carries the quiet backlog too
            yield (
                f"Deliver{e}",
                mk(buf=buf - {e}, inbox=inbox | {m} | backlog, backlog=frozenset(), pending=pending | {e}),
            )
        if ack_first and receiving:  # AckFirst(e)
            q, a = ack(m, g)
            yield f"AckFirst{e}", mk(buf=buf - {e}, pending=pending | {e}, queued=q, acked=a)
        if quiet and not ack_first and receiving:  # Quiet(e)
            q, a = ack(m, g)
            yield f"Quiet{e}", mk(buf=buf - {e}, backlog=backlog | {m}, queued=q, acked=a)
    for e in pending:
        m, g = e
        if not ack_first and alive:  # Ack(e)
            q, a = (queued - {m}, acked | {m}) if g == gen and g != 0 and up and owner == "relay" else (queued, acked)
            yield f"Ack{e}", mk(pending=pending - {e}, queued=q, acked=a)
        if ack_first and alive:  # LateInbox(e)
            yield f"LateInbox{e}", mk(pending=pending - {e}, inbox=inbox | {m})

    lost_session_only = frozenset(e for e in buf if e[1] == 0)
    if alive and up and owner == "relay" and gen < max_gen:  # Takeover
        yield "Takeover", mk(owner="cli", gen=gen + 1)
    if alive and up and owner == "cli" and gen < max_gen:  # TakeBack
        yield "TakeBack", mk(owner="relay", gen=gen + 1, buf=lost_session_only)
    if up:  # BrokerStop
        yield "BrokerStop", mk(brokerUp=False, owner="none")
    if not up and gen < max_gen:  # BrokerStart
        yield "BrokerStart", mk(brokerUp=True, owner="relay" if alive else "none", gen=gen + 1, buf=lost_session_only)
    if receiving and not pending and gen < max_gen:  # ReExec
        yield "ReExec", mk(gen=gen + 1, buf=frozenset())
    if alive:  # SessionEnd
        e = frozenset()
        yield "SessionEnd", mk(alive=False, owner="none", buf=e, pending=e, backlog=e)


def holds(s, c):
    """Each invariant's truth in state s, as RelayDelivery.tla defines them."""
    published, queued, sent, buf, pending, inbox, acked, backlog, owner, gen, up, alive = s
    msgs, entries = set(c["Msgs"]), {(m, g) for m in c["Msgs"] for g in range(c["MaxGen"] + 1)}
    return {
        "TypeOK": published <= msgs
        and queued <= published
        and sent <= entries
        and buf <= entries
        and pending <= entries
        and inbox <= msgs
        and acked <= msgs
        and backlog <= msgs
        and owner in ("relay", "cli", "none")
        and 0 <= gen <= c["MaxGen"],
        "NoLoss": all(m in inbox or m in queued for m in published),
        "AckAfterInbox": acked <= inbox,
    }


def explore(c, stop_at=None):
    """Breadth first over every reachable state. Returns (states, {invariant: shortest violating trace})."""
    start = initial()
    parent = {start: None}
    q = deque([start])
    bad = {}
    while q:
        s = q.popleft()
        for inv, ok in holds(s, c).items():
            if not ok and inv not in bad:
                trace, x = [], s
                while x is not None:
                    trace.append(x)
                    x = parent[x][0] if parent[x] else None
                bad[inv] = (s, parent)
                if stop_at == inv:
                    return len(parent), bad
        for action, t in successors(s, c):
            if t not in parent:
                parent[t] = (s, action)
                q.append(t)
    return len(parent), bad


def trace_of(s, parent):
    steps = []
    while parent[s] is not None:
        prev, action = parent[s]
        steps.append((action, s))
        s = prev
    return list(reversed(steps))


def show(s):
    d = dict(zip(FIELDS, s, strict=True))
    return " ".join(f"{k}={sorted(v) if isinstance(v, frozenset) else v}" for k, v in d.items())


def main(argv):
    if argv[:1] == ["--trace"]:
        cfg = HERE / argv[1]
        c, must = load_cfg(cfg)
        inv = argv[2] if len(argv) > 2 else must
        _, bad = explore(c, stop_at=inv)
        if inv not in bad:
            print(f"{argv[1]}: {inv} holds in every reachable state")
            return 1
        s, parent = bad[inv]
        print(f"{argv[1]}: shortest behaviour violating {inv}:")
        print(f"  Init: {show(initial())}")
        for action, t in trace_of(s, parent):
            print(f"  {action}: {show(t)}")
        return 0
    cfgs = [HERE / a for a in argv] or sorted(HERE.glob("*.cfg"))
    failed = False
    for cfg in cfgs:
        c, must = load_cfg(cfg)
        t0 = time.monotonic()
        n, bad = explore(c)
        took = time.monotonic() - t0
        if must:
            ok = must in bad
            verdict = f"fails {must} as expected" if ok else f"UNEXPECTEDLY holds {must}: the spec does not bite"
        else:
            ok = not bad
            verdict = "holds every invariant" if ok else f"VIOLATES {', '.join(sorted(bad))}"
        print(
            f"{cfg.name}: {n} states in {took:.1f} s, {verdict}"
            + (f" (also violated: {', '.join(sorted(set(bad) - {must}))})" if must and set(bad) - {must} else "")
        )
        failed |= not ok
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
