"""Where the machine's capacity goes: CPU and memory per Claude session and per process, read from /proc only.

CPU is the share of one core between two samples, so a busy process on a 4-core machine can show up to 400%. Memory
is resident set size; pages shared between processes are counted in each of them, so sums overstate a little."""

import os
import threading
import time

from . import processes, sessions

CLK = os.sysconf("SC_CLK_TCK")
FRESH_S = 1.0
TOP, TREE = 15, 12
lock = threading.Lock()
state = {"prev": {}, "at": None, "snap": None}


def machine():
    """Load averages, core count and memory of the whole machine."""
    out = {"cpus": os.cpu_count() or 1, "load": None, "mem_total": None, "mem_available": None}
    try:
        with open("/proc/loadavg") as f:
            out["load"] = [float(x) for x in f.read().split()[:3]]
        with open("/proc/meminfo") as f:
            mem = {k: int(v.split()[0]) * 1024 for k, _, v in (line.partition(":") for line in f) if v.strip()}
        out["mem_total"], out["mem_available"] = mem.get("MemTotal"), mem.get("MemAvailable")
    except (OSError, ValueError):
        pass
    return out


def _sample():
    """Every process with its CPU percentage since the previous sample (None on the first one)."""
    procs, now = processes.table(), time.monotonic()
    prev, at = state["prev"], state["at"]
    dt = now - at if at else 0
    for pid, i in procs.items():
        before = prev.get((pid, i["start"]))
        i["pct"] = round((i["cpu"] - before) * 100 / (dt * CLK), 1) if before is not None and dt > 0 else None
    state["prev"], state["at"] = {(pid, i["start"]): i["cpu"] for pid, i in procs.items()}, now
    return procs


def _row(pid, i, owner=None):
    return {
        "pid": pid,
        "cmd": " ".join(i["argv"])[:160] or f"[{i['comm']}]",
        "comm": i["comm"],
        "cpu": i["pct"],
        "rss": i["rss"],
        "mine": i["uid"] == os.getuid(),
        "session": owner,
    }


def snapshot():
    """Machine totals, every live Claude session with its whole process tree summed, and the top processes."""
    with lock:
        if state["snap"] and time.monotonic() - state["at"] < FRESH_S:
            return state["snap"]
        procs = _sample()
        owners, rows = {}, []
        # One row per claude process: a session's own agent and the subagents sharing its pid are one tree.
        for v in sorted(sessions.entities(), key=lambda v: (not v["in_registry"], not v["on_bus"])):
            pid = v["pid"]
            if not v["alive"] or pid in owners or pid not in procs:
                continue
            tree = [pid, *processes.descendants(pid, procs)]
            for p in tree:
                owners.setdefault(p, v["key"])
            parts = sorted(tree, key=lambda p: -(procs[p]["pct"] or 0))
            rows.append(
                {
                    "key": v["key"],
                    "friendly": v["friendly"],
                    "kind": v["kind"],
                    "pid": pid,
                    "cpu": round(sum(procs[p]["pct"] or 0 for p in tree), 1),
                    "rss": sum(procs[p]["rss"] for p in tree),
                    "procs": len(tree),
                    "tree": [_row(p, procs[p]) for p in parts[:TREE]],
                }
            )
        top = sorted(procs, key=lambda p: (-(procs[p]["pct"] or 0), -procs[p]["rss"]))[:TOP]
        state["snap"] = {
            "ts": time.time(),
            "machine": machine(),
            "sessions": sorted(rows, key=lambda r: (-r["cpu"], -r["rss"])),
            "top": [_row(p, procs[p], owners.get(p)) for p in top],
            "first": all(i["pct"] is None for i in procs.values()),
        }
        return state["snap"]
