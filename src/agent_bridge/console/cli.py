"""agent-bridge-admin: the admin console from the shell, through the running console's API.

It reads the console's token file and sends the same Host, Origin and token headers the web page does, so every
action is checked and audited by the console exactly as a click would be. It never opens a bus connection: the
console alone holds the admin identity. Output is one JSON object per line; errors are one line with exit status 1.
Kills and stops only preview unless --confirm repeats the room or address."""

import argparse
import http.client
import json
import os
import re
import sys
import time
import urllib.parse

from . import auth


class Failed(Exception):
    """A one-line reason the command did not work."""


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        sys.exit(f"{self.prog}: {message}")


def request(method, path, body=None, raw=False):
    try:
        with open(auth.TOKEN_FILE) as f:
            token = f.read().strip()
    except OSError:
        raise Failed(f"no admin token at {auth.TOKEN_FILE}: start agent-bridge-console once to create it") from None
    host = f"127.0.0.1:{auth.PORT}"
    headers = {"Host": host, "X-Admin-Token": token}
    data = None
    if body is not None:
        headers |= {"Origin": f"http://{host}", "Content-Type": "application/json"}
        data = json.dumps(body).encode()
    c = http.client.HTTPConnection("127.0.0.1", auth.PORT, timeout=120)
    try:
        c.request(method, path, body=data, headers=headers)
        r = c.getresponse()
        out = r.read()
    except ConnectionRefusedError:
        raise Failed(f"no admin console on {host}: start it with agent-bridge-console (or --headless)") from None
    except OSError as e:
        raise Failed(f"admin console on {host}: {e}") from None
    finally:
        c.close()
    if raw and r.status == 200:
        return out.decode()
    try:
        reply = json.loads(out)
    except ValueError:
        raise Failed(f"admin console on {host} answered HTTP {r.status}") from None
    if r.status != 200:
        raise Failed(reply.get("error") or f"HTTP {r.status}")
    return reply


def get(path, **query):
    query = {k: v for k, v in query.items() if v is not None}
    return request("GET", path + ("?" + urllib.parse.urlencode(query) if query else ""))


def post(path, **body):
    return request("POST", path, {k: v for k, v in body.items() if v is not None})


def show(*rows):
    for row in rows:
        print(json.dumps(row), flush=True)


AGENT_FIELDS = ("addr", "session", "parent", "friendly", "kind", "subagent", "alive", "status", "rooms", "pid")


def since(text):
    """A unix time, or a duration back from now such as 90s, 30m, 6h or 2d."""
    if m := re.fullmatch(r"(\d+(?:\.\d+)?)([smhd])", text or ""):
        return time.time() - float(m[1]) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[m[2]]
    try:
        return float(text)
    except (TypeError, ValueError):
        raise Failed(f"bad time {text!r}: use a unix time or a duration like 30m, 6h or 2d") from None


# commands


def status(_a):
    st = get("/api/state")
    agents = st["agents"]
    rooms = {r for a in agents for r in a["rooms"]} | set(st["rooms"])
    show(
        {
            "connected": st["connected"],
            "on_bus": sum(a["on_bus"] for a in agents),
            "sessions": len(agents),
            "alive": sum(a["alive"] is True for a in agents),
            "rooms": len(rooms),
            "messages": len(st["messages"]),
            "jobs_running": sum(j["state"] == "running" for j in st["jobs"]),
        }
    )


def capacity(_a):
    c = get("/api/capacity")
    show({"machine": c["machine"]}, *({k: v for k, v in row.items() if k != "tree"} for row in c["sessions"]))


def leftovers(_a):
    d = get("/api/leaks")
    show({"busy": d["busy"]}, *({"container": c} for c in d["containers"]), *({"process": p} for p in d["processes"]))


def agents(_a):
    show(*({k: a.get(k) for k in AGENT_FIELDS} for a in get("/api/state")["agents"]))


def _rooms(st):
    members = {}
    for a in st["agents"]:
        for r in a["rooms"]:
            members.setdefault(r, []).append(a["addr"])
    for room in sorted(set(members) | set(st["rooms"])):
        rec = st["rooms"].get(room, {})
        yield {
            "room": room,
            "members": sorted(members.get(room, [])),
            "mode": rec.get("mode", "open"),
            "moderators": rec.get("moderators", []),
        }


def rooms(_a):
    show(*_rooms(get("/api/state")))


def room(a):
    info = get("/api/room", room=a.room)
    members = next((r["members"] for r in _rooms(get("/api/state")) if r["room"] == a.room), [])
    show({**info, "members": members})


def say(a):
    show(post("/api/send", kind="room", target=a.room, text=a.text))


def dm(a):
    show(post("/api/send", kind="dm", target=a.addr, text=a.text))


def broadcast(a):
    show(post("/api/broadcast", text=a.text))


def member(a):
    show(post("/api/room/member", room=a.room, action=a.action, addr=a.addr))


def mod(a):
    show(post("/api/room/moderator", room=a.room, action=a.action, addr=a.addr))


def room_set(a):
    if a.mode is None and a.rules is None:
        raise Failed("give --mode and/or --rules")
    show(post("/api/room/settings", room=a.room, mode=a.mode, rules=a.rules))


def _kill(path, scope, a, only=None):
    options = {"signal": a.signal, "kill_sessions": a.sessions}
    if a.confirm is None:
        preview = get(path, **scope, signal=a.signal, kill_sessions=str(a.sessions).lower())
        targets = [t for t in preview["targets"] if only is None or t["addr"] in only]
        show(*({"preview": t} for t in targets))
        name = scope.get("room") or scope.get("addr")
        print(f"preview only: add --confirm {name} to do this", file=sys.stderr)
        return
    reply = post(path, **scope, confirm=a.confirm, only=only, **options)
    show(*({"target": t} for t in reply["targets"]), {"result": reply["result"]})


def kill_room(a):
    only = [x for x in a.only.split(",") if x] if a.only else None
    _kill("/api/room/kill", {"room": a.room}, a, only)


def stop_agent(a):
    _kill("/api/agent/stop", {"addr": a.addr}, a)


def signal_(a):
    target = {"session": a.target} if a.session else {"addr": a.target}
    show(post("/api/signal", **target, signal=a.signal))


def cleanup(a):
    if a.kind == "container":
        if len(a.ids) != 1:
            raise Failed("cleanup container NAME")
        show(post("/api/leaks/container", name=a.ids[0]))
    else:
        if len(a.ids) != 2 or not all(x.isdigit() for x in a.ids):
            raise Failed("cleanup process PID START (both numbers, from leftovers)")
        show(post("/api/leaks/process", pid=int(a.ids[0]), start=int(a.ids[1])))


def audit(a):
    reply = get(
        "/api/audit",
        action=a.action,
        target=a.target,
        since=a.since and since(a.since),
        until=a.until and since(a.until),
    )
    show(*reply["entries"][-a.last :])


def metrics(_a):
    sys.stdout.write(request("GET", "/metrics", raw=True))


def main():
    p = _Parser(prog="agent-bridge-admin", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    s = p.add_subparsers(dest="cmd", required=True, metavar="COMMAND")

    def cmd(name, fn, help, *args):
        r = s.add_parser(name, help=help)
        for flags, kw in args:
            r.add_argument(*flags, **kw)
        r.set_defaults(fn=fn)

    text, rm, addr = (["text"], {}), (["room"], {}), (["addr"], {})
    signal_opt = (["--signal"], {"choices": ["terminate", "kill"], "default": "terminate"})
    whole = (["--sessions"], {"action": "store_true", "help": "also kill whole Claude sessions (their primary agent)"})
    cmd("status", status, "console connection, counts of agents, sessions, rooms and messages")
    cmd("capacity", capacity, "machine load and memory, CPU and memory per Claude session")
    cmd("leftovers", leftovers, "leftover test containers and orphaned agent-bridge processes")
    cmd("agents", agents, "every agent and Claude session")
    cmd("rooms", rooms, "every room with members, mode and moderators")
    cmd("room", room, "one room's settings and members", rm)
    cmd("say", say, "post to a room as admin", rm, text)
    cmd("dm", dm, "direct message an agent as admin", addr, text)
    cmd("broadcast", broadcast, "broadcast to every agent and session", text)
    for action in ("mute", "unmute", "kick", "ban", "unban"):
        cmd(action, member, f"{action} an agent in a room", rm, addr)
        s.choices[action].set_defaults(action=action)
    cmd("mod", mod, "add or remove a room moderator", (["action"], {"choices": ["add", "remove"]}), rm, addr)
    cmd(
        "room-set",
        room_set,
        "change a room's mode or rules",
        rm,
        (["--mode"], {"choices": ["open", "announce"]}),
        (["--rules"], {}),
    )
    cmd(
        "kill-room",
        kill_room,
        "stop a room's agents (preview unless --confirm ROOM)",
        rm,
        (["--confirm"], {"metavar": "ROOM"}),
        whole,
        signal_opt,
        (["--only"], {"metavar": "ADDR,ADDR", "help": "only these members"}),
    )
    cmd(
        "stop-agent",
        stop_agent,
        "stop one agent (preview unless --confirm ADDR)",
        addr,
        (["--confirm"], {"metavar": "ADDR"}),
        whole,
        signal_opt,
    )
    cmd(
        "signal",
        signal_,
        "signal a live Claude session",
        (["target"], {"help": "address, or session id with --session"}),
        (["signal"], {"choices": ["interrupt", "terminate", "kill"]}),
        (["--session"], {"action": "store_true"}),
    )
    cmd(
        "cleanup",
        cleanup,
        "remove a leftover: cleanup container NAME | cleanup process PID START",
        (["kind"], {"choices": ["container", "process"]}),
        (["ids"], {"nargs": "+"}),
    )
    cmd(
        "audit",
        audit,
        "audit log entries, oldest first",
        (["--action"], {}),
        (["--target"], {}),
        (["--since"], {"help": "unix time or a duration back, like 6h"}),
        (["--until"], {}),
        (["--last"], {"type": int, "default": 100}),
    )
    cmd("metrics", metrics, "the console's Prometheus metrics")
    a = p.parse_args()
    try:
        a.fn(a)
    except Failed as e:
        sys.exit(str(e))
    except KeyboardInterrupt:
        sys.exit(130)
    except BrokenPipeError:
        # The reader went away (| head): point stdout at /dev/null so the exit flush does not complain.
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        sys.exit(1)


if __name__ == "__main__":
    main()
