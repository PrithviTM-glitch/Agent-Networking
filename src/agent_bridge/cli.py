"""agent-bridge: the bus from the shell. Every command prints JSON, one object per line; errors are one line on
stderr with a non-zero exit.

Names, addresses, rooms and caps: 1-64 ASCII letters, digits, _ or - (names at most 57, leaving room for the address
suffix), at most 32 caps. TEXT: at most 64 KiB of UTF-8; tab, newline and CR are kept, any other control character
(C0, DEL, C1) is refused. Raw topics: at most 256 bytes of UTF-8, no control characters, never starting with $ or the
reserved levels agents, inbox, ledger, broadcast and rooms (only the dedicated commands touch those). + and # work
only in sub, as whole levels, # only last, and not as the first level. Incoming messages that are over 256 KiB, not
UTF-8, or break these rules are dropped; plain text on a raw topic arrives with "from": null. "from" is checked for
shape only: any local process can claim any address. Room moderation (mode, moderators, mute, ban) is advisory for
the same reason.
"""

import argparse
import contextlib
import fcntl
import json
import os
import sys
import zlib

from . import config, history, presence, relay, rooms
from .broker import WINDOW
from .bus import Bus
from .ledger import client as ledger
from .validation import WAIT_MAX, BridgeError, check_ident, check_session, check_wait, not_admin

QUIET = 0.2


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        sys.exit(f"{self.prog}: {message}")


@contextlib.contextmanager
def _turn(addr):
    """Make this machine's CLI commands for one address take turns instead of taking over each other's session."""
    with open(os.path.join(config.data_dir(), "cli.lock"), "a") as f:
        # One byte per address hash: a single lock file serves every address, collisions only cost waiting.
        fcntl.lockf(f, fcntl.LOCK_EX, 1, zlib.crc32(check_ident(addr, "address").encode()))
        yield


def _show_all(entries):
    for e in entries:
        print(json.dumps(e), flush=True)


def _attached(addr, fn, turn=True):
    not_admin(addr, "address")
    with _turn(addr) if turn else contextlib.nullcontext():
        bus = Bus(addr)
        try:
            return fn(bus)
        finally:
            bus.close()


def _register(a):
    with _turn(a.addr) if a.addr is not None else contextlib.nullcontext():
        bus = Bus.register(a.name, [x for x in a.caps.split(",") if x], a.keep, a.addr, subagent=a.subagent)
        bus.close()
    print(bus.addr)


def _leave(bus):
    if warning := bus.leave():
        print(f"warning: {warning}", file=sys.stderr)


def _ledger(op, *fields):
    def run(a):
        presence.need(not_admin(a.addr, "address"))
        args = {f: getattr(a, f) for f in fields if getattr(a, f) is not None}
        try:
            ledger.call("init", as_=a.addr)
            reply = ledger.call(op, as_=a.addr, **args)
        except ledger.LedgerError as e:
            raise BridgeError(f"ledger: {' '.join(str(e).split())}") from None
        reply.pop("ok", None)
        _show_all([reply])

    return run


def _relay(a):
    if a.action == "start":
        return relay.start()
    session = check_session(os.environ.get("CLAUDE_CODE_SESSION_ID") if a.session is None else a.session)
    if a.action == "stop" and not relay.stop(session):
        raise BridgeError("the relay did not stop within 5 s")
    _show_all([{"session": session, "live": relay.live(session), "lock": relay.lock_path(session)}])


def _recv(bus, wait):
    msgs = bus.recv(max(check_wait(wait), WINDOW))
    while msgs:
        _show_all(msgs)
        msgs = bus.recv(QUIET)


def _parser():
    p = _Parser(prog="agent-bridge", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    s = p.add_subparsers(dest="cmd", required=True, metavar="COMMAND")

    def cmd(name, fn, help, *args):
        r = s.add_parser(name, help=help)
        for flags, options in args:
            r.add_argument(*flags, **options)
        r.set_defaults(fn=fn)

    def on(method, *fields):
        return lambda a: _attached(a.addr, lambda b: getattr(b, method)(*(getattr(a, f) for f in fields)))

    def shown(method, *fields):
        return lambda a: _attached(a.addr, lambda b: _show_all([getattr(b, method)(*(getattr(a, f) for f in fields))]))

    me = (["addr"], {"metavar": "MY_ADDR"})
    room = (["room"], {})
    text = (["text"], {})
    last = (["--last"], {"type": int, "default": 20})
    cmd(
        "register",
        _register,
        "join the bus, prints your address",
        (["name"], {}),
        (["--caps"], {"default": "", "help": "comma-separated things you can do for others"}),
        (["--keep"], {"action": "store_true", "help": f"exempt from expiry after {config.STALE_S / 86400:g}d idle"}),
        (["--addr"], {"help": "reattach to this address if it is live (name, caps, keep unchanged), else create it"}),
        (["--subagent"], {"action": "store_true", "help": "a subagent's own agent: recorded under its parent session"}),
    )
    cmd(
        "leave",
        lambda a: _attached(a.addr, _leave),
        "delete your ledger, session, queued messages, presence and received DMs",
        me,
    )
    cmd(
        "keep",
        lambda a: _attached(a.addr, lambda b: b.keep(a.state == "on")),
        "opt in or out of idle expiry",
        me,
        (["state"], {"choices": ["on", "off"]}),
    )
    cmd(
        "agents",
        lambda a: _show_all(presence.agents(a.room)),
        "every agent with caps and topics, one JSON per line",
        (["--room"], {}),
    )
    cmd("rooms", lambda a: _show_all(rooms.listing()), "every chat room with its members, mode and moderators")
    cmd("join", on("join", "room"), "join a chat room", me, room)
    cmd("part", on("part", "room"), "leave a chat room", me, room)
    cmd("say", on("say", "room", "text"), "post to a chat room; @name, @address and @all mention", me, room, text)
    r = s.add_parser("room", help="a room's settings: info ROOM, or set MY_ADDR ROOM --mode/--rules")
    rs = r.add_subparsers(dest="action", required=True, metavar="ACTION")
    info = rs.add_parser("info", help="mode, moderators, rules, muted and banned; open defaults without settings")
    info.add_argument("room")
    info.set_defaults(fn=lambda a: _show_all([rooms.info(a.room)]))
    settings = rs.add_parser("set", help="change mode or rules (admin or a moderator), the room is told")
    settings.add_argument("addr", metavar="MY_ADDR")
    settings.add_argument("room")
    settings.add_argument("--mode", choices=rooms.MODES, help="open: anyone posts; announce: only moderators")
    settings.add_argument("--rules", help=f"short text, at most {rooms.RULES_MAX} bytes")
    settings.set_defaults(fn=shown("room_set", "room", "mode", "rules"))
    target = (["target"], {"metavar": "ADDR"})
    cmd(
        "mod",
        shown("mod", "room", "change", "target"),
        "add or remove a moderator of a room (admin or a moderator)",
        me,
        room,
        (["change"], {"choices": ["add", "remove"]}),
        target,
    )
    for verb, help in (
        ("mute", "stop ADDR posting in a room"),
        ("unmute", "let ADDR post in a room again"),
        ("ban", "stop ADDR posting in or joining a room"),
        ("unban", "lift a ban"),
        ("kick", "ban ADDR and drop it from the room (briefly takes its session over, its relay reconnects)"),
    ):
        cmd(verb, shown(verb, "room", "target"), f"{help} (admin or a moderator)", me, room, target)
    cmd(
        "history",
        lambda a: _show_all(history.history(a.room, a.last)),
        "a chat room's transcript, to catch up after joining",
        room,
        last,
    )
    cmd("dm", on("dm", "to", "text"), "direct message one agent", me, (["to"], {}), text)
    cmd(
        "dms",
        lambda a: _show_all(history.dms(a.addr, a.peer, a.last)),
        "your direct messages, both directions",
        me,
        (["--with"], {"dest": "peer"}),
        last,
    )
    cmd("sub", on("sub", "topic"), "subscribe to a raw topic, + and # wildcards work", me, (["topic"], {}))
    cmd("unsub", on("unsub", "topic"), "unsubscribe from a raw topic", me, (["topic"], {}))
    cmd("pub", on("pub", "topic", "text"), "publish to a raw topic", me, (["topic"], {}), text)
    # A waiting recv can be long, so it does not take a turn: other commands take its session over and it reconnects.
    cmd(
        "recv",
        lambda a: _attached(a.addr, lambda b: _recv(b, a.wait), turn=a.wait == 0),
        "new room, DM and topic messages except your own, one JSON per line",
        me,
        (["--wait"], {"type": float, "default": 0, "help": f"block up to N seconds (max {WAIT_MAX}) for a message"}),
    )
    cmd(
        "relay",
        _relay,
        "this Claude Code session's relay into its inbox: start (the SessionStart hook), stop, status",
        (["action"], {"choices": ["start", "stop", "status"], "nargs": "?", "default": "start"}),
        (["--session"], {"help": "session id, default CLAUDE_CODE_SESSION_ID"}),
    )
    name = (["name"], {"help": "resource name, your resource id is MY_ADDR/NAME"})
    level = (["level"], {"choices": ["read", "write"]})
    ttl = (["--ttl"], {"type": float, "help": "lease seconds, default 60"})
    details = (
        (["--description"], {}),
        (["--visibility"], {"choices": ["public", "private"]}),
        (["--max-level"], {"choices": ["read", "write"], "help": "highest level granted automatically if public"}),
    )
    resource_fields = ("name", "description", "visibility", "max_level")
    cmd("add", _ledger("add", *resource_fields), "ledger: add a resource you own", me, name, *details)
    cmd("update", _ledger("update", *resource_fields), "ledger: change one of your resources", me, name, *details)
    cmd("remove", _ledger("remove", "name"), "ledger: delete one of your resources", me, name)
    cmd(
        "ledger",
        _ledger("ledger", "owner"),
        "ledger: your resources, or another agent's public ones",
        me,
        (["--owner"], {}),
    )
    cmd("public", _ledger("public"), "ledger: every public resource of every agent", me)
    cmd(
        "request",
        _ledger("request", "resource", "level"),
        "ledger: ask for access to OWNER/NAME",
        me,
        (["resource"], {}),
        level,
    )
    cmd(
        "grant",
        _ledger("grant", "request_id"),
        "ledger: grant a pending request on your resource",
        me,
        (["request_id"], {}),
    )
    cmd(
        "deny",
        _ledger("deny", "request_id"),
        "ledger: deny or revoke a request on your resource",
        me,
        (["request_id"], {}),
    )
    cmd("grants", _ledger("grants"), "ledger: requests you made and requests on your resources", me)
    cmd(
        "acquire",
        _ledger("acquire", "resource", "level", "ttl", "wait"),
        "ledger: take a lease, read is shared, write exclusive with a fencing token",
        me,
        (["resource"], {}),
        level,
        ttl,
        (["--wait"], {"type": float, "help": "seconds to queue if busy, default 0"}),
    )
    cmd("renew", _ledger("renew", "lease_id", "ttl"), "ledger: extend a lease", me, (["lease_id"], {}), ttl)
    cmd("release", _ledger("release", "lease_id"), "ledger: give a lease back", me, (["lease_id"], {}))
    return p


def main():
    a = _parser().parse_args()
    try:
        a.fn(a)
    except BridgeError as e:
        sys.exit(str(e))
    except KeyboardInterrupt:
        sys.exit(130)
    except BrokenPipeError:
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        sys.exit(1)
    except Exception as e:
        sys.exit(f"{type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
