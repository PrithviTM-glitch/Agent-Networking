"""The Bus: one agent's persistent MQTT connection, with client id = its address, so it owns the agent's session.

Incoming messages are buffered by the network thread and acked only when recv hands them out, so a takeover or a
crash never loses them. Every failure raises BridgeError (or LedgerError from ledger) with a one-line message.
"""

import collections
import contextlib
import json
import math
import threading
import time
import uuid

from . import broker, config, history, identity, presence, rooms
from .ledger import client as ledger
from .protocol import BROADCAST, parse_message
from .validation import (
    ADMIN,
    ID_MAX,
    NAME_MAX,
    TOPICS_MAX,
    BridgeError,
    check_caps,
    check_ident,
    check_text,
    check_topic,
    check_wait,
    is_ident,
    not_admin,
)

TOUCH_S = 60
BUFFER_MAX = 1000


class Bus:
    """One agent on the bus. Bus(addr) attaches to a live address; Bus.register joins the bus."""

    agents = staticmethod(presence.agents)
    room_info = staticmethod(rooms.info)  # before the next line, whose attribute shadows the rooms module here
    rooms = staticmethod(rooms.listing)
    history = staticmethod(history.history)

    def __init__(self, addr, rec=None, owner=True):
        """Attach to a live address; rec is only for register. owner=False publishes as the agent over a connection
        of its own, leaving the agent's session (subscriptions, queued messages) to the process that owns it."""
        self.addr = check_ident(addr, "address")
        self.owner = owner
        self._rooms = {}  # room -> record, kept current by the rooms/+ subscription
        self._synced = threading.Event()  # set once the retained room records of this connection are all in
        self._barrier = None
        self.rec = rec or presence.need(addr)
        self._buf = collections.deque()  # (mid to ack or None, message)
        self._acks = {}
        self._cv = threading.Condition()
        self._up = threading.Event()
        self._touched = -math.inf
        self.on_arrival = None  # called from the network thread after each buffered message
        self._admin = False
        self._open()
        if rec is None and any(self.rec.get(k) != v for k, v in self._fresh_pid().items()):
            self._touch(refresh=False)

    @classmethod
    def register(cls, name, caps=(), keep=False, addr=None, admin=False, owner=True, subagent=False):
        """Join the bus. With addr, reattach to it if it is live (its record stays as is), else create it.
        admin=True is only for the console: Bus.register("admin", addr="admin", keep=True, admin=True).
        subagent=True marks a subagent's own agent: "kind": "subagent" and the creating session as "parent" instead
        of "session", so neither the session's relay nor the console mistakes it for the session itself."""
        if admin and ((name, addr) != (ADMIN, ADMIN) or subagent):
            raise BridgeError("admin=True is only for name and address admin")
        if not admin:
            not_admin(name, "name")
            not_admin(addr, "address")
        rec = {
            "name": check_ident(name, "name", ID_MAX if addr else NAME_MAX),
            "caps": check_caps(caps),
            "topics": [],
            "keep": bool(keep),
        }
        # Origin fields are written here, once; later attaches only refresh pid and pid_start (see _fresh_pid).
        here = {} if admin else identity.origin()
        if subagent:
            rec |= {"kind": "subagent", **({"parent": here["session"]} if "session" in here else {})}
            here = {k: v for k, v in here.items() if k != "session"}
        rec |= here
        if addr is not None and (live := presence.find(addr)):
            rec = live
        elif addr is not None:
            presence.wipe(addr)
        else:
            addr = f"{name}-{uuid.uuid4().hex[:6]}"
        history.prune()
        bus = cls(addr, rec, owner)
        bus._admin = admin
        bus._touch(refresh=False)
        return bus

    def _open(self):
        c = self._c = broker.new_client(self.addr if self.owner else "", self._on_message, manual_ack=True)
        c.on_connect = self._on_connect
        c.on_disconnect = lambda *_: (self._up.clear(), self._synced.clear())
        c.on_subscribe = c.on_unsubscribe = self._on_ack
        # mosquitto only writes a session to disk if its latest connect had clean_start=False, even under MQTT 5.
        broker.connect(c, not self.owner, presence.expiry(self.rec) if self.owner else 0, self._up)

    def _on_connect(self, c, _u, flags, rc, _p):
        if rc.is_failure:
            return
        # The retained room records follow this SUBACK on the wire, and the ack of the next request follows them:
        # once that ack is back the room cache is complete.
        c.subscribe(rooms.TOPICS, options=rooms.SUB)
        if self.owner:
            with self._cv:
                # A surviving session resends everything not yet acked; a lost one never will, so keep those unacked.
                self._buf = collections.deque(
                    (None, m) for mid, m in self._buf if mid is None or not flags.session_present
                )
            # broadcast on every connect, so agents whose sessions predate it pick it up on their next attach.
            fresh = () if flags.session_present else (f"inbox/{self.addr}", *self.rec["topics"])
            _, self._barrier = c.subscribe([(t, broker.SUB) for t in (BROADCAST, *fresh)])
        else:
            _, self._barrier = c.unsubscribe(f"inbox/{self.addr}")  # never subscribed here: only the barrier
        self._up.set()

    def _on_message(self, c, _u, m):
        try:
            topic = m.topic
        except ValueError:
            return c.ack(m.mid, m.qos)
        if topic.startswith("rooms/"):
            if is_ident(room := topic[6:]):
                with self._cv:
                    if rec := rooms.parse_room(m.payload):
                        self._rooms[room] = rec
                    else:
                        self._rooms.pop(room, None)
            return c.ack(m.mid, m.qos)
        msg = parse_message(topic, m.payload)
        with self._cv:
            # Unacked QoS 1 messages are already capped by ReceiveMaximum, only QoS 0 floods need BUFFER_MAX.
            if msg and (m.qos or len(self._buf) < BUFFER_MAX):
                self._buf.append((m.mid if m.qos else None, msg))
                self._cv.notify_all()
                if self.on_arrival:
                    self.on_arrival()
                return
        c.ack(m.mid, m.qos)

    def _on_ack(self, _c, _u, mid, codes, _p):
        with self._cv:
            self._acks[mid] = not any(r.is_failure for r in codes)
            self._cv.notify_all()
        if mid == self._barrier:
            self._synced.set()

    def _ready(self):
        if not (self._up.wait(broker.TIMEOUT) and self._synced.wait(broker.TIMEOUT)):
            raise BridgeError("not connected to the broker")

    def _touch(self, refresh=True):
        if refresh:
            self.rec = presence.find(self.addr) or self.rec
        self.rec |= self._fresh_pid()
        self.rec["seen"] = time.time()
        self._ready()
        broker.publish(
            self._c,
            f"agents/{self.addr}",
            json.dumps(self.rec),
            retain=True,
            expire=None if self.rec["keep"] else config.STALE_S,
        )
        self._touched = time.monotonic()

    def _fresh_pid(self):
        """pid and pid_start of this process's claude, when it runs in the very session the record names (its
        session, or a subagent's parent); else nothing. Any other process acting as the agent, a moderator's kick
        among them, must not rewrite who the agent belongs to."""
        here = identity.origin()
        owner = self.rec.get("session") or self.rec.get("parent")
        if self._admin or not owner or "pid" not in here or here.get("session", "").lower() != owner.lower():
            return {}
        return {"pid": here["pid"], "pid_start": here["pid_start"]}

    def _owner_does(self, method, *args):
        """Subscriptions and session expiry live in the agent's session: change them through a brief owner
        connection, which takes the session over from its owner until that reconnects."""
        b = Bus(self.addr)
        try:
            result = getattr(b, method)(*args)
        finally:
            b.close()
        self.rec = b.rec
        return result

    def _subscribe(self, topic, on):
        """Subscribe or unsubscribe; True if that changed the agent's topics."""
        if not self.owner:
            return self._owner_does("_subscribe", topic, on)
        topics = set((presence.find(self.addr) or self.rec)["topics"])
        if on and topic not in topics and len(topics) >= TOPICS_MAX:
            raise BridgeError(f"at most {TOPICS_MAX} subscriptions")
        self._ready()
        rc, mid = self._c.subscribe(topic, options=broker.SUB) if on else self._c.unsubscribe(topic)
        with self._cv:
            if rc or not self._cv.wait_for(lambda: mid in self._acks, broker.TIMEOUT) or not self._acks.pop(mid):
                raise BridgeError(f"broker refused or did not confirm {'sub' if on else 'unsub'} {topic}")
        self.rec["topics"] = sorted(topics | {topic} if on else topics - {topic})
        self._touch(refresh=False)
        return (topic in topics) != on

    def _announce(self, room, verb):
        if rooms.refusal(self._rooms.get(room), self.addr):
            return
        caps = self.rec["caps"]
        self._send(
            f"room/{room}", f"[{verb} #{room}] {self.rec['name']}" + (f", caps: {', '.join(caps)}" if caps else "")
        )

    def _send(self, topic, body, mentions=None):
        entry = {"from": self.addr, "body": check_text(body), "ts": time.time()}
        if mentions:
            entry["mentions"] = mentions
        self._ready()
        broker.publish(self._c, topic, json.dumps(entry))
        history.append(topic, entry)

    def ledger(self, op, **fields):
        """One ledgerd request as this agent, over this connection; raises LedgerError."""
        return ledger.call(op, as_=self.addr, client=self._c, **fields)

    def leave(self):
        """Delete this agent: its ledger, session, queued messages, presence and received DMs in the log. Its rooms
        get a "[left #room]" message.
        Returns a warning if ledgerd could not drop the ledger; the agent leaves the bus regardless."""
        warning = _drop_ledger(lambda: self.ledger("drop", timeout=3))
        for topic in (presence.find(self.addr) or self.rec)["topics"]:
            if topic.startswith("room/"):
                with contextlib.suppress(BridgeError):
                    self._announce(topic[5:], "left")
        self.close()
        presence.wipe(self.addr)
        history.prune(drop_topic=f"inbox/{self.addr}")
        return warning

    def describe(self, name=None, caps=None):
        """Change how other agents see this one; the address stays."""
        if self.addr != ADMIN:
            not_admin(name, "name")
        self.rec = presence.find(self.addr) or self.rec
        self.rec["name"] = self.rec["name"] if name is None else check_ident(name, "name")
        self.rec["caps"] = self.rec["caps"] if caps is None else check_caps(caps)
        self._touch(refresh=False)

    def keep(self, on):
        """on=True exempts the agent from idle expiry, False opts back in."""
        if not self.owner:
            return self._owner_does("keep", on)
        self.rec = presence.find(self.addr) or self.rec
        self.rec["keep"] = bool(on)
        self._touch(refresh=False)
        self.close()
        self._open()  # the session expiry interval only changes on connect

    def join(self, room):
        """Join a room; its members get a "[joined #room]" message unless this agent was already in it."""
        self._ready()
        if self.addr in self._rooms.get(check_ident(room, "room"), rooms.OPEN)["banned"]:
            raise BridgeError(f"you are banned from #{room}")
        if self._subscribe(f"room/{room}", True):
            self._announce(room, "joined")

    def part(self, room):
        """Leave a room; its members get a "[left #room]" message unless this agent was not in it."""
        if self._subscribe(f"room/{check_ident(room, 'room')}", False):
            self._announce(room, "left")

    def sub(self, topic):
        self._subscribe(check_topic(topic, filter=True), True)

    def unsub(self, topic):
        self._subscribe(check_topic(topic, filter=True), False)

    def broadcast(self, body):
        """Send body to every agent; only the console's Bus, registered with admin=True, may."""
        if not self._admin:
            raise BridgeError("only the admin console can broadcast")
        self._send(BROADCAST, body)

    def say(self, room, body):
        """Post to a room. @address or @name (every member with that name) mentions members: their relays wake for
        it, other relays keep it quiet. @all mentions nobody in particular, like no mention."""
        self._ready()
        if why := rooms.refusal(self._rooms.get(check_ident(room, "room")), self.addr):
            raise BridgeError(f"you may not post in #{room}: {why}")
        tagged = rooms.MENTION.search(check_text(body))
        self._send(f"room/{room}", body, tagged and rooms.mentions(room, body, presence.agents(room)))

    def dm(self, to, body):
        check_text(body)
        presence.need(to)
        self._send(f"inbox/{to}", body)

    def pub(self, topic, body):
        self._send(check_topic(topic), body)

    def dms(self, peer=None, last=20):
        return history.dms(self.addr, peer, last)

    def pending(self):
        with self._cv:
            return len(self._buf)

    def recv(self, wait=0, deliver=None, limit=None):
        """Drain up to limit buffered messages, blocking up to wait seconds for the first. They are acked once
        returned, or with deliver once deliver(messages) returns; if it raises they go back to the buffer."""
        check_wait(wait)
        if not self.owner:
            raise BridgeError("this connection does not own the agent's session, its owner receives the messages")
        self._ready()  # while reconnecting, buffered messages may still be resent, so wait until on_connect sorted them
        if time.monotonic() - self._touched > TOUCH_S:
            self._touch()
        with self._cv:
            self._cv.wait_for(lambda: self._buf, wait)
            got = [self._buf.popleft() for _ in range(min(len(self._buf), limit or len(self._buf)))]
        msgs = [
            {**m, "for_me": self.addr in m["mentions"]} if "mentions" in m else m for _, m in got if self._allowed(m)
        ]
        if deliver and msgs:
            try:
                deliver(msgs)
            except BaseException:
                with self._cv:
                    self._buf.extendleft(reversed(got))
                raise
        for mid, _ in got:
            if mid is not None:
                self._c.ack(mid, 1)
        return msgs

    def _allowed(self, m):
        """False for a room message its room's record forbids: from a banned or muted address, or a non-moderator in
        an announce room. Spoofed or raw publishes are dropped too, unless they claim a moderator's address."""
        return not m["topic"].startswith("room/") or not rooms.refusal(self._rooms.get(m["topic"][5:]), m["from"])

    def _moderate(self, room, change):
        """Apply change(record) -> (fields, notes) to room's record as this agent, if it may, then post each note to
        the room as "<note> by <this address>". Returns the room's new settings."""
        room = check_ident(room, "room")
        self._ready()
        current = self._rooms.get(room)
        if not rooms.is_moderator(current, self.addr):
            raise BridgeError(
                f"only admin or a moderator of #{room} may change it" + ("" if current else ", admin names the first")
            )
        fields, notes = change({**rooms.OPEN, **(current or {})})
        rec = rooms.check_record({**rooms.OPEN, **(current or {}), **fields, "by": self.addr, "ts": time.time()})
        broker.publish(self._c, f"rooms/{room}", json.dumps(rec), retain=True)
        with self._cv:
            self._rooms[room] = rec
        for note in notes:
            self._send(f"room/{room}", f"{note} by {self.addr}")
        return rooms.view(room, rec)

    def room_set(self, room, mode=None, rules=None):
        """Change a room's mode ("open" or "announce") and/or its rules text; admin or a moderator only."""
        if mode is None and rules is None:
            raise BridgeError("nothing to change: give mode or rules")

        def change(rec):
            fields, notes = {}, []
            if mode is not None:
                fields["mode"] = rooms.check_mode(mode)
                notes.append(f"[mode #{room}] {mode}")
            if rules is not None:
                fields["rules"] = rooms.check_rules(rules)
                notes.append(f"[rules #{room}] updated")
            return fields, notes

        return self._moderate(room, change)

    def mod(self, room, action, addr):
        """action "add" or "remove" addr as a moderator of room."""
        if action not in ("add", "remove"):
            raise BridgeError("action must be add or remove")
        return self._listed(room, addr, "moderators", action == "add", "moderator", "+" if action == "add" else "-")

    def mute(self, room, addr):
        return self._listed(room, addr, "muted", True, "muted")

    def unmute(self, room, addr):
        return self._listed(room, addr, "muted", False, "unmuted")

    def ban(self, room, addr):
        return self._listed(room, addr, "banned", True, "banned")

    def unban(self, room, addr):
        return self._listed(room, addr, "banned", False, "unbanned")

    def kick(self, room, addr):
        """Ban addr from room and drop its subscription. That takes a brief connection with addr's client id, which
        takes its session over: its relay or MCP server reconnects about a second later."""
        settings = self._listed(room, addr, "banned", True, "kicked")
        if presence.find(addr):
            target = Bus(addr)
            try:
                target._subscribe(f"room/{room}", False)
            finally:
                target.close()
        return settings

    def _listed(self, room, addr, field, on, verb, sign=""):
        target = check_ident(addr, "address")
        if target.lower() == ADMIN:
            raise BridgeError("admin moderates every room and cannot be moderated")

        def change(rec):
            return {field: sorted(set(rec[field]) | {target} if on else set(rec[field]) - {target})}, [
                f"[{verb} #{room}] {sign}{target}"
            ]

        return self._moderate(room, change)

    def close(self):
        """Disconnect; the session and anything not yet received stay with the broker."""
        broker.close(self._c)
        self._up.clear()


def _drop_ledger(call):
    try:
        call()
    except ledger.LedgerError as e:
        if not str(e).startswith("no ledger for"):
            return f"ledger not dropped: {e}"
    return None
