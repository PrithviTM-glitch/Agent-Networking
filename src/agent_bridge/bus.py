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

from . import broker, config, history, identity, presence
from .ledger import client as ledger
from .protocol import BROADCAST, ORIGIN, parse_message
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
    not_admin,
)

TOUCH_S = 60
BUFFER_MAX = 1000


class Bus:
    """One agent on the bus. Bus(addr) attaches to a live address; Bus.register joins the bus."""

    agents = staticmethod(presence.agents)
    rooms = staticmethod(presence.rooms)
    history = staticmethod(history.history)

    def __init__(self, addr, rec=None, owner=True):
        """Attach to a live address; rec is only for register. owner=False publishes as the agent over a connection
        of its own, leaving the agent's session (subscriptions, queued messages) to the process that owns it."""
        self.addr = check_ident(addr, "address")
        self.owner = owner
        self.rec = rec or presence.need(addr)
        self._buf = collections.deque()  # (mid to ack or None, message)
        self._acks = {}
        self._cv = threading.Condition()
        self._up = threading.Event()
        self._touched = -math.inf
        self.on_arrival = None  # called from the network thread after each buffered message
        self._admin = False
        self._open()
        if rec is None and {k: self.rec[k] for k in ORIGIN if k in self.rec} != identity.origin():
            self._touch(refresh=False)

    @classmethod
    def register(cls, name, caps=(), keep=False, addr=None, admin=False, owner=True):
        """Join the bus. With addr, reattach to it if it is live (its record stays as is), else create it.
        admin=True is only for the console: Bus.register("admin", addr="admin", keep=True, admin=True)."""
        if admin and (name, addr) != (ADMIN, ADMIN):
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
        c.on_disconnect = lambda *_: self._up.clear()
        c.on_subscribe = c.on_unsubscribe = self._on_ack
        # mosquitto only writes a session to disk if its latest connect had clean_start=False, even under MQTT 5.
        broker.connect(c, not self.owner, presence.expiry(self.rec) if self.owner else 0, self._up)

    def _on_connect(self, c, _u, flags, rc, _p):
        if rc.is_failure:
            return
        if not self.owner:
            return self._up.set()
        with self._cv:
            # A surviving session resends everything not yet acked; a lost one never will, so keep those unacked.
            self._buf = collections.deque((None, m) for mid, m in self._buf if mid is None or not flags.session_present)
        # broadcast on every connect, so agents whose sessions predate it pick it up on their next attach.
        fresh = () if flags.session_present else (f"inbox/{self.addr}", *self.rec["topics"])
        c.subscribe([(t, broker.SUB) for t in (BROADCAST, *fresh)])
        self._up.set()

    def _on_message(self, c, _u, m):
        try:
            msg = parse_message(m.topic, m.payload)
        except ValueError:
            msg = None
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

    def _ready(self):
        if not self._up.wait(broker.TIMEOUT):
            raise BridgeError("not connected to the broker")

    def _touch(self, refresh=True):
        if refresh:
            self.rec = presence.find(self.addr) or self.rec
        # The console's own presence must not borrow whichever Claude session happened to launch it.
        self.rec = {k: v for k, v in self.rec.items() if k not in ORIGIN} | ({} if self._admin else identity.origin())
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
        caps = self.rec["caps"]
        self._send(
            f"room/{room}", f"[{verb} #{room}] {self.rec['name']}" + (f", caps: {', '.join(caps)}" if caps else "")
        )

    def _send(self, topic, body):
        entry = {"from": self.addr, "body": check_text(body), "ts": time.time()}
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
        if self._subscribe(f"room/{check_ident(room, 'room')}", True):
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
        self._send(f"room/{check_ident(room, 'room')}", body)

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
        msgs = [m for _, m in got]
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
