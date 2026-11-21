--------------------------- MODULE RelayDelivery ---------------------------
(***************************************************************************)
(* A session's relay delivering QoS 1 bus messages into the session's       *)
(* inbox. The property: every message the broker accepted for the session's *)
(* address is in the inbox or still queued at the broker, and no message is *)
(* acked to the broker before the inbox took it. It must hold across a      *)
(* re-exec on an upgraded binary, a CLI takeover of the client id, a broker *)
(* restart with persistent sessions, and the session ending.                *)
(*                                                                          *)
(* Each action names the code path it models (file:line in the 0.3.3 tree).*)
(* Two switches, both FALSE in MC.cfg:                                      *)
(*   AckBeforeInbox  the deliberately broken order (ack, then write the     *)
(*                   inbox): MC_broken.cfg must FAIL, so the spec bites.    *)
(*   QuietFilter     relay wake's quiet backlog (relay.rs:224-233): quiet   *)
(*                   messages are acked into an in-memory backlog before    *)
(*                   any inbox write. MC_quiet.cfg FAILS today, which is a  *)
(*                   finding, not a modelling error.                        *)
(* Assumption: the broker stops cleanly (persistence true keeps the queue). *)
(* A hard crash can lose up to autosave_interval (60 s) of accepted         *)
(* messages, a mosquitto limit outside the relay, not modelled.             *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Msgs,           \* the messages published to the session's address (2 in MC.cfg)
    MaxGen,         \* bound on connection generations (takeovers, restarts, re-execs)
    AckBeforeInbox, \* TRUE: the broken variant
    QuietFilter     \* TRUE: relay wake filters hold messages in the quiet backlog

VARIABLES
    published,  \* accepted by the broker (it sent the publisher its PUBACK)
    queued,     \* accepted and not yet acked by the receiver: the broker's persistent session
    sent,       \* <<m, g>>: queued message m already sent on connection generation g
    buf,        \* <<m, g>>: the relay Bus's receive buffer; g = 0 means "no ack" (a lost session)
    pending,    \* <<m, g>>: taken from buf by recv, between the deliver callback and the ack
    inbox,      \* taken by the session's inbox (h.deliver returned Ok)
    acked,      \* acked to the broker
    backlog,    \* quiet messages acked into the relay's in-memory backlog (QuietFilter only)
    owner,      \* "relay" | "cli" | "none": who holds the client id's broker session now
    gen,        \* the current connection's generation (mqtt.rs Ack.generation)
    brokerUp,
    alive       \* the session, and so its relay, is running

vars == <<published, queued, sent, buf, pending, inbox, acked, backlog, owner, gen, brokerUp, alive>>

Gens == 0..MaxGen
Entries == Msgs \X Gens

TypeOK ==
    /\ published \subseteq Msgs /\ queued \subseteq published
    /\ sent \subseteq Entries /\ buf \subseteq Entries /\ pending \subseteq Entries
    /\ inbox \subseteq Msgs /\ acked \subseteq Msgs /\ backlog \subseteq Msgs
    /\ owner \in {"relay", "cli", "none"} /\ gen \in Gens
    /\ brokerUp \in BOOLEAN /\ alive \in BOOLEAN

Init ==
    /\ published = {} /\ queued = {} /\ sent = {} /\ buf = {} /\ pending = {}
    /\ inbox = {} /\ acked = {} /\ backlog = {}
    /\ owner = "relay" /\ gen = 1 /\ brokerUp = TRUE /\ alive = TRUE

\* The relay receives only while its connection is the live one (relay.rs:278-292 waits while not connected).
Receiving == alive /\ brokerUp /\ owner = "relay"

\* A sender's QoS 1 publish to inbox/<addr>: the broker queues it for the persistent session.
Publish(m) ==
    /\ brokerUp /\ m \notin published
    /\ published' = published \cup {m}
    /\ queued' = queued \cup {m}
    /\ UNCHANGED <<sent, buf, pending, inbox, acked, backlog, owner, gen, brokerUp, alive>>

\* The broker sends a queued, unacked message once per connection; the reader thread buffers it
\* (mqtt.rs read loop -> bus.rs Handler::message pushes <<ack, msg>> onto c.buf).
Send(m) ==
    /\ Receiving /\ m \in queued /\ <<m, gen>> \notin sent
    /\ sent' = sent \cup {<<m, gen>>}
    /\ buf' = buf \cup {<<m, gen>>}
    /\ UNCHANGED <<published, queued, pending, inbox, acked, backlog, owner, gen, brokerUp, alive>>

\* bus.recv drains buf and calls the relay's deliver callback, which posts the frame to the inbox
\* (bus.rs:826-828 -> relay.rs:236-237 h.deliver). On success the entry waits for its ack.
\* A refused post pushes the batch back onto buf unacked (bus.rs:829-834): a stutter, not modelled.
Deliver(e) ==
    /\ ~AckBeforeInbox /\ Receiving /\ e \in buf
    /\ buf' = buf \ {e}
    /\ inbox' = inbox \cup {e[1]} \cup backlog   \* the frame carries the quiet backlog too (relay.rs:236-237, 241)
    /\ backlog' = {}
    /\ pending' = pending \cup {e}
    /\ UNCHANGED <<published, queued, sent, acked, owner, gen, brokerUp, alive>>

\* After deliver returned Ok, recv acks each message on the current connection, which skips an ack
\* from an older generation (bus.rs:836-841 -> mqtt.rs:336-339).
Ack(e) ==
    /\ alive /\ e \in pending
    /\ pending' = pending \ {e}
    /\ IF e[2] = gen /\ e[2] # 0 /\ brokerUp /\ owner = "relay"
          THEN /\ queued' = queued \ {e[1]}
               /\ acked' = acked \cup {e[1]}
          ELSE UNCHANGED <<queued, acked>>
    /\ UNCHANGED <<published, sent, buf, inbox, backlog, owner, gen, brokerUp, alive>>

\* The broken variant: ack first, write the inbox afterwards.
AckFirst(e) ==
    /\ AckBeforeInbox /\ Receiving /\ e \in buf
    /\ buf' = buf \ {e}
    /\ pending' = pending \cup {e}
    /\ IF e[2] = gen /\ e[2] # 0
          THEN /\ queued' = queued \ {e[1]}
               /\ acked' = acked \cup {e[1]}
          ELSE UNCHANGED <<queued, acked>>
    /\ UNCHANGED <<published, sent, inbox, backlog, owner, gen, brokerUp, alive>>

LateInbox(e) ==
    /\ AckBeforeInbox /\ alive /\ e \in pending
    /\ pending' = pending \ {e}
    /\ inbox' = inbox \cup {e[1]}
    /\ UNCHANGED <<published, queued, sent, buf, acked, backlog, owner, gen, brokerUp, alive>>

\* relay wake: a message that doesn't wake the session goes to the quiet backlog, and recv acks it
\* when the callback returns (relay.rs:224-233, "from here on they live only in the backlog").
Quiet(e) ==
    /\ QuietFilter /\ ~AckBeforeInbox /\ Receiving /\ e \in buf
    /\ buf' = buf \ {e}
    /\ backlog' = backlog \cup {e[1]}
    /\ IF e[2] = gen /\ e[2] # 0
          THEN /\ queued' = queued \ {e[1]}
               /\ acked' = acked \cup {e[1]}
          ELSE UNCHANGED <<queued, acked>>
    /\ UNCHANGED <<published, sent, pending, inbox, owner, gen, brokerUp, alive>>

\* A CLI command acting as the agent connects with its client id and takes the session over; it does
\* not recv (posting goes through a publish-only Bus: attach_as with owner=false, bus.rs:285-287).
\* The broker moves the session; the relay's connection is closed.
Takeover ==
    /\ alive /\ brokerUp /\ owner = "relay" /\ gen < MaxGen
    /\ owner' = "cli" /\ gen' = gen + 1
    /\ UNCHANGED <<published, queued, sent, buf, pending, inbox, acked, backlog, brokerUp, alive>>

\* The relay redials and takes the session back. connected() with session_present drops the buffered
\* QoS 1 entries, which the broker resends (bus.rs:104-118).
TakeBack ==
    /\ alive /\ brokerUp /\ owner = "cli" /\ gen < MaxGen
    /\ owner' = "relay" /\ gen' = gen + 1
    /\ buf' = {e \in buf : e[2] = 0}
    /\ UNCHANGED <<published, queued, sent, pending, inbox, acked, backlog, brokerUp, alive>>

\* The broker stops cleanly: persistence true keeps every queued message (config/mosquitto.conf:4).
BrokerStop ==
    /\ brokerUp
    /\ brokerUp' = FALSE /\ owner' = "none"
    /\ UNCHANGED <<published, queued, sent, buf, pending, inbox, acked, backlog, gen, alive>>

\* The broker comes back with the persistent session; the relay redials (relay.rs:278-292), and
\* connected() sees session_present and drops buffered QoS 1 entries (bus.rs:113-118).
BrokerStart ==
    /\ ~brokerUp /\ gen < MaxGen
    /\ brokerUp' = TRUE
    /\ owner' = IF alive THEN "relay" ELSE "none"
    /\ gen' = gen + 1
    /\ buf' = {e \in buf : e[2] = 0}
    /\ UNCHANGED <<published, queued, sent, pending, inbox, acked, backlog, alive>>

\* The relay re-executes itself on an upgraded binary, only when idle and never inside recv (so with
\* nothing between deliver and ack). The new image inherits the lock through AGENT_BRIDGE_RELAY_LOCK_FD,
\* the quiet backlog goes through the backlog file (relay.rs:256-276, 264 save, 212 restore), the
\* in-memory buffer is lost, and the broker resends everything unacked to the new connection.
ReExec ==
    /\ Receiving /\ pending = {} /\ gen < MaxGen
    /\ gen' = gen + 1 /\ buf' = {}
    /\ UNCHANGED <<published, queued, sent, pending, inbox, acked, backlog, owner, brokerUp, alive>>

\* The session's claude process ends: the relay exits (relay.rs:248 loop condition, 304-309). Its memory
\* (buffer, quiet backlog) is gone; the broker keeps the session's queue.
SessionEnd ==
    /\ alive
    /\ alive' = FALSE /\ owner' = "none"
    /\ buf' = {} /\ pending' = {} /\ backlog' = {}
    /\ UNCHANGED <<published, queued, sent, inbox, acked, gen, brokerUp>>

Next ==
    \/ \E m \in Msgs : Publish(m) \/ Send(m)
    \/ \E e \in Entries : Deliver(e) \/ Ack(e) \/ AckFirst(e) \/ LateInbox(e) \/ Quiet(e)
    \/ Takeover \/ TakeBack \/ BrokerStop \/ BrokerStart \/ ReExec \/ SessionEnd

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* The properties.                                                          *)
(***************************************************************************)

\* Every accepted message is in the inbox or still queued at the broker.
NoLoss == \A m \in published : m \in inbox \/ m \in queued

\* Nothing is acked to the broker before the inbox took it.
AckAfterInbox == \A m \in acked : m \in inbox
=============================================================================
