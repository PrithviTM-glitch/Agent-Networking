------------------------------- MODULE Ledger -------------------------------
(***************************************************************************)
(* ledgerd's lease protocol for one resource, write level only, with the   *)
(* broker's QoS 1 at-least-once delivery, ledgerd restarts and a clock.    *)
(*                                                                         *)
(* What is modelled, and where it lives in rust/crates/ledgerd/src:        *)
(*  - one request at a time, tick first: main.rs:201-220 (serve loop)      *)
(*  - the dedupe store (replay instead of re-run): store.rs:405-426        *)
(*  - acquire: grant, queue a waiter, or busy: store.rs:805-863            *)
(*  - release: store.rs:940-957                                            *)
(*  - pump, FIFO waiters: store.rs:1140-1156                               *)
(*  - tick: expire leases then pump, then drop due waiters: 1263-1284      *)
(*  - fencing counter in the meta table: store.rs:753-763                  *)
(*  - ack only after the transaction commits: main.rs:218-220              *)
(*                                                                         *)
(* Abstracted: permissions (both clients hold write grants), read leases,  *)
(* renew (it only moves expires of an existing lease, store.rs:931),       *)
(* other resources, cancel (store.rs:1107) and replies' wording. Time is   *)
(* integer ticks; a ledgerd restart loses nothing durable (everything is   *)
(* in SQLite) and is the same as a message being delivered again, which   *)
(* Deliver allows, so it needs no action of its own except to show what    *)
(* an in-memory fencing counter would break (FencePersisted = FALSE).      *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Clients,          \* the agents asking for the resource
    MaxTime,          \* the last tick explored
    TTL,              \* a lease's ttl, in ticks
    Wait,             \* an acquire's wait, in ticks
    Life,             \* a request's MessageExpiryInterval: the client listens this long
    Dedupe,           \* DEDUPE_S, store.rs:28
    MaxKeep,          \* MAX_KEEP, store.rs:29
    Margin,           \* REPLY_MARGIN, store.rs:31
    MaxDeliveries,    \* deliveries of one message: 1 plus the redeliveries explored
    MaxReqs,          \* requests per client
    KeepForLifetime,  \* TRUE: store.rs:423 keeps an answer for the request's lifetime (0.2.1)
    CapWaitAtExpiry,  \* TRUE: store.rs:849 ends a wait before the client stops listening (0.2.1)
    FencePersisted    \* TRUE: the fencing counter is in the meta table, store.rs:756

ASSUME /\ Life > 0 /\ TTL > 0 /\ Dedupe > 0 /\ Margin >= 0 /\ MaxDeliveries >= 1

Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b

VARIABLES
    now,       \* the clock
    broker,    \* request messages the broker holds: [id, deliveries]
    reqs,      \* every request ever sent, by id: [client, op, sent, lease]
    nextId,    \* the next request id
    sentBy,    \* requests sent per client
    replies,   \* the dedupe store: request id -> the last tick its answer is kept
    leases,    \* live leases: [id (the acquire's request id), holder, token, expires]
    waiters,   \* queued acquires, FIFO: [req, holder, deadline]
    fence,     \* the fencing counter
    executed,  \* history: request id -> times ledgerd ran it (not replayed it)
    tokens,    \* history: every fencing token handed out, in order
    grants     \* history: [req, at] for every lease handed out

vars == <<now, broker, reqs, nextId, sentBy, replies, leases, waiters, fence, executed, tokens, grants>>

Listening(id) == now < reqs[id].sent + Life
Holds(c) == \E l \in leases : l.holder = c
\* A client learns its lease from the reply, which it reads only while it listens.
Knows(l) == \E g \in grants : g.req = l.id /\ g.at < reqs[l.id].sent + Life

TickDue == \/ \E l \in leases : l.expires <= now
           \/ \E i \in 1..Len(waiters) : waiters[i].deadline <= now

Init ==
    /\ now = 0
    /\ broker = {}
    /\ reqs = <<>>
    /\ nextId = 1
    /\ sentBy = [c \in Clients |-> 0]
    /\ replies = <<>>
    /\ leases = {}
    /\ waiters = <<>>
    /\ fence = 0
    /\ executed = <<>>
    /\ tokens = <<>>
    /\ grants = {}

(* The lease goes to the head waiter when the resource is free (store.rs:1140-1156), given the leases ls and   *)
(* waiters ws left by the step that freed it. One resource at write level hands out at most one lease.         *)
Pump(ls, ws) ==
    IF ls = {} /\ ws /= <<>>
    THEN LET w == Head(ws) IN
         /\ leases' = {[id |-> w.req, holder |-> w.holder, token |-> fence + 1, expires |-> now + TTL]}
         /\ waiters' = Tail(ws)
         /\ fence' = fence + 1
         /\ tokens' = Append(tokens, fence + 1)
         /\ grants' = grants \cup {[req |-> w.req, at |-> now]}
    ELSE /\ leases' = ls
         /\ waiters' = ws
         /\ UNCHANGED <<fence, tokens, grants>>

(* ---- clients ---- *)

\* A client with no lease and no acquire still being answered asks for the resource.
SendAcquire(c) ==
    /\ sentBy[c] < MaxReqs
    /\ ~Holds(c)
    /\ \A id \in DOMAIN reqs : (reqs[id].client = c /\ reqs[id].op = "acquire") => ~Listening(id)
    /\ reqs' = reqs @@ (nextId :> [client |-> c, op |-> "acquire", sent |-> now, lease |-> 0])
    /\ broker' = broker \cup {[id |-> nextId, deliveries |-> 0]}
    /\ nextId' = nextId + 1
    /\ sentBy' = [sentBy EXCEPT ![c] = @ + 1]
    /\ UNCHANGED <<now, replies, leases, waiters, fence, executed, tokens, grants>>

\* A client releases a lease it knows it holds.
SendRelease(c) ==
    /\ sentBy[c] < MaxReqs
    /\ \E l \in leases :
        /\ l.holder = c /\ Knows(l)
        /\ reqs' = reqs @@ (nextId :> [client |-> c, op |-> "release", sent |-> now, lease |-> l.id])
    /\ broker' = broker \cup {[id |-> nextId, deliveries |-> 0]}
    /\ nextId' = nextId + 1
    /\ sentBy' = [sentBy EXCEPT ![c] = @ + 1]
    /\ UNCHANGED <<now, replies, leases, waiters, fence, executed, tokens, grants>>

(* ---- ledgerd ---- *)

Acquire(id) ==
    LET r == reqs[id]
        remaining == r.sent + Life - now                                  \* the request's MessageExpiry left
        w == IF CapWaitAtExpiry THEN Min(Wait, remaining - Margin) ELSE Wait \* store.rs:849
    IN
    IF Holds(r.client) THEN UNCHANGED <<leases, waiters, fence, tokens, grants>>    \* "you already hold", 843
    ELSE IF leases = {} /\ waiters = <<>>                                           \* store.rs:830
    THEN /\ leases' = {[id |-> id, holder |-> r.client, token |-> fence + 1, expires |-> now + TTL]}
         /\ fence' = fence + 1
         /\ tokens' = Append(tokens, fence + 1)
         /\ grants' = grants \cup {[req |-> id, at |-> now]}
         /\ UNCHANGED waiters
    ELSE IF w <= 0 THEN UNCHANGED <<leases, waiters, fence, tokens, grants>>          \* busy, store.rs:850
    ELSE /\ waiters' = Append(waiters, [req |-> id, holder |-> r.client, deadline |-> now + w])  \* 856-859
         /\ UNCHANGED <<leases, fence, tokens, grants>>

Release(id) ==
    LET r == reqs[id] IN
    IF \E l \in leases : l.id = r.lease /\ l.holder = r.client
    THEN Pump({l \in leases : l.id /= r.lease}, waiters)                             \* store.rs:948-957
    ELSE UNCHANGED <<leases, waiters, fence, tokens, grants>>                        \* "no such lease"

\* ledgerd takes one delivered message, after the tick (main.rs:212) and before its ack (main.rs:220). A copy
\* whose answer is still stored is replayed and changes nothing; otherwise the op runs in one transaction and its
\* answer is stored for keep ticks (store.rs:409-424).
Deliver(m) ==
    /\ m \in broker
    /\ m.deliveries < MaxDeliveries
    /\ Listening(m.id)                      \* the broker drops a message at its expiry
    /\ ~TickDue
    /\ broker' = (broker \ {m}) \cup {[m EXCEPT !.deliveries = @ + 1]}
    /\ LET remaining == reqs[m.id].sent + Life - now
           keep == IF KeepForLifetime THEN Min(Max(Dedupe, remaining), MaxKeep) ELSE Dedupe
       IN
       IF m.id \in DOMAIN replies /\ now <= replies[m.id]
       THEN UNCHANGED <<replies, executed, leases, waiters, fence, tokens, grants>>
       ELSE /\ replies' = (m.id :> now + keep) @@ replies
            /\ executed' = (m.id :> (IF m.id \in DOMAIN executed THEN executed[m.id] ELSE 0) + 1) @@ executed
            /\ IF reqs[m.id].op = "acquire" THEN Acquire(m.id) ELSE Release(m.id)
    /\ UNCHANGED <<now, reqs, nextId, sentBy>>

\* The ack after the commit removes the message. Never acking it (a crash or restart between commit and ack)
\* leaves it to Deliver again, up to MaxDeliveries.
Ack(m) ==
    /\ m \in broker /\ m.deliveries >= 1
    /\ broker' = broker \ {m}
    /\ UNCHANGED <<now, reqs, nextId, sentBy, replies, leases, waiters, fence, executed, tokens, grants>>

\* The broker drops an expired message (MessageExpiryInterval).
Expire(m) ==
    /\ m \in broker /\ ~Listening(m.id)
    /\ broker' = broker \ {m}
    /\ UNCHANGED <<now, reqs, nextId, sentBy, replies, leases, waiters, fence, executed, tokens, grants>>

\* tick, part 1 (store.rs:1266-1277): expired leases go, then the head waiter may get the lease. This pump runs
\* before part 2 drops waiters whose deadline has come, as in the code.
TickLeases ==
    /\ \E l \in leases : l.expires <= now
    /\ Pump({l \in leases : l.expires > now}, waiters)
    /\ UNCHANGED <<now, broker, reqs, nextId, sentBy, replies, executed>>

\* tick, part 2 (store.rs:1279-1284): waiters whose deadline has come get busy, then the pump runs again.
TickWaiters ==
    /\ ~\E l \in leases : l.expires <= now
    /\ \E i \in 1..Len(waiters) : waiters[i].deadline <= now
    /\ Pump(leases, SelectSeq(waiters, LAMBDA w : w.deadline > now))
    /\ UNCHANGED <<now, broker, reqs, nextId, sentBy, replies, executed>>

\* ledgerd sleeps until its next deadline (main.rs:203), so time moves on only once nothing is due. The dedupe
\* store is read as if purged the moment an answer's keep ends (store.rs:1265), the case least kind to dedupe.
Advance ==
    /\ now < MaxTime
    /\ ~TickDue
    /\ now' = now + 1
    /\ UNCHANGED <<broker, reqs, nextId, sentBy, replies, leases, waiters, fence, executed, tokens, grants>>

\* Only an in-memory fencing counter (FencePersisted = FALSE) has anything to lose in a restart.
Restart ==
    /\ ~FencePersisted
    /\ fence' = 0
    /\ UNCHANGED <<now, broker, reqs, nextId, sentBy, replies, leases, waiters, executed, tokens, grants>>

Next ==
    \/ \E c \in Clients : SendAcquire(c) \/ SendRelease(c)
    \/ \E m \in broker : Deliver(m) \/ Ack(m) \/ Expire(m)
    \/ TickLeases \/ TickWaiters \/ Advance \/ Restart

Spec == Init /\ [][Next]_vars

(* ---- invariants ---- *)

TypeOK ==
    /\ now \in 0..MaxTime
    /\ fence \in Nat
    /\ \A l \in leases : l.holder \in Clients /\ l.token \in Nat
    /\ \A i \in 1..Len(waiters) : waiters[i].holder \in Clients

\* At most one live write lease per resource.
AtMostOneWriteLease == Cardinality(leases) <= 1

\* Fencing tokens strictly increase, across restarts too.
FencingIncreases == \A i \in 1..Len(tokens) - 1 : tokens[i] < tokens[i + 1]

\* Each request takes effect at most once, however often the broker delivers it.
AtMostOnce == \A id \in DOMAIN executed : executed[id] <= 1

\* A lease goes only to a client still listening for it.
LeaseOnlyToListener == \A g \in grants : g.at < reqs[g.req].sent + Life

=============================================================================
