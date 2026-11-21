---------------------------- MODULE WorktreeLock ----------------------------
(***************************************************************************)
(* The worktree lock (docs/ACCESS.md 5.6 and 6): a lease in ledgerd, a git *)
(* lock on linked worktrees, and the guard's last-known file               *)
(* (guard-locks.json), which the guard falls back on while ledgerd is      *)
(* unreachable.                                                            *)
(*                                                                         *)
(* Safety: while an agent holds a worktree lease (its acquire has returned *)
(* and its release hasn't started) and the lease is live, every other      *)
(* agent's write into that worktree is refused, ledgerd up or down.        *)
(*                                                                         *)
(* Three configurations:                                                   *)
(*   MC_today.cfg    0.3.2's order: no record step, remember overwrites.   *)
(*                   FAILS: a lease on the main worktree (never git-locked)*)
(*                   is invisible once ledgerd goes down before any guard  *)
(*                   lookup.                                               *)
(*   MC_addonly.cfg  acquire records its lease, but remember still         *)
(*                   overwrites the file from a reply it asked for before  *)
(*                   the grant. FAILS: the lost update drops the record.   *)
(*   MC_fixed.cfg    record on acquire and a merging remember (0.3.3).     *)
(*                   PASSES.                                               *)
(*   MC_window.cfg, MC_reach.cfg: reachability checks that must report a   *)
(*                   violation (the grant-to-record window; a held main    *)
(*                   worktree with ledgerd down), so the passes mean       *)
(*                   something.                                            *)
(*                                                                         *)
(* Modelling choices:                                                      *)
(* - One holder process (the agent taking leases) and one guard process    *)
(*   (another agent's PreToolUse hook). Every lease belongs to the holder, *)
(*   so the guard's refusal of a tree is "some lock names that tree".      *)
(* - A guard decision is atomic with its lookup: the check-to-write window *)
(*   of a hook is out of scope (no hook can close it). The lookup's write  *)
(*   of guard-locks.json (remember) is a separate, later step, as in code. *)
(* - Every update of guard-locks.json is one atomic step: in the code each *)
(*   is a read-modify-write under an exclusive flock (guard/mod.rs:783).   *)
(* - ledgerd keeps its leases across a crash (SQLite), so "up" only says   *)
(*   whether it answers. A lease may expire at any time (its renewal       *)
(*   failed or the session died): Expire.                                  *)
(* - Time is a counter, advanced by the two steps that read it (a record's *)
(*   "added" stamp and a lookup's asked_at); MC bounds it with CONSTRAINT. *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Trees,            \* the worktrees of one repository
    Main,             \* the main worktree: git can't lock it
    MaxLeases,        \* how many acquires the holder makes
    RecordOnAcquire,  \* acquire records its lease in guard-locks.json (0.3.3)
    MergeOnRemember   \* remember keeps entries recorded after its query (0.3.3)

ASSUME Main \in Trees /\ MaxLeases \in Nat
ASSUME RecordOnAcquire \in BOOLEAN /\ MergeOnRemember \in BOOLEAN

NoLease == 0
NoTime == 0   \* "added" of an entry remember wrote from a ledgerd reply

VARIABLES
    up,       \* ledgerd answers
    live,     \* live leases in ledgerd: [id |-> Nat, tree |-> Trees]
    gitlock,  \* tree -> the lease id its git lock names, or NoLease
    file,     \* guard-locks.json: [id, tree, added]
    clock,
    hpc,      \* the holder: "idle", "granted", "recorded", "holding", "snapshot", "unlocked", "released"
    hl,       \* the holder's current lease: [id, tree]
    nextId,
    gpc,      \* the guard's lookup: "idle", or "asked" (reply in hand, file not yet written)
    gS,       \* that reply: the live leases when it asked
    gT        \* when it asked (asked_at)

vars == <<up, live, gitlock, file, clock, hpc, hl, nextId, gpc, gS, gT>>

Lease == [id : 1..MaxLeases, tree : Trees]
Entry == [id : 1..MaxLeases, tree : Trees, added : Nat]

TypeOK ==
    /\ up \in BOOLEAN
    /\ live \subseteq Lease
    /\ gitlock \in [Trees -> 0..MaxLeases]
    /\ file \subseteq Entry
    /\ clock \in Nat
    /\ hpc \in {"idle", "granted", "recorded", "holding", "snapshot", "unlocked", "released"}
    /\ nextId \in 1..(MaxLeases + 1)
    /\ gpc \in {"idle", "asked"}
    /\ gS \subseteq Lease
    /\ gT \in Nat

Init ==
    /\ up = TRUE
    /\ live = {}
    /\ gitlock = [t \in Trees |-> NoLease]
    /\ file = {}
    /\ clock = 1
    /\ hpc = "idle"
    /\ hl = [id |-> 1, tree |-> Main]
    /\ nextId = 1
    /\ gpc = "idle"
    /\ gS = {}
    /\ gT = 0

-----------------------------------------------------------------------------
(* The guard's decision for a write into tree t by an agent other than the *)
(* holder (guard/mod.rs:221 decide, :679 live_locks). Online: ledgerd's    *)
(* live leases. Offline (:709): the last-known file UNION the git lock     *)
(* reasons; the file has no expiry filter, so it can only add locks.       *)
Refuses(t) ==
    IF up THEN \E l \in live : l.tree = t
    ELSE (\E e \in file : e.tree = t) \/ gitlock[t] # NoLease

-----------------------------------------------------------------------------
(* The holder: acquire (core/src/worktree.rs:288 acquire_with).            *)

\* :294 ledger::worktree_acquire: ledgerd grants the lease.
Grant(t) ==
    /\ hpc = "idle" /\ up /\ nextId <= MaxLeases
    /\ ~\E l \in live : l.tree = t
    /\ hl' = [id |-> nextId, tree |-> t]
    /\ live' = live \cup {hl'}
    /\ nextId' = nextId + 1
    /\ hpc' = IF RecordOnAcquire THEN "granted" ELSE "recorded"
    /\ UNCHANGED <<up, gitlock, file, clock, gpc, gS, gT>>

\* :302 guard::record (0.3.3): add-only, replacing the same tree's entry.
Record ==
    /\ hpc = "granted"
    /\ file' = {e \in file : e.tree # hl.tree} \cup {[id |-> hl.id, tree |-> hl.tree, added |-> clock]}
    /\ clock' = clock + 1
    /\ hpc' = "recorded"
    /\ UNCHANGED <<up, live, gitlock, hl, nextId, gpc, gS, gT>>

\* :311-317: the record failed (no data dir, disk full), so acquire gives the lease back and returns an error: it never
\* holds a lease the guard can't see offline. With ledgerd down the release fails too and the lease lapses (Expire).
RecordFails ==
    /\ hpc = "granted"
    /\ live' = IF up THEN live \ {hl} ELSE live
    /\ hpc' = "idle"
    /\ UNCHANGED <<up, gitlock, file, clock, hl, nextId, gpc, gS, gT>>

\* :319 git_lock: a linked worktree is git-locked (it may fail, with a warning); the main worktree can't be.
\* acquire then registers the lease with the relay (:327) and returns: the holder holds it.
GitLock ==
    /\ hpc = "recorded"
    /\ \/ /\ hl.tree # Main
          /\ gitlock' = [gitlock EXCEPT ![hl.tree] = hl.id]
       \/ UNCHANGED gitlock
    /\ hpc' = "holding"
    /\ UNCHANGED <<up, live, file, clock, hl, nextId, gpc, gS, gT>>

(* The holder: release (worktree.rs:367 release_lease).                    *)

\* :369 snapshot the uncommitted work; from here the holder no longer writes.
Snapshot ==
    /\ hpc = "holding"
    /\ hpc' = "snapshot"
    /\ UNCHANGED <<up, live, gitlock, file, clock, hl, nextId, gpc, gS, gT>>

\* :373 git_unlock, only a lock that names this lease.
GitUnlock ==
    /\ hpc = "snapshot"
    /\ gitlock' = IF gitlock[hl.tree] = hl.id THEN [gitlock EXCEPT ![hl.tree] = NoLease] ELSE gitlock
    /\ hpc' = "unlocked"
    /\ UNCHANGED <<up, live, file, clock, hl, nextId, gpc, gS, gT>>

\* :375 ledgerd releases (an already expired lease is fine). Offline the release fails and is retried, and the
\* file entry stays: :379 returns before :384.
LedgerRelease ==
    /\ hpc = "unlocked" /\ up
    /\ live' = live \ {hl}
    /\ hpc' = IF RecordOnAcquire THEN "released" ELSE "idle"
    /\ UNCHANGED <<up, gitlock, file, clock, hl, nextId, gpc, gS, gT>>

\* :384 guard::forget (0.3.3), after ledgerd let go. If it fails the entry stays (a warning): over-locking only,
\* until a lookup rewrites the file.
Forget ==
    /\ hpc = "released"
    /\ \/ file' = {e \in file : e.id # hl.id}
       \/ UNCHANGED file
    /\ hpc' = "idle"
    /\ UNCHANGED <<up, live, gitlock, clock, hl, nextId, gpc, gS, gT>>

-----------------------------------------------------------------------------
(* The guard's lookup (guard/mod.rs:679 live_locks): it asks ledgerd       *)
(* (:686 asked_at, :687 the worktrees call), decides at once, and writes   *)
(* the reply to guard-locks.json afterwards (:694 remember).               *)

Ask ==
    /\ gpc = "idle" /\ up
    /\ gS' = live
    /\ gT' = clock
    /\ clock' = clock + 1
    /\ gpc' = "asked"
    /\ UNCHANGED <<up, live, gitlock, file, hpc, hl, nextId>>

\* :805 remember. 0.3.2 overwrote the file with the reply; 0.3.3 also keeps
\* the entries recorded after the query started (added >= asked_at).
Remember ==
    /\ gpc = "asked"
    /\ LET fromReply == {[id |-> l.id, tree |-> l.tree, added |-> NoTime] : l \in gS}
           newer == {e \in file : e.added # NoTime /\ e.added >= gT /\ ~\E l \in gS : l.tree = e.tree}
       IN file' = IF MergeOnRemember THEN fromReply \cup newer ELSE fromReply
    /\ gpc' = "idle"
    /\ UNCHANGED <<up, live, gitlock, clock, hpc, hl, nextId, gS, gT>>

-----------------------------------------------------------------------------
(* The environment.                                                        *)

Crash   == up /\ up' = FALSE /\ UNCHANGED <<live, gitlock, file, clock, hpc, hl, nextId, gpc, gS, gT>>
Restart == ~up /\ up' = TRUE /\ UNCHANGED <<live, gitlock, file, clock, hpc, hl, nextId, gpc, gS, gT>>

\* A lease lapses: its renewal failed (the relay died, ledgerd was down for a ttl) or its session ended.
Expire(l) == /\ l \in live
             /\ live' = live \ {l}
             /\ UNCHANGED <<up, gitlock, file, clock, hpc, hl, nextId, gpc, gS, gT>>

Next ==
    \/ \E t \in Trees : Grant(t)
    \/ Record \/ RecordFails \/ GitLock \/ Snapshot \/ GitUnlock \/ LedgerRelease \/ Forget
    \/ Ask \/ Remember
    \/ Crash \/ Restart
    \/ \E l \in live : Expire(l)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* A lease is held from acquire's return until its release starts.         *)
Held == hpc = "holding" /\ hl \in live

HeldIsRefused == Held => Refuses(hl.tree)

\* Not a requirement, shown to be reachable by MC_fixed (TLC reports it violated): ledgerd can go down between the grant
\* and the record, while acquire hasn't returned, so the lease is briefly invisible offline; no write relies on it yet.
GrantedNotRecorded == ~(hpc = "granted" /\ ~up /\ hl \in live /\ ~Refuses(hl.tree))

\* Not a requirement, a vacuity check (MC_reach.cfg, TLC reports it violated): the fixed model does reach a held lease
\* on the main worktree with ledgerd down, so HeldIsRefused passing there is not for want of the hard case.
HeldMainOffline == ~(Held /\ ~up /\ hl.tree = Main)
=============================================================================
