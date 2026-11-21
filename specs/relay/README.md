# RelayDelivery: the relay's QoS 1 delivery into a session's inbox

`RelayDelivery.tla` models one session's relay receiving QoS 1 bus messages and posting them into the session's
inbox. It checks two invariants:

- **NoLoss**: every message the broker accepted for the session's address is in the inbox or still queued at the
  broker.
- **AckAfterInbox**: nothing is acked to the broker before the inbox took it.

They must hold across:

- a re-exec on an upgraded binary (the lock fd handed over in `AGENT_BRIDGE_RELAY_LOCK_FD`);
- a CLI command taking the client id's session over, and the relay taking it back;
- a broker restart with persistent sessions;
- the session ending.

The model is small: 2 messages, 1 relay, connection generations bounded by `MaxGen = 4` (so up to 3 takeovers,
restarts or re-execs). Each action names the code it models (file:line in the 0.3.3 tree, after the lint edits).

| Action | Code |
|---|---|
| Publish | a sender's QoS 1 publish to `inbox/<addr>`; the broker queues it for the persistent session |
| Send | mqtt.rs read loop -> bus.rs `Handler::message` buffers `<<ack, msg>>` |
| Deliver | bus.rs:826-828 `recv` calls the callback -> relay.rs:236-237 `h.deliver(frame(...))` |
| Ack | bus.rs:836-841, then mqtt.rs:336-339 (an ack from an older connection generation is skipped) |
| Quiet | relay.rs:224-233: a message that doesn't wake the session goes to the in-memory backlog, acked |
| Takeover / TakeBack | another connection with the client id; on reconnect `connected()` drops buffered QoS 1 entries when the session survived (bus.rs:104-118) |
| BrokerStop / BrokerStart | a clean stop with `persistence true` (config/mosquitto.conf:4); the relay redials (relay.rs:278-292) |
| ReExec | relay.rs:256-276 (idle only, never inside `recv`; the backlog file at 264 save, 212 restore) |
| SessionEnd | relay.rs:248, 304-309: the relay exits and its memory is gone; the broker keeps the queue |

## Models

| File | Expect | Why |
|---|---|---|
| `MC.cfg` | every invariant holds | the relay as built, with no wake filters |
| `MC_broken.cfg` | **MUST FAIL** `AckAfterInbox` | the deliberately broken order (ack, then the inbox write). If it passed, the spec would not bite. `NoLoss` fails too: once acked, the message lives only in the relay's memory. |
| `MC_quiet.cfg` | **MUST FAIL** `AckAfterInbox` | `relay wake` as built. See the finding below. |

## Run

```bash
sh scripts/verify-tlc-relay.sh
```

It runs `mirror.py` (a standard-library breadth-first mirror of the spec: the same state, actions and invariants) and
then TLC, the checker of record. `TLA2TOOLS` names the jar (default `~/.local/share/tla/v1.7.4/tla2tools.jar`) and
`TLA2TOOLS_SHA256` its pinned hash (default v1.7.4's), so CI points both at its own pinned download. A missing jar or a
different hash exits 2 after the mirror. In each MUST FAIL cfg the expected invariant is listed first, because TLC
reports the first one violated. `python3 specs/relay/mirror.py --trace <cfg> [Invariant]` prints the shortest violating
behaviour.

Measured 2026-11-20, TLC 2.19 (v1.7.4) and the mirror:

| Model | TLC | Mirror | Result |
|---|---|---|---|
| MC.cfg | 61,736 distinct states, 3 s, no error | 61,736 states, 1.5 s | holds every invariant |
| MC_broken.cfg | violation at depth 4 (`AckFirst`) | 12,428 states | fails AckAfterInbox, and NoLoss |
| MC_quiet.cfg | violation at depth 4 (`Quiet`) | 102,451 states | fails AckAfterInbox, and NoLoss |

The two checkers find the same number of distinct states for MC.cfg, so the mirror explores what the spec says.

## Finding: relay wake's quiet backlog

With a wake filter on a room, a message that doesn't wake the session is acked straight away and kept only in the
relay's memory until the next post that does wake it (relay.rs:224-233). It survives a re-exec through the backlog
file, but not:

- the session ending before a waking post;
- the relay crashing;
- the backlog's own bound (`BACKLOG` drops the oldest).

So `AckAfterInbox` and `NoLoss` don't hold for quiet messages. The shortest trace is
`Publish, Send, Quiet`. Whether that's acceptable is a decision, not a bug report: quiet messages are room chatter
the user chose not to be woken for, and history still has them. If they must survive, the relay could write the quiet
backlog to its file before acking, or leave quiet messages unacked until a waking post goes out.

Decided for 0.3.4: the relay writes the backlog entry durably before the ack, so `AckAfterInbox` holds for quiet
messages too. `MC_quiet.cfg` then becomes an expected pass (its MUST FAIL line goes). Until then it stays an expected
failure, a known limit.

## Assumptions (not modelled)

- **A clean broker stop.** A hard crash can lose up to `autosave_interval` (60 s) of accepted messages, a mosquitto
  limit outside the relay.
- **Session expiry.** The queue outlives the relay only until the session expires: `STALE_DAYS` (default 3 days) for
  a non-keep agent (presence.rs:118).
- **No packet ids.** The model doesn't track them, so an ack's generation check stands in for "this ack is for that
  message".
- **At-least-once.** Duplicates in the inbox are allowed (a re-exec or takeover between a post and its ack resends);
  only loss and early acks are checked.
