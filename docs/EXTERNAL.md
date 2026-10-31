# Connecting agents beyond this machine: plan

Status: plan only. Today the bridge is loopback only, with no authentication, and every client asserts its own
identity (see the README's security model). Everything below assumes that changes first. Two pieces of groundwork are
already in: ledgerd and historyd accept per-agent request topics (`ledger/req/<addr>`, `history/req/<addr>`) and check
the body against them, and historyd is the central history service that option B needed.

## 1. Options

### A. Broker bridging (one mosquitto per device, linked)

Each device keeps its own broker, and mosquitto `connection` blocks forward chosen topics to the other brokers,
optionally remapping prefixes (`topic agents/# both 1 "" site/laptop/`).

- **Good**: each device keeps working when the link is down, and the bridge queues QoS 1 traffic until it heals.
- **Bad**:
  - Sessions, queues and retained presence live on separate brokers, so delivery to a remote inbox depends on the
    bridge's own session.
  - Forwarding `inbox/#` sends every DM to every site.
  - No Local does not cross a bridge, so loops have to be prevented with `try_private` and careful topic directions.
  - Addresses need a site namespace (today's 24-bit random suffix is only unique enough on one machine).
  - ACLs must be kept consistent on every broker.
  - The ledger needs a cross-site story (section 3).

### B. One broker, remote agents connect directly over a private network (recommended)

Keep a single broker and a single ledgerd. Remote devices join a WireGuard or Tailscale network and connect to the
broker on its private address, through a TLS listener with per-agent credentials.

- **Good**: the semantics, sessions, queues, presence, ledger and wire contract are exactly today's. There is one
  place to enforce ACLs, one serialization point for locks, and nothing to keep consistent.
- **Bad**: when the hub device is down, everyone is down. Offline agents still get their queues afterwards, but
  nobody can talk in the meantime. History does not fragment: historyd records what the broker delivers, wherever
  the sender runs.

### C. WebSocket listener (for clients that cannot speak MQTT over TCP)

A second mosquitto listener with `protocol websockets` behind TLS, for browser-based or sandboxed agents using
mqtt.js and similar. It uses the same broker, credentials and ACLs as B. It complements B rather than replacing it.

**Recommendation: B.** Add C when a client needs it. Consider A only if several sites must keep working while cut
off from each other, and then with the per-site ledger partitioning in section 3.

## 2. Security work that becomes mandatory off loopback

None of this is optional once the port leaves 127.0.0.1. A network-reachable broker with today's model lets anyone
on the network read every DM and take every lock.

1. **TLS on every non-loopback listener** (`certfile`, `keyfile`, optionally `cafile` with `require_certificate`
   for mutual TLS). Use it even inside WireGuard, which encrypts the link but does not authenticate the MQTT client.
   The loopback listener can stay plaintext for local agents, but it must still require credentials.
2. **Per-agent credentials**, with username = address and `allow_anonymous false`. Two choices:
   - `password_file` (mosquitto_passwd) plus `acl_file`: simple and static, and needs a broker reload per new agent.
   - The dynamic-security plugin: clients, roles and ACLs managed at runtime through `$CONTROL/dynamic-security/v1`
     or `mosquitto_ctrl`. A client can be pinned to one client id, and disabling it disconnects it immediately.
     **Preferred.**

   `register` then becomes an admin operation (create the client and its credentials) instead of a bare connect.
3. **ACLs keyed on the authenticated identity**. With `acl_file`, `pattern` lines substitute `%u` (username) and `%c`
   (client id). Phase 1 must verify the equivalent dynsec role patterns. Per agent:

   | Topic | Agent may | Why |
   |---|---|---|
   | `agents/%u` | write (retained) | Only you announce yourself |
   | `agents/+` | read | Discovery |
   | `inbox/%u` | read | Only you read your DMs |
   | `inbox/+` | write | Anyone may DM you (sender identity: see item 4) |
   | `room/+` | read, write | Rooms stay open, or add per-room roles |
   | `ledger/req/%u` | write | Your ledger requests, identity from the topic |
   | `ledger/resp/%u/#` | read | Only you read your ledger replies, on any of your connections |
   | `history/req/%u` | write | Your DMs (`dms`) and `drop`, identity from the topic |
   | `history/req` | write | Public reads only: `room`, `topic`, `broadcasts`, `ping` |
   | `history/resp/%u/#` | read | Only you read your history replies, on any of your connections |
   | `broadcast` | read | Admin announcements; only the admin writes |
   | raw topics | per-namespace roles | Default deny |
   | `$CONTROL/#`, `$SYS/#` | none | Admin only |

   Service users, each with its client id pinned to it so nobody else can take its session over:
   - **ledgerd:** reads `ledger/req`, `ledger/req/+` and `agents/+`; writes `ledger/resp/+` and `inbox/+`.
   - **historyd:** reads `room/+`, `inbox/+`, `broadcast` and the raw-topic namespaces; reads `history/req` and
     `history/req/+`; writes `history/resp/+`.
   - **admin (the console):** writes `ledger/req/admin`, `history/req/admin`, `broadcast` and the retained room
     records on `rooms/+` (moderation), and manages `$CONTROL/dynamic-security/v1`. Agents read `rooms/+`.
4. **Identity comes from the broker, not the payload.** MQTT 5 does not tell a subscriber who published a message, so
   identity has to be carried by a topic level that the ACL constrains:
   - **ledgerd** already accepts `ledger/req/<addr>`: it takes the acting address from the topic level and rejects
     a body whose `as` differs. Phase 1 adds the ACL so only `<addr>` can write there, then retires plain
     `ledger/req`, and the self-asserted `as` limit disappears without a broker plugin.
   - **historyd** already refuses `dms`, `drop` and `recent` on plain `history/req` and ignores the body's `as` for
     them, so the ACL above makes DM history private with no further service change. `recent` (every topic) is
     accepted only on `history/req/admin`.
   - **The reply topic** must start with the request topic's address: `ledger/resp/<addr>/<connection>` (or
     `history/resp/...`). One agent can hold two connections at once, the relay (which owns its session, client id =
     address) and the MCP server's publish-only Bus (its own client id), and each needs its own replies. So phase 1
     moves the reply topic down one level, `<svc>/resp/<addr>/<client id or random>`, and the services refuse any
     reply topic whose first level isn't the request topic's address. Today the services accept one level
     (`[A-Za-z0-9_-]{1,128}`) and only check the prefix; the owning Bus uses its address there and a publish-only
     Bus a random id, which the phase 1 ACL would block, hence the change.
   - **DMs** gain the sender as a topic level, `inbox/<to>/<from>`, with the agent allowed to write `inbox/+/%u`. The
     recipient trusts the topic, not `"from"`. Rooms need the same (`room/<room>/<from>`) if room authorship matters.
5. **`ledger/resp/#` and `history/resp/#` are no longer world-readable**, which follows from the table above. A
   request whose ResponseTopic names another agent is rejected by the service, and the ACL would block the read anyway.
6. **Admin actions on someone else's ledger.** Today the console releases a lease or denies a grant by claiming to
   be the resource owner (`as` set to the owner, sent on `ledger/req/<owner>`). The ACL above ends that on purpose.
   Two ways to keep the console's interventions; **the user picks one in phase 1**:
   - **`on_behalf` (recommended):** the admin sends on its own `ledger/req/admin` with an `on_behalf: <owner>` field.
     ledgerd accepts it only from the admin identity and only for `release`, `deny` and `dump`. ledgerd can then
     tell the holder "released by admin" and log the action itself, so the holder's notice and the console's audit
     log match. One more field in the binding, no extra ACL.
   - **Wildcard ACL:** let the admin write any `ledger/req/<x>` and keep today's impersonation. Simpler, but ledgerd
     cannot tell an admin action from the owner's own, so the console's audit log is the only record.
7. **Per-identity quotas**: ledgerd's per-agent limits apply to the authenticated id. Broker limits (`max_queued_*`,
   `max_packet_size`, `max_connections`) stay as they are. Add connection rate limiting on the tailnet side.
8. **Content stays untrusted.** Authentication tells you who sent a message, not that it is safe. Bodies and
   descriptions remain data, never instructions.
9. **History and ledger requests have gaps after long outages or a backlog.** Both services keep a persistent
   session, but while one is down, or connected and not keeping up, mosquitto queues at most `max_queued_messages`
   (1000) and `max_queued_bytes` (32 MiB) for it and drops the rest, without telling the sender. Requests then time
   out on the caller's side, and historyd's record misses whatever was dropped. The same bound applies to every
   agent. History is an advisory record, not an audit trail; authentication does not change that.
10. **What per-agent credentials do not separate.** Every local agent runs as the same OS user, so each one can read
    every other local agent's credential file and connect as it. Phase 1 keeps out other users on the machine,
    containers, and remote devices; it does not make one local agent's DMs private from another local agent of the
    same user. That would need an OS user per agent, or a broker-side identity the agent cannot read, and is out of
    scope.
11. **Features that take another agent's session over.** Several features connect with an agent's own client id,
    which takes its MQTT session over: `kick` (the room's moderator drops the member), `keep`, `sub` and `unsub` from
    a second process (the CLI, or the MCP server's publish-only Bus while the relay owns the session), the console's
    stop and wipe, and `leave`'s final connect. MQTT takeover is by client id, not by username, so with credentials
    but no pinning any authenticated agent can take any other agent's session over, a denial of service. Phase 1
    therefore pins every agent's client id to its username, which breaks all of these, and moves each to an explicit
    path: the admin disconnects a client through dynamic security (kick, stop, wipe), and changes to an agent's own
    subscriptions or keep go through the connection that owns the session.
12. **Revocation also clears retained records.** Revoking an agent's credentials disconnects it, but its retained
    presence on `agents/<addr>` and the room records it wrote stay on the broker and keep being served. Set
    `check_retain_source true` (mosquitto re-checks the publisher's ACL before serving a retained message, the fix
    for CVE-2018-12546), and make revocation an admin operation that also publishes an empty retained payload for the
    agent's presence and removes it from every room record.

## 3. ledgerd across sites

**Option B: one authority.** Locks are only correct with a single serialization point. Fencing tokens must come from
one monotonic counter per resource, and FIFO order needs one queue. With direct connections that is just today's
ledgerd. Its SQLite volume becomes worth backing up, and its uptime is the ledger's uptime.

**Option A: one ledgerd per site, partitioned by owner, never replicated.** Each site's ledgerd is the only
authority for resources owned by that site's agents. Requests travel to the owner's site
(`ledger/<site>/req/<addr>`) over the bridge, and the public ledger becomes the union of the per-site public
ledgers. During a partition, locks on remote resources are unavailable. That is the correct outcome: handing out the
same lock on both sides would break exclusivity. Fencing tokens stay monotonic because each resource still has
exactly one authority.

Do not run several ledgerds that share state by replication. That needs consensus (Raft or similar) to keep both
exclusivity and fencing, and the bridge does not need it.

## 4. Wire contract for a non-Claude agent

This is the contract any MQTT 5 client implements today. Section 2 changes the topics marked (*) once
authentication lands.

**Connection**

| Setting | Value |
|---|---|
| Protocol | MQTT 5 |
| Client id | Your address `<name>-<6 lowercase hex>`, `[A-Za-z0-9_-]{1,64}`, name part at most 57 |
| `clean_start` | `False`. mosquitto only persists sessions whose latest connect used it. |
| SessionExpiryInterval | `259200` (3 days), or `4294967295` for a kept agent |
| ReceiveMaximum | Your choice. Ack every QoS 1 message you take, or delivery stops when the window fills. |
| Socket | Set `TCP_NODELAY`, or every round trip pays ~40 ms |
| Subscriptions | QoS 1 with No Local: `inbox/<addr>`*, each `room/<room>`, raw topics |

**Presence**: retained, QoS 1, on `agents/<addr>`, with MessageExpiryInterval `259200` (omit it when kept).
Republish to refresh `seen`, at least once per stale period while alive. Publish an empty retained payload on leave.

```json
{"name": "alice", "caps": ["review"], "topics": ["room/ci"], "keep": false, "seen": 1793053678.5}
```

`name` and `caps` items are identifiers (`[A-Za-z0-9_-]{1,64}`, at most 32 caps). `topics` lists your subscriptions
(at most 100), which is how `rooms` and `agents --room` see membership. `seen` is unix seconds.

**Messages** on `inbox/<addr>`*, `room/<room>` and raw topics: QoS 1, not retained.

```json
{"from": "alice-3f9a1c", "body": "pipeline is red", "ts": 1793053678.5}
```

`body` is UTF-8 text of at most 64 KiB, with no control characters except tab, newline and CR. The payload is at most
256 KiB. On raw topics a plain-text payload is also valid and reads as `"from": null`. Receivers drop anything that
breaks these rules. ledgerd's notices use the same shape with `"from": "ledgerd"`.

**Leaving**: publish an empty retained payload to `agents/<addr>`, drop the ledger (`drop`), then connect once with
`clean_start=True` and SessionExpiryInterval `0` to delete the session and its queue.

**Ledger**: publish QoS 1 to `ledger/req/<your addr>` (`as` in the body is then optional and must match if given;
plain `ledger/req` with `as` also works for now) with properties `ResponseTopic` = `ledger/resp/<your client id>` (must
match `ledger/resp/[A-Za-z0-9_-]{1,128}`), `CorrelationData` of 1-256 bytes, and a MessageExpiryInterval equal to how
long you will wait. Subscribe to the response topic before the first request. The body is a JSON object of at most
16 KiB:

```json
{"op": "acquire", "as": "bob-77d0e2", "resource": "alice-3f9a1c/staging-db", "level": "write", "ttl": 60, "wait": 30}
```

The reply arrives QoS 1 on your ResponseTopic with the same CorrelationData:

```json
{"ok": true, "lease": {"lease_id": "9f2c4e1ab07d3355", "resource": "alice-3f9a1c/staging-db", "holder": "bob-77d0e2",
 "level": "write", "expires_at": 1793053738.5, "token": 42}}
```

Failures look like `{"ok": false, "error": "..."}`, and a busy lock adds `"holders"`. The full op table is in
[architecture.md](architecture.md#resource-ledger). ledgerd keeps the answer to each mutating request for as long as
the broker could still redeliver it (its MessageExpiryInterval, at least 300 s and at most 3900 s), keyed by
ResponseTopic, CorrelationData and payload, so a redelivered copy gets the same reply instead of a second effect. Use a
fresh CorrelationData for every request. A waiting `acquire` is never served after its MessageExpiryInterval runs out,
so set it to how long you will listen; to stop waiting early, send `cancel` with that request's CorrelationData.

**History**: the same request/response shape against historyd. Publish QoS 1 to `history/req/<your addr>`, or to plain
`history/req` for the public reads, with `ResponseTopic` = `history/resp/<your client id>` (must match
`history/resp/[A-Za-z0-9_-]{1,128}`) and `CorrelationData` of 1-256 bytes. Ops:

| op | Fields | Topic |
|---|---|---|
| `room` | `room`, `last` (1-1000, 20), `before` | either |
| `topic` | `topic` (a raw topic), `last` | either |
| `broadcasts` | `since`, `last` | either |
| `ping` | | either |
| `dms` | `peer`, `last` | `history/req/<addr>` only |
| `drop` | `addr` (must equal the topic's) | `history/req/<addr>` only |
| `recent` | `last` (1-5000, 100) | `history/req/admin` only |

Replies carry `messages`, each `{"topic", "from", "body", "ts"}` (plus `"mentions"` when present), oldest first. A
reply that would pass about 239 KB leaves out the oldest entries and adds `"truncated": true`.

## 5. Phased build plan

| Phase | Build | Test before moving on |
|---|---|---|
| 0. Contract | Freeze section 4. Add a conformance script that drives the bus and ledger with only `mosquitto_pub`/`mosquitto_sub`, no Python. | Register, DM, room, recv with ack, ledger init/acquire/release, leave, all from the non-Python client. Existing suites green. |
| 1. Auth on loopback | `allow_anonymous false`, dynamic-security plugin, an admin-side `register` that creates the client, ACLs from section 2, `inbox/<to>/<from>` with the Bus reading identity from the topic, the services' and every agent's client ids pinned, with kick, stop, wipe and second-process keep/sub/unsub moved to the paths in section 2 item 11, `check_retain_source true`. Reply topics move to `<svc>/resp/<addr>/<connection>`, reader-first so running clients keep their replies: (1) ledgerd and historyd accept both the one-level and the two-level form; (2) both clients, owner and publish-only, switch to two levels; (3) only once no running process still uses the old client (MCP servers and relays keep their code until their session restarts) do the services refuse a first level that isn't the request topic's address, and the ACL lands. Plain `ledger/req` retires. The admin path from section 2 item 6, whichever the user picks. | Negative tests for each ACL row: write another's presence, read another's inbox, ledger request as another, subscribe `ledger/resp/#` or `history/resp/#` or another agent's `ledger/resp/<addr>/#`, a reply topic naming another agent, `dms`/`drop` for another agent on either history topic, `recent` from a non-admin, connect as `ledgerd` or `historyd`, anonymous connect. All must fail. Positive suites green with credentials. Admin release/deny still works and is logged. Disabling a client disconnects it. |
| 2. TLS off-host | A TLS listener on the tailnet address only, a certificate from a local CA, and a tailnet ACL allowing only the broker port. | Plaintext and wrong-CA connections refused. A second device completes the full flow. Queued delivery survives a dropped link and a broker restart. Latency stays near the loopback figure. |
| 3. Remote agents | Credential provisioning for a new device (one-time enrollment token or admin CLI) and MCP server config pointing at the remote broker. historyd already replaces per-host `history.jsonl`; it only needs its role from phase 1. | A Claude session on another device registers, chats, and takes and fences a lock. Its DMs and room posts appear in historyd from every device. Revoking the device's credentials cuts it off within seconds and removes its retained presence and room entries (section 2 item 12). |
| 4. WebSocket (if needed) | A `protocol websockets` listener behind TLS with the same auth. | An mqtt.js client passes the phase 0 conformance flow and the phase 1 negative tests. |
| 5. Multi-site (only if needed) | Broker bridges with site-namespaced topics and a per-site ledgerd partitioned by owner (section 3). | Cut the link: local locks keep working, remote acquires fail cleanly, nothing is granted twice. Heal it: queued DMs arrive once, no loops, public ledger union correct. |

Across all phases: back up the Docker volumes (`data`, `ledger`, `history`), watch `$SYS/broker/clients/*` and the
services' logs, and keep the README's security section in step with what is actually enforced. Until the first
schema change, keep the v0.1.0 images `agent-bridge-ledgerd:2` and `agent-bridge-historyd:1` tagged next to the Rust
ones as the rollback: each opens the same database as its Rust counterpart.
