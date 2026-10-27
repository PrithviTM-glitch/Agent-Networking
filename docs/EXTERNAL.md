# Connecting agents beyond this machine: plan

Status: plan only, nothing here is built. Today the bridge is loopback only, with no authentication, and every client
asserts its own identity (see the README's security model). Everything below assumes that changes first.

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
  nobody can talk in the meantime. The history log is written by the sending host, so it fragments across devices
  until a central history service replaces it (phase 3).

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
   | `ledger/resp/%u` | read | Only you read your ledger replies |
   | raw topics | per-namespace roles | Default deny |
   | `$CONTROL/#`, `$SYS/#` | none | Admin only |

   ledgerd's own user reads `ledger/req/+` and `agents/+` and writes `ledger/resp/+` and `inbox/+`. The client id
   `ledgerd` is pinned to that user, so nobody else can take its session over.
4. **Identity comes from the broker, not the payload.** MQTT 5 does not tell a subscriber who published a message, so
   identity has to be carried by a topic level that the ACL constrains:
   - **ledgerd** moves from `ledger/req` to `ledger/req/<addr>` and takes `as` from the topic level, rejecting a body
     whose `as` differs. Only `<addr>` can write there, so the self-asserted `as` limit disappears without a broker
     plugin.
   - **The reply topic** must be exactly `ledger/resp/<addr>` for the same addr. ledgerd already requires the
     `ledger/resp/` prefix, and ledger.py already names it after the client id.
   - **DMs** gain the sender as a topic level, `inbox/<to>/<from>`, with the agent allowed to write `inbox/+/%u`. The
     recipient trusts the topic, not `"from"`. Rooms need the same (`room/<room>/<from>`) if room authorship matters.
5. **`ledger/resp/#` is no longer world-readable**, which follows from the table above. A request whose ResponseTopic
   names another agent is rejected by ledgerd, and the ACL would block the read anyway.
6. **Per-identity quotas**: ledgerd's per-agent limits apply to the authenticated id. Broker limits (`max_queued_*`,
   `max_packet_size`, `max_connections`) stay as they are. Add connection rate limiting on the tailnet side.
7. **Content stays untrusted.** Authentication tells you who sent a message, not that it is safe. Bodies and
   descriptions remain data, never instructions.

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

**Ledger**: publish QoS 1 to `ledger/req`* with properties `ResponseTopic` = `ledger/resp/<your client id>` (must
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

Failures look like `{"ok": false, "error": "..."}`, and a busy lock adds `"holders"`. The full op table is in the
README. Requests are processed at least once, so make retries harmless by releasing leases you did not expect.

## 5. Phased build plan

| Phase | Build | Test before moving on |
|---|---|---|
| 0. Contract | Freeze section 4. Add a conformance script that drives the bus and ledger with only `mosquitto_pub`/`mosquitto_sub`, no Python. | Register, DM, room, recv with ack, ledger init/acquire/release, leave, all from the non-Python client. Existing suites green. |
| 1. Auth on loopback | `allow_anonymous false`, dynamic-security plugin, an admin-side `register` that creates the client, ACLs from section 2, `ledger/req/<addr>` and `inbox/<to>/<from>` with ledgerd and the Bus reading identity from the topic, ledgerd's client id pinned. | Negative tests for each ACL row: write another's presence, read another's inbox, ledger request as another, subscribe `ledger/resp/#`, connect as `ledgerd`, anonymous connect. All must fail. Positive suites green with credentials. Disabling a client disconnects it. |
| 2. TLS off-host | A TLS listener on the tailnet address only, a certificate from a local CA, and a tailnet ACL allowing only the broker port. | Plaintext and wrong-CA connections refused. A second device completes the full flow. Queued delivery survives a dropped link and a broker restart. Latency stays near the loopback figure. |
| 3. Remote agents | Credential provisioning for a new device (one-time enrollment token or admin CLI), MCP server config pointing at the remote broker, and a central history service subscribed to rooms and inboxes under its own role, replacing per-host `history.jsonl`. | A Claude session on another device registers, chats, and takes and fences a lock. History is complete from every device. Revoking the device's credentials cuts it off within seconds. |
| 4. WebSocket (if needed) | A `protocol websockets` listener behind TLS with the same auth. | An mqtt.js client passes the phase 0 conformance flow and the phase 1 negative tests. |
| 5. Multi-site (only if needed) | Broker bridges with site-namespaced topics and a per-site ledgerd partitioned by owner (section 3). | Cut the link: local locks keep working, remote acquires fail cleanly, nothing is granted twice. Heal it: queued DMs arrive once, no loops, public ledger union correct. |

Across all phases: back up both Docker volumes (`data`, `ledger`), watch `$SYS/broker/clients/*` and ledgerd's log,
and keep the README's security section in step with what is actually enforced.
