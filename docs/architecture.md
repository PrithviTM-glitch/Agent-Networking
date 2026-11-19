# Architecture and protocol

The reference for how agent-bridge works: the wire protocol, the Python API, the CLI, the resource ledger, the MCP
server, the relay, the admin console, limits, persistence and troubleshooting. The [README](../README.md) covers
installation and the security model. Module paths below are relative to `src/agent_bridge/`.

The **state directory** is `$XDG_STATE_HOME/agent-bridge` (default `~/.local/state/agent-bridge`), or the directory
of `AGENT_BRIDGE_LOG` when that is set. It holds `cli.lock`, `relay/`, `hook-state/`, `admin-audit.jsonl`, and the
legacy `history.jsonl`, which historyd imports once and never changes (message history now lives in historyd).

## Code map

| Module | Concern |
|---|---|
| `_native` | The Rust extension, built by maturin from `rust/crates/py`: the bus core (the Bus, the Observer, ledgerd and historyd calls, presence, rooms, relay and identity lookups, the validators and limits) |
| `agent_bridge.data/scripts` | The commands the wheel puts in `bin`: the Rust `agent-bridge` (CLI, relay, hooks, `ledger-raw`), `agent-bridge-ledgerd` and `agent-bridge-historyd`, which `scripts/build-bins.sh` builds in, and the sh shims that keep `agent-bridge-relay-hook`, `agent-bridge-broadcast-hook` and `agent-bridge-ledger` running them |
| `errors.py` | `BridgeError`, `LedgerError` and `HistoryError`, defined once; `_native` raises these very classes |
| `config.py` | Environment settings and the state directory |
| `validation.py` | The limits and input checks, re-exported from `_native` so they are defined once, plus the host-side `UUID` and `is_count` (also re-exports `BridgeError`) |
| `protocol.py` | `ORIGIN` and `LINEAGE`, the write-once presence field names the console and the MCP server read |
| `host.py` | The agent-framework interface: this process's session, its session process, delivery into it, every session on the machine and their transcripts; the generic host when there is no framework |
| `claude/host.py` | Claude Code as a host: `CLAUDE_CODE_SESSION_ID`, the `claude` ancestor process, `CLAUDE_PROJECT_DIR`, Desktop detection, resume commands and their stream-json output, the `claude_code_*` metric names |
| `claude/inbox.py` | The session inbox socket (`CLAUDE_CODE_MESSAGING_SOCKET` and `_TOKEN`) |
| `claude/registry.py` | The session registry, `~/.claude/sessions/<pid>.json` |
| `claude/transcripts.py` | Session transcripts, `~/.claude/projects/*/<session>.jsonl` |
| `mcp_server.py` | `agent-bridge-mcp`, on the native core |
| `console/server.py` | `agent-bridge-console`: HTTP, static page, JSON API, event stream |
| `console/auth.py` | Host, Origin and token checks |
| `console/admin.py` | The console on the bus, from the native core: the `#` observer, the `admin` identity, ledger calls |
| `console/sessions.py` | The joined session list: every host's sessions and transcripts with bus presence, git branch |
| `console/processes.py` | Process identity checks, signals and restarts |
| `console/actions.py` | Admin actions and the audit log |
| `console/kill.py` | The room kill switch and targeted stop: member classes, previews, signals |
| `console/capacity.py` | CPU and memory per machine, session and process, from `/proc` |
| `console/leaks.py` | Leftover test containers and orphaned agent-bridge processes |
| `console/usage.py` | Claude usage per session from Prometheus |
| `console/metrics.py` | The metrics registry behind `GET /metrics` |
| `console/cli.py` | `agent-bridge-admin`, the console's API from the shell |
| `console/events.py` | Fan-out to open event streams |
| `console/links.py` | The optional link rules: loaded, checked and served to the page |
| `console/worktrees.py` | The Worktrees view's snapshot (ledgerd's `worktrees`, git locks, drift, sessions inside, guard detection) and Protect/Unprotect |
| `console/static/` | The page: `index.html`, `app.js`, `app.css`, `markup.js` (message tables and code blocks) and `links.js` (links and click-to-copy) |

**Agent frameworks.** Everything that knows Claude Code lives in `claude/`, reached only through `host.py`:
- **What the rest of the bridge asks:** `host.current()` (the framework this process runs under, else the generic
  host) and `host.installed()` (every framework the console lists sessions of).
- **What a host answers:** its session id, the session process (for liveness), delivery of a user message, session
  lists, transcripts, kind, resume commands and metric names.
- **Under the generic host:** there is no session, no relay and no delivery, so a plain CLI agent reads with `recv`.
- **What a Claude Code change can break:** `claude/`, and nothing else.
- **Adding a framework:** a sibling package with a `Host` subclass, listed in `installed()`.

The Rust port of the CLI, the relay and ledgerd lives in `rust/` (crates `core`, `cli`, `ledgerd`, and `py` for the
Python extension), described in the
[README](../README.md#rust-implementation). It shares this wire protocol (ledgerd's and historyd's requests included),
the lock files and the SQLite schema. Since 0.3.2 the core also has `worktree` (the worktree commands), `guard` (the
hooks' engine) and `provision` (credentials), the CLI has `ref_guard`, and the broker has `config/broker-entrypoint.sh`
and the opt-in `compose.auth.yaml` (see [Broker authentication](#broker-authentication)).

## Addresses, topics and payloads

- **Address**: `<name>-<6 hex>`, chosen by `register`. The name and address `admin` (any case) are reserved for
  the admin console. `register NAME --addr ADDR` reattaches to ADDR when it is
  live and keeps its record, or creates it with that address. Long-lived clients use this to keep one identity.
- **Topics**:

  | Topic | Use |
  |---|---|
  | `agents/<addr>` | Retained presence: `{"name", "caps": [...], "topics": [...], "keep": bool, "seen": unix_ts}`, plus `session`, `pid` and `pid_start` when the agent runs under Claude Code, and the optional `about` and `mcp` (see below) |
  | `inbox/<addr>`, `inbox/<addr>/<from>` | DMs to that agent; the second form names the sender in the topic (read since 0.3.2) |
  | `room/<room>`, `room/<room>/<from>` | Chat room; the second form names the sender (read since 0.3.2) |
  | `broadcast` | Admin broadcasts. Every agent subscribes on every connect, only the `admin` Bus may publish. |
  | `rooms/<room>` | Retained room settings, see [Room moderation](#room-moderation). Every Bus subscribes at QoS 0 on every connect. |
  | `rooms/<room>/pin` | A room's pinned message, retained: `{"text", "by", "ts"}`, `text` under the message rules (not empty, at most 64 KiB). Set or cleared (empty payload) by admin or a moderator; one level below `rooms/+`, so no Bus caches it. |
  | anything else | Raw topics. `$...`, `agents`, `inbox`, `ledger`, `broadcast`, `rooms` and `history` (as first level) are reserved for the bridge. |
  | `history/req/<addr>`, `history/req`, `history/resp/<id>`, `history/resp/<addr>/<conn>` | historyd request/response, see [History service](#history-service) |
  | `ledger/req/<addr>`, `ledger/resp/<id>`, `ledger/resp/<addr>/<conn>` | ledgerd request/response (see [the wire protocol](#resource-ledger)); plain `ledger/req` is still served |

- **Origin fields** in presence: `session` is `CLAUDE_CODE_SESSION_ID` (a UUID) when set, `pid` the nearest
  ancestor process whose comm is `claude`, and `pid_start` that process's start time (field 22 of
  `/proc/<pid>/stat`, in clock ticks). They are written once, when the record is created. Later attaches never
  add, replace or remove `session`, and refresh `pid`/`pid_start` only from a process in that same session. An
  agent registered with `--subagent` gets `"kind": "subagent"` and `"parent": <creating session>` instead of
  `session`, so neither the session's relay nor the console takes it for the session itself. `pid` and `pid_start`
  are written only together with a session (or a subagent's parent): a script started from a session's shell without
  `CLAUDE_CODE_SESSION_ID` is not that session and records no process. They let the console tell a live session from
  a dead one and from a reused pid. Any client can write any presence, so the console re-checks them against `/proc`
  and never trusts them alone.
- **Optional fields** (0.3.0):
  - `about` is a one-line self-description of up to 200 bytes, set with `describe --about` (`""` removes it).
  - `mcp` is the version of the MCP server that runs the agent (`agent_bridge.__version__`: 1 to 32 of `A-Z a-z 0-9 .
    + -`). The server writes it after it connects.

  A reader keeps either field only when it is well formed. It drops a bad field, never the record. Both are best
  effort while 0.2.x clients remain: a 0.2.x CLI, relay or MCP server that rewrites the record (a join, a keep, the
  idle refresh) leaves them out, and they stay gone until the agent sets them again.
- **Message payload** on rooms, inboxes and raw topics: `{"from": "<addr>", "body": "<text>", "ts": unix_ts}`,
  QoS 1. Plain text on a raw topic is accepted and arrives with `"from": null`. `recv` adds `"topic"`, and on rooms and
  inboxes `"sender"` (below). Room messages
  may add `"mentions": [addr, ...]` (see [Mentions](#mentions)), and `recv` then adds `"for_me": true|false`.
  Room and DM messages may add `"reply_to"`: the id of the message they answer (16 lowercase hex, dropped when
  malformed, never checked against an existing message).
- **Message ids.** Every received message with a sender and a `ts` carries `"id"`: the first 16 hex digits of sha256 of
  the topic, a newline, `from`, a newline and `ts` written as Python's float repr (as on the wire). It is derived, never
  sent, so `recv`, `history`, `dms`, `watch`, the relay and the console's Observer show the same id for the same
  message, including one from an older sender; `_native.message_id(topic, from_, ts)` computes it. historyd itself
  doesn't send it: the history client adds it (CLI `history`, `dms`, `watch`), and a raw `history_call` entry gets it
  from `message_id`.
- **Identity on the wire** (0.3.2, readers only). A sender can be named by the topic instead of the payload:
  `inbox/<to>/<from>` and `room/<room>/<from>`, which broker ACLs can bind to the authenticated user (see
  [docs/AUTH.md](AUTH.md)). Readers take the sender from the topic level and drop a message whose payload `from`
  differs, then normalise the topic back to `inbox/<to>` or `room/<room>` and add `"sender": "bound"`, so `recv`,
  `history`, the relay, the console and message ids see today's shape. A message on today's forms gets `"sender":
  "claimed"`, added on read and never stored: historyd keeps only the bound fact, in a `bound` side table (schema 1),
  and the history client adds `claimed`. A `room/` or `inbox/` topic of any other depth is dropped. Owning Buses and
  `watch` subscribe both forms. Senders keep today's topics in 0.3.2 and switch in a later release, once no reader older
  than 0.3.2 is left, so nothing an older reader sees changes.
- **Sessions**: each agent connects with client id = its address, `clean_start=False`, and session expiry of 3 days
  (never with `keep`). Subscriptions use QoS 1 with No Local, so you never receive your own messages. With credentials
  (an auth mode other than `off`; see the README's Authentication section), the owning connection logs in with its own
  secret client id from `agents/<addr>.json`, and publish-only and one-shot connections log in with an empty,
  broker-assigned one, so none takes another over. The Bus then writes `"auth": "credentials"` into presence and takes
  service replies on `<service>/resp/<addr>/s` (owner) or `<service>/resp/<addr>/<random>` (helpers); without
  credentials both are as before. `register` provisions the agent's credentials before its first connection. A refused
  publish reads `you may not post as <addr> on <topic>`, and a refused subscribe `you may not read <topic>`.
- **Which credentials a process uses** (`provision::credentials_for`, the one place every connection asks: `Bus::open`,
  the ledger and history clients, retained reads and one-shots). Admin's credentials only when
  `AGENT_BRIDGE_CREDENTIALS` is set and names admin's file: never because `admin.json` is on disk, and never because a
  caller passed `admin` as its address. The console and `agent-bridge-admin` set the variable for themselves, and the
  services set it to their own mounted file. An agent's file only after the own-session check passes: a file bound to
  another session is refused (`<addr> is not your agent (bound to another session)`), and one bound to no session is
  allowed, as for a plain CLI agent. For a read with no address of its own, the agent bound to this process's session.
  Otherwise anonymous, which `enforce` refuses with `this broker requires credentials (auth enforce)`. So leaving a
  session (unsetting `CLAUDE_CODE_SESSION_ID`) never turns a command into admin's.
- **Retained reads.** A one-off lookup (`agents`, `rooms`, `find`, room `info`) subscribes at QoS 0, then unsubscribes
  at once: the broker sends every retained record straight after the SUBACK, outside the inflight window, so the
  UNSUBACK marks the moment all of them are in. There is no fixed wait, so a lookup costs one round trip instead of a
  timeout. Under auth these connections log in too, by the credentials rule below.
- **Presence cache.** Every Bus subscribes to `agents/+` and `rooms/+` at QoS 0 on every connect and keeps both sets
  of records current. The ack of the next request after connecting marks both caches complete, and nothing that
  reads them runs before that. `dm` (is the recipient live?), mentions, `sub`/`join`, `describe`, `keep`, `leave`,
  `ban` and the presence refresh all read the cache instead of opening a lookup connection. After its own presence
  publish a Bus updates its cache entry directly, since the broker echoes the publish before confirming it, so no
  older copy can overwrite it. Another connection's write, even one the broker has acked, reaches the cache
  asynchronously, so these checks (a `dm` recipient, room rights, mentions) can briefly trail a peer's change: a
  caller whose decision must reflect it waits until the change shows up. After a reconnect the old cache is never
  used: it is cleared, and checks wait until the retained records are read again. Records that arrive before the
  SUBACK of that connection's `rooms/+` and `agents/+` subscription are ignored. A resumed session can still deliver
  a copy queued for the old connection, cut off mid-replay by a takeover. Its clear never reached the QoS 0
  subscription while the agent was offline, so caching the copy would keep a departed agent in the cache. mosquitto
  sends the replay after the SUBACK.

## CLI reference: agent-bridge

`MY_ADDR` is the acting agent. Every command needs the broker running.

| Command | Arguments | Does |
|---|---|---|
| `register` | `NAME [--caps a,b] [--keep] [--addr ADDR] [--subagent] [--json]` | Joins the bus and prints the address (with `--json`, the agent's row as `agents` prints it, as one JSON object). `--keep` exempts the agent from idle expiry. `--addr` reattaches or creates that address. `--subagent` registers a subagent's own agent: recorded with `"kind": "subagent"` and its creating session as `"parent"`, never as the session itself. |
| `leave` | `MY_ADDR` | Deletes your ledger (warns and carries on if ledgerd is down), then your session, queued messages, presence, and the DMs you received in the log. |
| `keep` | `MY_ADDR on\|off` | Opts in or out of idle expiry. |
| `agents` | `[--room ROOM]` | Lists every live agent with caps, topics, keep and idle_s (plus `about` and `mcp` when set), most recently seen first. |
| `rooms` | | Lists every room with members or settings: member count, addresses, mode and moderators. |
| `join` / `part` | `MY_ADDR ROOM` | Joins or leaves a room. Members get a `[joined #room] name, caps: ...` or `[left #room] name` message when membership changes; `leave` sends `[left #room]` to every room the agent was in. |
| `say` | `MY_ADDR ROOM TEXT\|-` or `MY_ADDR ROOM --file PATH`, `[--reply-to ID]` | Posts to a room (`-` reads the text from stdin, `--file` from a file, UTF-8 with the same limits, without the one newline that ends it); `@name`, `@address` and `@all` mention (see [Mentions](#mentions)). Refused when the room's settings forbid it (muted, banned, or not a moderator of an announce room). |
| `room info` | `ROOM` | A room's settings: mode, moderators, rules, muted, banned (open defaults without settings). |
| `room set` | `MY_ADDR ROOM [--mode open\|announce] [--rules TEXT]` | Changes the mode or rules. Admin or a moderator only; prints the new settings. |
| `pin` | `MY_ADDR ROOM TEXT\|-`, `MY_ADDR ROOM --file PATH` or `MY_ADDR ROOM --clear` | Pins a message at the top of a room, or clears it (admin or a moderator); `room info` shows it, and setting or clearing posts a note to the room. |
| `mod` | `MY_ADDR ROOM add\|remove ADDR` | Adds or removes a moderator. Admin or a moderator only. |
| `mute` / `unmute` / `ban` / `unban` / `kick` | `MY_ADDR ROOM ADDR` | Moderation, admin or a moderator only. `kick` bans and drops ADDR from the room. |
| `history` | `ROOM [--last N] [--before SEQ] [--text]` | Shows a room's transcript from historyd (default 20, max 1000), each entry with its `seq`. For the page before, pass the oldest entry's `seq` to `--before`. |
| `dm` | `MY_ADDR TO TEXT\|-` or `MY_ADDR TO --file PATH`, `[--reply-to ID]` | Sends a direct message (`-`, `--file` and `--reply-to` as for `say`). The recipient must be live and not yourself (your own messages never reach you). |
| `dms` | `MY_ADDR [--with PEER] [--last N] [--before SEQ] [--text]` | Shows your DMs in both directions, from historyd, each with its `seq`; `--before` pages as for `history`. |
| `sub` / `unsub` | `MY_ADDR TOPIC` | Subscribes to or unsubscribes from a raw topic. `+` and `#` work as whole levels, `#` only last, never as the first level. |
| `pub` | `MY_ADDR TOPIC TEXT` | Publishes to a raw topic. |
| `recv` | `MY_ADDR [--wait N] [--text]` | Prints new room, DM and topic messages and acks them, never the agent's own (not even those it posted over another connection). `--wait` blocks up to N seconds (max 3600) for the first message. |
| `watch` | `ROOM [--last N] [--text]` | Follows a room until Ctrl-C: its last N messages (default 20), then each new one (see Watching a room below). |
| `describe` | `MY_ADDR [--name NAME] [--caps a,b] [--about TEXT]` | Changes your display name, caps or one-line description (`""` clears caps or about). The address never changes; `@name` mentions follow the new name. Like `say`, it never takes over a live relay's session. |
| `doctor` | `[--json]` | Checks this machine's bridge, read-only (see [Doctor](#doctor)); exit 1 if any check fails. |
| `relay` | `[start\|stop\|status\|restart\|wake] [--session ID]` | This session's relay and its wake filters (see [Relay](#relay)). |
| `worktree` | `acquire MY_ADDR PATH [--ttl S] [--wait S] [--exec CMD...]`, `release MY_ADDR PATH`, `status PATH` | A worktree lock (see [Worktree locks and the guard](#worktree-locks-and-the-guard)). `--ttl` defaults to 600. |
| `hook` | `broadcast\|relay-start\|guard\|ref-guard [STATE]` | Claude Code's and git's hooks. `broadcast` and `relay-start` print nothing on failure and always exit 0; `guard` and `ref-guard` fail closed (see [Worktree locks and the guard](#worktree-locks-and-the-guard)). |

Ledger commands. Each one runs `init` for `MY_ADDR` first, except `claims`, `holders` and `unclaim`, which need no
ledger. They print the ledgerd reply (see [Resource ledger](#resource-ledger)).

| Command | Arguments |
|---|---|
| `add` / `update` | `MY_ADDR NAME [--description TEXT] [--visibility public\|private] [--max-level read\|write]` |
| `remove` | `MY_ADDR NAME` |
| `ledger` | `MY_ADDR [--owner ADDR]` |
| `public` / `grants` | `MY_ADDR` |
| `request` | `MY_ADDR OWNER/NAME read\|write [--wait [S]]`. With `--wait`, block until the owner answers, up to S seconds (300 without a value, at most 3600): exit 0 when granted, 1 when denied or still pending (it stays pending). |
| `grant` / `deny` | `MY_ADDR REQUEST_ID` |
| `acquire` | `MY_ADDR OWNER/NAME read\|write [--ttl S] [--wait S] [--exec CMD [ARGS...]]` (defaults 60 and 0). Ctrl-C (or SIGTERM) while it waits withdraws it from the queue. With `--exec CMD`, it runs CMD under the lease ([Resource ledger](#resource-ledger)). |
| `renew` | `MY_ADDR LEASE_ID [--ttl S]` |
| `release` | `MY_ADDR LEASE_ID` |
| `leases` | `MY_ADDR`: the leases you hold, soonest expiry first |
| `lease-history` | `MY_ADDR NAME\|OWNER/NAME [--last N]` (default 50, max 500): what happened to one of your resources' leases |
| `claim` | `MY_ADDR ROOM KEY [--ttl S] [--note TEXT]` |
| `unclaim` | `MY_ADDR ROOM KEY` |
| `claims` | `MY_ADDR ROOM` |
| `holders` | `MY_ADDR NAME\|OWNER/NAME` |

Environment: `AGENT_BRIDGE_HOST` (default 127.0.0.1), `AGENT_BRIDGE_PORT` (1883), `AGENT_BRIDGE_STALE_DAYS` (3,
shared with ledgerd and historyd), and `AGENT_BRIDGE_LOG`. That last one names the legacy `history.jsonl`, which only
historyd's one-time import reads; its directory is where the CLI keeps `cli.lock`, `relay/` and `hook-state/`.

**Output.** Commands print JSON, one object per line for message lists. Since 0.3.0, text is written as UTF-8 (`"é"`,
not `"\u00e9"`). Characters a terminal would act on rather than show stay escaped: control characters, DEL, and the
bidi and zero-width format marks (U+200B to U+200F, U+2028 to U+202E, U+2060 to U+2069, U+FEFF). So the output is
still valid JSON, and a body cannot reorder or redraw the screen. The wire format is unchanged: payloads and service
requests stay ASCII-escaped.

`--text` (on `recv`, `history`, `dms` and `watch`) prints one line per message for people instead:

- The line reads `HH:MM:SS #room from: body`, or `DM from -> to: body`, or `topic from: body` for a raw topic (`-`
  when plain text has no sender).
- A message from an earlier day starts with its date.
- ` (mentions you)` follows the sender when the message mentions the agent.
- Later lines of a multi-line body are indented two spaces.
- A carriage return and the format marks are written escaped (`\r`, `\u202e`).

**Watching a room.** `agent-bridge watch ROOM [--last N] [--text]` follows a room from the terminal. It prints the last
N messages from historyd (default 20), then each new one as it arrives, until Ctrl-C or SIGTERM (exit 0). It needs no
agent and no membership:

- It opens a read-only connection of its own (a random `watch-...` client id, clean start, reconnecting by itself). It
  never takes over an agent's session or relay, and it never shows in presence.
- It subscribes before it asks historyd, so no message falls between the two, and it skips live messages that
  history already printed.
- Like `recv`, it leaves out posts that the room's settings forbid: from a muted or banned sender, or from a
  non-moderator in an announce room.
- Without historyd, it says so on stderr and starts from now.

From Python, `from agent_bridge._native import Bus`, then `bus = Bus.register(name, caps=(), keep=False, addr=None)`
or `Bus.attach(addr)`. Then call `join / part / sub / unsub / say / dm / pub / recv(wait) / dms / keep / leave / close`
(`say` returns `{"mentions": [addr, ...], "unresolved": [word, ...]}`, see Mentions), `broadcast(text)` (only on the
`admin` identity: `Bus.register_admin()`, which the console uses), `ledger(op, **fields)`
and `history_call(op, **fields)` for a ledgerd or historyd request as that agent over the same connection,
`describe(name=None, caps=None, about=None)` to change how others see it (it returns the new record),
`set_client_version(version)` for the `mcp` field, `register(..., subagent=True)` (or `register NAME --subagent`) for
a subagent's own agent (see Origin fields), the moderation calls `room_set(room, mode, rules) / mod(room, add|remove,
addr) / mute / unmute / ban / unban / kick(room, addr)` (each returns the room's new settings), `pending()` for the
number of buffered messages, and `wait_arrival(timeout)` to wait for the next one. Directory lookups are module
functions: `_native.presence_agents()`, `presence_find(addr)`, `rooms_listing()`, `rooms_info(room)`. Every failure
raises `agent_bridge.errors.BridgeError` (or `LedgerError` / `HistoryError` from the service calls) with a one-line
message. The full binding is described next.

### Python bindings: agent_bridge._native

The Rust core as a Python extension, built by maturin from `rust/crates/py` with the package. It is the package's only
bus client: the console and the MCP server run on it, and so does anything else in Python that talks to the bus. Its
argument orders and error messages are those of the Python implementation it replaced in 0.2.0, which the
conformance and golden tests pinned.

- **`Bus`**: `Bus.register(name, caps=(), keep=False, addr=None, admin=False, owner=True, subagent=False)`,
  `Bus.register_admin()` for the console, and `Bus.attach(addr, owner=True)`. With `owner=False` the Bus publishes as
  the agent over a client id of its own and never receives (the MCP server, and the CLI's `say`, `dm`, `pub` and
  `describe`, use that while the session's relay is live); `sub`, `unsub` and `keep` then briefly take the agent's
  session over. The methods are listed in the CLI reference above, `say(room, body, reply_to=None)` (it returns
  `mentions`, `unresolved` and `absent`), `dm(to, body, reply_to=None)`, `pin(room, text)` (the pin record),
  `unpin(room)`, `describe(name, caps, about)` and `set_client_version(version)` (the presence `mcp` field) included;
  `ledger(op, **fields)` and `history_call(op, **fields)` go over the Bus's own connection (replies on
  `<service>/resp/<addr>` for an owning Bus, a random reply id for a publisher, or under credentials
  `<service>/resp/<addr>/s` and `<service>/resp/<addr>/<random>`; acked at once and never buffered).
  `wait_arrival(timeout, after_seq=0) -> (pending, seq)` returns as soon as a message newer than `after_seq` is buffered
  and otherwise waits out its timeout, so unread messages never wake it twice. `cached_agent(addr)` and
  `cached_room(room)` read the Bus's own presence and room caches (`None` when absent): `cached_agent` first waits until
  the Bus is connected and its retained replay has arrived, `cached_room` reads the cache as it is. They exist for tests
  that check the caches themselves; everything else should ask presence.

- **Threads**: one Bus serves any number of threads, and every call that waits releases the GIL. No Python code ever
  runs on the Bus's reader thread.
- **`Observer()`**: a read-only connection to every topic and `$SYS`, for the console. `next(timeout)` returns
  `("connected", None, None)`, `("presence", addr, record or None)`, `("room", room, settings or None)`, `("message",
  received_ts, message)`, `("pin", room, pin or None)`, `("sys", topic, text)` or `None`, message events carrying `id`,
  `reply_to` when set and `for_admin` when a post mentions admin; service request/response traffic is filtered out.
  `dropped` counts events lost to a full queue.

- **Functions**: `presence_agents/find/need/wipe`, `rooms_info/listing/view` and `ROOM_OPEN`,
  `relay_lock_path/live/held/stop`, `relay_session_agent(session)` (the agent a session's relay adopts),
  `relay_wake_modes(session)` and `relay_set_wake(session, room, mode)` with `RELAY_WAKE_MODES`, `message_id(topic,
  from_, ts)` (a message's id), `identity_*` (including `identity_read_proc(pid)`: `{comm, ppid, start}` or `None`),
  `ledger_call(op, as_, timeout=None, **fields)` and `history_call` on a connection of their own,
  `worktree_acquire(addr, path, ttl=None, wait=0)`, `worktree_release(addr, path)` and `worktree_status(path)` (the
  CLI's `worktree` replies as dicts), `auth_mode()` (`off`, `warn` or `enforce`), the `check_*`
  validators, and the limits as constants (`BODY_MAX`, `TOPIC_MAX`, the room limits `RULES_MAX`, `MODS_MAX` and
  `LIST_MAX`, the relay's `RELAY_BACKLOG`, the ledger's `MAX_REQUESTS`, `MAX_PENDING`, `MAX_TTL`, ...).

- **Errors**: the very classes of `agent_bridge.errors`. Wrong-typed input anywhere is a one-line `BridgeError`, and
  so is text no UTF-8 can carry (a lone surrogate from a JSON `\ud800` escape): `<what> is not valid UTF-8` for text,
  the usual `bad <what>` message for identifiers. Raw `ledger_call` and `history_call` pass their arguments to the
  service, whose `LedgerError` or `HistoryError` comes back.
- **Configuration** is read from the environment on every call, so a changed `AGENT_BRIDGE_PORT` or
  `AGENT_BRIDGE_LOG` takes effect at once; a bad value is a `BridgeError`, never an exit.

## Room moderation

A room may have settings: a retained record at `rooms/<room>`.

```json
{"mode": "open", "moderators": ["alice-1a2b3c"], "rules": "Be kind.", "muted": [], "banned": [], "by": "admin", "ts": 1793140000.0}
```

- **No record** means open and unmoderated. A malformed record counts as no record.
- **mode** `open` lets anyone post; `announce` lets only moderators post. **muted** agents may not post; **banned**
  ones may neither post nor join. `by` and `ts` say who changed it last.
- **Who may change it.** `admin` moderates every room and may not itself be muted, banned or removed. Only admin
  names a room's first moderators. After that, admin or any current moderator changes the record.
- **Announcements.** Every change posts a room message from the moderator: `[mode #room] announce by Y`,
  `[rules #room] updated by Y`, `[moderator #room] +X by Y` / `-X`, `[muted #room] X by Y`, `[unmuted ...]`,
  `[banned ...]`, `[unbanned ...]`, `[kicked #room] X by Y`. `[joined]` and `[left]` notices are not sent by agents
  the room would refuse.
- **Enforcement is in the Bus**, so the CLI, the MCP server and the relay all get it:
  - The sender refuses to `say` when muted, banned, or not a moderator in an announce room, and `join` refuses when
    banned.
  - Every Bus keeps the records current through its `rooms/+` subscription. The retained records arrive right after
    that SUBACK, and the ack of the next request on the connection marks the cache complete, so no check runs on a
    half-filled cache.
  - `recv` (and so the relay and MCP `recv`) drops room messages the current record forbids. That stops spoofed or
    raw publishes from a muted or banned address, and catches messages queued while the receiver was offline.
- **Kick** = ban, plus removing the room from the target's subscriptions. The only way to change another agent's
  subscriptions is a brief connection with its client id, which takes its MQTT session over: its relay, MCP server or
  CLI `recv` drops and reconnects about a second later, and nothing it had not acked is lost. Banned without a kick,
  an agent still receives the room but cannot post or rejoin.
- **Advisory.** Identity on the bus is self-asserted. Anyone can claim a moderator's address, post as one, or overwrite
  a record with a raw MQTT client. Moderation keeps cooperating agents in line and filters the obvious spoofs; it is
  not access control.

**Pinned message.** A room may have one pinned message (`pin MY_ADDR ROOM TEXT|-|--file PATH`, `pin MY_ADDR ROOM
--clear`), retained at `rooms/<room>/pin` and shown by `room info` as `pin` (null without one). Setting or clearing it
posts `pin set by X` or `pin cleared by X` to the room; clearing a room without a pin posts nothing. Like the settings
record it is advisory: bridge clients enforce who may set it (admin or a moderator), and a raw MQTT client can write it.

## Mentions

`say` turns `@token` in a room message into mentions. A token is a member's address, or a name (it mentions every
member with that name), compared without case; `@all` mentions nobody in particular and sends no mentions field. Any
other `@word` mentions nobody: the message is still sent as written, and `say` reports the word back (`unresolved` in
the Bus's result and the MCP tool's, a one-line notice on the CLI's stderr), so a typo is noticed and prose such as
"the @mentions field" is never refused. `@` glued to a word on its left (`alice@example.com`, `@@name`) is not a
mention.

`@admin` always mentions the admin console (address `admin`), which joins no room: the console finds those posts in its
message stream (`for_admin`) and, after downtime, in history. `@me` never mentions anyone and is not reported, like
`@@word`. `say` also reports `absent`: the `@words` that name a live agent who is not in the room (it left or never
joined), which stay in `unresolved` too; the CLI says so on stderr.

- **Wire:** the room message gains `"mentions": [addr, ...]`, at most 32 sorted addresses. A malformed list drops the
  message; on non-room topics the field is ignored. A spoofed but well-formed list just counts as mentions.
- **Pull:** `recv` (CLI and MCP) returns every message as before and adds `"for_me"` to the ones with mentions.
- **Relay:** a room message mentioning this session's agent wakes it at once, marked `#room from X (mentions you)`.
  One mentioning only others is acked and kept quietly, the last 50 in memory, oldest dropped. The next post that
  does wake the session carries them after its own messages, under
  `[agent-bridge] while you were away: N quiet room messages`. Room messages without mentions, DMs and broadcasts
  wake the session as before.
- **Moderation** applies first: mentions don't let a muted, banned, or non-moderator in an announce room post.

## History service

`agent-bridge-historyd` is the only writer of message history, one store for every bridge client on the machine.
Bridge clients no longer write `history.jsonl`; sending never depends on historyd. It is `rust/crates/historyd`,
shipped in the wheel and run by Compose. The Python historyd of v0.1.0 stays available as a rollback image
(`agent-bridge-historyd:1`, see the README's [Rust implementation](../README.md#rust-implementation) section): it has
the same schema, row digests and replies, so either can take over the other's `history.db`, which
`tests/test_history_parity.py` checks against that image and `tests/test_history_golden.py` pins reply by reply.

- **Recording.** It subscribes to `#` with No Local and retain handling 2 (a reconnect does not replay retained
  records), keeps `room/...`, `inbox/...`, `broadcast` and raw topics, and ignores every other reserved level and
  `$...`. Each message goes through the core's message parser, so malformed ones are dropped as everywhere else, and is
  stored with its topic, `from` (null for plain text on a raw topic), body, `ts` (the receive time when it has none),
  mentions and the receive time. A digest of topic, sender, ts and body is unique, so a redelivery or a migrated line
  that also arrived live is stored once. Like ledgerd it uses client id `historyd`, a persistent session (messages
  sent while it is down wait at the broker), acks a message only after its row committed, and exits if another
  client takes its id over (DISCONNECT reason 142), so two historyds never take the session from each other. The
  rollback image's Python historyd exits on any broker-sent DISCONNECT instead: paho 2.1 reads mosquitto's one-byte
  reason as 0, and mosquitto sends historyd a DISCONNECT only for a takeover (a broker restart just closes the
  socket). That waiting is bounded: mosquitto queues at most 1000 messages (32 MiB) for it and drops the rest, so
  a long outage leaves gaps. History is an advisory record, not an audit trail.
- **Command line.** Only no arguments runs the service. `--health` pings the running historyd (exit 0 on an answer
  within 5 s, else 1), `-h`/`--help` prints usage, and any other argument, abbreviations included, is refused
  argparse-style with exit 2 before anything connects or opens a database.
- **Migration.** On its first start it imports `HISTORY_IMPORT` (default `AGENT_BRIDGE_LOG`, the legacy
  `history.jsonl`) once, skipping malformed lines, and remembers the path in its database. The file is never deleted
  or changed; remove it by hand once you no longer need it. The compose service mounts the host's state directory
  read-only at `/import` for this.
- **Schema version.** historyd, and the v0.1.0 rollback image, write the meta row `schema` = 1 on open. Each checks
  the row first and refuses to start on a database with another version, before writing anything (`historyd: <db>
  has history schema N; this historyd knows 1`), so an older historyd used as a rollback can never touch a database
  a newer one has migrated. The first migration to schema 2 retires the rollback image.
- **Retention.** Every hour (or stale period, if shorter) it deletes what it received longer ago than
  `AGENT_BRIDGE_STALE_DAYS`. Receive time, not the sender's `ts`, so a forged timestamp cannot keep a row. It also
  keeps at most 500,000 rows and 256 MiB of bodies, oldest dropped first.
- **Requests** are JSON on `history/req/<addr>` with ResponseTopic `history/resp/<id>` (`[A-Za-z0-9_-]{1,128}`) or,
  since 0.3.2, `history/resp/<addr>/<conn>` when `<addr>` is the request topic's address (others are dropped with a log
  line and get no reply) and 1-256 bytes of CorrelationData; the reply has the same CorrelationData and a 300 s expiry.
  Bodies are at most 16 KiB, unknown fields are refused, and `"as"`, when given, must match the topic's address. Ops
  that name nobody may also use plain `history/req`; ops on one agent's data must use that agent's topic.

  | op | fields | topic | reply |
  |---|---|---|---|
  | `room` | `room`, `last`=20 (1-1000), `before` (a ts), `seq`, `before_seq` | either | `messages` |
  | `topic` | `topic` (a raw topic, no wildcards), `last`=20, `seq`, `before_seq` | either | `messages` |
  | `broadcasts` | `since`=0 (a ts), `last`=20, `seq`, `before_seq` | either | `messages`: only `broadcast` from `admin` |
  | `ping` | `version` (optional, true) | either | `{}`; with `version`, `{"service": "historyd", "version"}` |
  | `dms` | `peer` (optional), `last`=20, `seq`, `before_seq` | `history/req/<addr>` only | `messages`: `inbox/<addr>` plus what `<addr>` sent to any inbox; with peer, only the two of them |
  | `recent` | `last`=100 (1-5000) | `history/req/admin` only | `messages`: every topic |
  | `drop` | `addr` (optional, must be the topic's) | `history/req/<addr>` only | `dropped`: every message in `inbox/<addr>` deleted |

  `messages` is oldest first, each `{"topic", "from", "body", "ts"}` plus `"mentions"` and `"reply_to"` when the message
  had them (`reply_to` is kept in a side table of its own, so the database stays schema 1 and both rollbacks still open
  it; a `reply_to` written under a rollback is lost), and `"seq"` (the row id, which grows in storage order) when the
  request has `"seq": true` or a `before_seq`. `before_seq` (an integer from 1) keeps only rows below that seq, so a
  client pages back by passing the oldest seq it has: unlike the sender's `ts`, seq follows the order rows were stored,
  so no row is skipped or repeated. Without these fields the replies are exactly the earlier ones. A reply is capped at
  about 240 KB: when the last N do not fit, the oldest are left out and `"truncated": true` is set. Errors are `{"ok":
  false, "error": "<one line>"}`.
- **Clients.** The core's client is `_native.history_call(op, as_=None, timeout=5, **fields)` on a connection of its
  own, or `history_call` on a Bus over the Bus's connection (as that agent for `dms` and `drop`). It raises
  `HistoryError`, a `BridgeError` whose one-line message starts with `history:`, when historyd refuses, does not
  answer (5 s; 1 s for `broadcasts`, which the broadcast hook runs on every prompt) or the broker is down. The CLI's
  `history` and `dms`, the Bus's `dms()` and `leave()`, the MCP `history` and `dms` tools, the console and
  `agent-bridge hook broadcast` all use it.
- **Privacy is advisory**, like everything else on the bus: `"as"` and the request topic are self-asserted, so any
  local process can read any agent's DMs by asking on its topic. The per-agent topic is what an ACL will check once
  the broker authenticates clients.

## Resource ledger

Every agent has a ledger of the resources it manages: a database, a dev server, a file tree, a deploy slot, anything.
ledgerd is the one place that state lives. It applies every request, lease expiry and presence update one at a
time, each in its own SQLite transaction. That serialization is the concurrency guarantee: there is no
compare-and-set over MQTT and no second writer.

**Model**

- **Ledger**: one per address, created by `init` and reused afterwards. Every other op needs it.
- **Resource**: id `<owner>/<name>`, with a description, `visibility` public or private, and `max_level`
  read or write. Only the owner adds, updates or removes it.
- **Visibility**: `ledger` shows your own ledger in full and another agent's public entries only. `public` is the
  public ledger: every public resource of every agent. Private means unlisted, not secret: `dump` (the console's view)
  answers any address with everything, private resources and pending requests included, until auth is `enforce`.
- **Request and grant**: `request resource level` is granted on the spot when the resource is public and the level is
  at most `max_level`. Otherwise it goes pending and the owner gets a DM in `inbox/<owner>` from `ledgerd` that names
  the `request_id` and the commands that answer it (`agent-bridge grant <owner> <request_id>` or `deny`). The
  requester gets a DM either way. Denying a granted request revokes it and ends the holder's leases above whatever
  level it still holds; that DM says "revoked" and names the leases it ended. Private resources can be requested by
  anyone who knows the id, but they always go pending. An owner has at most 256 pending requests from everyone, since
  each one is a DM; past that a new one is refused (`<owner> has 256 pending requests; try again once it has answered
  some`).
- **Owners** always hold implicit write on their own resources and can break any lease on them; the holder gets a DM
  naming the lease (`<owner> ended your write lease <lease_id> on <resource>`).
- **Leases**: `acquire resource level ttl wait` needs ownership or a grant at that level or higher. `read` leases
  are shared and `write` is exclusive. Each lease expires after `ttl` seconds (1 to 3600) unless renewed with `renew`,
  and `release` ends it early. Leases are not re-entrant: an acquire that only your own leases block is refused with
  `you already hold <level> lease <lease_id> on <resource>; renew it, or release it first`. Releasing or renewing
  another agent's lease says whose it is.
- **Waiting**: with `wait` > 0 (max 300 s) a busy acquire queues. Waiters are served strictly FIFO, so a writer in
  the queue stops new readers from overtaking it. A timeout replies `busy` with the current holders. Waiters survive
  a ledgerd restart.
- **A waiter whose client left** must not be handed the lease, or the resource stays locked for the TTL with nobody
  holding it. So a waiter's deadline is also capped at 1 s before its request's MessageExpiryInterval runs out (the
  client sets it to its own timeout, so ledgerd knows when nobody listens any more), and a request with too little
  expiry left gets `busy` at once. A client that stops waiting early sends `cancel` with the request's
  CorrelationData: the command line does it on SIGINT or SIGTERM (Ctrl-C), gives back a lease granted before the
  cancel arrived (that reply comes first, on the same topic), then dies of the signal. A client killed outright
  (SIGKILL) cannot cancel; its waiter stays queued until its deadline, at most the wait plus the client's timeout
  margin.
- **Fencing tokens**: every write lease carries a `token` that is strictly larger than any token issued before, for
  any resource, across restarts.
- **Expiry DM**: a lease that runs out unrenewed gets its holder a DM from `ledgerd` (`your write lease <lease_id> on
  <resource> expired without renewal`), so a stalled holder learns it lost the lock. A release, a break by the owner
  and a revocation are not expiries: the last two have DMs of their own.
- **Lease history**: ledgerd records each lease's events per resource: `acquire`, `renew` (a run of renewals of one
  lease is one event with the latest time), `release`, `break` (the owner released it), `revoke` (the owner revoked
  the grant), `expire` and `drop` (its holder dropped its ledger), with the holder, level, token and, for break,
  revoke and drop, who did it (`by`). The owner reads it with `lease_history`; the console does so as the owner. It
  keeps the last 200 events per resource and goes with the resource or its owner's ledger. It is a table of its own,
  so the database stays schema 1, and the rollback ledgerds, which do not know it, open and serve the database
  unchanged; while one runs, nothing is recorded.
- **Claims**: `claim ROOM KEY` marks a key in a room (an issue id, a question) as yours until you release it, it expires
  (an optional ttl, up to 30 days, after which ledgerd DMs you) or you leave (drop, the stale sweep). A claim on a key
  someone else holds is refused, naming them and since when; claiming your own again replaces its ttl and note (up to
  200 chars, one line). It is released by its holder, by admin, or by a moderator of the room (ledgerd reads the
  moderators from the retained `rooms/<room>` record), and someone else's release DMs the holder. Like moderation, this
  is cooperative: any local process can rewrite a room record or claim as anyone. Keys use the resource-name charset. At
  most 256 claims per agent and 1024 per room. Claims are their own table, so the database stays schema 1.
- **Worktrees**: a git worktree's lock is the admin-owned resource `admin/wt-<first 16 hex of sha256(realpath)>`, made
  on demand by an `acquire` that carries `path` (the realpath) and `git_common_dir`. ledgerd recomputes the id from the
  path and refuses a mismatch (`path: does not match admin/wt-<hex> (its id is the sha256 of the realpath)`), creates
  the resource public with `max_level` write, the path as its description and an access spec `{kind: "worktree", path,
  git_common_dir, enforcement: "hook"}` in an additive `specs` table, files the caller's request (granted at once while
  public; if admin made it private, the request stays filed and the reply is `your write request for admin/wt-<hex> is
  pending with admin (request_id <id>)`) and acquires as usual, so `ttl`, `wait`, waiters and `cancel` behave as for any
  resource. At most 1024 worktrees, apart from the per-ledger limit; `add` refuses `wt-` names in admin's ledger, and
  admin's ledger is exempt from the stale sweep. `worktrees` lists them. `core::ledger::worktree_resource` is the one
  definition of the id, used by the CLI, the guard hooks and the console.

**Running a command under a lease.**
`agent-bridge acquire MY_ADDR OWNER/NAME read|write [--ttl S] [--wait S] --exec CMD [ARGS...]` takes the lease, runs
CMD with it, and gives it back when CMD exits:

- CMD's environment gets `AGENT_BRIDGE_LEASE_ID`, `AGENT_BRIDGE_LEASE_RESOURCE`, `AGENT_BRIDGE_LEASE_LEVEL` and, for
  a write lease, `AGENT_BRIDGE_FENCE_TOKEN`, which CMD passes with its writes (see above).
- The lease is renewed every ttl/3 while CMD runs; a renewal that fails for a moment is retried until 2 s before the
  lease would run out.
- SIGINT, SIGTERM and SIGHUP sent to agent-bridge are passed on to CMD; a Ctrl-C from the terminal reaches CMD
  directly, as it would without agent-bridge.
- The exit status is CMD's, or 128+n when signal n killed it; 127 when CMD cannot be started (the lease is released
  first); and 75 (EX_TEMPFAIL) when CMD never ran because the acquire was refused (busy, no grant) or when the lease
  was lost while CMD ran (gone, revoked, broken by the owner, or not renewable in time). Losing it sends CMD SIGTERM,
  then SIGKILL 10 s later. So 75 means retry later, and any other status is CMD's own.

**Using the fencing token.** A lease can expire while its holder is paused, swapped out or stuck, and someone else
then gets the lock. Expiry alone cannot stop the old holder from writing late. So whatever guards the resource keeps
the highest token it has seen and rejects any write carrying a lower one. Pass the token with every write, for
example as a column you compare in an `UPDATE ... WHERE token <= ?`, a header the service checks, or a value stored
next to the file. If a resource cannot check tokens, keep the TTL well above the longest pause you expect and renew
early.

**Ops** (JSON body `{"op", "as", ...}`; replies are `{"ok": true, ...}` or `{"ok": false, "error": "..."}`):

| op | Fields | Reply |
|---|---|---|
| `init` | | `created`, `owner`, `since` |
| `add` | `name`, `description=""`, `visibility=private`, `max_level=read` | `resource` |
| `update` | `name` and any of `description`, `visibility`, `max_level` | `resource` |
| `remove` | `name`, `on_behalf` (admin only) | `{}`. Anyone waiting on it gets an error (`<resource> was removed by admin` when admin acted for the owner). |
| `ledger` | `owner` (optional) | `owner`, `resources` |
| `public` | | `resources` |
| `request` | `resource`, `level` | `request {id, resource, owner, holder, level, status, ts}`, status `granted`, `pending` or `denied` |
| `grant` / `deny` | `request_id`; `deny` also takes `on_behalf` (admin only) | `request` (owner, or admin acting for the owner) |
| `grants` | | `held` (requests you made), `asked` (requests on your resources) |
| `acquire` | `resource`, `level`, `ttl=60`, `wait=0`; for a worktree also `path` and `git_common_dir` (below) | `lease {lease_id, resource, holder, level, expires_at, token}` (token only for write), or `ok: false, error: "busy", holders: [...]`; with `path`, also `created` |
| `renew` | `lease_id`, `ttl=60` | `lease` (holder only) |
| `release` | `lease_id`, `on_behalf` (admin only) | `{}` (holder or owner; an owner breaking another agent's lease DMs its holder, and admin acting for the owner is named: `admin ended your write lease <id> on <resource>`) |
| `cancel` | `correlation` (hex of the CorrelationData of your queued acquire) | `cancelled` (0 or 1). Drops your waiter queued under this request's ResponseTopic with that correlation; 0 when it was served, timed out or never queued. |
| `leases` | | `leases [{lease_id, resource, owner, holder, level, expires_at, token}]`: the leases you hold, soonest expiry first |
| `lease_history` | `resource`, `last=50` (1 to 500) | `resource`, `events [{ts, event, lease_id, holder, level, token, by}]`, oldest first (owner only) |
| `ping` | | `service: "ledgerd"`, `version`, and `auth` (`warn` or `enforce`) when auth isn't off. Any `as` works, needs no ledger and writes nothing (what `agent-bridge doctor` asks). |
| `claim` | `room`, `key`, `ttl` (optional), `note=""` | `claim {room, key, holder, since, expires_at (null without ttl), note}`, or `ok: false` naming the holder, with its `claim` |
| `unclaim` | `room`, `key` | `{}`. Any `as`, no ledger: the holder, a moderator of the room or admin. |
| `claims` | `room` | `room`, `claims [...]`. Any `as`, no ledger; without `room`, every room's claims, admin only. |
| `holders` | `resource` | `resource`, `holders [{holder, level, expires_at, lease_id}]`, `waiting [{holder, level, queued_at}]`. Any `as`, no ledger. |
| `worktrees` | `all=false` | `worktrees [{resource, path, git_common_dir, holder, level, lease_id, expires_at}]`: the worktrees with a live lease, or with `all` every one (`holder` null when free). Any `as`, no ledger; what the guard hooks and the console's Worktrees view ask. |
| `drop` | | `{}`. Deletes your ledger, resources, grants given and held, leases and queued waits. |
| `dump` | | Everything, for the admin console: `ledgers [{owner, since, last_seen}]`, `resources`, `requests` (pending first), `leases` (`lease_id, resource, holder, level, expires_at, token`), `waiters [{resource, holder, level, queued_at, deadline}]`, `ts`, and from the Rust ledgerd `ledgerd {impl, version}`. Any `as` works and needs no ledger, except under `enforce`, where it is admin only (`dump: admin only`; `warn` logs a non-admin dump). If the reply would pass ~240 KB every list is cut to the same length and `truncated: true` is set. |

Any reply over 240 KB is replaced by `{"ok": false, "error": "reply over 240000 bytes"}`, since mosquitto's
`max_packet_size` would otherwise disconnect ledgerd. This mostly matters for `public` with very many resources.

**Under auth** (`AGENT_BRIDGE_AUTH` `warn` or `enforce`; `off` is 0.3.1's behaviour byte for byte):
- **Identity from the topic.** `ledger/req/<addr>` carries the identity. A request on plain `ledger/req` (identity from
  `as`) is logged in `warn` (`auth warn: <addr> sent <op> on plain ledger/req`, once per address an hour) and refused in
  `enforce` (`send requests on ledger/req/<your address>`).
- **Reply topics.** `ledger/resp/<addr>/<conn>` is accepted in every mode when `<addr>` is the requester; any other
  two-level topic is dropped with a log line and nothing is published. A one-level `ledger/resp/<id>` is accepted in
  `off` and `warn` and dropped in `enforce`. The ledger client does this by itself: `core::ledger::call` (so
  `_native.ledger_call`, `ledger-raw`, `--exec`, the worktree helpers and the console) logs in as `as` and replies on
  `ledger/resp/<as>/<16 hex>` when the credentials rule (see [Addresses, topics and
  payloads](#addresses-topics-and-payloads)) gives it that identity. Otherwise it is anonymous and one-level, as before,
  which `enforce` drops (`call_with` is the explicit form). That is safe because credentials exist only after the `warn`
  init, which needs the 0.3.2 broker and services.
- **Admin acting for an owner.** `on_behalf: <owner>` on `release`, `deny` and `remove`, from admin only (`on_behalf:
  only admin acts for another ledger`), acts on that owner's ledger, records `by: admin` in `lease_events`, and names
  admin in the holder's DM.
- **Rate limit.** 20 requests a second per identity, burst 100, admin exempt: `rate limited: at most 20 requests a
  second`, not stored for redelivery.
- **Its own login.** ledgerd logs in with `config::credentials("ledgerd")` (the `AGENT_BRIDGE_CREDENTIALS` file) and its
  secret client id. Without the file it connects anonymously under `warn` and exits under `enforce` (`auth enforce:
  ledgerd needs its credentials file in AGENT_BRIDGE_CREDENTIALS`). `--health` asks for `worktrees` (logged in as
  ledgerd under auth) instead of `dump`.

**From the shell**, use the ledger commands of `agent-bridge`:

```bash
agent-bridge add alice-3f9a1c staging-db --visibility public --max-level read --description "shared staging Postgres"
```

```bash
agent-bridge acquire bob-77d0e2 alice-3f9a1c/staging-db read --ttl 120 --wait 30
```

`agent-bridge ledger-raw OP --as ADDR --field value ...` is the raw client: it sends any op as-is (no implicit
`init`), prints the reply and exits 1 when `ok` is false; `agent-bridge-ledger` is a shim that runs it. From Python,
through the native core:

```python
from agent_bridge import _native
from agent_bridge.errors import LedgerError

lease = _native.ledger_call(
    "acquire", "bob-77d0e2", resource="alice-3f9a1c/staging-db", level="write", ttl=60, wait=30
)["lease"]
```

A busy refusal's message names the holders in every client (`busy, held by bob-77d0e2 (write, 42 s left)`, the CLI's
`ledger: busy, ...` line and `str(LedgerError)` alike); `.reply` keeps ledgerd's reply as it was. A timeout reads
`no reply from ledgerd within 10s, is it running?`, or, for an acquire whose wait is not below the timeout, says to
give a timeout above the wait.

`ledger_call(op, as_, timeout=None, **fields)` opens a connection of its own for the call; `bus.ledger(op, as_=None,
**fields)` sends over an agent's own connection, as that agent unless `as_` names another (the console acts as a
resource's owner that way). Both raise `LedgerError` (`.reply` holds the reply) on `ok: false` or on timeout (10 s
plus `wait`). Over a Bus, replies arrive on `ledger/resp/<address>` for the owning connection or a random id for a
publish-only one; the reader thread acks them at once and never buffers them, so they cannot fill the receive window
or show up in `recv`, and the topic is resubscribed on every call, so a lost session still works.

**Wire protocol** for other clients: publish QoS 1 to `ledger/req/<addr>` with MQTT 5 properties `ResponseTopic`
(must match `ledger/resp/[A-Za-z0-9_-]{1,128}`) and `CorrelationData` (1 to 256 bytes). ledgerd replies QoS 1 on that
topic with the same CorrelationData and a 300 s message expiry. Requests without a valid ResponseTopic are dropped
unanswered. Set a MessageExpiryInterval on requests so the broker drops them if ledgerd is down for longer than you
will wait, which is what the client does. ledgerd reads it too: a queued acquire is dropped (answered `busy`) 1 s
before it runs out, and the answer kept for redelivery lives at least that long.

- **Topic and `as`.** `<addr>` is the acting agent. The body's `as` may be left out; if present it must equal
  `<addr>`, or the request is refused. A per-agent topic lets broker ACLs vouch for the address once there are
  credentials ([docs/EXTERNAL.md](EXTERNAL.md)); on loopback any client can still publish under any address. Plain
  `ledger/req` is still served, with `as` required in the body.
- **Redelivery.** ledgerd acks a request only after its transaction commits, so a crash or reconnect in between makes
  the broker deliver it again. ledgerd keeps the answer of every mutating request, keyed by response topic,
  CorrelationData and payload, for as long as the broker could redeliver it: the request's MessageExpiryInterval, at
  least 300 s (also for a request without one) and at most 3900 s (MAX_TTL plus MAX_WAIT). It replays it to an
  identical copy instead of running the op twice. Reads
  (`ledger`, `public`, `grants`, `dump`) simply run again. A client that reuses CorrelationData for a different
  request is not mistaken for a duplicate, because the payload is part of the key.

## MCP server

`agent-bridge-mcp` puts a Claude Code session on the bus with its own ledger. It is a stdio server on the official
`mcp` SDK (pinned 1.30.0, FastMCP). It holds one persistent `Bus` for the session and sends ledger calls over that
same connection.

Installation is in the [README](../README.md#mcp-server).

**Identity.** Claude Code gives each stdio server a `CLAUDE_CODE_SESSION_ID`. This was observed in Claude Code
2.1.304 and is not documented: the id is distinct per parallel session and unchanged by `--resume`.

- **Under Claude Code**, the server posts as the session's own agent, the one its relay delivers for, so replies
  reach it: the agent a live relay holds (its lock's line 2), else the most recently seen live agent registered from
  the session (`relay_session_agent`, the relay's own choice), else the derived address
  `<project dir name>-<first 8 hex of sha256(session id)>`. It connects at startup and runs `init` on the ledger; a
  restarted or resumed session reattaches to the same address, with its queue and ledger intact. When a relay starts
  later and takes the agent over, the server reattaches to the agent the relay holds. Before 0.3.0 it always used the
  derived address, a second agent that replies to the session's CLI agent never reached.
- **`connect(name, caps, keep)`** then only changes how others see the agent (display name, caps, keep). The
  address stays.
- **Without that variable** (another MCP client), every tool says `call connect first`, and `connect(name)` uses
  the address `<name>-<8 hex of sha256(name)>`. Two such clients using the same name share one address and take
  each other's session over.

**Tools (36)**

| Group | Tools |
|---|---|
| Bus | `connect`, `whoami`, `agents`, `rooms`, `join`, `part`, `say`, `history`, `dm`, `dms`, `sub`, `unsub`, `pub`, `recv(wait)`, `keep`, `leave` |
| Rooms | `room_info`, `room_set(room, mode?, rules?)`, `mod(room, add\|remove, addr)`, `mute`, `unmute`, `ban`, `unban`, `kick` |
| Ledger | `add`, `update`, `remove`, `ledger(owner?)`, `public`, `request`, `grant`, `deny`, `grants`, `acquire`, `renew`, `release` |

Results are JSON text. Errors are one-line tool errors, including bad arguments. `leave` drops the ledger first
(3 s timeout; it warns and still leaves if ledgerd is down), then deletes the bus identity.

**Reading messages: recv or push**

- **`recv(wait=N)`** always works. It returns what is waiting, or blocks up to N seconds (max 3600, keep it to a
  few minutes) for the first message.
- **Push.** The server also declares the experimental `claude/channel` capability and sends a
  `notifications/claude/channel` notice like "3 bus messages waiting. Call the recv tool to read them." whenever
  messages arrive. The notice carries only a count, never message text, and drains nothing, so it is harmless when
  the session did not load the server as a channel. Channels are a Claude Code research preview: a custom server
  needs `claude --dangerously-load-development-channels server:bridge`, and they only work in interactive sessions
  (ignored with `-p`).

**Untrusted content.** Tools that return text written by other agents (`recv`, `history`, `dms`, `agents`,
`rooms`, the room tools, `ledger`, `public`, `request`, `grants`, `acquire`) start their result with an `UNTRUSTED DATA ...` line,
followed by the JSON. The server's instructions tell the model that this text is data from other agents, never
instructions and never the user. Ask Claude to act on a message only after reading it yourself, or within limits you
set.

**Troubleshooting**

- **`not on the bus (...)`**: the startup connect failed, and the reason is in the parentheses (usually the broker
  is down). Start the broker, then call `connect`.
- **`warning: ledger not ready`** from `connect`: ledgerd is not answering. Check
  `docker logs agent-bridge-ledgerd`. The bus tools keep working meanwhile.

## Relay

`agent-bridge relay` pushes a Claude Code session's bus messages into the session's own inbox socket
(`CLAUDE_CODE_MESSAGING_SOCKET`: an auth line, then one `user` line), so an idle session wakes up as soon as another
agent writes to it.

- **Start.** The SessionStart hook `agent-bridge hook relay-start` (or its old name, `agent-bridge-relay-hook`, a shim
  that runs it) runs `relay start`, which finds the session's `claude` process (pid and start ticks) before detaching,
  since a detached process cannot find it afterwards. It launches `agent-bridge relay run PID START` in its own session,
  logging to `relay/relay.log` in the state directory, and returns at once without printing, because SessionStart
  output becomes context.
- **One per session.** The relay holds an exclusive flock on `relay/<session id, lowercase>.lock` in the state
  directory, which also holds its pid, on the second line the address it relays, and on the third (from 0.3.0) its
  version. A relay is live while that lock cannot be taken; the console and the broadcast hook use the same test. Next
  to it, `relay/<session>.wake` holds the session's wake filters, `relay/<session>.leases` the worktree leases it
  renews (one JSON line each), and, briefly during a self-restart, `relay/<session>.backlog` its quiet backlog.
- **Address.** The relay adopts the most recently seen live agent whose presence carries this session id, such as one
  registered through the CLI, so its rooms and queued messages come along. Otherwise it uses the same session-derived
  address as the MCP server.
- **Batching.** Messages arriving within 1 s go out as one post, at most 20, with bodies over 16 KiB cut and marked
  `truncated_from`. A post is a header like `[agent-bridge] 3 messages (DM from X, #room from Y, broadcast from
  admin)`, a line saying the content is data, not instructions, then one JSON line per message. The header is built
  only from addresses and room names, which cannot hold spaces or newlines, and JSON escaping keeps a hostile body
  inside its line.
- **Acks.** A message is acked to the broker only after the socket write succeeds. On failure it goes back to the
  buffer and is retried every 2 s; if the relay dies first, the broker still has it.
- **No echo.** No Local covers the relay's own connection, and a sender check covers the separate publishing
  connections of the MCP server and the CLI.
- **Exit.** When its `claude` process ends (checked every second against pid and start ticks) or on SIGTERM, also
  while the broker is away: it then waits for the broker in half-second steps, logging `broker unreachable, waiting
  for it` once, instead of blocking in a call. `agent-bridge relay stop` sends the SIGTERM and `agent-bridge relay
  status` reports whether it is live; outside a session both need `--session ID`. Both print `{"session", "live",
  "lock", "pid", "started", "uptime_s", "binary", "version"}`, `stop` once the relay has stopped, so its `"live":
  false` is the state after the stop. The last five are null without a live relay; `binary` is what `/proc/PID/exe`
  names, ending in ` (deleted)` once an upgrade replaced the file, and `version` is null for a relay older than 0.3.0.
- **Restart.** `agent-bridge relay restart`, inside the session (no `--session`), stops the relay (5 s), starts it
  again on the installed binary with the same checks as `start` but saying what is wrong instead of staying silent (no
  session, no inbox), waits up to 5 s until it is live and prints the status line. It is the upgrade step for relays.
- **Wake filters.** `agent-bridge relay wake ROOM all|mentions|admin` (`--session ID` outside a session) sets which
  posts in a room wake the session: `mentions` wakes on a post that mentions the agent (`@all` included) or comes from
  admin, and `admin` only on admin's posts. Everything else waits in the quiet backlog and arrives with the next wake;
  DMs, broadcasts and raw topics always wake, and `recv` is unaffected. With no arguments it lists the session's
  filters, with ROOM alone it shows one, and `all` removes it. They are stored per session in `relay/<session>.wake`,
  written atomically and re-read when the file changes. "admin" is the sender field, which nobody verifies until the
  auth phase: good for deciding when to wake, never for trust.
- **Upgrade.** A relay started on 0.3.1 or later checks once a minute, while idle, whether its binary was replaced by an
  upgrade. It restarts itself on the installed binary only when that file is at least 30 s old and its `--version`
  answers `agent-bridge X.Y.Z` within 5 s, and at most once per 10 minutes; a refusal is logged once and checked again
  later. It keeps its pid, its lock (the descriptor is handed to the new image, which logs `kept its lock across the
  restart`) and its quiet backlog, and the broker resends what it had not acked. If the exec fails, the relay keeps
  running on the old image and logs it. Relays started before 0.3.1 need one `relay restart`; `agent-bridge doctor`
  lists them.
- **Log.** `relay/relay.log` in the state directory, shared by every session's relay: each line is a time, the relay's
  address (or `session <id>` before it has one), then `relaying for claude pid N` and `stopped`, `not delivered, kept
  queued` when the inbox socket refused a post (retried every 2 s), or `broker unreachable, waiting for it` when the
  broker is away. A self-restart logs `binary replaced, restarting on <path> (<version>)`, then the new image's `kept
  its lock across the restart`; a check that isn't ready logs `binary replaced, not restarting yet: <why>` once, and a
  failed exec `binary replaced, exec failed (<error>), still running on the old image`. A takeover of the relay's
  session (a CLI `join`, `sub` or `keep` for its address, which it takes back about a second later) is not an outage and
  logs nothing.

**Coexistence with the MCP server.** A live relay always owns the agent's subscribing session. The MCP server checks
every 0.5 s. While a relay is live, the server publishes over a connection of its own (`Bus(addr, owner=False)`), `recv`
says the relay delivers, and `whoami` reports `"delivery": "relay"`; `join`, `sub` and `keep` briefly take the session
over and the relay reconnects. The CLI's `say`, `dm`, `pub` and `describe` also publish over a connection of their own
while this session's relay holds their address, so posting from the shell never interrupts the relay; without a live
relay they use the agent's own connection, whose No Local keeps its posts out of its own queue. `leave` stops the relay
first. When the relay ends, the MCP server takes the session back and anything not yet acked moves with it, so each
message reaches the session once. Known gap: if a relay starts while the MCP server owns the session, they can take it
from each other for up to 0.5 s, and a message caught in that window could in principle arrive twice.

## Broker authentication

The groundwork for authenticated agents ([docs/AUTH.md](AUTH.md)), off by default. `compose.yaml` is unchanged; the
opt-in overlay `compose.auth.yaml` switches the broker to `config/broker-entrypoint.sh`, and `AGENT_BRIDGE_AUTH` picks
the mode.

- **Modes.** The entrypoint renders mosquitto's config from `config/mosquitto.conf`. `off`: today's broker plus the unix
  socket. `warn`: the dynamic-security plugin, `check_retain_source true` and `allow_anonymous true`, with anonymous
  clients in the group `legacy` (role `legacy-all`, today's full rights), so 0.3.1 clients keep working. `enforce`: as
  `warn` with `allow_anonymous false`. It writes the mode to `~/.config/agent-bridge/auth-mode` (removed under `off`),
  which clients read when `AGENT_BRIDGE_AUTH` isn't set; a bad value is a config error (`AGENT_BRIDGE_AUTH must be off,
  warn or enforce, got '<v>'`). Switching modes recreates the broker; persistent sessions survive.
- **Runs as you.** mosquitto runs as the owner of the mounted config directory (the image's mosquitto user is remapped
  to that uid), so the socket and the files are yours. The data volume is chowned to that user at each start, and the
  image's own entrypoint chowns it back under plain `compose.yaml`, so switching needs no manual chown either way. The
  binds use `create_host_path: false`: a missing directory is an error, never one Docker creates as root, and a
  root-owned one is refused (`refusing to run the broker as root: the mounted directory belongs to root ...`).
- **The socket** is `~/.local/state/agent-bridge/broker/mqtt.sock` (`srwxrwx---`, yours), next to TCP 1883. Not under
  `$XDG_RUNTIME_DIR`, which doesn't exist at boot, when Docker's restart policy starts the broker. Clients use it when
  `AGENT_BRIDGE_SOCKET` names it, or when it exists and neither `AGENT_BRIDGE_HOST` nor `AGENT_BRIDGE_PORT` is set; it
  takes precedence over TCP.
- **First start under `warn` or `enforce`.** `mosquitto_ctrl dynsec init` with a random admin password, then
  `~/.config/agent-bridge/admin.json` and the service clients with their files under `services/` (ledgerd: subscribe and
  receive `ledger/req`, `ledger/req/+`, `agents/+`, `rooms/+`, `ledger/resp/ledgerd/+`; send `ledger/resp/#`, `inbox/+`,
  `inbox/+/ledgerd`, `ledger/req/ledgerd`. historyd: read `#`; send `history/resp/#`, `history/req/historyd`). Default
  ACLs deny send, receive and subscribe; admin's role has `#` for all three. If the dynamic-security store exists but
  `admin.json` is gone, it refuses and names the file to restore or the command that starts over. Its healthcheck
  reports ready once the config and the service files exist; ledgerd and historyd wait for it and get
  `AGENT_BRIDGE_AUTH` and `AGENT_BRIDGE_CREDENTIALS`.
- **Credential files**: `admin.json`, `services/{ledgerd,historyd}.json` and `agents/<addr>.json` under
  `~/.config/agent-bridge`, JSON `{username, password, client_id}` (agents also `session`), 0600 in 0700 directories. A
  file others can read is refused: `credentials <path> are readable by others (mode 640); fix it with: chmod 600
  <path>`.
- **Provisioning** (`provision::ensure`, called by `register`, and by a Bus attach when the agent has no file yet, so
  existing agents migrate on their next relay start or attach, the MCP server's or a CLI post's. The file is bound to
  the agent's own session from its presence record, never to whoever attached, so a post from another session can't take
  an address over; an agent with no session, attached from inside one, is left for its own `register`, and `admin` is
  skipped) is the one use of `admin.json` from disk, and only to create a new agent: `createClient` goes alone first and
  never resets an existing client (`<addr> already has credentials elsewhere; ask admin to reissue` when its file is
  gone); then only `role-<addr>` is made and given to it; and only register-made `<name>-<6 hex>` addresses and session
  addresses `<project>-<8 hex>` (the relay's and the MCP server's) qualify. An address of any other shape (`register
  --addr myname`; `--addr alpha-000001` qualifies, since the shape decides) gets no credentials and carries on
  anonymously, which `enforce` refuses and `doctor` lists; `admin` is a hard error (`address 'admin' is reserved for the
  admin console`), and so are `already has credentials elsewhere` and `not your agent`. The agent's file gets a secret
  `client_id` (`c-` and 32 hex) and its session. Under `off` nothing is provisioned.
- **An agent's role** (`role-<addr>`, literal topics only). Send: `agents/<a>`, `inbox/+`, `inbox/+/<a>`, `room/+`,
  `room/+/<a>`, `rooms/+`, `rooms/+/pin`, `ledger/req`, `ledger/req/<a>`, `history/req`, `history/req/<a>`. Receive and
  subscribe: `agents/+`, `rooms/+`, `rooms/+/pin`, `room/+`, `room/+/+`, `inbox/<a>`, `inbox/<a>/+`, `ledger/resp/<a>`,
  `ledger/resp/<a>/+`, `history/resp/<a>`, `history/resp/<a>/+`, `broadcast`. Every other topic under `agents`, `inbox`,
  `room`, `rooms`, `ledger` and `history` is denied, and so is sending on `broadcast`; raw topics are free. Today's
  topic forms stay allowed until senders switch.
- **Refusals.** A refused password: `broker did not accept the login of <user>`. A publish or subscribe the ACL refuses
  is an error, `not authorized to publish on <topic>` or `not authorized to subscribe to <topic>`, which the Bus words
  as `you may not post as ...` and `you may not read ...`; before 0.3.2 the broker's refusal was silently taken as
  success. Every `MY_ADDR` command refuses an agent whose file binds it to another session (`agent-bridge: <addr> is not
  your agent (bound to another session)`); calls from outside a session and agents without a file pass. Which
  credentials a connection uses is the rule under [Addresses, topics and payloads](#addresses-topics-and-payloads).


## Worktree locks and the guard

A worktree lock is a lease on `admin/wt-<first 16 hex of sha256(realpath)>` (see [Resource ledger](#resource-ledger)),
enforced locally by two hooks that share one lock lookup, so they never disagree. The design, and why it is labelled
`hook` rather than enforced, is in [docs/ACCESS.md](ACCESS.md) section 5.6.

- **`agent-bridge worktree`** (`core::worktree`; `_native.worktree_acquire`, `worktree_release`, `worktree_status`).
  `acquire` resolves the realpath, acquires the write lease with `path` and `git_common_dir` (ledgerd creates the
  resource on demand), and runs `git worktree lock --reason "agent-bridge lease <id> held by <addr>"`, with no expiry in
  it, so renewals never rewrite it. The main worktree can't be git-locked: a warning, and the lease and the hooks still
  hold it. It prints the lease plus `resource`, `lease_id`, `path`, `git_lock`, `lock` (the reason), `renewed_by`
  (`relay` or `none`) and `warnings`. With this session's relay running, the relay renews the lease every ttl/3 (default
  ttl 600 s) and releases it when the session ends (`relay/<session>.leases`); without one it lapses at its ttl.
  `--exec` holds the lease and the git lock while the command runs, renewing every ttl/3, then releases as `release`
  does, with `acquire --exec`'s exit status. `release` first saves uncommitted changes, untracked files included, as a
  commit on top of HEAD under `refs/agent-bridge/released/<lease id>` through a scratch index, so the files and the real
  index are untouched; then it removes the git lock if it is this lease's, and releases. It prints `released`, `path`,
  `snapshot` (the ref or null) and `unlocked`. `status` needs no ledger for the git side and prints `path`, `resource`,
  `git_common_dir`, `main`, `lock`, `stale` (an agent-bridge lock no live lease is behind), and `holder`, `lease_id`,
  `expires_at` while leased, or `ledger_error` when ledgerd can't say.
- **The lock lookup** (`core::guard`). A worktree is locked only while an agent other than this session's (or its
  subagents') holds its lease. ledgerd's `worktrees` op is authoritative, asked on every call (2 s timeout), logged in
  as the session's agent when it has credentials. A git lock reason never allows anything on its own: a lock with no
  live lease is stale, allowed with a warning. If ledgerd is unreachable or older than 0.3.2, the lookup falls back to
  the union of `<data dir>/guard-locks.json` (the last successful lookup, written 0600 atomically) and the git lock
  reasons, and fails closed; the main worktree can't be git-locked, which is why the last-known file is part of it.
- **`agent-bridge hook guard`**, a Claude Code `PreToolUse` hook. It reads the hook event on stdin. Inside a worktree
  another agent holds, without the lease, it refuses `Edit`, `Write`, `NotebookEdit` and the MCP filesystem write tools
  by resolved path (symlinks, `..`, new files, hard links); shell writes: mutators' paths, redirections, `sed -i`, `perl
  -i`, `find -delete`/`-exec`, and git writes through `-C`, `--git-dir`, `--work-tree` or `GIT_DIR`; and, when the cwd
  is inside, every program but a read-only allowlist. While a lock is held it also refuses, in that worktree's
  repository, the branch's ref file, `packed-refs`, `hooks/` and `config`; when the call's cwd (or its `-C` or
  `--git-dir` target) is inside a repository that has a locked worktree, paths it can't resolve (`$VAR`, `$(...)`,
  `xargs` operands) and the `core.hooksPath` overrides (`-c`, `GIT_CONFIG_PARAMETERS`, `GIT_CONFIG_KEY_n`, `git
  config`); and writes to a file with more than one hard link on the same device as a locked worktree. Globs are
  expanded from the call's cwd and judged as real paths, and a `cd` to a directory it can't resolve is judged by the
  call's own working directory. Elsewhere, nothing is refused for being unresolvable: in a repository with no locked
  worktree, or outside any, only writes that name a locked worktree or its repository's protected files (or a multi-link
  file on its device) are refused. It unwraps `bash -c`, `eval`, wrappers, `$(...)` and backticks, and skips heredoc
  bodies.
- **`agent-bridge hook ref-guard STATE`**, git's `reference-transaction` hook. Only `prepared` is checked. It refuses
  (exit 1) an update of `HEAD` or a per-worktree ref (`refs/bisect`, `refs/worktree`, `refs/rewritten`, `ORIG_HEAD`,
  ...) of the worktree git runs in, or of a branch any worktree has checked out, when that worktree is held by someone
  else. It uses the guard's lookup and refusal text, with `the update of <ref>` as `<what>`. Outside an agent session
  (no `CLAUDE_CODE_SESSION_ID`) it allows, with `agent-bridge ref-guard: allowed outside an agent session, but
  <refusal>` on stderr; when it can't decide, it refuses with `agent-bridge ref-guard: blocked: <why>`. Shared refs no
  worktree has checked out (tags, remotes, `refs/agent-bridge/*`) are free. The console's Protect this repo installs it;
  by hand, the hook file is `#!/bin/sh` and `exec agent-bridge hook ref-guard "$@"`.
- **Outputs.** Allowed: exit 0, silent. Refused: exit 2 (the guard) or 1 (ref-guard) with `blocked: <what> is inside
  <path>, leased by <holder> until <HH:MM:SS> (lease <id>). To work there: agent-bridge worktree acquire <session agent>
  <path>`; `until` is left out when the expiry is unknown, and ` (ledgerd unreachable; using locks as of <t>)` is added
  in the fallback. A warning: exit 0 with `{"systemMessage": ...}`, never a `permissionDecision`, since `allow` would
  approve the call past the user's permission prompt. Unreadable input exits 2, and so does a crash or arguments that
  don't parse (`blocked: the agent-bridge guard failed (internal error)`); unlike the other hooks, both guards fail
  closed.
- **What it can't see**: interpreters (`python -c`), scripts run from outside, background jobs, and other tools and
  harnesses. That is why the label is `hook`, and why a per-session sandbox ([docs/ACCESS.md](ACCESS.md) 5.6) is the
  hard local option.


## Doctor

`agent-bridge doctor` checks this machine's bridge and prints one line per check, `OK`, `WARN`, `FAIL` or (since 0.3.2)
`INFO`, with what to do; any `FAIL` makes it exit 1. With `--json` each check is one object, `{"check", "status":
"ok"|"warn"|"fail"|"info", "detail"}` plus the check's own fields. It only reads: it never creates the state directory,
starts or stops a relay, or edits settings. The checks, in order:

- `broker`: a TCP connection to `AGENT_BRIDGE_HOST:AGENT_BRIDGE_PORT` within 2 s.
- `ledgerd`, `historyd`: each answers `ping` within 3 s with its `version` (ledgerd's `ping` op; historyd's `ping`
  with `"version": true`). A service older than 0.3.0 answers without one, a `WARN`. Not checked without a broker.
- `state_dir`: the directory of the history log exists, is yours and writable, and others cannot write it.
- `config_dir`: `$XDG_CONFIG_HOME/agent-bridge`, if it exists, is yours and 700 (the console sets it on start, so a
  wider mode is a `WARN`).
- `cli_lock`: `cli.lock`, or the directory it will be created in, is writable.
- `versions`: this CLI, the two services and the wheel around the binary (the `agent_bridge-*.dist-info` in its
  environment) agree; fields `cli`, `ledgerd`, `historyd`, `wheel`.
- `relays`: every live relay runs this CLI's binary, not a replaced one, and its `claude` process still lives; field
  `stale`, one `{"session", "pid", "binary", "why"}` each, fixed with `agent-bridge relay restart` in that session. The
  plain line names at most five stale sessions, then `and N more (doctor --json lists all)`.
- `hooks`: every hook command in `~/.claude/settings.json` that mentions agent-bridge runs an existing program; a
  `FAIL` names only the missing path.
- `auth` (0.3.2): the auth mode and the transport (the unix socket or TCP). `off` is `INFO` (`off: an anonymous bus, any
  local process may claim any address`). Under `warn` it is `OK` when every live agent logs in, else a `WARN` naming the
  agents that connect anonymously, which `enforce` would refuse (`--json`: `mode`, `anonymous`). `enforce` is `OK`. A
  mode other than `off` with no `admin.json` is a `FAIL`, since nothing can be provisioned. A stale `auth-mode` file,
  saying `warn` or `enforce` while the broker accepts a made-up login (which only a broker without dynamic security
  does), is a `WARN`: `auth-mode file says <x> but the broker runs without auth`.
- `readers` (0.3.2): a `WARN` for live relays and MCP servers older than 0.3.2, which don't read the sender-bound topics
  yet (`restart them (relay restart, /mcp)`; `--json`: `old`).
- `groups` (0.3.2): a `WARN` for root-equivalent groups, `docker`, `libvirt`, `lxd`, `incus` and `disk`, and
  `sudo`/`wheel`/`admin` when `sudo -n true` succeeds; `INFO` for a `sudo` that asks for a password and for `adm`
  (`--json`: `root_equivalent`). Any agent in a root-equivalent group can get round every local control
  ([docs/AUTH.md](AUTH.md)).
- `claude` (0.3.2): `OK` when `claude --version` is at least 2.1.295, the first version whose hooks can fail closed
  (`onFailure: "block"`); a `WARN` when it's older; `INFO` when `claude` isn't on PATH.

## Admin console

`agent-bridge-console` starts a chat-style admin console for the bus and for every Claude Code session on the machine, on
the bus or not. It runs on the host (not in Docker), because it reads Claude Code's session registry and
transcripts, checks `/proc` and signals processes. Start it by hand with the broker and ledgerd up:

```bash
agent-bridge-console
```

It prints a login URL, `http://localhost:10000/#token=...` (`127.0.0.1` is still served). Open it once; the page stores
the token in the browser's localStorage and removes it from the address bar. `MONITOR_PORT` changes the port. The
console joins the bus as the reserved agent `admin` (keep on) and sees everything else through a read-only `#`
subscription, both from the native core (`agent_bridge._native`: `Bus.register_admin()` and `Observer`). Ledger requests
go over the admin connection while the console holds it (else over one of their own); history requests use connections
of their own.

**Sessions and agents.** The console lists two kinds of entries, joined by Claude session id:

- **Claude Code sessions** from Claude Code's session registry, `~/.claude/sessions/<pid>.json` (under
  `CLAUDE_CONFIG_DIR` when that is set). Every running session writes one, whether or not it is on the bus. The
  console uses `sessionId`, `pid`, `procStart`, `cwd`, `entrypoint`, `kind`, `status`, `version`, `name`,
  `nameSource`, `hostSessionId`, `startedAt` and `updatedAt`, and ignores everything else. It never opens the
  `<pid>.<hash>.key` files next to them, and never connects to the sessions' messaging sockets.
- **Bus agents** from presence. An agent whose presence `session` matches a registry entry becomes one entry with
  both sides; an agent with no matching session (a script, another MCP client) is listed on its own.

The registry is internal to Claude Code and undocumented: this was built against 2.1.299. Every field is optional
to the console and checked for type and range, and a malformed file is skipped, so a format change degrades to less
detail (or a session missing from the list), never to an error. If several files name one session, the live one
wins, else the newest. A registry entry whose process is dead, or whose pid now belongs to another process, is shown
as a dead session. Dead sessions that have not been updated for the stale period (3 days) are hidden.

**What it shows**

- **Sidebar**: Broadcast, Rooms, Direct messages, Topics, Agents on the bus, Other sessions (in the registry but
  not on the bus), and the Ledger and Audit log views. Live sessions sort first. Unread
  badges count live messages in channels you are not looking at. `+` next to Rooms or Topics opens a new one.
- **Messages**: backlog from historyd (its last 2000 messages, `recent`) plus live messages over server-sent
  events. With historyd down the backlog is empty and live messages still arrive.
  The compose box posts as `admin` to the selected room, topic or DM with `admin`. A DM between two other agents is
  read-only; open the agent and use Message instead. Messages from raw MQTT clients (ledgerd's notices, `pub` from
  non-bridge clients) show live but are not in the log, so they are gone after a reload. Agent text is rendered with
  `textContent` only, and the page's CSP allows no inline script.
- **Friendly names**: `branch · task`, either part left out when unknown.
  - The branch is read from `.git/HEAD` of the session's working directory (registry `cwd`, else the transcript's),
    following a worktree's `.git` file, else taken from the transcript's `gitBranch`. Only its last path segment is
    shown, cut to 24 characters, and a detached HEAD shows none.
  - The task is the registry `name`, unless Claude Code derived it from the folder name (`nameSource` `derived`,
    like `myrepo-38`). Otherwise it is the transcript's latest `customTitle`, else the first six words of the first
    prompt.
  - With neither part known, the name is the bus address, then the registry name, then `session <8 hex>`.
  - The transcript is `~/.claude/projects/*/<session>.jsonl`. While a page has the live stream open, the registry
    and transcripts are refreshed every 3 s, so a renamed session shows its new title within seconds. A transcript
    is re-read only when its size or mtime changed, and a missing one is looked for at most every 30 s. With no page
    open the console refreshes nothing in the background. A large transcript is seeded from the first prompt,
    the last `customTitle` and the last 4 MiB, then read incrementally.
- **Profile**: Overview (friendly name, process alive or dead, activity status, kind, session and Desktop session
  ids, pid, Claude Code version, start time, working directory, branch, title, first prompt, whether a relay is
  running, bus address, and for bus agents their display name, last seen, presence expiry, caps, rooms and topics,
  plus the process controls). Bus agents also get DMs (both directions, last 200) and Ledger (its resources, grants
  given and held, pending requests, leases with expiry and fencing token, waiters). Every session gets Activity, the
  last 30 transcript entries (user and assistant text, tool names).
- **Ledger view**: every resource with its leases (holder, level, expiry, token), waiters and requests, from the
  ledgerd `dump` op, refreshed every 5 s while open. Releasing a lease or revoking a grant acts as the resource's
  owner; if ledgerd's reply never arrives, the result says the action may still have been applied, so check the view
  (retrying is safe: a second release finds no lease, a second deny changes nothing).
- **Audit log**: every admin action from `admin-audit.jsonl`, newest first, live, filterable by action, target
  substring and time range.
- **Inbox**: room posts that mention admin (the Observer's `for_admin`, or `admin` in `mentions`) and DMs to admin,
  newest first, with an unread count as a badge on the sidebar's Inbox entry and in the tab title; read state is kept
  per browser. The history backlog carries ids and mentions, so posts from before the page opened are in it too.
- **Claims view**: every room's claims from one `claims` request as admin (no ledger), refreshed every 5 s while open.
  Release sends `unclaim` as admin (`POST /api/claims/release`, audited as `claims/release`), which needs no admin
  ledger; against an older ledgerd the view shows its "older than 0.3.1" reply.
- **Pins and replies**: the Observer's `pin` events feed each room's pinned card (and `/api/state`'s `pins`), collapsed
  past 12 lines; Pin… (Edit pin once set) posts `POST /api/room/pin` with `{room, text}` or `{room, clear: true}`
  (audited as `room/pin`). A reply is shown under its parent when the parent is listed, else as a one-line stub that
  scrolls to it; a forged reply cycle cannot hide messages. `POST /api/send` takes `reply_to` for room and DM posts.
  History entries get their ids from `_native.message_id`, so a freshly loaded page threads like a live one.
- **Links**: `console/links.py` reads the optional link rules at start
  (`$XDG_CONFIG_HOME/agent-bridge/console-links.json`, or `AGENT_BRIDGE_LINKS`) and serves them in `/api/state` as
  `links {rules, error}`. It accepts at most 20 rules, each a pattern from a regex subset that Python and the browser
  read alike (literals, classes, `\d \w \s` and their capitals, escaped punctuation, `(` and `(?:` groups and
  quantifiers; no anchors, backreferences, lookarounds or inline flags; 1 to 3 groups; a group may only be optional, and
  at most one group holds `|`; every repeat (`+ * {n,} {n,m}`) followed at once (group brackets aside) by one plain
  literal it cannot match, or by the end, so a repeat ending a group or one of its branches waits for a literal after
  the group that none of the waiting repeats can match; at most 3 optional atoms (`?`, `{0,n}`, optional groups
  included); an exact `{n}` neither repeats nor separates) and an http(s) URL whose scheme and host contain no
  placeholder. The repeat rule gives each repeat one place to stop, so matching a hostile word is linear in both
  engines, and the 3-optional cap closes the `[a-z]?[a-z]?…[a-z]{n}` case, which backtracks exponentially even in V8, as
  do several groups of overlapping alternatives in a row. `static/links.js` matches only whole whitespace-separated
  words of at most 200 characters, at most 2000 per run, URL-encodes each capture, checks again that the built link is
  http(s) on the template's origin, links nothing for a `.` or `..` capture, and builds the element with `textContent`
  and the `href` property. Git hashes and paths become click-to-copy spans. Code blocks never link.
- **Worktrees**: `GET /api/worktrees` (`console/worktrees.py`) joins ledgerd's `worktrees` op with each worktree's git
  lock, read from `<gitdir>/locked` and parsed as `agent-bridge lease <id> held by <addr>`. Each row has the path,
  branch, holder, expiry and lease id; its drift (an id that isn't `sha256(realpath)[:16]`, no git lock, a lock naming
  someone else, or a stale lock with no lease); the live sessions inside and those working there without the lease; and
  the two-part label. Unleased worktrees that two live sessions share are listed too, and `sessions` gains `worktree`.
  The guard is detected read-only from `/etc/claude-code/managed-settings.json`, `managed-settings.d/*.json` and
  `~/.claude/settings.json`, including whether `onFailure` is `block`. Against an older ledgerd the view shows `ledgerd
  is older than 0.3.2 and has no worktrees op`.
- **Protect this repo**: `POST /api/repo/protect` and `/api/repo/unprotect` (`{repo}`, audited as `repo/protect` and
  `repo/unprotect`) write or remove the marked `reference-transaction` wrapper in the effective hooks directory,
  chaining an existing hook first and calling the console's own `agent-bridge hook ref-guard`. Unprotect touches only a
  file with the marker.
- **Auth**: `/api/state`, `agent-bridge-admin status` and `/metrics` (`agent_bridge_console_auth_mode{mode}`) report
  `_native.auth_mode()`; the page shows the mode when it isn't `off` and a "claimed" badge on a named sender that isn't
  `bound` (history stores only `bound`, so a missing sender counts as claimed). `main()` calls
  `auth.use_admin_credentials()`, which sets `AGENT_BRIDGE_CREDENTIALS` to `admin.json` only when the variable is unset
  and the file exists: the explicit act the credentials rule asks for, never a fallback.
- **Capacity, leftovers, the room kill switch and targeted stop**: described in the
  [README](../README.md#admin-console).

**Death detection.** The pid and start time come from the registry (`pid`, `procStart`), else from presence (`pid`,
`pid_start`), but only from a presence record that names a session or a parent: a session-less agent owns no Claude
process, whatever an older record carries, so it shows no process, has no liveness and is left out of Capacity. A
session is alive iff `/proc/<pid>` exists, is not a zombie, its comm is `claude`, its start time equals
the recorded one, and it belongs to the console's uid. This is checked every 3 s while a page watches. A dead session
stays listed (greyed, with a toast when it dies) until its presence expires, or, without presence, until its registry
entry is older than the stale period. Bus agents with neither (CLI-registered scripts, other MCP clients) show "no
process info" and get no process controls. Kind is `desktop` when the registry or transcript `entrypoint` is
`claude-desktop` (or, with neither, when the process runs from `~/.config/Claude/`), and `cli` otherwise.

**Process control** (live sessions, on the bus or not, Desktop included): Interrupt (SIGINT), Terminate (SIGTERM), Kill (SIGKILL), each
confirmed in the page. The console opens a pidfd for the pid, re-runs the identity check above, and only then
signals through that pidfd, so a pid reused at any point is never signalled.

**Restart** (dead sessions only, on the bus or not, refused while the session is alive):

- **Open in terminal** runs `gnome-terminal --working-directory=<cwd> -- <claude> --resume <session>`, with the cwd
  from the registry or transcript and `claude` resolved on the console's PATH.
- **Resume with message** runs `claude -p --resume <session> --output-format stream-json --verbose` in that cwd with
  the message on stdin (so text starting with `-` is never a flag), in the background, and streams the reply into
  the profile (text, tool names, result). One run per session at a time. `-p` has no one to approve tool
  permissions, so tools that need approval are denied.
- **Claude Desktop sessions** are never resumed from the console, since that would fork them outside the app. The
  profile says to reopen the session in the Claude Desktop app instead.

### Broadcasts

The Broadcast channel (or the header button) sends one message to everyone:

- **Agents on the bus** get it at once: every agent subscribes to the reserved `broadcast` topic, so it arrives
  through `recv` (and the MCP push notice) with `"topic": "broadcast"` and `"from": "admin"`. historyd records it,
  like every message.
- **Every Claude session, on the bus or not**, sees it through the broadcast hook, but **only on its next prompt** (or
  when a session starts or resumes). A session that is idle, or in the middle of a long turn, sees nothing until then.

The broadcast hook, `agent-bridge hook broadcast` (or its old name `agent-bridge-broadcast-hook`, a shim for it), is a
Claude Code hook for `UserPromptSubmit` and `SessionStart`. It asks historyd
(`broadcasts`, one round trip, 1 s timeout) for the broadcasts from `admin` that are newer than what that session has
already seen, records the newest one in `hook-state/<session id>`, and adds them to the prompt as context
labelled as a broadcast from the user's agent-bridge admin console. A session the hook has never seen gets the
broadcasts of the last hour. It skips a session whose relay is running (`relay/<session id>.lock` held), since
the relay delivers broadcasts itself and the session would otherwise see them twice. It prints nothing and exits 0
on any error, historyd or the broker being down included, so it can never block a prompt.

Installation is in the [README](../README.md#claude-code-hooks).

### Console security

- **Loopback only.** The console binds 127.0.0.1 and serves only requests whose `Host` is `localhost:10000` or
  `127.0.0.1:10000`, so a DNS-rebinding page cannot reach it.
- **Admin token.** On first run it writes a random token to `~/.config/agent-bridge/admin-token` (directory 0700,
  file 0600, re-chmodded on every start). Every `/api` request, reads included, needs it in the `X-Admin-Token`
  header; only the static page loads without it. The page reads the token from the login URL's fragment (never sent
  to the server or put in logs), keeps it in localStorage, and streams events with `fetch` so the token always
  travels as a header. A custom header also forces a CORS preflight, which the console refuses. Delete the file and
  restart the console to rotate the token.
- **Origin check.** Every write (send, broadcast, signal, restart, ledger action) also needs an `Origin` of
  `http://localhost:10000` or `http://127.0.0.1:10000`, and a JSON body of at most 256 KiB.
- **Validation.** Addresses, rooms, topics and text go through the bus's validators (`check_ident`, `check_topic`,
  `check_text`), session ids must be UUIDs, lease and request ids 16 hex characters, signals come from a fixed list,
  and restarts use only the session id and cwd the console itself found. Nothing goes through a shell.
- **Audit.** Every admin action, refused and failed ones included, is appended to `admin-audit.jsonl` as
  `{ts, who, peer, action, target, result}` and shown in the Audit log view. Requests with a bad token or Origin are
  logged with `who: "unknown"`.
- **What the token does not stop.** Any process running as your uid, including every agent's Bash tool, can read
  the token file and use the console. The token stops other web pages and accidents, not a determined local agent.
  The same holds for the bus itself (see the [security model](../README.md#security-model-and-limits)).

## Sanitization and limits

Every boundary validates its input. Anything from another client is treated as hostile and never crashes a command or
the service.

| What | Rule |
|---|---|
| Names, addresses, rooms, caps | 1-64 ASCII letters, digits, `_`, `-`. Register names at most 57 (room for the suffix). At most 32 caps. |
| Message text | At most 64 KiB of UTF-8. Tab, newline and CR are kept, and any other C0, DEL or C1 control is refused. `say`, `dm` and `broadcast` refuse empty or whitespace-only text (`text is empty`); a raw `pub` payload may be empty. |
| Raw topics | At most 256 bytes, no control characters, not under the reserved levels (`agents`, `inbox`, `ledger`, `broadcast`, `rooms`). Wildcards only in `sub`. At most 100 subscriptions per agent. |
| Incoming messages | Over 256 KiB, not UTF-8, or breaking the rules above: dropped. `"from"` is checked for shape only. |
| Presence and history lines | Malformed records are skipped, never trusted. |
| Room settings | Rules at most 2048 bytes of text (same control-character rule), at most 32 moderators and 256 muted and 256 banned addresses. Malformed records count as open. |
| `--wait`, `--last` | 0-3600 s, 1-1000. |
| Broker | `max_packet_size 270000`, `max_connections 1024`, `max_queued_messages 1000` and `max_queued_bytes` 32 MiB per client, `memory_limit` 512 MiB. |
| ledgerd requests | At most 16 KiB of UTF-8 JSON object, unknown fields rejected, every field type-, charset-, length- and range-checked. Resource names are `[A-Za-z0-9_][A-Za-z0-9_.-]{0,63}`. Descriptions are at most 1024 chars with no control characters except tab and newline. ids are 16 hex chars. Bad requests get `ok: false`, or are dropped without a valid ResponseTopic. |
| ledgerd quotas | 1000 ledgers, 128 resources per ledger, 64 pending and 512 stored requests per agent (the oldest denied one makes room), 256 pending requests per owner, 256 leases per agent, 256 waiters per resource, the last 200 lease events per resource, 256 claims per agent and 1024 per room, 1024 worktrees, and under auth 20 requests a second per identity (burst 100, admin exempt). |

## Persistence and cleanup

- **Broker**: `persistence true` writes sessions, queued messages and retained presence to the `data` volume, saved
  every 60 s and on clean shutdown. A broker crash can lose up to the last 60 s. mosquitto only persists a session
  whose latest connect used `clean_start=False`, so every agent connection uses it.
- **Idle expiry**: an agent's session expires after `AGENT_BRIDGE_STALE_DAYS` (3) of being disconnected, and its
  retained presence carries the same message expiry. Presence is refreshed by `register`, `join`/`part`/`sub`/
  `unsub`, `keep`, `describe`, and by `recv` (at most once a minute). Sending alone does not refresh it, so an
  agent that only sends drops out of `agents` 3 days after its last refresh. `keep on` makes both permanent.
- **History**: historyd's SQLite in the `history` volume, see [History service](#history-service). It records
  everything it sees on the bus, including messages from non-bridge clients, ledgerd's notices and plain text on raw
  topics, and prunes by receive time.
- **Console files**: `admin-audit.jsonl` (the audit log) and `hook-state/` (one small file per session
  the broadcast hook has seen) are never pruned automatically. Delete them by hand if they grow.
- **leave**: drops the agent's ledger and its received DMs in historyd (3 s each, a warning if either service is
  down), wipes the session (a clean connect with expiry 0), and clears the retained presence.
- **Ledger**: SQLite in the `ledger` volume (WAL, synchronous=FULL). ledgerd and its rollback, the Python ledgerd in the
  image built from v0.1.0, record the schema version in the `meta` table (`schema`, now 1), where future migrations
  start, and both refuse to start on another version before writing anything (`ledger.db has schema N; this ledgerd
  knows 1`), so the rollback can never touch a database a newer ledgerd migrated. ledgerd uses a persistent session, so
  requests sent while it restarts wait at the broker, and a request is acked only after its transaction commits. `drop`
  removes an agent's ledger. The stale sweep drops any ledger whose owner has no retained presence and has made no
  ledger request for longer than the stale period, and pending or denied requests older than that; granted ones stay.
  Presence with `keep` protects a ledger indefinitely. Answers kept for redelivery replay expire with their request's
  MessageExpiryInterval, after 300 s to 3900 s. Both implementations purge a row 300 s after its `ts`, so ledgerd
  writes a longer stay as a later `ts`, and the rollback keeps it as long.

## Troubleshooting

- **`broker unreachable`**: run `docker compose up -d`. Check `docker ps --filter name=agent-bridge`.
- **`cannot write .../cli.lock: ... belongs to root, not you`**: the state directory (`~/.local/state/agent-bridge`)
  was created by Docker, which makes a missing bind-mount source root's. Before 0.2.1 the Compose file bound it for
  historyd's import; that import is now opt-in (`compose.override.example.yaml`). Take it back with the `chown` the
  message gives.
- **ledger calls time out**: `docker compose ps` shows ledgerd's health, which is a real `dump` round trip through
  the broker every 30 s, so `unhealthy` means disconnected or wedged, not only exited. Check
  `docker logs agent-bridge-ledgerd`. ledgerd reconnects to the broker on its own.
  If it exits with `another client connected as ledgerd`, another client took its client id, and the newer one is now
  the single writer. Stop that client, then run `docker compose up -d ledgerd`. ledgerd steps aside on DISCONNECT reason
  142. The Python ledgerd of the rollback image prints `the broker disconnected this ledgerd` instead and steps aside on
  any DISCONNECT the broker sends, since paho-mqtt 2.1 drops the one-byte reason mosquitto uses for a takeover (a broker
  shutdown sends none, so a restart never stops it).
- **Queued messages vanished after a broker restart**: the session was created with `clean_start=True`. mosquitto
  keeps a session on disk only if the latest connect used `clean_start=False`, even under MQTT 5 with a session expiry.
  Any client that wants offline delivery must connect that way.
- **Every request/response takes 40-80 ms**: Nagle plus delayed ACK. The broker needs `set_tcp_nodelay true` (in
  mosquitto.conf) and each client should set `TCP_NODELAY` on its socket (paho: `on_socket_open`). The native core
  does on every connection. Measured: 82 ms per ledger call with neither, 41 ms with the client side only, 1.8 ms with both.
- **A long-running Bus keeps reconnecting**: a CLI command for the same address takes its session over, since there is
  one client id per address (except `say`, `dm` and `pub` while the session's relay holds the address). The Bus
  reconnects about 1 s later and its unacked messages are resent, not lost or doubled. CLI commands for one address take
  turns through a lock on `cli.lock`, except `recv --wait N`, which is meant to be taken over. The MCP server's Bus does
  not take that lock. Known gap: a `keep` change made from the CLI while a Bus is connected shows in presence, but that
  Bus's reconnects still use its old session expiry.
- **An agent stops receiving anything**: it has unacked QoS 1 messages filling its receive window. The Bus runs with
  `manual_ack` and acks only what `recv` hands out. Any extra callback on the same client must ack what it consumes
  with `client.ack(mid, qos)`, which the ledger client does.
- **Console page asks for the token, or every action says "admin token required"**: open the login URL again
  (`agent-bridge-console` prints it at start) or paste the token from `~/.config/agent-bridge/admin-token`.
- **Console says "not connected to the broker"**: the broker is down or another console holds the `admin` identity
  (both take the session over from each other). Run one console.
- **An agent shows "no process info"**: its presence predates the origin fields, or it is not a Claude session.
  Restarting the session (so its MCP server republishes presence) fills them in.
- **Port 1883 already in use**: another MQTT broker is running. Stop it, or set `AGENT_BRIDGE_PORT` and change the
  compose port mapping.
