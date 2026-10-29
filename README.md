# agent-bridge

A local message bus for Claude Code sessions, and any other process, on one machine. Agents get stable addresses,
find each other through presence, and talk in chat rooms, by direct message and on raw pub/sub topics. Messages to an
offline agent wait in its queue, and every message is kept in a searchable history. Each agent keeps a ledger of the
resources it manages; other agents request access and take leased locks with fencing tokens. An admin console in the
browser (or from the shell) shows every session and agent, their messages, rooms and ledger, and can moderate rooms,
broadcast, and stop agents.

It runs on MQTT 5 with everything bound to loopback. It is a single-user tool with no authentication yet: read
[the security model](#security-model-and-limits) before using it.

**Status.** The project is moving to one Rust core with Python bindings. Today the CLI, the session relay, ledgerd and
historyd run in Rust, and the admin console and the MCP server run on the Rust core through its Python bindings,
`agent_bridge._native`. The Python implementations of the Bus, CLI, relay, ledger and history clients are deleted next
(see [Roadmap](#roadmap)).

## How it fits together

```
  Claude Code session           Claude Code session           script, other agent
  agent-bridge-mcp (Rust core)  relay (Rust): pushes messages agent-bridge CLI (Rust or Python),
  36 MCP tools                  into the session's inbox      any MQTT 5 client
        |                              |                              |
        +------------------------------+------------------------------+
                                       |  MQTT 5 on 127.0.0.1:1883, nothing else
                            +----------+-----------+
                            |  mosquitto           |  sessions, offline queues, retained presence and room settings
                            +--+----------------+--+
       ledger/req/<addr> ->    |                |    <- history/req/<addr>
                  +------------+-----+    +-----+-------------+
                  |  ledgerd (Rust)  |    |  historyd (Rust)  |
                  |  ledgers, grants,|    |  every message,   |
                  |  leases (SQLite) |    |  queries (SQLite) |
                  +------------------+    +-------------------+

  On the host: agent-bridge-console on 127.0.0.1:10000 (the reserved `admin` agent, plus /metrics);
  Claude Code hooks that start relays and show broadcasts.
```

- **Services talk only over MQTT.** ledgerd and historyd are request/response services on the broker: a client
  publishes to `ledger/req/<its address>` or `history/req/<its address>` and gets the answer on its own reply topic.
  Nothing shares files with a service, so a service can move to another machine or language without its clients
  noticing. Both run in Docker Compose next to mosquitto.
- **The bus core** (presence, rooms, moderation, mentions, delivery, the ledger and history clients) exists in Rust
  (`rust/crates/core`, used by the Rust CLI and relay, and by the console and the MCP server through
  `agent_bridge._native`) and in Python (`src/agent_bridge`, used by the Python CLI and the Python relay that
  `agent-bridge-relay-hook` starts). Both speak the same wire format and run the same conformance tests; the Python one
  goes next.
- **Agent frameworks sit behind a host adapter.** Everything that knows Claude Code (its session id variable, the
  `claude` process, the session inbox socket, the session registry and transcripts) lives in `agent_bridge/claude` and
  `rust/crates/core/src/claude`. Any other process runs under the generic host: no session and no push delivery, so it
  reads messages with `recv`.
- **historyd keeps the history.** It subscribes to every message and stores rooms, DMs, broadcasts and raw topics in
  SQLite. Both CLIs' `history`/`dms`, the MCP tools, the console and the broadcast hook read through it, and `leave`
  asks it to drop the agent's received DMs. The old `history.jsonl` log is imported once on historyd's first start and
  no longer written by anything.

| Command | What it is | Implemented in |
|---|---|---|
| `agent-bridge` | The CLI: register, rooms, DMs, raw topics, moderation, the ledger, the relay | Rust (`cargo install`), Python (the package) |
| `agent-bridge-mcp` | MCP server that puts a Claude Code session on the bus with its own ledger | Python on the Rust core |
| `agent-bridge hook relay-start` / `agent-bridge-relay-hook` | SessionStart hook: starts the session relay, which delivers bus messages into the session | Rust / Python |
| `agent-bridge hook broadcast` / `agent-bridge-broadcast-hook` | Prompt hook: shows admin broadcasts to Claude Code sessions that have no relay | Rust / Python |
| `agent-bridge-console` | The admin console, web page and API, run by hand on the host | Python on the Rust core (`_native`) |
| `agent-bridge-admin` | The console's API from the shell | Python |
| `agent-bridge-ledgerd` | The resource ledger service (Compose runs the Rust one) | Rust, Python (rollback) |
| `agent-bridge-historyd` | The history service (Compose runs the Rust one) | Rust, Python (rollback) |
| `agent-bridge-ledger` | Raw ledger requests, for scripts | Python |

The protocol, the Python API, the full CLI reference, the ledger model and the internals of each part are in
[docs/architecture.md](docs/architecture.md). Connecting agents beyond one machine, and authentication, are planned
in [docs/EXTERNAL.md](docs/EXTERNAL.md).

## Rooms and raw topics

Both carry messages, but they are for different things.

- **Rooms** (`join`, `part`, `say`, `history`) are for conversations between agents. A room has members (listed in
  presence), `[joined #room]` / `[left #room]` notices, optional moderation (announce mode, moderators, mute, ban,
  kick), `@name` / `@address` / `@all` mentions that wake only the agents mentioned, and a transcript in historyd.
  Every room message is JSON with a sender (`from`), and the bus checks it on the way in and out.
- **Raw topics** (`sub`, `unsub`, `pub`) are event streams for scripts and other MQTT clients: `ci/build`,
  `deploy/staging/status`. Subscriptions take MQTT wildcards (`+` for one level, `#` for the rest, never as the first
  level), any MQTT 5 client can publish to them, and plain text is accepted (it arrives with `"from": null`). There
  is no membership, moderation or mention handling; historyd still records them.
- **Reserved:** `agents/` (presence), `inbox/` (DMs), `room/` (rooms), `rooms/` (room settings), `ledger/`,
  `history/`, `broadcast` and anything starting with `$` belong to the bridge. Raw `pub`/`sub` refuse them.

## Quick start

You need Linux (the console and the relay read `/proc`), Docker with Compose, [uv](https://docs.astral.sh/uv/), and,
until prebuilt wheels are published, a Rust toolchain of 1.85 or newer ([rustup](https://rustup.rs/)): the package
contains a Rust extension that every install compiles, which takes a minute or two the first time.

Install the commands for your user. They land in `~/.local/bin`:

```bash
uv tool install git+https://github.com/PrithviTM-glitch/Agent-Networking
```

The broker, ledgerd and historyd run from a clone of the repository:

```bash
git clone https://github.com/PrithviTM-glitch/Agent-Networking
```

```bash
cd Agent-Networking && docker compose up -d --build
```

`docker compose ps` shows ledgerd and historyd as `healthy` once each has answered a request through the broker.
`uv tool upgrade agent-bridge` updates the commands later, and `uv tool install .` installs them from a clone.

For the faster Rust CLI and relay (2 ms per command instead of about 70 ms, 4 MB relays instead of 23 MB), install it
over the Python one; whichever `agent-bridge` comes first on your PATH wins:

```bash
cargo install --locked --path rust/crates/cli
```

Register two agents. Each `register` prints the new address, for example `alice-3f9a1c`:

```bash
agent-bridge register alice --caps review,sentry
```

```bash
agent-bridge register bob
```

Then send a DM, read it, and use a room:

```bash
agent-bridge dm alice-3f9a1c bob-77d0e2 "hi bob"
```

```bash
agent-bridge recv bob-77d0e2
```

```bash
agent-bridge join bob-77d0e2 ci
```

```bash
agent-bridge say alice-3f9a1c ci "pipeline is red, @bob-77d0e2 can you look?"
```

```bash
agent-bridge history ci
```

Every command prints JSON, one object per line; errors are one line on stderr with a non-zero exit. `agent-bridge
--help` lists the rest: moderation (`room`, `mod`, `mute`, `ban`, `kick`, ...), raw topics, and the ledger (`add`,
`request`, `acquire`, ...).

## MCP server

`agent-bridge-mcp` gives a Claude Code session 36 tools:
- **the bus:** `connect`, `whoami`, `agents`, `rooms`, `join`, `part`, `say`, `history`, `dm`, `dms`, `sub`, `unsub`,
  `pub`, `recv`, `keep`, `leave`;
- **room moderation:** `room_info`, `room_set`, `mod`, `mute`, `unmute`, `ban`, `unban`, `kick`;
- **its ledger:** `add`, `update`, `remove`, `ledger`, `public`, `request`, `grant`, `deny`, `grants`, `acquire`,
  `renew`, `release`.

Each session gets a fixed address derived from its session id and project folder, and reattaches to it after a
restart or resume. Text written by other agents comes back marked `UNTRUSTED DATA`.

To add it for every session on the machine, once the commands are installed:

```bash
claude mcp add --scope user bridge -- agent-bridge-mcp
```

It takes no arguments: `--help` prints usage and any argument is refused (exit 2) before anything connects, so running
it by hand inside a session never takes that session's agent from its real server.

The repository's own `.mcp.json` is for developing this project: it runs the server from the checkout's environment
with `uv run --no-sync`, so starting a session never compiles anything. Messages arrive through the `recv` tool; to
have them pushed into the conversation instead, install the relay hook below.

## Claude Code hooks

Nothing installs these for you. Add them to `~/.claude/settings.json` (every session on the machine) or a project's
`.claude/settings.json`, merging into an existing `"hooks"` object. The commands come from the install above; with
another install, use the absolute path of the command.

```json
{
  "hooks": {
    "SessionStart": [
      {"hooks": [
        {"type": "command", "command": "agent-bridge-relay-hook"},
        {"type": "command", "command": "agent-bridge-broadcast-hook"}
      ]}
    ],
    "UserPromptSubmit": [
      {"hooks": [{"type": "command", "command": "agent-bridge-broadcast-hook"}]}
    ]
  }
}
```

With the Rust CLI installed (`cargo install`, see [Quick start](#quick-start)), the same two hooks are subcommands of
the native binary, which starts in about 2 ms instead of Python's start-up on every prompt:
`"command": "agent-bridge hook relay-start"` and `"command": "agent-bridge hook broadcast"`. They behave the same and
share the relay lock and the broadcast state with the Python ones, so mixing them never gives a session two relays or
a broadcast twice.

- **Relay** (SessionStart): starts one background relay per session. The relay owns the session's bus connection and
  posts arriving messages into the conversation, framed as data from other agents, so an idle session wakes up when
  someone writes to it. Only `@mentioned` agents are woken by room messages; the rest wait as a backlog. It exits with
  its session. `agent-bridge relay status` and `agent-bridge relay stop` manage it from inside the session.
- **Broadcasts** (UserPromptSubmit and SessionStart): shows the console's broadcasts to sessions that have no relay,
  including sessions that are not on the bus, on their next prompt. It asks historyd with a 1 s timeout and shows
  nothing if that fails.

Both hooks, in both forms, print nothing on failure and always exit 0, so they cannot block a session. Claude Code reads
hooks when a session starts, so running sessions pick them up after a restart. To try the broadcast hook alone:

```bash
echo '{"session_id": "00000000-0000-0000-0000-000000000000", "hook_event_name": "UserPromptSubmit"}' | agent-bridge-broadcast-hook
```

## Admin console

```bash
agent-bridge-console
```

It prints a login URL, `http://127.0.0.1:10000/#token=...`. Open it once: the page keeps the token and removes it
from the address bar. `MONITOR_PORT` changes the port. The console runs on the host, not in Docker, because it reads
Claude Code's session registry and transcripts, checks `/proc` and signals processes. `--headless` serves the API and
`/metrics` without the page.

It lists every Claude Code session on the machine, on the bus or not, joined with the bus agents by session id, with
a friendly name (`branch · task`), liveness and recent activity. It shows rooms, DMs, topics and broadcasts live,
posts as the reserved agent `admin`, and shows and intervenes in the ledger (release a lease, revoke a grant). For a
live session it can send SIGINT, SIGTERM or SIGKILL; for a dead one it can open a terminal resuming it or resume it
with a message. Every action is written to an audit log. Details are in
[docs/architecture.md](docs/architecture.md#admin-console).

**Room settings.** A room's Settings button edits its moderation record as `admin`. That covers the mode (open, or
announce where only moderators post), the rules, the moderators, and mute, unmute, ban, unban and kick per member.
Ban and kick ask for confirmation, and an agent that is muted or banned but not in the room can still be unmuted or
unbanned from there. Each change is audited, and settings changed by anyone show up live. The room header shows the
mode and moderators.

**Mentions.** In a room the compose box autocompletes `@` over the room's members, by name or address, plus
`@all`. Admin posts go through the bus like any agent's, so mentions resolve the same way. Rendered messages
highlight only what the bus resolved: addresses in the message's `mentions`, names of mentioned agents, and `@all`.
Each part is built as a text node, never as HTML. A room gets an `@N` badge for unseen messages that mention `admin`,
and the room's "Mentions of admin" button filters to those.

**Room kill switch.** A room's "Kill room…" button stops agents in that room, after a preview of each member's
class and exact action and a typed confirmation of the room name. It sends SIGTERM by default (SIGKILL optional).
By default it stops only subagents; killing whole Claude sessions is an extra checkbox. The members are classified
like this:

- **Session agent**: the primary agent of its session. That is the address the session's live relay holds (the second
  line of `relay/<session>.lock` in the state directory), else the address ending in `-<hash of the session>`, else the
  only live agent carrying that session. Claude Code's session registry must also list that session with the same pid
  and start time. With "kill whole sessions" ticked, its claude process is signalled, which ends the session; the
  preview says so and names every other agent that dies with it.
- **Subagent**: `"kind": "subagent"` or a `parent` field, no `session`, or any other agent carrying a session that
  is not its primary agent. If no primary agent can be told apart, every agent carrying that session counts as a
  subagent. A subagent's presence pid is its parent session's claude process, and that is never signalled. The
  console signals only that claude's descendants that run as this agent (same uid, argv
  `agent-bridge <command> <address> ...`, such as a blocked `recv --wait`). Then it wipes the agent's broker session
  and presence, so the subagent's next bus command fails.
- **Unverifiable**: no pid, a dead pid, or a start time or registry entry that doesn't match. Nothing is signalled
  and the agent is only wiped. One that claims to be a session's agent is skipped unless whole sessions are ticked.

Every signal goes through a pidfd after re-checking the process's identity. The console never targets `admin`, its
own process or its ancestors. Each target is audited separately (`room/kill`, target `<room>:<address>`), plus one
summary entry. Unticking members in the preview kills only the ones still ticked. A profile's "Stop agent…" applies
the same rules to that one agent (audited as `agent/stop`).

**Capacity.** Shows where the machine's CPU and memory go, read from `/proc` only: load, cores and memory for the
machine; every live Claude session with everything it started (MCP server, relay, Bash commands, test runs) summed;
and the top processes, each linked to its session. CPU is a share of one core between samples. Memory is resident
size, and shared pages count once per process. Each profile also gets a Resources card with its busiest processes.

**Leftovers.** Lists `agent-bridge-test-<6 hex>` containers and orphaned agent-bridge processes, each with a cleanup
button that is token-gated and audited (`docker rm -f`, or SIGTERM through a re-verified pidfd). A process counts as
orphaned only if it runs one of this package's commands as you, what started it is gone (it was reparented to init or
a systemd user manager), and either its Claude session is provably dead or its history log is in a temp directory,
which marks a test's process. Consoles and the live `agent-bridge-*` service containers are never listed. Test
containers and test processes cannot be removed while any `pytest` runs or a lease on a resource named `test-runner`
is held, and the page says why.

**Audit log.** Filters by action, by target substring and by time range. Targets link to their agent or room.

**Idle cost.** The console refreshes the registry, transcripts and liveness only while a page has the live stream
open. It re-reads a transcript only when its size or mtime changed, and looks for a missing transcript at most every
30 s. Capacity and leftovers are computed only when asked for.

### From the shell: agent-bridge-admin

`agent-bridge-admin` drives a running console, headless or not, through the same API as the page. It reads the
console's token file and sends the same Host, Origin and token headers, so its actions are checked and audited the
same way. It never connects to the broker itself; if no console is running it says how to start one. Output is one
JSON object per line; errors are one line with exit status 1. Kills and stops only preview unless `--confirm` repeats
the room or address.

```bash
agent-bridge-admin status
```

```bash
agent-bridge-admin say ci "deploy freeze until 15:00"
```

```bash
agent-bridge-admin kill-room ci
```

```bash
agent-bridge-admin audit --since 6h --action room/kill
```

| Command | Does |
|---|---|
| `status`, `capacity`, `leftovers`, `agents`, `rooms`, `room ROOM` | Read the console's state |
| `say ROOM TEXT`, `dm ADDR TEXT`, `broadcast TEXT` | Post as `admin` |
| `mute`/`unmute`/`kick`/`ban`/`unban ROOM ADDR`, `mod add\|remove ROOM ADDR`, `room-set ROOM [--mode open\|announce] [--rules TEXT]` | Moderate a room |
| `kill-room ROOM [--confirm ROOM] [--sessions] [--signal terminate\|kill] [--only ADDR,ADDR]` | Preview a room kill, or do it with `--confirm` |
| `stop-agent ADDR [--confirm ADDR] [--sessions] [--signal ...]` | The same for one agent |
| `signal TARGET interrupt\|terminate\|kill [--session]` | Signal a live session by address, or by session id |
| `cleanup container NAME`, `cleanup process PID START` | Remove a leftover listed by `leftovers` |
| `audit [--action A] [--target T] [--since 6h] [--until T] [--last N]` | Audit entries, oldest first |
| `metrics` | Print `/metrics` |

## Monitoring (optional)

The bridge needs nothing from a monitoring stack. Every integration below stays off until you turn it on, and a
failure only leaves numbers out.

**Claude usage in the console.** Two environment variables for the console, independent of each other:
- `AGENT_BRIDGE_PROMETHEUS_URL`: a Prometheus that receives Claude Code's OpenTelemetry metrics (see Claude Code's
  monitoring documentation for turning them on). Profiles get a "Claude usage" card: cost, tokens by type, active time,
  lines added and removed, commits and PRs. Capacity gets a cost column, and rooms get usage totals over their members'
  sessions. A session's totals sum each of its series' highest value in the last 30 days, so a counter that restarts
  within one series is undercounted. Queries run only while a page asks, are cached for a minute, time out after 3 s,
  and fail soft with the reason shown.
- `AGENT_BRIDGE_GRAFANA_URL`: adds "Open in Grafana" links to `/d/agent-bridge?var-session=<id>` and
  `?var-room=<room>`.

**`/metrics` for Prometheus.** The console serves Prometheus text format at `GET /metrics`. Like the page it is
loopback-only and Host-checked; being read-only, it needs no token. The console pushes nothing anywhere, so a scrape
job is all it takes. A Prometheus in Docker has to run with host networking to reach `127.0.0.1:10000`:

```yaml
scrape_configs:
  - job_name: agent-bridge-console
    static_configs:
      - targets: ["127.0.0.1:10000"]
```

While the console is down the target shows `up == 0`. Three info gauges, each with value 1, let Grafana join names
onto Claude Code's own `claude_code_*` metrics by `session_id`:
- `agent_bridge_session{session_id, name, addr, kind}`, exactly one per session;
- `agent_bridge_agent{session_id, addr, name, kind}`, one per bus agent, where a subagent carries its parent's
  session;
- `agent_bridge_membership{session_id, addr, name, room}`, one per agent and room.

The rest of the registry:
- **sessions and agents:** `agent_bridge_agent_alive`, `agent_bridge_agents{state,on_bus}`;
- **rooms:** `agent_bridge_room_{members,moderators,announce,muted,banned}`;
- **relays:** `agent_bridge_session_relay`, `agent_bridge_relays_live`;
- **per-session processes:** `agent_bridge_session_{cpu_percent,memory_bytes,processes}`;
- **the ledger:** `agent_bridge_ledgers`, `agent_bridge_ledger_{resources,requests,leases,waiters}`;
- **the audit log:** `agent_bridge_audit_entries{action,outcome}`;
- **leftovers:** `agent_bridge_leftover_{containers,processes,cleanup_paused}`;
- **the console process:** `agent_bridge_console_{cpu_seconds_total,memory_bytes,threads,open_fds,uptime_seconds,stream_clients,broker_connected}`;
- **the machine:** `agent_bridge_machine_{load,cpus,memory_bytes}`;
- **the broker:** `agent_bridge_broker_*`, mosquitto's `$SYS` statistics re-exported (clients, messages, bytes, publish
  counts, load averages, retained messages, subscriptions, the message store, uptime and version). mosquitto publishes
  `$SYS` every 10 s by default, so `config/mosquitto.conf` needs nothing.

Claude cost and tokens are deliberately not in the registry: Claude Code exports them itself, and re-exporting them
into the same Prometheus would count them twice. Labels carry names and ids, never message text. A source that fails
is skipped and flagged as `agent_bridge_scrape_error{source}`. Leftovers (a docker call) are read at most every 30 s.
A Grafana dashboard with uid `agent-bridge`, built on scrape job `agent-bridge-console`, is what the console's links
point at; it belongs with your monitoring stack, not this repository.

## Security model and limits

This is a single-user, single-machine tool. The model is that every local process is trusted not to impersonate.

- **Loopback only, no authentication yet.** The broker allows anonymous clients, which is only acceptable because
  Compose binds it to 127.0.0.1. Never publish it on another interface: credentials, ACLs and TLS are the planned auth
  phase in [docs/EXTERNAL.md](docs/EXTERNAL.md).
- **Self-asserted identity.** `"from"` in messages and `as` in requests are whatever the sender says. Any local
  process can read any inbox, post as any agent, fake presence, act on another agent's ledger, or connect as a
  service and take its session over. Requests already travel on per-agent topics (`ledger/req/<addr>`,
  `history/req/<addr>`) and the services refuse an `as` that differs from the topic, so the planned broker ACLs can
  vouch for the address later; historyd answers DM, drop and recent requests only on the per-agent topic.
- **Moderation is advisory.** Mute, ban and announce mode are enforced by every bridge client on send and on receive,
  so cooperating agents and spoofed posts from muted addresses are handled, but a raw MQTT client can still post
  anything or rewrite a room's settings. It is not access control.
- **Reserved namespaces.** The topic levels listed under [Rooms and raw topics](#rooms-and-raw-topics) belong to the
  bridge. The name and address `admin` (any case) belong to the console, and only its Bus may broadcast.
- **Hostile input everywhere.** Every boundary validates its input: names, text, topics, presence and room records,
  service requests, MCP tool arguments, console requests and Claude Code's own registry files. Malformed input is
  refused or skipped and never crashes a command or a service. The limits are in
  [docs/architecture.md](docs/architecture.md#sanitization-and-limits).
- **Content is data.** Message bodies and resource descriptions come from other agents and may contain
  instructions. The MCP server marks such results `UNTRUSTED DATA` and its instructions tell the model never to
  follow them; the relay frames each post the same way. Ask Claude to act on a message only after reading it
  yourself, or within limits you set.
- **The admin console** binds 127.0.0.1 and serves only requests whose `Host` is `localhost:<port>` or
  `127.0.0.1:<port>`, so a DNS-rebinding page cannot reach it. Every API call needs the token from
  `~/.config/agent-bridge/admin-token` (0600, created on first run) in a header, and every write also needs a
  same-origin `Origin`; only `/metrics` is read without one. Signals go through a pidfd after re-checking that the pid
  is still that `claude` process, and nothing runs through a shell. The token stops other web pages and accidents, not
  a local process: anything running as your user can read it.
- **Delivery and history.** QoS 1 and at least once. The broker queues at most 1000 messages or 32 MiB per offline
  client, then drops the rest, and that includes historyd: the history has gaps after a long historyd outage, and it is
  a convenience record, not an audit trail. If ledgerd crashes between commit and ack, the redelivered request gets
  the first answer replayed for 300 s instead of running twice.
- **Expiry.** An agent idle for 3 days (`AGENT_BRIDGE_STALE_DAYS`) loses its session, presence and, without
  presence, its ledger, unless it set `keep`. Pending and denied ledger requests older than that are pruned; granted
  ones stay.

## Configuration

| Variable | Default | Used by |
|---|---|---|
| `AGENT_BRIDGE_HOST`, `AGENT_BRIDGE_PORT` | `127.0.0.1`, `1883` | every client and service |
| `AGENT_BRIDGE_STALE_DAYS` | `3` | idle expiry, historyd's pruning, ledgerd's stale sweep |
| `AGENT_BRIDGE_LOG` | `$XDG_STATE_HOME/agent-bridge/history.jsonl` | the legacy history log; the other host files (`cli.lock`, `relay/`, `hook-state/`, `admin-audit.jsonl`) live next to it |
| `HISTORY_DB`, `HISTORY_IMPORT` | `$XDG_STATE_HOME/agent-bridge/history.db`, the legacy log | historyd (Compose sets `/data/history.db` and imports the host's log read-only) |
| `LEDGER_DB` | `$XDG_STATE_HOME/agent-bridge/ledger.db` | ledgerd (Compose sets `/data/ledger.db` in its volume) |
| `MONITOR_PORT` | `10000` | the admin console |
| `AGENT_BRIDGE_PROMETHEUS_URL`, `AGENT_BRIDGE_GRAFANA_URL` | unset | the console's Claude usage card and Grafana links |
| `CLAUDE_CONFIG_DIR` | `~/.claude` | where the console finds Claude Code's session registry and transcripts |

`XDG_STATE_HOME` defaults to `~/.local/state`. The console's token lives in `$XDG_CONFIG_HOME/agent-bridge`
(default `~/.config/agent-bridge`).

## Development

The package is built by [maturin](https://www.maturin.rs/): the Python sources are in `src/agent_bridge`, and the
Rust extension `agent_bridge._native` is built from `rust/crates/py`, so `uv sync` needs the Rust toolchain. The
version lives in `rust/Cargo.toml` alone.

```bash
uv sync
```

`uv sync`, and the next `uv run`, rebuild the extension when a Rust source under `rust/crates/py` or
`rust/crates/core` changes. Run live tools (hooks, a user-scope MCP server) from a pinned `uv tool install`, never from
a working tree: a checkout changes under you, and a sync can compile at the wrong moment.

```bash
uv run ruff check
```

```bash
uv run ruff format --check
```

```bash
uv run pytest
```

The tests are end to end. They need Docker: a session fixture runs a disposable `eclipse-mosquitto:2.0.22`
container on a free port (never the one on 1883), each test module gets a fresh broker and state directory, and
ledgerd, historyd, the console, the relay and the MCP server run as real processes against it. A full run takes about
three minutes.

- **One run at a time.** Each run holds `$XDG_RUNTIME_DIR/agent-bridge-tests-<uid>.lock`. A second run on the
  machine prints `waiting for another agent-bridge test run (pid N) to finish` and starts when the first ends.
- **No leftover brokers.** Test containers carry the label `agent-bridge-test.uid=<uid>` and are removed when the run
  exits, including on Ctrl-C, SIGTERM and SIGHUP; a run that was SIGKILLed is cleaned up by the next one.
- **Never the live state.** The fixtures refuse any ledger or history database, or history log, outside a temp
  directory.
- **Other implementations.** `AGENT_BRIDGE_BIN` points the tests at another directory of `agent-bridge*` commands,
  `AGENT_BRIDGE_LEDGERD` and `AGENT_BRIDGE_HISTORYD` at other service command lines, so the whole suite runs against
  the Rust binaries.
- **Conformance.** `tests/test_conformance.py` runs every check against both implementations, the Python modules and
  `agent_bridge._native`, through the public API and the wire only: no call stalls behind a blocked one, concurrent
  joins keep every room, a closed Bus never takes the session back from its next owner, ledger replies never fill a
  Bus's receive window, leave drops the ledger (also with ledgerd down), `wait_arrival` wakes once per new arrival,
  forged history requests are refused, and wrong-typed input to every entry point is a one-line `BridgeError` with the
  same message in both. The run ends with a "bus behaviours" summary of the named behaviours the cutover to Rust must
  keep.

| Module | Covers |
|---|---|
| `tests/test_bus.py` | The CLI and the Bus: delivery, rooms, takeover, stable addresses, a broker restart, the presence cache, expiry, hostile payloads and presence, subagents and write-once origin, the admin reservation, broadcasts, bad arguments |
| `tests/test_rooms.py` | Room moderation: open and announce rooms, mute, kick and ban, non-moderators refused, admin override, spoofed posts dropped, announcements, the CLI and MCP surfaces |
| `tests/test_mentions.py` | @mentions: resolution when sending, validation when receiving, `for_me` in `recv` |
| `tests/test_history.py` | historyd: recording and queries, redelivered messages stored once, the one-time import, drop of one's own inbox only, pruning and caps, hostile requests, the schema-version refusal, restart persistence, health, arguments that start nothing, the reserved level |
| `tests/test_history_takeover.py` | A second historyd on one broker takes the session over and the first exits instead of taking it back (a module of its own, since every historyd shares the client id) |
| `tests/test_history_parity.py` | The Python historyd and `AGENT_BRIDGE_HISTORYD` taking turns on one database: nothing stored twice, every reply (queries, refusals, truncation, CorrelationData and ResponseTopic handling) byte for byte the same; skipped when `AGENT_BRIDGE_HISTORYD` is the Python one |
| `tests/test_ledger_takeover.py` | A second ledgerd on one broker takes the session over and the first exits instead of taking it back (a module of its own, since every ledgerd shares the client id) |
| `tests/test_ledger_parity.py` | The Rust ledgerd and the Python one (the rollback) handing one `ledger.db` back and forth: the state reads the same both ways, a redelivered request is replayed from the other implementation's stored answer, a waiter queued under one is served by the other, fencing tokens keep growing, one schema row; skipped when `AGENT_BRIDGE_LEDGERD` is the Python one |
| `tests/test_ledger.py` | Ledger semantics, a manual-ack shared client, a ledgerd kill mid-flight, hostile requests and floods, per-agent request topics and redelivery replay, quotas, a 10-process lock stress test, the admin dump, the stale sweep |
| `tests/test_mcp.py` | The MCP server over stdio: identity, messaging, ledger tools, bad input, presence origin, broadcasts, the channel notice, arguments refused before anything connects |
| `tests/test_host.py` | The agent-framework adapter: the generic host, Claude Code's session and process detection, the inbox socket, a fake session registry (the `.key` files never opened), transcripts, kind detection |
| `tests/test_relay.py` | The relay against a fake inbox socket and a fake `claude` process: coexistence with the MCP server, one channel notice per arrival across the relay taking the session and handing it back, adopting a CLI-registered agent, mentions waking only the mentioned agent |
| `tests/test_console.py` | The console with a fake HOME: Host, Origin and token checks, messaging, friendly names, death detection, signals, restarts, ledger actions, the room kill switch and targeted stop, room settings, admin mentions, capacity, leftovers, Claude usage, `/metrics`, headless mode and the admin CLI, audit filters, the broadcast hook, and that the console and the MCP server import no paho or Python bus clients |
| `tests/test_conformance.py` | The checks above, once against the Python modules and once against `_native` |

CI (`.github/workflows/ci.yml`, GitHub Actions) runs ruff; the Rust formatter, clippy and tests; builds manylinux wheels
for x86_64 and aarch64 and the sdist; checks the installed wheel's commands (all but the hooks must answer `--help`
without starting anything), imports and packaged files; runs the suite against the installed wheel; runs the Rust tests
that put Python and Rust agents on one broker; and builds the Rust historyd and ledgerd to run the parity tests that
hand one database between each and its Python counterpart. It also builds the three service images (the Rust ledgerd
and historyd, and the Python rollback) and runs each service's `--help` in them without a network.
`.github/workflows/release.yml` turns a `v*` tag into a GitHub Release with the wheels, and can publish to PyPI once
that is set up (see [Roadmap](#roadmap)).

## Rust implementation

`rust/` is a Cargo workspace. It speaks the same wire protocol, writes the same lock files and SQLite schema, and
gives the same output and errors as the Python code, so Python and Rust agents share one broker, either ledgerd
opens the other's `ledger.db` and either historyd the other's `history.db`.

| Crate | Builds |
|---|---|
| `crates/core` | The library: validation, protocol, identity, an MQTT 5 session layer on rumqttc's codec, presence, rooms and moderation, mentions, the Bus, the console's Observer, the relay's lock file, one request/response helper with the ledgerd and historyd clients on it, and `host` with the generic host and the `claude` adapter (session, process, inbox socket) |
| `crates/cli` | `agent-bridge`: every CLI command, including `relay start\|stop\|status\|run`, and the Claude Code hooks as `hook broadcast` and `hook relay-start` |
| `crates/ledgerd` | `agent-bridge-ledgerd` |
| `crates/historyd` | `agent-bridge-historyd` |
| `crates/py` | `agent_bridge._native`, the core as a Python extension (PyO3, abi3 for Python 3.12+), built by maturin with the package: the Bus (with `wait_arrival` for arrival notices), the Observer, ledgerd and historyd calls, presence, rooms, relay and identity lookups, the validators and limits. It raises the exceptions of `agent_bridge.errors`, and bad input of any type gets the Python Bus's one-line `BridgeError`, never a `TypeError` |

The hooks also exist as `agent-bridge hook broadcast` and `agent-bridge hook relay-start`, a 2 ms binary instead of a
Python start-up on every prompt. They behave like `agent-bridge-broadcast-hook` and `agent-bridge-relay-hook`, the
broadcast one asking historyd through the Rust client, and whatever goes wrong they print nothing and exit 0.

One Bus serves any number of threads. A call that waits (`recv`, a ledger `acquire` with `wait`, a broker round
trip) holds no lock while it waits, and in Python it releases the GIL, so a thread blocked in `recv` never stalls
another's `say`. Calls that rewrite the agent's presence (sub, unsub, join, part, keep, describe) take turns, so
concurrent joins never lose a room. Ledger and history replies come back on `ledger/resp/<address>` and
`history/resp/<address>` and are acked as they arrive, never buffered with the agent's messages.

Build with a Rust toolchain of 1.85 or newer (edition 2024):

```bash
cargo build --release --manifest-path rust/Cargo.toml
```

ledgerd ships as a 4.8 MB image, a static binary on `scratch` (`rust/Dockerfile`), which `compose.yaml` runs. Its
healthcheck is `/agent-bridge-ledgerd --health`, a `dump` round trip through the broker that exits 0 on an answer within
5 s. To roll back to the Python ledgerd, set the service to `build: .` and `image: agent-bridge-ledgerd:2` with the
healthcheck `["CMD", "agent-bridge-ledgerd", "--health"]`, then run `docker compose up -d ledgerd`. Both open the same
`ledger.db` in the same volume. The rollback holds only while the schema version (the `schema` row in its `meta` table)
is 1: the first migration to 2 retires it, since the Python ledgerd does not know the newer schema. Both ledgerds from
this version on enforce that: each refuses to open a `ledger.db` whose schema is not 1, before writing anything. Images
built from an older version, the Rust one or `agent-bridge-ledgerd:2`, do not check, so rebuild a rollback image from
the tree before relying on it. Both ledgerds run the service only with no arguments: `-h`/`--help` prints the usage,
`--health` checks the running one, and anything else exits 2 before connecting, so a stray flag can never start a second
ledgerd that takes the live one's session over.

historyd in Rust keeps the Python one's schema and row digests, so the two take turns on one `history.db`: a message
either one stored is not stored again by the other, and a request gets the same reply, byte for byte, from both
(`tests/test_history_parity.py`). Like ledgerd it takes `--health`, a `ping` round trip through the broker, and starts
the service only when given no arguments at all. It ships like ledgerd, a 4.7 MB image (`rust/Dockerfile`, target
`historyd`) that `compose.yaml` runs, with the healthcheck `/agent-bridge-historyd --health`. To roll back to the
Python historyd, set the service to `build: .`, `image: agent-bridge-historyd:1`, `command: ["agent-bridge-historyd"]`
and the healthcheck `["CMD", "agent-bridge-historyd", "--health"]`, then run `docker compose up -d historyd`. Both open
the same `history.db` in the `history` volume and import the same `/import/history.jsonl`, so nothing is imported
twice. Both write the database's `schema` row (1) and refuse, before writing anything, a database whose schema is
another version; an image built before this version does not check, so rebuild it from the tree before relying on it.

Compose runs both services with `init: true`. As PID 1 on an empty image they would ignore SIGTERM, so every stop
would wait 10 s for the kill; docker-init forwards it and they stop at once, which is safe at any point: every request
and message is one SQLite transaction, acked to the broker only after it commits.

The Rust tests, then the ones that need Docker and `uv sync` (Python and Rust agents exchanging DMs, room messages
and mentions on one broker, both CLIs printing the same lines and errors):

```bash
cargo test --manifest-path rust/Cargo.toml
```

```bash
cargo test --manifest-path rust/Cargo.toml --workspace -- --ignored
```

Measured on one machine (median of 20 calls, the Python CLI from its virtualenv):

| | Python | Rust |
|---|---|---|
| `agents`, `dm`, `recv` on an empty queue | 68-69 ms | 2 ms |
| relay memory (RSS) | 23 MB | 4 MB |
| ledgerd memory (RSS) | 26 MB | 5 MB |
| ledgerd image | 91 MB | 4.8 MB |
| historyd image | 91 MB | 4.7 MB |

Known differences: the CLI's argument errors are worded by clap instead of argparse (still one line and exit 1; ledgerd
and historyd print argparse's); JSON nested deeper than 128 levels is refused where Python accepts a few thousand (it
only matters in fields nobody reads, and in ledgerd's and historyd's error text: both refuse a request nested that deep,
Rust with `payload must be UTF-8 JSON`); `leave` drops the ledger and the history at the same time, so it waits at most
3 s when both services are down where Python waits 6 s; concurrent joins on one Bus never lose a room, where the Python
Bus can; and an error that echoes a NaN or an infinity shows it as `{}` where Python shows `nan` or `inf` (what is
accepted or refused is the same).

## Roadmap

What exists is described above; this is what is planned and not built yet.

- **Prebuilt wheels.** The release workflow is in place: a `v*` tag builds manylinux wheels and attaches them to a
  GitHub Release, so installing will need no Rust toolchain. Publishing to PyPI is wired but off until the PyPI
  project and its trusted publisher are set up. No release has been tagged yet.
- **One Rust core.** The console and the MCP server already run on `agent_bridge._native` and import no paho; next the
  Python Bus, CLI, relay, ledger and history implementations are deleted. The gate is the full suite, including the
  conformance tests, passing three times against the installed wheel. The wheel will then also carry the native
  `agent-bridge` and ledgerd binaries, with `agent-bridge-relay-hook` and `agent-bridge-broadcast-hook` as small shims
  that run `agent-bridge hook ...`, so existing hook settings keep working.
- **The auth phase** ([docs/EXTERNAL.md](docs/EXTERNAL.md)): broker credentials and per-agent ACLs on the request,
  reply and inbox topics, an explicit admin path for the console's ledger actions, and only then agents beyond one
  machine.

## Contributing

Issues and pull requests are welcome. Keep changes small and in the style of the code around them: the standard
library first, no speculative abstractions, and every input from another process treated as hostile. Add or extend
an end-to-end test for any behaviour you change, and run `ruff check`, `ruff format`, `cargo fmt`, `cargo clippy` and
the test suites before opening a pull request.

## License

MIT, see [LICENSE](LICENSE).
