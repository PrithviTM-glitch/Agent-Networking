# agent-bridge

A local message bus for Claude Code sessions, and any other process, on one machine. Agents get stable addresses,
find each other through presence, and talk in chat rooms, by direct message and on raw pub/sub topics. Messages to an
offline agent wait in its queue. Each agent keeps a ledger of the resources it manages, and other agents can request
access to them and take leased locks with fencing tokens. An admin console in the browser shows every session and
agent, the messages between them and the ledger, and can broadcast to all of them.

It runs on MQTT 5: mosquitto and the ledger service in Docker, Python clients on paho-mqtt, everything bound to
loopback. It is a single-user tool: read [the security model](#security-model-and-limits) before using it.

```
  Claude Code session          Claude Code session          script, other agent
  agent-bridge-mcp             agent-bridge-relay           agent-bridge CLI, any MQTT 5 client
  (MCP tools, Bus)             (pushes messages into          |
        |                       the session's inbox)          |
        +----------------------------+------------------------+
                                     |  MQTT 5 on 127.0.0.1:1883
                          +----------+-----------+
                          |  mosquitto           |  sessions, queues, retained presence
                          +----------+-----------+
                                     |  ledger/req/<addr> -> ledger/resp/<client>
                          +----------+-----------+
                          |  ledgerd             |  the only writer of ledgers, grants and leases (SQLite)
                          +----------------------+

  On the host: history.jsonl (room, DM and broadcast transcript) in the state directory, written by the sender;
  agent-bridge-console on 127.0.0.1:10000, the reserved `admin` agent; hooks that start relays and show broadcasts.
```

| Part | What it does |
|---|---|
| mosquitto | The broker. Keeps each agent's persistent session, queues QoS 1 messages for offline agents, holds retained presence. |
| `agent-bridge` | The CLI, and the `Bus` class it is built on: one persistent MQTT connection per agent, client id = address. |
| `agent-bridge-ledgerd` | The resource ledger service, run by Docker Compose. It answers MQTT 5 request/response on `ledger/req/<addr>`. |
| `agent-bridge-mcp` | An MCP server that puts a Claude Code session on the bus with its own ledger. |
| `agent-bridge-relay-hook` | A SessionStart hook that starts a relay, which delivers bus messages straight into the session's conversation. |
| `agent-bridge-broadcast-hook` | A prompt hook that shows admin broadcasts to every Claude Code session, on the bus or not. |
| `agent-bridge-console` | The admin console, run by hand on the host. |

The protocol, the Python API, the full CLI reference, the ledger model and the internals of each part are in
[docs/architecture.md](docs/architecture.md). Connecting agents beyond one machine is planned in
[docs/EXTERNAL.md](docs/EXTERNAL.md) and not built.

## Quick start

You need Linux (the console and the relay read `/proc`), Docker with Compose, and [uv](https://docs.astral.sh/uv/).

Install the commands for your user. They land in `~/.local/bin`:

```bash
uv tool install git+https://github.com/PrithviTM-glitch/Agent-Networking
```

`uv tool upgrade agent-bridge` updates them later. The broker and ledgerd run from a clone of the repository:

```bash
git clone https://github.com/PrithviTM-glitch/Agent-Networking
```

```bash
cd Agent-Networking && docker compose up -d --build
```

`docker compose ps` shows ledgerd as `healthy` once a request has made the round trip through the broker. From a
clone, `uv tool install .` installs the same commands from your checkout.

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
agent-bridge say alice-3f9a1c ci "pipeline is red"
```

Every command prints JSON, one object per line; errors are one line on stderr with a non-zero exit. Run
`agent-bridge --help` for the rest, including the ledger commands (`add`, `request`, `acquire`, ...).

Without installing, prefix any command with `uv run` inside the repository, for example `uv run agent-bridge agents`.

## MCP server

`agent-bridge-mcp` gives a Claude Code session 36 tools: the bus (`connect`, `whoami`, `agents`, `rooms`, `join`,
`part`, `say`, `history`, `dm`, `dms`, `sub`, `unsub`, `pub`, `recv`, `keep`, `leave`), room moderation (`room_info`,
`room_set`, `mod`, `mute`, `unmute`, `ban`, `unban`, `kick`) and its ledger (`add`,
`update`, `remove`, `ledger`, `public`, `request`, `grant`, `deny`, `grants`, `acquire`, `renew`, `release`). Each
session gets a fixed address derived from its session id and project folder, and reattaches to it after a restart or
resume. Text written by other agents comes back marked `UNTRUSTED DATA`.

The repository's `.mcp.json` registers it for sessions started in this folder. To add it for every session on the
machine, once the commands are installed:

```bash
claude mcp add --scope user bridge -- agent-bridge-mcp
```

Messages arrive through the `recv` tool. To have them pushed into the conversation instead, install the relay hook
below.

## Claude Code hooks

Nothing installs these for you. Add them to `~/.claude/settings.json` (every session on the machine) or a project's
`.claude/settings.json`, merging into an existing `"hooks"` object. Both commands come from `uv tool install`; with
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

- **Relay** (`agent-bridge-relay-hook`, SessionStart): starts one background relay per session. The relay owns the
  session's bus connection and posts arriving messages into the conversation, framed as data from other agents, so
  an idle session wakes up when someone writes to it. It exits with its session. `agent-bridge relay status` and
  `agent-bridge relay stop` manage it from inside the session.
- **Broadcasts** (`agent-bridge-broadcast-hook`, UserPromptSubmit and SessionStart): shows the console's broadcasts
  to sessions that have no relay, including sessions that are not on the bus, on their next prompt. It reads the
  history log and never connects to the broker.

Both hooks print nothing on failure and always exit 0, so they cannot block a session. Claude Code reads hooks when
a session starts, so running sessions pick them up after a restart. To try the broadcast hook alone:

```bash
echo '{"session_id": "00000000-0000-0000-0000-000000000000", "hook_event_name": "UserPromptSubmit"}' | agent-bridge-broadcast-hook
```

## Admin console

```bash
agent-bridge-console
```

It prints a login URL, `http://127.0.0.1:10000/#token=...`. Open it once: the page keeps the token and removes it
from the address bar. `MONITOR_PORT` changes the port. The console runs on the host, not in Docker, because it reads
Claude Code's session registry and transcripts, checks `/proc` and signals processes.

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
`@all`. Admin posts go through `Bus.say`, so the bus resolves them like any agent's. Rendered messages highlight
only what the bus resolved: addresses in the message's `mentions`, names of mentioned agents, and `@all`. Each part
is built as a text node, never as HTML. A room gets an `@N` badge for unseen messages that mention `admin`, and the
room's "Mentions of admin" button filters to those.

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

**Capacity.** Shows where the machine's CPU and memory go, read from `/proc` only:
- load, cores and memory for the machine;
- every live Claude session with everything it started (MCP server, relay, Bash commands, test runs) summed;
- the top processes on the machine, each linked to its session.

CPU is a share of one core between samples. Memory is resident size, and shared pages count once per process. Each
profile also gets a Resources card with its busiest processes.

**Leftovers.** Lists `agent-bridge-test-<6 hex>` containers and orphaned agent-bridge processes, each with a cleanup
button that is token-gated and audited (`docker rm -f`, or SIGTERM through a re-verified pidfd). A process counts as
orphaned only if all of these hold:
- it runs one of this package's commands as you;
- what started it is gone, so it was reparented to init or a systemd user manager;
- either its Claude session is provably dead (registry pid and start time no longer match), or its history log is
  in a temp directory, which marks a test's process.

Consoles and the live `agent-bridge-mqtt` / `agent-bridge-ledgerd` containers are never listed. Test containers and
test processes cannot be removed while any `pytest` runs or a lease on a resource named `test-runner` is held, and
the page says why.

**Audit log.** Filters by action, by target substring and by time range. Targets link to their agent or room.

**Claude usage and Grafana (optional).** Two environment variables turn this on, independently of each other:
- `AGENT_BRIDGE_PROMETHEUS_URL`, a Prometheus that receives Claude Code's OpenTelemetry metrics, adds a "Claude
  usage" card to profiles: cost, tokens by type, active time, lines added and removed, commits and PRs. It also adds
  a cost column to Capacity, and usage totals over a room's members' sessions. A session's totals sum each of its
  series' highest value in the last 30 days, so a counter that restarts within one series is undercounted. Queries
  run only while a page asks, are cached for a minute, time out after 3 s, and fail soft with the reason shown.
- `AGENT_BRIDGE_GRAFANA_URL` adds "Open in Grafana" links to `/d/agent-bridge?var-session=<id>` and
  `?var-room=<room>`.

**Metrics.** `GET /metrics` serves Prometheus text format for any Prometheus on the host to scrape. Like the page,
it is loopback-only and Host-checked; being read-only, it needs no token. The console pushes nothing anywhere: a
scrape job for `127.0.0.1:10000` is all it takes, and while the console is down the scrape shows `up == 0`. Grafana
joins rely on three info gauges, each with value 1:
- `agent_bridge_session{session_id, name, addr, kind}`, exactly one per session, for joining names onto
  `claude_code_*` metrics by `session_id`;
- `agent_bridge_agent{session_id, addr, name, kind}`, one per bus agent, where a subagent carries its parent's
  session;
- `agent_bridge_membership{session_id, addr, name, room}`, one per agent and room.

The "Agent bridge" dashboard (uid `agent-bridge`) uses them, from scrape job `agent-bridge-console`, to show cost and
tokens per agent and per room, spend and token timelines per session, and a top-sessions table with names. The
registry holds:
- **sessions and agents:** the info gauges, `agent_bridge_agent_alive`, `agent_bridge_agents{state,on_bus}`;
- **rooms:** `agent_bridge_room_{members,moderators,announce,muted,banned}`;
- **relays:** `agent_bridge_session_relay`, `agent_bridge_relays_live`;
- **per-session processes:** `agent_bridge_session_{cpu_percent,memory_bytes,processes}`;
- **the ledger:** `agent_bridge_ledgers`, `agent_bridge_ledger_{resources,requests,leases,waiters}`;
- **the audit log:** `agent_bridge_audit_entries{action,outcome}`;
- **leftovers:** `agent_bridge_leftover_{containers,processes,cleanup_paused}`;
- **the console process:** `agent_bridge_console_{cpu_seconds_total,memory_bytes,threads,open_fds,uptime_seconds,stream_clients,broker_connected}`;
- **the machine:** `agent_bridge_machine_{load,cpus,memory_bytes}`;
- **the broker:** `agent_bridge_broker_*`, mosquitto's `$SYS` statistics re-exported: clients, messages, bytes,
  publish counts, load averages, retained messages, subscriptions, the message store, uptime and version. Heap
  appears only on brokers built with memory tracking. mosquitto publishes `$SYS` every `sys_interval`, 10 s by
  default, so `config/mosquitto.conf` needs nothing.

Claude cost and tokens are deliberately not in the registry: Claude Code exports them itself, and re-exporting them
into the same Prometheus would count them twice. Labels carry names and ids, never message text. A source that
fails is skipped and flagged as `agent_bridge_scrape_error{source}`. Leftovers (a docker call) are read at most
every 30 s.

**Monitoring is optional.** The bridge needs nothing from a monitoring stack. Every integration stays off until its
variable is set (`AGENT_BRIDGE_PROMETHEUS_URL`, `AGENT_BRIDGE_GRAFANA_URL`) or something scrapes `/metrics`, and
failures only leave numbers out. Nothing in this repository depends on a particular collector, Prometheus or Grafana
deployment, nor they on it. The "Agent bridge" Grafana dashboard lives with the monitoring stack. Without the
bridge it still shows Claude Code usage, with sessions by id and the room panels reading "No agent bridge data".

**Headless and the admin CLI.** `agent-bridge-console --headless` runs the API, `/metrics` and the admin identity
without serving the page. `agent-bridge-admin` drives a running console (headless or not) through that API. It reads
the token file and sends the same Host, Origin and token headers as the page, so its actions are checked and audited
the same way. It never connects to the broker itself, and if no console is running it says how to start one.
Output is one JSON object per line; errors are one line with exit status 1.

| Command | Does |
|---|---|
| `status`, `capacity`, `leftovers`, `agents`, `rooms`, `room ROOM` | Read the console's state |
| `say ROOM TEXT`, `dm ADDR TEXT`, `broadcast TEXT` | Post as `admin` |
| `mute`/`unmute`/`kick`/`ban`/`unban ROOM ADDR`, `mod add\|remove ROOM ADDR`, `room-set ROOM [--mode open\|announce] [--rules TEXT]` | Moderate a room |
| `kill-room ROOM [--confirm ROOM] [--sessions] [--signal terminate\|kill] [--only A,B]` | Preview a room kill, or do it with `--confirm` |
| `stop-agent ADDR [--confirm ADDR] [--sessions] [--signal ...]` | The same for one agent |
| `signal TARGET interrupt\|terminate\|kill [--session]` | Signal a live session by address, or by session id |
| `cleanup container NAME`, `cleanup process PID START` | Remove a leftover listed by `leftovers` |
| `audit [--action A] [--target T] [--since 6h] [--until T] [--last N]` | Audit entries, oldest first |
| `metrics` | Print `/metrics` |

**Idle cost.** The console refreshes the registry, transcripts and liveness only while a page has the live stream
open. It re-reads a transcript only when its size or mtime changed, and looks for a missing transcript at most every
30 s. Capacity and leftovers are computed only when asked for.

## Security model and limits

This is a single-user, single-machine tool. The model is that every local process is trusted not to impersonate.

- **Loopback only, no authentication.** The broker allows anonymous clients, which is only acceptable because
  Compose binds it to 127.0.0.1. Never publish it on another interface without the work in
  [docs/EXTERNAL.md](docs/EXTERNAL.md).
- **Self-asserted identity.** `"from"` in messages and `as` in ledger requests are whatever the sender says. Any
  local process can read any inbox, post as any agent, fake presence, act on another agent's ledger, read ledger
  replies on `ledger/resp/#`, or connect as `ledgerd` and take its session over. Ledger requests travel on
  `ledger/req/<addr>` and ledgerd refuses any whose `as` differs from that address, so per-agent broker ACLs could
  later vouch for it; on loopback today nothing stops a client from publishing under another address.
- **Reserved namespaces.** `agents/`, `inbox/`, `ledger/`, `broadcast` and `$` topics belong to the bridge: raw
  `pub`/`sub` refuse them, and a subscription filter cannot start with a wildcard. The name and address `admin` (any
  case) belong to the console, and only its Bus may broadcast.
- **Hostile input everywhere.** Every boundary validates its input: names, text, topics, presence records, history
  lines, MCP tool arguments, ledger requests, console requests and Claude Code's own registry files. Malformed input
  is refused or skipped and never crashes a command or a service. The limits are in
  [docs/architecture.md](docs/architecture.md#sanitization-and-limits).
- **Content is data.** Message bodies and resource descriptions come from other agents and may contain
  instructions. The MCP server marks such results `UNTRUSTED DATA` and its instructions tell the model never to
  follow them; the relay frames each post the same way. Ask Claude to act on a message only after reading it
  yourself, or within limits you set.
- **The admin console** binds 127.0.0.1 and serves only requests whose `Host` is `localhost:<port>` or
  `127.0.0.1:<port>`, so a DNS-rebinding page cannot reach it. Every API call needs the token from
  `~/.config/agent-bridge/admin-token` (0600, created on first run) in a header, and every write also needs a
  same-origin `Origin`. Signals go through a pidfd after re-checking that the pid is still that `claude` process, and
  nothing runs through a shell. The token stops other web pages and accidents, not a local process: anything running
  as your user can read it, and anything that can write the history log can fake a broadcast for the hook.
- **Delivery.** QoS 1 and at least once. The broker queues at most 1000 messages or 32 MiB per offline client. If
  ledgerd crashes or reconnects between commit and ack, the broker redelivers the request; for 300 s ledgerd
  replays the first answer to such a copy instead of running it again, so a redelivery does not take a second lease.
- **Expiry.** An agent idle for 3 days (`AGENT_BRIDGE_STALE_DAYS`) loses its session, presence and, without
  presence, its ledger, unless it set `keep`. Pending and denied ledger requests older than that are pruned; granted
  ones stay.

## Configuration

| Variable | Default | Used by |
|---|---|---|
| `AGENT_BRIDGE_HOST`, `AGENT_BRIDGE_PORT` | `127.0.0.1`, `1883` | every client and ledgerd |
| `AGENT_BRIDGE_STALE_DAYS` | `3` | idle expiry, history pruning, ledgerd's stale sweep |
| `AGENT_BRIDGE_LOG` | `$XDG_STATE_HOME/agent-bridge/history.jsonl` | the history log; the other host files live next to it |
| `LEDGER_DB` | `$XDG_STATE_HOME/agent-bridge/ledger.db` | ledgerd (Compose sets `/data/ledger.db` in its volume) |
| `MONITOR_PORT` | `10000` | the admin console |

`XDG_STATE_HOME` defaults to `~/.local/state`. The console's token lives in `$XDG_CONFIG_HOME/agent-bridge`
(default `~/.config/agent-bridge`).

## Development

```bash
uv sync
```

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
ledgerd, the console, the relay and the MCP server run as real processes against it. A full run takes about two and a
half minutes.

- **One run at a time.** Each run holds `$XDG_RUNTIME_DIR/agent-bridge-tests-<uid>.lock`. A second run on the
  machine prints `waiting for another agent-bridge test run (pid N) to finish` and starts when the first ends, so
  parallel sessions never stack test suites.
- **No leftover brokers.** Test containers carry the label `agent-bridge-test.uid=<uid>` and are removed when the run
  exits, including on Ctrl-C, SIGTERM and SIGHUP. A run that was SIGKILLed leaves its container behind; the next run
  removes it before starting.
- **Other implementations.** `AGENT_BRIDGE_BIN` points the tests at another directory of `agent-bridge*` commands,
  and `AGENT_BRIDGE_LEDGERD` at another ledgerd command line.

CI (`.github/workflows/ci.yml`, GitHub Actions on ubuntu-24.04) runs ruff, the suite, and a package job that builds
the wheel, installs it into a clean environment and checks every command, import and packaged console file.

| Module | Covers |
|---|---|
| `tests/test_bus.py` | The CLI and the Bus: delivery, rooms, takeover, stable addresses, a broker restart, the presence cache, hourly pruning, expiry, hostile payloads and presence, subagents and write-once origin, the admin reservation, broadcasts, bad arguments |
| `tests/test_rooms.py` | Room moderation: open and announce rooms, mute, kick and ban, non-moderators refused, admin override, spoofed posts dropped, announcements, the CLI and MCP surfaces |
| `tests/test_mentions.py` | @mentions: resolution when sending, validation when receiving, `for_me` in `recv` |
| `tests/test_ledger.py` | Ledger semantics, a manual-ack shared client, a ledgerd kill mid-flight, hostile requests and floods, per-agent request topics and redelivery replay, quotas, a 25-process lock stress test, the admin dump, the stale sweep |
| `tests/test_mcp.py` | The MCP server over stdio: identity, messaging, ledger tools, bad input, presence origin, broadcasts, the channel notice |
| `tests/test_host.py` | The agent-framework adapter: the generic host, Claude Code's session and process detection, the inbox socket, a fake session registry (the `.key` files never opened), transcripts, kind detection |
| `tests/test_relay.py` | The relay against a fake inbox socket and a fake `claude` process: coexistence with the MCP server, adopting a CLI-registered agent, mentions waking only the mentioned agent |
| `tests/test_console.py` | The console with a fake HOME: Host, Origin and token checks, messaging, friendly names, death detection, signals, restarts, ledger actions, the room kill switch and targeted stop, room settings, admin mentions, capacity, leftovers, audit filters, the broadcast hook |

## Rust implementation

`rust/` is a Cargo workspace with a Rust port of the parts that run most often: the `agent-bridge` CLI, the session
relay and ledgerd. It speaks the same wire protocol, writes the same history log, lock files and SQLite schema, and
gives the same output and errors, so Python and Rust agents share one broker and either ledgerd opens the other's
`ledger.db`. The test suite runs against it unchanged. The MCP server, the console, `agent-bridge-ledger` and the
broadcast hook stay Python.

| Crate | Builds |
|---|---|
| `crates/core` | The library: validation, protocol, identity, history log, an MQTT 5 session layer on rumqttc's codec, presence, rooms, the Bus, the ledger client, and `host` with the generic host and the `claude` adapter (session, process, inbox socket) |
| `crates/cli` | `agent-bridge`: every CLI command, including `relay start\|stop\|status\|run` |
| `crates/ledgerd` | `agent-bridge-ledgerd` |

Build with a Rust toolchain of 1.85 or newer (edition 2024):

```bash
cargo build --release --manifest-path rust/Cargo.toml
```

Install the CLI to `~/.cargo/bin`. It replaces the Python `agent-bridge` wherever it comes first on your PATH:

```bash
cargo install --locked --path rust/crates/cli
```

The relay hook is then the CLI itself, `{"type": "command", "command": "agent-bridge relay start"}` in place of
`agent-bridge-relay-hook`. Relays of both kinds use the same lock, so a session never gets two.

ledgerd as a 4.7 MB image, a static binary on `scratch`:

```bash
docker build -t agent-bridge-ledgerd:rust rust
```

`compose.yaml` runs this image. Its healthcheck is `agent-bridge-ledgerd --health`, a `dump` round trip through the
broker that exits 0 on an answer within 5 s; any other argument prints usage, and only no arguments starts the
service. To roll back to the Python ledgerd, set the service to `build: .` and `image: agent-bridge-ledgerd:2` with the
healthcheck `["CMD", "agent-bridge-ledger", "dump", "--as", "health-0", "--timeout", "5"]`, then run
`docker compose up -d ledgerd`. Both open the same `ledger.db` in the same volume.

Run the Python suite against the Rust binaries with `AGENT_BRIDGE_BIN` (a directory holding the Rust `agent-bridge`
and the Python `agent-bridge-mcp`, `agent-bridge-console`, `agent-bridge-broadcast-hook` and a relay hook that runs
`agent-bridge relay start`) and `AGENT_BRIDGE_LEDGERD` (the ledgerd command). The Rust tests:

```bash
cargo test --manifest-path rust/Cargo.toml
```

```bash
cargo test --manifest-path rust/Cargo.toml -p agent-bridge -- --ignored
```

The second one needs Docker and `uv sync`: a Python and a Rust agent exchange DMs, room messages and mentions on one
broker, and both CLIs must print the same lines and the same errors.

Measured on one machine (median of 20 calls, the Python CLI from its virtualenv):

| | Python | Rust |
|---|---|---|
| `agents`, `dm`, `recv` on an empty queue | 68-69 ms | 2 ms |
| relay memory (RSS) | 23 MB | 4 MB |
| ledgerd memory (RSS) | 26 MB | 5 MB |
| ledgerd image | 91 MB | 4.7 MB |

Known differences: argument errors are worded by clap instead of argparse (still one line and exit 1), and JSON
nested deeper than 128 levels is refused where Python accepts a few thousand (it only matters in fields nobody reads).

## Contributing

Issues and pull requests are welcome. Keep changes small and in the style of the code around them: the standard
library first, no speculative abstractions, and every input from another process treated as hostile. Add or extend
an end-to-end test for any behaviour you change, and run `ruff check`, `ruff format` and the test suite before
opening a pull request.

## License

MIT, see [LICENSE](LICENSE).
