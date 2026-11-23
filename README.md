# agent-bridge

A local message bus for Claude Code sessions, and any other process, on one machine. Agents get stable addresses,
find each other through presence, and talk in chat rooms, by direct message and on raw pub/sub topics. Messages to an
offline agent wait in its queue, and every message is kept in a searchable history. Each agent keeps a ledger of the
resources it manages; other agents request access and take leased locks with fencing tokens. An admin console in the
browser (or from the shell) shows every session and agent, their messages, rooms and ledger, and can moderate rooms,
broadcast, and stop agents.

It runs on MQTT 5 with everything bound to loopback. It is a single-user tool with no authentication yet: read
[the security model](#security-model-and-limits) before using it.

**Status.** One Rust core with Python bindings. The CLI, the session relay, ledgerd and historyd are native binaries,
and the admin console and the MCP server are Python on the same core through `agent_bridge._native`. The Python
implementations of the Bus, CLI, relay, ledger and history that 0.1.0 still shipped are gone since 0.2.0.

## How it fits together

```
  Claude Code session           Claude Code session           script, other agent
  agent-bridge-mcp (Rust core)  relay (Rust): pushes messages agent-bridge CLI (Rust),         
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
- **One bus core.** Presence, rooms, moderation, mentions, delivery and the ledger and history clients are Rust
  (`rust/crates/core`). The CLI and the relay are built on it, and the console and the MCP server reach it through
  `agent_bridge._native`. The Python package keeps the console, the MCP server, the Claude Code host adapter and the
  shared settings and checks; the conformance tests pin the bindings' behaviour.
- **Agent frameworks sit behind a host adapter.** Everything that knows Claude Code (its session id variable, the
  `claude` process, the session inbox socket, the session registry and transcripts) lives in `agent_bridge/claude` and
  `rust/crates/core/src/claude`. Any other process runs under the generic host: no session and no push delivery, so it
  reads messages with `recv`.
- **historyd keeps the history.** It subscribes to every message and stores rooms, DMs, broadcasts and raw topics in
  SQLite. The CLI's `history`/`dms`, the MCP tools, the console and the broadcast hook read through it, and `leave`
  asks it to drop the agent's received DMs. Nothing writes the old `history.jsonl` log any more; a machine that has
  one from before historyd imports it once through `compose.override.example.yaml` (copy it to
  `compose.override.yaml`).

| Command | What it is | Built as |
|---|---|---|
| `agent-bridge` | The CLI: register, rooms, DMs, raw topics, moderation, the ledger, the relay | Native binary |
| `agent-bridge-mcp` | MCP server that puts a Claude Code session on the bus with its own ledger | Python on the Rust core |
| `agent-bridge hook relay-start` | SessionStart hook: starts the session relay, which delivers bus messages into the session | Native (`agent-bridge-relay-hook` is a shim for it) |
| `agent-bridge hook broadcast` | Prompt hook: shows admin broadcasts to Claude Code sessions that have no relay | Native (`agent-bridge-broadcast-hook` is a shim for it) |
| `agent-bridge-console` | The admin console, web page and API, run by hand on the host | Python on the Rust core |
| `agent-bridge-admin` | The console's API from the shell | Python |
| `agent-bridge-ledgerd` | The resource ledger service, which Compose runs | Native binary and image |
| `agent-bridge-historyd` | The history service, which Compose runs | Native binary and image |
| `agent-bridge ledger-raw` | Raw ledger requests, for scripts | Native (`agent-bridge-ledger` is a shim for it) |

Every command in the table is in the wheel: the native binaries and the shims are installed next to the Python entry
points, so one install gives all of them.

The protocol, the Python bindings, the full CLI reference, the ledger model and the internals of each part are in
[docs/architecture.md](docs/architecture.md). Connecting agents beyond one machine, and authentication, are planned
in [docs/EXTERNAL.md](docs/EXTERNAL.md).

## Rooms and raw topics

Both carry messages, but they are for different things.

- **Rooms** (`join`, `part`, `say`, `history`) are for conversations between agents. A room has members (listed in
  presence), `[joined #room]` / `[left #room]` notices, optional moderation (announce mode, moderators, mute, ban,
  kick), `@name` / `@address` / `@all` mentions that wake only the agents mentioned (matched without case; any other
  `@word` stays plain text and is reported back), and a transcript in historyd.
  Every room message is JSON with a sender (`from`), and the bus checks it on the way in and out.
- **Raw topics** (`sub`, `unsub`, `pub`) are event streams for scripts and other MQTT clients: `ci/build`,
  `deploy/staging/status`. Subscriptions take MQTT wildcards (`+` for one level, `#` for the rest, never as the first
  level), any MQTT 5 client can publish to them, and plain text is accepted (it arrives with `"from": null`). There
  is no membership, moderation or mention handling; historyd still records them.
- **Reserved:** `agents/` (presence), `inbox/` (DMs), `room/` (rooms), `rooms/` (room settings), `ledger/`,
  `history/`, `broadcast` and anything starting with `$` belong to the bridge. Raw `pub`/`sub` refuse them.

## Quick start

You need Linux (the console and the relay read `/proc`), Docker with Compose, and [uv](https://docs.astral.sh/uv/).

Install the commands for your user from a release wheel; they land in `~/.local/bin`. Download the wheel for your
machine (x86_64 or aarch64) from the
[latest release](https://github.com/PrithviTM-glitch/Agent-Networking/releases/latest) and install the downloaded file:

```bash
uv tool install ./agent_bridge-*-manylinux_2_28_$(uname -m).whl
```

[Upgrading](#upgrading) covers newer releases. To build from a clone instead you need a Rust toolchain of
1.85 or newer ([rustup](https://rustup.rs/)): `scripts/build-bins.sh` builds the native commands, then
`uv tool install .` builds the extension and installs everything, which takes a few minutes the first time. A plain
`uv tool install` of the git URL or the sdist gives no native commands, since nothing builds them.

The broker, ledgerd and historyd run from a clone of the repository:

```bash
git clone https://github.com/PrithviTM-glitch/Agent-Networking
```

```bash
cd Agent-Networking && docker compose up -d --build
```

`docker compose ps` shows ledgerd and historyd as `healthy` once each has answered a request through the broker.
Then `doctor` checks the whole setup: the broker, both services and their versions, your state and config directories,
and your Claude Code hooks. It prints one OK, WARN or FAIL line per check, with what to do, and changes nothing:

```bash
agent-bridge doctor
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

```bash
agent-bridge watch ci --text
```

Commands that return data print JSON, one object per line (`recv` one per message, so possibly none), with text as
written in UTF-8. `--text` on `recv`, `history`, `dms` and `watch` prints one line per message for people instead.
`watch ROOM` follows a room until Ctrl-C: first its recent history, then each new message. It needs no agent. `register`
prints just the address, and `keep`, `join`, `part`, `say`, `dm`, `sub`, `unsub`, `pub`, `leave` and `relay start` print
nothing when they succeed. Errors are one line on stderr with a non-zero exit. A `say` whose `@word` matches no member
of the room is still sent, with a one-line notice on stderr that it mentions nobody, or that the agent it names is not
in the room. `@admin` always reaches the admin console, while `@me` and `@@word` never mention anyone. `agent-bridge
--help` lists the rest: moderation (`room`, `mod`, `mute`, `ban`, `kick`, ...), raw topics, and the ledger (`add`,
`request`, `acquire`, ...). Every command, the native and the Python ones, also answers `--version`.

## Everyday commands

- **Page back through history.** Each `history` and `dms` entry carries a `seq`. `--before SEQ`, given the oldest seq
  you have, returns the page before it.
- **Long or piped text.** `say` and `dm` take the text as an argument, `-` to read it from stdin, or `--file PATH`
  (UTF-8, at most 64 KiB, not empty).
- **Describe yourself.** `describe MY_ADDR --name N --caps a,b --about TEXT` changes your display name, caps or
  one-line description (at most 200 bytes; `""` clears it). The address never changes, and `@name` mentions follow the
  new name.
- **Your leases.** `leases MY_ADDR` lists the leases you hold, soonest expiry first. `lease-history MY_ADDR NAME`
  shows the last events of one of your resources (acquire, renew, release, break, revoke, expire, drop), and the
  holder of a lease that expires without being renewed gets a DM saying so. `request ... --wait [S]` blocks until the
  owner grants or denies the request (default 300 s, at most 3600).
- **Reply to a message.** Every message carries an `id` (16 hex digits, the same in `recv`, `history`, `dms`, `watch`,
  the MCP tools and the console). `say` and `dm` take `--reply-to ID` to answer one; the reply carries it as `reply_to`,
  `--text` shows it as `(re 3f9a01b2)`, and the console groups replies under their message.
- **Pin a room's essentials.** `pin MY_ADDR ROOM TEXT` (or `-`, or `--file PATH`) puts a message at the top of a room,
  such as its format, a digest or the state of a lane, up to 64 KiB; `pin MY_ADDR ROOM --clear` removes it. Admin or a
  moderator of the room sets it, the room is told, and `room info` and the console show it. Like moderation, only bridge
  clients enforce who may set it.
- **Quiet a busy room.** Inside a session, `agent-bridge relay wake ROOM mentions` lets only posts that mention you (or
  `@all`, or come from admin) wake the session; `admin` wakes only on admin's posts, and `all` goes back to every post.
  The rest wait and arrive with the next wake; DMs and broadcasts always wake, and `recv` sees everything. `relay wake`
  alone lists the session's filters. "admin" here is just the sender field, so use it to decide when to wake, not to
  trust a message.
- **Claim work.** `claim MY_ADDR ROOM KEY [--ttl S] [--note TEXT]` marks a key in a room, such as an issue id or a
  question, as yours; a second claimant is refused and told who holds it and since when. `claims MY_ADDR ROOM` lists a
  room's claims, and `unclaim MY_ADDR ROOM KEY` releases one (by its holder, a moderator of the room or admin; a release
  by anyone else DMs the holder). A claim lasts until it is released, until its optional ttl (up to 30 days), or until
  its holder leaves. `holders MY_ADDR NAME|OWNER/NAME` shows who holds a resource and who waits for it. Like moderation,
  claims keep cooperating agents from doubling up; they do not stop a process that ignores them.
- **Lock a worktree.** `worktree acquire MY_ADDR PATH` keeps other agents out of the git worktree PATH is in until the
  session ends; `worktree release MY_ADDR PATH` gives it back, saving any uncommitted work under a ref, and `worktree
  status PATH` shows who holds it. It is enforced by hooks you install: see [Worktree locks](#worktree-locks).
- **Run a command under a lock.** With `--exec`, `acquire` takes the lease, runs the command, renews the lease while
  it runs and releases it when it exits. `--exec` comes last: everything after it is the command.

  ```bash
  agent-bridge acquire bob-77d0e2 alice-3f9a1c/staging-db write --exec ./migrate.sh --dry-run
  ```

  The command finds `AGENT_BRIDGE_LEASE_ID`, `AGENT_BRIDGE_LEASE_RESOURCE`, `AGENT_BRIDGE_LEASE_LEVEL` and, for a write
  lease, the fencing token `AGENT_BRIDGE_FENCE_TOKEN` in its environment. SIGINT, SIGTERM and SIGHUP are passed on to
  it. `acquire` exits with the command's status (128+N if signal N killed it), 127 if the command could not start,
  and 75 if the command did not run to the end with the lease: the lease was refused (busy, no grant), or it was lost
  while the command ran, in which case the command gets SIGTERM, then SIGKILL 10 s later. So 75 means "retry", while
  any other code is the command's own.

## Subagents

A Claude Code subagent (a Task running inside a session) shares its session's environment, so a plain `register` from
it would be recorded as the session itself. Give it an agent of its own instead:

```bash
ADDR=$(agent-bridge register --subagent reviewer)
```

The new agent is recorded with `"kind": "subagent"` and its creating session as `"parent"`, never as the session, so
the session's relay never adopts it and the console counts it under its session. `register` prints just the address,
one line with no JSON, so `$(...)` captures it as it is; `register --json` prints the agent's presence record as one
JSON object instead (`jq -r .addr`). There is no need to unset `CLAUDE_CODE_SESSION_ID` first.

## Upgrading

What each release changes, and anything to do when upgrading, is in [CHANGELOG.md](CHANGELOG.md), which is also the
text of each GitHub release. Upgrade the services first: a new release's services keep answering the previous
release's commands, but new commands may need new services (0.3.0's `history` and `dms` need historyd 0.3.0, and
0.3.1's claims and `holders` need ledgerd 0.3.1, and 0.3.2's worktree locks need ledgerd 0.3.2). From your clone,
check out the tag of the release you are installing:

```bash
git fetch --tags && git checkout v0.3.2 && docker compose up -d --build
```

Then install the release's wheel over the old one, the same way as in the Quick start:

```bash
uv tool install --force ./agent_bridge-*-manylinux_2_28_$(uname -m).whl
```

Running Claude Code sessions keep the old code until each part restarts:
- **Hooks** run the new commands on their next start; nothing to do.
- **Relays** started on 0.3.1 or later restart themselves on the new binary once they are idle and the install has
  settled, keeping their lock and queued messages. Older ones need, once, in each session: `agent-bridge relay
  restart` (before 0.3.0: `relay stop`, then `relay start`), or a new session. `relay stop` prints the relay's status
  after stopping it, so its `"live": false` is expected.
- **MCP servers**: run `/mcp` in each session and reconnect `bridge`, or restart the session. A session that skips this
  keeps the previous version's server for as long as it runs; with 0.3.0 that also means it keeps posting as a second,
  derived address instead of the session's own agent.

`agent-bridge doctor` then shows whether the CLI, the services and the installed wheel agree, and lists any relay still
running a replaced binary.

**0.3.2** changes nothing until you choose to. Authentication ships `off`, `compose.yaml` is unchanged, and the worktree
guard and ref guard are hooks you install yourself. Each is optional, and each has its own section: [Worktree
locks](#worktree-locks) (install the guard, protect a repository) and [Authentication](#authentication) (the overlay and
`warn`). Before switching to `enforce`, let `doctor` show that every relay and MCP server is 0.3.2 and no live agent
connects anonymously. Received messages now carry `"sender"`.

To roll back, check out the previous release's tag and run `docker compose up -d --build`, then install its wheel the
same way. Roll back the commands together with the services: 0.3.0's `history`, `dms`, `leases` and `lease-history`,
0.3.1's `claim`, `unclaim`, `claims` and `holders`, and 0.3.2's `worktree` commands fail with a one-line error against
older services. Switch auth back to `off` before rolling back past 0.3.2. ledgerd and
historyd refuse a database whose schema is newer than they know, so a rollback across a schema change fails at start
rather than touching the data.

## MCP server

`agent-bridge-mcp` gives a Claude Code session 49 tools:
- **the bus:** `connect`, `whoami`, `describe`, `agents`, `rooms`, `join`, `part`, `say`, `history`, `dm`, `dms`, `sub`,
  `unsub`, `pub`, `recv`, `keep`, `leave`, `pin`, `unpin`, `wake` (`history` and `dms` entries carry their `id` and
  `seq`, and `before=SEQ` pages back past them; `say` and `dm` take `reply_to`, the id of the message they answer, and
  `say` reports `absent` mentions; `describe` sets the name, caps and a one-line `about` in place; `wake` sets the
  session's wake filter for a room);
- **room moderation:** `room_info` (with the room's pin), `room_set`, `mod`, `mute`, `unmute`, `ban`, `unban`, `kick`;
- **its ledger:** `add`, `update`, `remove`, `ledger`, `public`, `request` (with `wait`, it blocks for the owner's
  answer), `grant`, `deny`, `grants`, `acquire`, `renew`, `release`, `leases`, `lease_history`, `claim`, `unclaim`,
  `claims`, `holders`;
- **worktree locks:** `worktree_acquire`, `worktree_release`, `worktree_status` (see [Worktree locks](#worktree-locks)).

It posts as the session's own agent, the one the session's relay delivers for (an agent the session registered through
the CLI), so replies reach it. It picks that agent when it starts: after the relay switches to an agent registered
later, the MCP tools still post as the earlier one until `/mcp` reconnects (a known limit). A session with no agent gets
a fixed address derived from its session id and project folder, and reattaches to it after a restart or resume. Its
presence record carries its version as `mcp`. Text written by other agents comes back marked `UNTRUSTED DATA`.

To add it for every session on the machine, once the commands are installed:

```bash
claude mcp add --scope user bridge -- agent-bridge-mcp
```

It takes no arguments: `--help` prints usage, `--version` prints the version, and any other argument is refused
(exit 2) before anything connects, so running it by hand inside a session never takes that session's agent from its
real server.

The repository's own `.mcp.json` runs the installed `agent-bridge-mcp` from your `PATH`, the same release every other
session uses, so a session opened in a clone never puts unreleased code on the bus. It also keeps sessions safe from
work in the clone: `uv sync` rewrites the checkout's native module in place, which crashes any server that has it
loaded. Test changes to the server with the suite, which builds its own wheel and broker. Messages arrive through the
`recv` tool; to have them pushed into the conversation instead, install the relay hook below.

## Claude Code hooks

Nothing installs these for you. Add them to `~/.claude/settings.json` (every session on the machine) or a project's
`.claude/settings.json`, merging into an existing `"hooks"` object. The commands come from the install above; with
another install, use the absolute path of the command.

```json
{
  "hooks": {
    "SessionStart": [
      {"hooks": [
        {"type": "command", "command": "agent-bridge hook relay-start"},
        {"type": "command", "command": "agent-bridge hook broadcast"}
      ]}
    ],
    "UserPromptSubmit": [
      {"hooks": [{"type": "command", "command": "agent-bridge hook broadcast"}]}
    ]
  }
}
```

Settings written for 0.1.0 keep working: `agent-bridge-relay-hook` and `agent-bridge-broadcast-hook` are installed as
shims that run these two subcommands.

- **Relay** (SessionStart): starts one background relay per session. The relay owns the session's bus connection and
  posts arriving messages into the conversation, framed as data from other agents, so an idle session wakes up when
  someone writes to it. Only `@mentioned` agents are woken by room messages; the rest wait as a backlog. It exits with
  its session. `agent-bridge relay status`, `relay stop` and `relay restart` manage it from inside the session; they act
  on whichever Claude Code session's environment (`CLAUDE_CODE_SESSION_ID`) they run in. `relay status` shows the
  relay's pid, uptime, binary, version and `addr`, the agent it delivers for, and `agent-bridge doctor` flags a relay
  still running a replaced binary after an upgrade, or relaying for the wrong agent. The relay follows the session's
  agent: a named agent registered from the session wins over the session-derived address, and one holding rooms or
  topics wins over one holding none. Registering or joining from the session tells the relay at once, and it also
  rechecks every 60 s when idle; when the choice changes, it restarts itself on the new address, at most once every 10
  s. A relay started before 0.3.4 picks once, at start: run `agent-bridge relay restart` after registering.
- **Broadcasts** (UserPromptSubmit and SessionStart): shows the console's broadcasts to sessions that have no relay,
  including sessions that are not on the bus, on their next prompt. It asks historyd with a 1 s timeout and shows
  nothing if that fails.

Both hooks, under either name, print nothing on failure and always exit 0, so they cannot block a session. The
worktree guard is a third, optional hook that does fail closed, by design: see [Worktree locks](#worktree-locks). Claude
Code reads hooks when a session starts, so running sessions pick them up after a restart. To try the broadcast hook
alone:

```bash
echo '{"session_id": "00000000-0000-0000-0000-000000000000", "hook_event_name": "UserPromptSubmit"}' | agent-bridge hook broadcast
```

## Worktree locks

A worktree lock keeps other agents out of a git worktree while one agent works in it. It is a lease in the ledger, so it
has a holder, an expiry and a queue, and two local hooks enforce it. **Nothing installs itself**: you run each step
below, and each one says how to undo it. What a lock can and can't stop is in [docs/ACCESS.md](docs/ACCESS.md) section
5.6.

```bash
agent-bridge worktree acquire MY_ADDR ~/Code/project-feature                 # take it; held until the session ends
agent-bridge worktree status ~/Code/project-feature                          # who holds it
agent-bridge worktree release MY_ADDR ~/Code/project-feature                 # give it back
agent-bridge worktree acquire MY_ADDR ~/Code/project-feature --exec make test   # hold it around one command
```

- **`acquire`** takes the write lease on the worktree the path is in (ledgerd creates its lock resource on demand) and
  locks it in git with the reason `agent-bridge lease <id> held by <addr>`, which `git worktree remove` respects. Inside
  a session, the session's relay renews it and releases it when the session ends; without a relay it lapses at `--ttl`
  (600 s by default). The main worktree can't be git-locked: you get a warning, and the lease and the hooks still hold
  it.

- **`release`** first saves any uncommitted changes, untracked files included, as a commit under
  `refs/agent-bridge/released/<lease id>`, without touching your files or index, then unlocks and releases. Get the work
  back with `git checkout refs/agent-bridge/released/<id> -- .` or `git cherry-pick`.

- **The MCP server** has the same three as `worktree_acquire`, `worktree_release` and `worktree_status`, and the
  console's Worktrees view shows every lock (see [Admin console](#admin-console)).

**What enforces it.** Each worktree's label has two parts, and the console shows both:

| Part | `hook` when | What it stops | What it doesn't |
|---|---|---|---|
| **files** | the guard hook is installed (below) | Another Claude Code session's `Edit`, `Write`, `NotebookEdit`, MCP write tools and shell writes inside the worktree, `git -C` into it, and anything but read-only commands from inside it | Interpreters (`python -c`), scripts run from outside, background jobs, other tools and harnesses |
| **history** | the repository is protected (below) | Any git command that moves the locked worktree's branch, from any worktree, `--no-verify` included | Direct writes to ref files, which the guard refuses while it's installed |

Without the guard, files are `soft`; without protection, history is `none`; in a per-session sandbox, files are
`enforced` ([docs/ACCESS.md](docs/ACCESS.md) 5.6). The guard stops agents that forget. It doesn't stop a process that
sets out to get around it, because every agent runs as your OS user.

**Work elsewhere is never wedged.** The guard only judges a call against worktrees someone else holds. While a lock is
held in one repository, `rm $X`, `rm *.tmp`, `git -c core.hooksPath=... status` and `cd "$OUT" && make` still run in any
other repository or directory. In the locked repository, commands whose paths it can't resolve (`$VAR`, `$(...)`) are
refused, and a write that names the locked worktree, its branch's ref files, or the repository's `hooks/` or `config` is
refused from anywhere. A refusal says what to run:

```text
blocked: Write /work/feature/app.py is inside /work/feature, leased by bob-77d0e2 until 14:05:31 (lease
9f2c4e1ab07d3355). To work there: agent-bridge worktree acquire alice-3f9a1c /work/feature
```

### Install the guard hook

The guard is a Claude Code `PreToolUse` hook, `agent-bridge hook guard`. A refusal holds in every permission mode,
`bypassPermissions` included. Unlike the other hooks it fails closed: a crash or bad input refuses the call.

**Recommended: managed settings**, which agents can't edit and a user can't switch off with `disableAllHooks`. As root,
merge this into `/etc/claude-code/managed-settings.json` (macOS: `/Library/Application
Support/ClaudeCode/managed-settings.json`), or put it in a file of its own under `managed-settings.d/`:

```json
{
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "Bash|Edit|Write|NotebookEdit|mcp__terminal__run_in_terminal",
        "hooks": [
          {"type": "command", "command": "agent-bridge hook guard", "timeout": 10, "onFailure": "block"}
        ]
      }
    ]
  }
}
```

- `onFailure: "block"` makes a guard that times out refuse the call too. It needs Claude Code 2.1.295 or later; older
  versions ignore it, so a timeout lets the call through. `agent-bridge doctor` checks the version.

- Use the absolute path from `command -v agent-bridge` if the sessions' PATH may not find it.

- Claude Code reloads settings files while it runs; restart a session if the change doesn't show.

- To undo: remove the entry, or the file under `managed-settings.d/`.

**Alternative: user settings**, the same block in `~/.claude/settings.json`. Simpler, but any agent can edit that file,
so the guard is then a reminder rather than a rule.

### Protect a repository

The ref guard is git's `reference-transaction` hook, `agent-bridge hook ref-guard`, installed per repository. The
console's **Protect this repo** button installs it into the repository's effective hooks directory (honouring
`core.hooksPath`), keeps any existing hook and runs it first, and audits the change; **Unprotect** removes only what it
wrote. By hand, for a repository with no hook of its own:

```bash
hooks=$(git -C ~/Code/project rev-parse --git-path hooks)
printf '#!/bin/sh\nexec agent-bridge hook ref-guard "$@"\n' > "$hooks/reference-transaction"
chmod +x "$hooks/reference-transaction"
```

To undo, delete that file (and, if the console chained an older hook, rename
`reference-transaction.agent-bridge-chained` back). If `agent-bridge` is ever missing, every ref update in a protected
repository fails until you do. Run from your own terminal, outside an agent session, the ref guard allows a move of a
leased branch with a warning, so you are never locked out.

## Admin console

```bash
agent-bridge-console
```

It prints a login URL, `http://localhost:10000/#token=...` (`127.0.0.1` is still served). Open it once: the page keeps
the token and removes it from the address bar. `MONITOR_PORT` changes the port. The console runs on the host, not in
Docker, because it reads Claude Code's session registry and transcripts, checks `/proc` and signals processes.
`--headless` serves the API and `/metrics` without the page.

It lists every Claude Code session on the machine, on the bus or not, joined with the bus agents by session id, with a
friendly name (`branch · task`), liveness and recent activity. It shows rooms, DMs, topics and broadcasts live (a
Markdown pipe table in a message, up to 50 columns and 500 rows, renders as a table, a ``` block as monospace, at most
200 blocks per message), posts as the reserved agent `admin`, and shows and intervenes in the ledger (release a lease,
revoke a grant). For a live session it can send SIGINT, SIGTERM or SIGKILL; for a dead one it can open a terminal
resuming it or resume it with a message. Every action is written to an audit log. Details are in
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

**Inbox, pins and replies.** The Inbox collects posts that mention `@admin` in any room and DMs to admin, newest first,
with an unread count as a badge on the Inbox entry in the sidebar and in the tab title (read state is kept per browser).
A room's pinned message sits at the top of the room, collapsed past 12 lines, and Pin… (Edit pin once one is set) sets,
changes or clears it as admin. A reply sits under the message it answers, or shows a one-line stub that scrolls to it,
and every room or DM message has a Reply action. Pins and replies come from the bus, so ones made from the CLI or MCP
show up the same way.

**Claims.** The Claims view lists every room's claims (key, holder, since, expiry) and releases one as admin, audited
and after a confirmation.

**Links.** Message text can link to your own tools: an optional `~/.config/agent-bridge/console-links.json` (or the file
`AGENT_BRIDGE_LINKS` names), read when the console starts, maps patterns to URLs. Nothing is built in, so the example
uses placeholder hosts:

```json
[
  {"name": "mr", "pattern": "!([0-9]+)", "url": "https://git.example.com/group/project/-/merge_requests/{1}"},
  {"name": "issue", "pattern": "([A-Z]+-[0-9A-Z]+)", "url": "https://issues.example.com/issue/{1}"}
]
```

A pattern is matched against whole whitespace-separated words of at most 200 characters, in plain text and table cells,
never in code blocks. Up to 20 rules; each pattern has 1 to 3 capture groups and uses only literals, classes, `\d` `\w`
`\s`, escaped punctuation, `(` and `(?:` groups and quantifiers, so it means the same in Python and in the browser. A
group may be optional (`?`) but never repeated, and only one group may hold `|`. Every repeated part (`+`, `*`, `{n,}`
or `{n,m}`) must be followed at once (a group's closing bracket aside) by one plain character it cannot match, or end
the pattern; after a group, that character must be one none of the group's trailing repeats can match. A pattern has at
most 3 optional parts (`?` or `{0,n}`, an optional group included): each repeat then has one place to stop, so no
crafted word can make matching slow. `!([0-9]+)`, `([A-Z]+-[0-9A-Z]+)` and `(?:SENTRY|APP)-([0-9A-Z]+)` pass;
`[a-z]+[a-z]+`, `[.\w]+\.py` and `(?:[a-z]+|x)[a-z]+` are refused. In the URL, `{1}` to `{3}` may appear only after the
host, so a message can never choose where a link goes, and each capture is URL-encoded. A bad file is refused whole,
with the reason on stderr and in `agent-bridge-admin status`. Git hashes and paths become click-to-copy without any
configuration.

**Worktrees.** The Worktrees view lists the leased worktrees (path, branch, holder, expiry, lease id) and labels each
one honestly in two parts, `files: hook|soft · history: hook|none` (see [Worktree locks](#worktree-locks)). It flags
drift: a lock resource whose id doesn't match its path, a lease with no git lock, a git lock naming someone else, or a
stale lock with no lease. It shows the live sessions working inside each worktree, and the sidebar marks a session that
shares its worktree with another live one (⧉N), from where the sessions run, with no lease needed. The view also shows
whether the guard hook is installed, read from the managed and user settings files, and whether it blocks on failure.

**Protect this repo.** A repository row's button installs the ref guard: it writes a `reference-transaction` hook marked
`# agent-bridge ref-guard v1` into the repository's effective hooks directory (`git rev-parse --git-path hooks`, so
`core.hooksPath` is honoured). An existing hook is renamed `reference-transaction.agent-bridge-chained` and runs first,
with the same arguments and input. Either one refusing aborts the ref update. **Unprotect** removes only a hook carrying
that marker and puts the chained one back, and refuses a hook that isn't ours. Both are audited. If `agent-bridge` is
ever missing, every ref update in a protected repository fails: Unprotect, or delete the hook and rename the chained one
back.

**Auth.** When auth isn't `off`, the header shows the mode and a message whose sender isn't bound to its topic carries a
"claimed" badge (see [Authentication](#authentication)). The console logs in as admin by setting
`AGENT_BRIDGE_CREDENTIALS` to `~/.config/agent-bridge/admin.json` for itself, only when that variable is unset and the
file exists.

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
which marks a test's process. A temp-log process whose broker (its `AGENT_BRIDGE_PORT`) is up and is not a test
container is a stack someone runs on purpose and is not listed. Consoles and the live `agent-bridge-*` service
containers are never listed. Test
containers and test processes cannot be removed while any `pytest` runs or a lease on a resource named `test-runner`
is held, and the page says why.

**Audit log.** Filters by action, by target substring and by time range. Targets link to their agent or room. A
refused action is logged with the same target as a successful one (`room:ops`, `dm:ADDR`, `ROOM:ADDR`), so a target
filter finds both.

**Dropped messages.** The header shows "N dropped" when mosquitto has dropped publishes since it started: at its queue
limits, an agent that does not read its messages loses QoS 0 and QoS 1 messages silently once its queue is full, and
the sender still saw success. The count is also in `agent-bridge-admin status` and `/metrics`
(`agent_bridge_broker_publish_messages{kind="dropped"}`).

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
| `status`, `capacity`, `leftovers`, `agents`, `rooms`, `room ROOM` | Read the console's state. `status` counts agents on the bus, real Claude sessions, all entries, rooms, messages and the broker's dropped publishes; `rooms` and `room` give `members` as a count and `who` as the addresses; `leftovers` starts with a summary line |
| `say ROOM TEXT`, `dm ADDR TEXT`, `broadcast TEXT` | Post as `admin` |
| `mute`/`unmute`/`kick`/`ban`/`unban ROOM ADDR`, `mod add\|remove ROOM ADDR`, `room-set ROOM [--mode open\|announce] [--rules TEXT]` | Moderate a room |
| `kill-room ROOM [--confirm ROOM] [--sessions] [--signal terminate\|kill] [--only ADDR,ADDR]` | Preview a room kill, or do it with `--confirm` |
| `stop-agent ADDR [--confirm ADDR] [--sessions] [--signal ...]` | The same for one agent |
| `signal TARGET interrupt\|terminate\|kill [--session]` | Signal a live session by address, or by session id |
| `cleanup container NAME`, `cleanup process PID START` | Remove a leftover listed by `leftovers` |
| `audit [--action A] [--target T] [--since 6h] [--until T] [--last N]` | Audit entries, oldest first. `--action` takes the API action (`send`, `room/member`, `leaks/process`, ...) or a command name: `kick`, `mute`, `unmute`, `ban` and `unban` all match `room/member`, `say` and `dm` match `send`, `cleanup` matches both `leaks/` actions |
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
loopback-only and Host-checked; being read-only, it needs no token. Without one it carries no names, though:
session and agent names (session titles and branches) are only in a scrape that sends the admin token, as
`X-Admin-Token` or as a bearer credential. The console pushes nothing anywhere, so a scrape job is all it takes. A
Prometheus in Docker has to run with host networking to reach `127.0.0.1:10000`, and needs the token file mounted for
the names:

```yaml
scrape_configs:
  - job_name: agent-bridge-console
    authorization:
      type: Bearer
      credentials_file: /etc/agent-bridge/admin-token  # ~/.config/agent-bridge/admin-token, mounted read-only
    static_configs:
      - targets: ["127.0.0.1:10000"]
```

While the console is down the target shows `up == 0`. Three info gauges, each with value 1, let Grafana join names
onto Claude Code's own `claude_code_*` metrics by `session_id` (`name` only with the token):
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

## Authentication

0.3.2 ships the groundwork for authenticated agents, **off by default**: with nothing changed, the broker and every
client behave as in 0.3.1. Nothing switches itself on; you choose a mode, and going back is one command. The design, and
what it does and doesn't protect against, is in [docs/AUTH.md](docs/AUTH.md).

| Mode | The broker | A 0.3.1 client | A 0.3.2 client |
|---|---|---|---|
| `off` (default) | Anonymous, as today | Works | Works, anonymously |
| `warn` | Per-agent credentials and roles; anonymous clients still allowed, with today's rights | Works; `doctor` lists it as anonymous | Logs in with its own credentials |
| `enforce` | Anonymous clients refused | Refused | Works, within its role |

**Turn it on.** The opt-in overlay `compose.auth.yaml` runs the broker as you, with a unix socket next to TCP 1883;
`compose.yaml` itself is unchanged. Create its two directories yourself first (a missing one is an error, so Docker
never creates them as root), then start in `warn`:

```bash
mkdir -p -m 700 ~/.config/agent-bridge/services ~/.local/state/agent-bridge/broker
```

```bash
AGENT_BRIDGE_AUTH=warn docker compose -f compose.yaml -f compose.auth.yaml up -d
```

- The first start creates the broker's admin, `~/.config/agent-bridge/admin.json`, and ledgerd's and historyd's
  credentials under `services/`. Every credential file is 0600 in a 0700 directory, and a file others can read is
  refused.

- From then on each agent gets its own credentials, `~/.config/agent-bridge/agents/<addr>.json`, and a role that may
  only post as itself: a new one when it registers, and one that already existed the next time it attaches, for example
  when its session's relay restarts, so running sessions migrate without registering again. What decides is the
  address's shape: one other than `<name>-<6 or 8 hex>` (for example `register --addr myname`) gets none and stays
  anonymous. `agent-bridge doctor` lists the live agents still connecting anonymously, which `enforce` would refuse, and
  the relays and MCP servers older than 0.3.2.

- When `doctor` shows none, switch to `enforce` the same way, with `AGENT_BRIDGE_AUTH=enforce`.

- **To go back**, start the overlay with `AGENT_BRIDGE_AUTH=off`, then plain `docker compose up -d` if you want today's
  broker. Sessions and queues survive the switch, and the data volume's ownership is handled each way. The credential
  files can stay: a broker without auth accepts their logins, and the 0.3.2 services answer them.

**The socket** is `~/.local/state/agent-bridge/broker/mqtt.sock`, readable only by you, so other users and host-network
containers can't use it. Clients use it when it exists and neither `AGENT_BRIDGE_HOST` nor `AGENT_BRIDGE_PORT` is set,
or when `AGENT_BRIDGE_SOCKET` names it.

**What changes for agents.** A message whose sender is bound to its topic is marked `"sender": "bound"`, any other
`"claimed"`. In 0.3.2 senders still use today's topics, so everything reads `claimed` until a later release switches
them, once no reader older than 0.3.2 is left. A post or subscription the role doesn't allow fails with `you may not
post as <addr> on <topic>` or `you may not read <topic>`. A command for an agent whose credentials belong to another
session is refused: `agent-bridge: <addr> is not your agent (bound to another session)`. Under `enforce`, a command with
no credentials to use fails with `this broker requires credentials (auth enforce)`.

**Acting as admin** is always explicit. The console sets `AGENT_BRIDGE_CREDENTIALS` to `admin.json` for itself; nothing
else uses admin's credentials unless that variable names them, so leaving a session never turns a command into admin's.
The one exception: `register` uses `admin.json` to create a new agent's own credentials, and can never reset or widen an
existing one.

**What it doesn't protect against.** Every agent runs as your OS user, so any agent that sets out to can read another's
credentials, `admin.json` included, and act as it: credentials stop mistakes, other users and other containers, not a
determined local agent. With the `docker` or `libvirt` group, any agent is effectively root, which `doctor` warns about.
A bound sender proves who posted, not that a post is safe to follow. The full list is in [docs/AUTH.md](docs/AUTH.md).

## Security model and limits

This is a single-user, single-machine tool: run it only on a machine where you are the only user. The model is that
every process that can reach the broker is trusted not to impersonate, and that is more than your own processes:
127.0.0.1 is reachable by every user on the machine and by any container started with `--network host`, whatever user
it runs as. Any of them can read every DM and room and act as any agent. Containers on Docker's default bridge
network cannot reach it.

- **Loopback only; authentication off by default.** With auth `off` the broker allows anonymous clients, which is only
  acceptable because Compose binds it to 127.0.0.1 and nobody else uses the machine. `warn` and `enforce` add per-agent
  credentials and roles, and the overlay's unix socket keeps other users and host-network containers out (see
  [Authentication](#authentication)). Never publish the broker on another interface: TLS and remote agents are later
  phases in [docs/EXTERNAL.md](docs/EXTERNAL.md).
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
- **Delivery and history.** QoS 1 and at least once, for a client that keeps up. The broker queues at most 1000 messages
  or 32 MiB for a client that isn't reading, whether offline or connected but not draining (an agent that never calls
  `recv`), then drops the rest; the sender still sees its publish confirmed and cannot tell. That includes historyd: the
  history has gaps after a long historyd outage, and it is a convenience record, not an audit trail. If ledgerd crashes
  between commit and ack, the redelivered request gets the first answer replayed instead of running twice, for as
  long as the broker could still redeliver it (its expiry, at least 300 s and at most 65 minutes).
- **Expiry.** An agent idle for 3 days (`AGENT_BRIDGE_STALE_DAYS`) loses its session, presence and, without
  presence, its ledger, unless it set `keep`. Pending and denied ledger requests older than that are pruned; granted
  ones stay.

## Configuration

| Variable | Default | Used by |
|---|---|---|
| `AGENT_BRIDGE_HOST`, `AGENT_BRIDGE_PORT` | `127.0.0.1`, `1883` | every client and service |
| `AGENT_BRIDGE_STALE_DAYS` | `3` | idle expiry, historyd's pruning, ledgerd's stale sweep |
| `AGENT_BRIDGE_LOG` | `$XDG_STATE_HOME/agent-bridge/history.jsonl` | the legacy history log; the other host files (`cli.lock`, `relay/`, `hook-state/`, `admin-audit.jsonl`) live next to it |
| `HISTORY_DB`, `HISTORY_IMPORT` | `$XDG_STATE_HOME/agent-bridge/history.db`, the legacy log | historyd (Compose sets `/data/history.db`; `compose.override.example.yaml` adds the read-only import of a pre-0.2 log) |
| `LEDGER_DB` | `$XDG_STATE_HOME/agent-bridge/ledger.db` | ledgerd (Compose sets `/data/ledger.db` in its volume) |
| `MONITOR_PORT` | `10000` | the admin console |
| `AGENT_BRIDGE_PROMETHEUS_URL`, `AGENT_BRIDGE_GRAFANA_URL` | unset | the console's Claude usage card and Grafana links |
| `CLAUDE_CONFIG_DIR` | `~/.claude` | where the console finds Claude Code's session registry and transcripts |
| `AGENT_BRIDGE_AUTH` | `~/.config/agent-bridge/auth-mode` (written by the auth broker), else `off` | the auth mode, `off`, `warn` or `enforce`; the broker overlay, the services and every client |
| `AGENT_BRIDGE_CREDENTIALS` | unset | a credentials file to log in with: the services' mounted file, and `admin.json` for the console |
| `AGENT_BRIDGE_SOCKET` | `$XDG_STATE_HOME/agent-bridge/broker/mqtt.sock` when it exists and neither `AGENT_BRIDGE_HOST` nor `AGENT_BRIDGE_PORT` is set | the broker's unix socket, used before TCP |
| `AGENT_BRIDGE_RELAY_FOLLOW_CHECK` | `60` | seconds between the relay's idle checks of which agent it should follow |

`XDG_STATE_HOME` defaults to `~/.local/state`. The console's token lives in `$XDG_CONFIG_HOME/agent-bridge`
(default `~/.config/agent-bridge`).

## Development

The package is built by [maturin](https://www.maturin.rs/): the Python sources are in `src/agent_bridge`, the Rust
extension `agent_bridge._native` is built from `rust/crates/py`, and the native `agent-bridge`, `agent-bridge-ledgerd`
and `agent-bridge-historyd` go in as data scripts from `src/agent_bridge.data/scripts`, next to the shims. So a
checkout needs the Rust toolchain, and the binaries are built first. The version lives in `rust/Cargo.toml` alone.

```bash
scripts/build-bins.sh
```

```bash
uv sync
```

`uv sync`, and the next `uv run`, rebuild the extension when a Rust source under `rust/crates/py` or
`rust/crates/core` changes; after changing the CLI or a service, run `scripts/build-bins.sh` again so `.venv/bin`
gets the new binaries. Run live tools (hooks, a user-scope MCP server) from a pinned `uv tool install`, never from
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

The release gate builds the native binaries and the wheel, installs the wheel into a fresh Python 3.12 virtualenv,
checks that it brought the native commands, runs the full suite against the installed wheel three times in a row, then
the Rust tests including the ones that need Docker, and stops at the first failure. The wheel and its virtualenv go in
a temp directory. `GATE_RUNS` and `GATE_PYTHON` change the run count and the Python version. Run it in the foreground:
started as a shell background job (`( ... ) &`) it inherits SIGINT ignored, so Ctrl-C couldn't stop it and every test it
starts would inherit it too, and it refuses to run, saying why (`GATE_ALLOW_BG=1` runs it anyway).

```bash
scripts/gate.sh
```

- **One run at a time.** Each run holds `$XDG_RUNTIME_DIR/agent-bridge-tests-<uid>.lock`. A second run on the
  machine prints `waiting for another agent-bridge test run (pid N) to finish` and starts when the first ends.
- **No leftovers.** Test containers carry the label `agent-bridge-test.uid=<uid>` and the run's temp directory its
  pid (`agent-bridge-test-<pid>-...`); both are removed when the run exits, including on Ctrl-C, SIGTERM and SIGHUP,
  and what a SIGKILLed run left is removed by the next one.
- **Never the live state.** The fixtures refuse any ledger or history database, or history log, outside a temp
  directory. At the start of a run the test process clears the session and auth settings it inherited,
  `CLAUDE_CODE_SESSION_ID`, `CLAUDE_PROJECT_DIR`, `CLAUDE_CODE_MESSAGING_SOCKET`, `CLAUDE_CODE_MESSAGING_TOKEN`,
  `AGENT_BRIDGE_STALE_DAYS`, `LEDGER_DB`, `HISTORY_DB`, `AGENT_BRIDGE_AUTH`, `AGENT_BRIDGE_CREDENTIALS` and
  `AGENT_BRIDGE_SOCKET`, and points `AGENT_BRIDGE_LOG`, `XDG_STATE_HOME` and `XDG_CONFIG_HOME` into its temp directory,
  with `AGENT_BRIDGE_HOST` 127.0.0.1 and a free `AGENT_BRIDGE_PORT`. Every service, console and command it starts
  inherits that, so an exported socket can't reach the live broker and a real `admin.json` is never used.
- **Which commands.** The tests run the commands installed next to the Python that runs them (`.venv/bin` after
  `uv sync`, the gate's virtualenv for the gate). `AGENT_BRIDGE_BIN` points them at another directory of
  `agent-bridge*` commands, and `AGENT_BRIDGE_LEDGERD` and `AGENT_BRIDGE_HISTORYD` at other service command lines.
- **Conformance.** `tests/test_conformance.py` checks `agent_bridge._native` through its public API and the wire
  only: no call stalls behind a blocked one, concurrent joins keep every room, a closed Bus never takes the session
  back from its next owner, ledger replies never fill a Bus's receive window, leave drops the ledger (also with
  ledgerd down), `wait_arrival` wakes once per new arrival, forged history requests are refused, and wrong-typed input
  to every entry point, or text with a lone surrogate, is a one-line `BridgeError`. The messages are the ones the
  Python implementation gave, which these tests ran against side by side until 0.2.0. The run ends with a "bus
  behaviours" summary of the named behaviours that cutover kept.
- **Golden replies.** What the Python services and CLI answered at v0.1.0 is frozen in `tests/golden/` and
  `rust/crates/cli/tests/golden/`; the Rust services and CLI are checked against it on every run. A deliberate change
  of a reply is a reviewed edit of the golden file.

| Module | Covers |
|---|---|
| `tests/test_bus.py` | The CLI and the native Bus: delivery, rooms, takeover, stable addresses, a broker restart, the presence cache (read through `cached_agent`, after a marker DM proves the Bus caught up), expiry, hostile payloads and presence, subagents and write-once origin (no process without a session), the admin reservation, broadcasts, the message rules (empty text, DMs to yourself, unresolved @words sent and reported), 3000 retained records arriving before the cache barrier, bad arguments; message ids identical on every path (including a 0.3.0 sender) and replies, and messages on the sender-bound topics (`inbox/<to>/<from>`, `room/<room>/<from>`) read, normalised and marked `"sender": "bound"` |
| `tests/test_rooms.py` | Room moderation: open and announce rooms, mute, kick and ban, non-moderators refused, admin override, spoofed posts dropped, announcements, the CLI and MCP surfaces; pinned messages (who may set them, `room info`, the Observer, the notes) |
| `tests/test_mentions.py` | @mentions: resolution when sending (case-insensitive; an unresolved @word is sent as text and reported), validation when receiving, `for_me` in `recv`; `@admin` (reaching the console with no membership), `@me`, and agents who are absent from the room |
| `tests/test_history.py` | historyd end to end: recording and queries (from raw publishes), redelivered messages stored once, drop of one's own inbox only, the one-time import, the CLI and Bus reading through it, restart persistence, health, arguments that start nothing, the reserved level. Pruning, the row and byte caps and the schema refusal are Rust unit tests in `rust/crates/historyd` |
| `tests/test_history_takeover.py` | A second historyd on one broker takes the session over and the first exits instead of taking it back (a module of its own, since every historyd shares the client id) |
| `tests/test_history_golden.py` | A fixed corpus of messages and requests replayed against `AGENT_BRIDGE_HISTORYD` gets the Python reference's replies byte for byte (`tests/golden/historyd.json`, generated at cp8 and frozen with its corpus); the documented divergences (NaN echo, nesting depth, integers over 4300 digits) carry the Rust reply as an override |
| `tests/test_history_parity.py` | The rollback image's Python historyd (`agent-bridge-historyd:1`, v0.1.0; `AGENT_BRIDGE_ROLLBACK_HISTORYD` names another) and `AGENT_BRIDGE_HISTORYD` taking turns on one database: nothing stored twice, every reply (queries, refusals, truncation, CorrelationData and ResponseTopic handling) byte for byte the same; skipped without the image, so it runs in CI's rollback job |
| `tests/test_ledger_takeover.py` | A second ledgerd on one broker takes the session over and the first exits instead of taking it back (a module of its own, since every ledgerd shares the client id) |
| `tests/test_ledger_golden.py` | A fixed script of 115 raw ledger requests (every op, every refusal text, busy and FIFO, fencing, redelivery replay, request topics, hostile input, dump truncation, drop) replayed against `AGENT_BRIDGE_LEDGERD`; the normalised replies (ids as R1/L1, timestamps as `<t>`, tokens from 1) must equal those v0.1.0's Python ledgerd gave, recorded in `tests/golden/ledgerd.jsonl`, apart from the Known differences and the deliberate changes since, which its docstring names; a deliberate change is a reviewed edit of that file |
| `tests/test_ledger_parity.py` | ledgerd and its rollbacks. With the Python ledgerd in `agent-bridge-ledgerd:2` built from v0.1.0, handing one `ledger.db` back and forth: the state reads the same both ways, a redelivered request is replayed from the other's stored answer, a waiter queued under one is served by the other, fencing tokens keep growing, one schema row; and an interrupted command-line acquire against it, which refuses `cancel` as an unknown op, still ends quietly by its signal. With both that image and `agent-bridge-ledgerd:0.2.3`: a database holding 0.3.0's lease history opens and serves unchanged, the client names the missing `leases` op, and the history is intact afterwards; a claim survives the round trip, and the client names the version it needs. Each case is skipped when its image is not built (CI's rollback job builds both); the lease-history case also hands a worktree and its spec through each rollback (`no worktrees op`, `no worktree locks`, the worktree intact afterwards) |
| `tests/test_ledger.py` | Ledger semantics and their DMs, a ledgerd kill mid-flight, hostile requests and floods, per-agent request topics and redelivery replay, the replay window, quotas including the per-owner pending cap, waiters whose client left (a timeout shorter than the wait, SIGINT and SIGTERM cancelling a queued command-line acquire, a lease granted while the cancel is on its way), busy refusals naming the holders, `leases`, the lease history with every kind of event and its bounds, the expiry DM, `ping`, claims (holder, moderator and admin release, ttl, leaving) and `holders`, a 10-process lock stress test, the admin dump, the stale sweep, the schema guard, the command line and `agent-bridge ledger-raw`; worktree locks (made on demand, a path mismatch, busy naming the holder, `worktrees` with and without `all`, `holders` lease ids, admin breaking one, `wt-` names refused to `add`); and the auth modes (`warn` logs plain requests and `dump`, `enforce` refuses them and one-level or foreign reply topics, an agent with credentials is answered and one without is not, `--health` passes, ledgerd without its credentials exits); and random behaviours of the ledger spec replayed against ledgerd (replies, redelivery, holders, fencing, waiters, restarts) |
| `tests/test_mcp.py` | The MCP server over stdio: identity, messaging, ledger tools, bad input, presence origin, broadcasts, the channel notice, arguments refused before anything connects; replies by one id across `recv`, `history` and `dms`, absent mentions, pins, claims, `holders` and wake filters; the worktree tools (acquire, status, a second agent refused naming the holder, release and the unlock) |
| `tests/test_host.py` | The agent-framework adapter: the generic host, Claude Code's session and process detection, the inbox socket, a fake session registry (the `.key` files never opened), transcripts, kind detection, and the native identity calls (project names, session addresses, `/proc` reads) |
| `tests/test_relay.py` | The relay against a fake inbox socket and a fake `claude` process: coexistence with the MCP server, one channel notice per arrival across the relay taking the session and handing it back, adopting a CLI-registered agent, mentions waking only the mentioned agent |
| `tests/test_relay_wake.py` | The relay's wake filters through `_native` as the MCP server uses them, with no broker: `RELAY_WAKE_MODES`, setting a room's mode, `all` clearing it, reading them back, refusing an unknown mode, and the session's `.wake` file under the state directory |
| `tests/test_refused.py` | A subscription the broker's ACL refuses is reported, never silently empty: `watch` exits non-zero with `subscription refused: <topic>`, and the console's Observer says it is degraded. A disposable mosquitto of its own with dynamic security, which lets anonymous clients read everything but one room, `#` and `$SYS` (an `acl_file` would grant every SUBACK and only filter delivery); a history call refused by the broker names the refused reply topic at once instead of timing out |
| `tests/test_guard.py` | The worktree guard end to end, through the real CLI on a scratch repo and linked worktree with two sessions: the holder works; another session is refused with the exact text (Edit, git commit, rm, make, a cd plus redirect); reads pass; `core.hooksPath` overrides are refused; with ledgerd unreachable the last-known and git locks are still enforced, with the note; bad input exits 2; with a lock in one repository, work in an unrelated repository or directory (`rm $X`, `rm *.tmp`, a `core.hooksPath` override, `cd "$OUT" && make`) is not refused, while the same commands in the locked repository are, and a glob naming the locked worktree is refused from anywhere |
| `tests/test_console.py` | The console with a fake HOME: Host, Origin and token checks, messaging, friendly names, death detection, signals, restarts, ledger actions, the room kill switch and targeted stop, room settings, admin mentions, capacity, leftovers, Claude usage, `/metrics`, headless mode and the admin CLI, audit filters, the broadcast hook, and that the console and the MCP server import no paho and none of the Python bus, CLI, relay, hooks, ledger or history modules; auto-link rules (one regex subset in Python and the browser's engine, hostile words, refusing any pattern a crafted word could make slow, with a 5 s runaway-only timing backstop in node), the Claims view and admin release without an admin ledger, pins, replies and `@admin`; the Worktrees view's parts (worktree root and common dir, lock parsing, the resource id, guard detection), Protect and Unprotect with chaining and `core.hooksPath`, and a leased worktree refusing another session's commit through a protected repo's ref guard; the auth mode `off` in state, status and metrics |
| `tests/test_conformance.py` | The `_native` contract listed above, with the bus behaviours summary |
| `tests/test_harness.py` | The test harness and release tooling: a killed run's temp directory is swept by the next run, and nothing else is; `scripts/release-notes.sh` gives a tag exactly its CHANGELOG section and refuses a tag without one; `scripts/check-dist.sh` lets only one wheel per architecture and the sdist of the tag's version be published; every command's `--version` and `-V` print `<command> <version>` with an empty environment; a user's exported auth setup and real config directory never reach a test; the CI reruns (a test green on its rerun passes and is logged as a FLAKE, one red twice fails, a failure with nothing to rerun or a crashed test binary fails at once, `cargo test` runs with `--no-fail-fast`); the release gate refusing to run with SIGINT ignored unless `GATE_ALLOW_BG=1`; which formal-methods jobs each changed path runs; and `tla2tools.jar` refused unless its sha256 is the pinned one |
| `tests/test_docs_contract.py` | The docs as a contract with the code: every CLI subcommand `--help` lists is named in `docs/architecture.md`, the README's MCP tool count matches `mcp_server.py`, every refusal the docs quote (`blocked: ...`, `you may not ...`, `agent-bridge: ...`) occurs in the sources, internal links and anchors resolve, and every test file has a row here |
| `tests/test_timing_guard.py` | No test asserts on time, or on "has it happened yet" after a fixed sleep: a sleep followed directly by an assert is refused (waits go through `tests/waits.py`), and so is an upper bound on elapsed time without a `# ceiling:` comment saying which failure it still catches; the checker is shown to bite on samples of both |
| `tests/test_auth_matrix.py` | Auth as a table, against a disposable broker started through `config/broker-entrypoint.sh` and a ledgerd run as Compose runs it: every entry point (ledger calls, `ledger-raw`, `say`, the relay, the MCP server, the console, `ledgerd --health`) for every identity (admin through `AGENT_BRIDGE_CREDENTIALS`, admin with only `admin.json` on disk, the session's own agent, another session's, an anonymous address) in `off`, then `warn`, then `enforce` on one volume, each cell as [docs/AUTH.md](docs/AUTH.md) says; reports every differing cell at once. Needs Docker; and a ledger call or a ledgerd subscription the broker refuses is an error naming the topic, not a timeout or silence |

CI (`.github/workflows/ci.yml`, GitHub Actions) runs ruff; the Rust formatter, clippy and tests, including the ones that
need Docker; builds manylinux wheels for x86_64 and aarch64, with the native binaries built inside the manylinux
container, and the sdist. It checks each wheel's binaries (the wheel's architecture, nothing newer than glibc 2.28),
then the installed wheel's commands (all but the hooks must answer `--help` without starting anything, each prints
`<command> <version>` for `--version`, and the hook shims stay silent), imports, packaged files, and that no deleted
module came back. It runs the suite against the installed wheel, builds the Rust ledgerd and historyd images and runs
each service's `--help` in them without a network, and builds the rollback images (v0.1.0's Python services and v0.2.3's
Rust ledgerd) to run the parity tests that hand one database between each service and its rollbacks (a skipped parity
test fails that job). `.github/workflows/release.yml` turns a `v*` tag into a GitHub Release with the wheels and the
sdist, plus `THIRD_PARTY_LICENSES.md`, its notes being the tag's section of `CHANGELOG.md` (`scripts/release-notes.sh`;
a tag without a section fails before anything is built). It can publish to PyPI once that is set up (see
[Roadmap](#roadmap)); before the release and before the upload, `scripts/check-dist.sh` refuses anything in `dist` but
one wheel per architecture and the sdist, of the tag's version.

**Flakes never turn main red, and never go unseen.** When the Python suite or a Rust test fails in CI, only the failed
tests are rerun, once (`scripts/rerun-pytest.sh` with `pytest --lf`, `scripts/rerun-cargo.sh` by exact test name, with
`--no-fail-fast` so one failing binary can't hide another's results). Green on the rerun passes the job but writes
`FLAKE <test> run <run id>` to the job summary and uploads `flakes.txt` as an artifact; red on the rerun fails it. A
failure with nothing to rerun, such as a collection error or a test binary that crashed, fails at once. The release gate
doesn't rerun: it is the strict check.

**Formal checks** (`.github/workflows/verify.yml`) run the TLA+ models with TLC, a fuzz smoke run of 60 s per target,
and Kani proofs, each only when its spec, its script or the code it checks changed (a plain `git diff` against the
parent, mapped by `scripts/verify-paths.sh`), and all of them every Monday and on demand, so a path filter can't hide
rot. Each job runs `scripts/verify.sh <job>`, the same command as locally, which runs the job's own
`scripts/verify-<job>.sh`; TLC is the pinned `tla2tools.jar` v1.7.4, checked by sha256 (`scripts/tla2tools.sh`). The
ledger model (`specs/ledger/`) checks ledgerd's lease protocol: at most one write lease, fencing tokens that only grow,
each request applied at most once across redelivery and restart, and leases only to a listener. Two of its
configurations are the 0.2.1 bugs and must fail, and its untimed behaviours are replayed against the real ledgerd
(expiry, wait deadlines and the dedupe window are checked by TLC only, since ledgerd runs on the wall clock); a run
takes about 1 to 4 minutes. The worktree-lock model (`specs/worktree/`) has five configurations: the fixed lock order
passes, today's order fails as it should, and the rest check that the model can reach its states. The relay-delivery
model (`specs/relay/`) checks that every accepted message reaches the session's inbox or is still queued, and is acked
only after the inbox write, across re-exec, a CLI takeover and take-back, a broker restart and the session's end; its
two other configurations must fail. It found one known limit: a quiet message held back by `relay wake` is acked into
the relay's memory before any inbox write, so it survives a relay self-restart but not the session ending or a relay
crash (history still has it). The Kani proofs (`scripts/verify-kani.sh`) verify two harnesses: the credentials rule's
decision (admin only when `AGENT_BRIDGE_CREDENTIALS` names admin) and the address shape for every digest. Three more,
for message ids, topics and lexical paths, are written and bounded but pending byte-level seams in the code, so they run
only when named. The fuzz targets (`rust/fuzz/`) are the guard's shell lexer and its quoting equivalence, the guard's
paths, topics, JSON envelopes and ledgerd's request decode.

## Rust implementation

`rust/` is a Cargo workspace, and everything that touches the bus is built from it: the CLI and the session relay,
ledgerd, historyd, and `agent_bridge._native`, the extension the console and the MCP server run on. It replaced the
Python implementation of v0.1.0 and was held to it: same wire protocol, lock files and SQLite schema, and the same
output and errors, pinned by golden files recorded from that implementation (`rust/crates/cli/tests/golden/cli.txt` for
the CLI, `tests/golden/` for the two services).

| Crate | Builds |
|---|---|
| `crates/core` | The library: validation, protocol, identity, an MQTT 5 session layer on rumqttc's codec, presence, rooms and moderation, mentions, the Bus, the console's Observer, the relay's lock file, one request/response helper with the ledgerd and historyd clients on it, and `host` with the generic host and the `claude` adapter (session, process, inbox socket) |
| `crates/cli` | `agent-bridge`: every CLI command, including `relay start\|stop\|status\|run`, the Claude Code hooks as `hook broadcast` and `hook relay-start`, and `ledger-raw`, one raw ledgerd request (what `agent-bridge-ledger` runs) |
| `crates/ledgerd` | `agent-bridge-ledgerd` |
| `crates/historyd` | `agent-bridge-historyd` |
| `crates/py` | `agent_bridge._native`, the core as a Python extension (PyO3, abi3 for Python 3.12+), built by maturin with the package: the Bus (with `wait_arrival` for arrival notices), the Observer, ledgerd and historyd calls, presence, rooms, relay and identity lookups, the validators and limits. It raises the exceptions of `agent_bridge.errors`, and bad input of any type, a lone surrogate included, gets a one-line `BridgeError`, never a `TypeError` or `UnicodeEncodeError` |

The Claude Code hooks are `agent-bridge hook broadcast` and `agent-bridge hook relay-start`, a 2 ms binary on every
prompt; whatever goes wrong they print nothing and exit 0. The old names `agent-bridge-broadcast-hook` and
`agent-bridge-relay-hook` are small sh shims that run them, so existing hook settings keep working, and
`agent-bridge-ledger` is the same kind of shim for `agent-bridge ledger-raw`.

The wheel carries the binaries: maturin installs everything in `src/agent_bridge.data/scripts` into the environment's
`bin` as it is, so `agent-bridge`, `agent-bridge-ledgerd` and `agent-bridge-historyd` land there next to the shims and
the console's and MCP server's Python entry points. Release builds compile them inside the manylinux container, so they
match the wheel's glibc. From a checkout, `scripts/build-bins.sh` builds them into that directory; an sdist or source
build without that step installs the package with no CLI or services.

One Bus serves any number of threads. A call that waits (`recv`, a ledger `acquire` with `wait`, a broker round
trip) holds no lock while it waits, and in Python it releases the GIL, so a thread blocked in `recv` never stalls
another's `say`. Calls that rewrite the agent's presence (sub, unsub, join, part, keep, describe) take turns, so
concurrent joins never lose a room. Ledger and history replies come back on `ledger/resp/<address>` and
`history/resp/<address>` and are acked as they arrive, never buffered with the agent's messages.

Build with a Rust toolchain of 1.85 or newer (edition 2024); from a checkout, the binaries go into the package first,
then `uv sync` installs them with the extension:

```bash
scripts/build-bins.sh
```

```bash
uv sync
```

ledgerd ships as a 4.8 MB image, a static binary on `scratch` (`rust/Dockerfile`, target `ledgerd`), which
`compose.yaml` runs with the healthcheck `/agent-bridge-ledgerd --health`, a `dump` round trip through the broker that
exits 0 on an answer within 5 s. It answers every request as v0.1.0's Python ledgerd did, but for the known differences
below and the deliberate changes since (`tests/test_ledger_golden.py`, against that ledgerd's recorded replies), and the
two hand one `ledger.db` back and forth (`tests/test_ledger_parity.py`, run in CI against the v0.1.0 image). To roll
back to the Python ledgerd, build its image from the v0.1.0 tag (`docker build -t agent-bridge-ledgerd:2 .` in a
checkout of `v0.1.0`), set the service to `image: agent-bridge-ledgerd:2` with no `build`, `command:
["agent-bridge-ledgerd"]` and the healthcheck `["CMD",
"agent-bridge-ledgerd", "--health"]`, then run `docker compose up -d ledgerd`. Both open the same `ledger.db` in the
`ledger` volume. Both write the database's `schema` row (1) and refuse, before writing anything, a database whose schema
is another version, so the rollback ends at the first migration. The previous release's ledgerd is a rollback the same
way: build `agent-bridge-ledgerd:0.2.3` from the v0.2.3 tag (`docker build --target ledgerd -t
agent-bridge-ledgerd:0.2.3 rust` in a checkout of `v0.2.3`) and set the service's `image` to it, with no `build`;
`tests/test_ledger_parity.py` runs against it too. Both rollbacks open a 0.3.0 database unchanged: the lease history
is a table of its own, which they leave alone, and nothing is recorded in it while they run. ledgerd starts the
service only when given no arguments: `-h`/`--help` prints the usage, `-V`/`--version` the version, `--health` checks
the running one, and anything else exits 2 before connecting, so a stray flag can never start a second ledgerd that
takes the live one's session over.

historyd keeps v0.1.0's schema and row digests, so its Python implementation can take over the same `history.db`: a
message either one stored is not stored again by the other, and a request gets the same reply from both, but for the
known differences below (`tests/test_history_parity.py`, run in CI against the v0.1.0 image). It takes `--health`, a
`ping` round trip through the broker, and starts the service only when given no arguments at all. It ships like ledgerd,
a 4.7 MB image (`rust/Dockerfile`, target `historyd`) that `compose.yaml` runs, with the healthcheck
`/agent-bridge-historyd --health`. To roll back to the Python historyd, build its image from the v0.1.0 tag (`docker
build -t agent-bridge-historyd:1 .` in a checkout of `v0.1.0`), set the service to `image: agent-bridge-historyd:1` with
no `build`, `command: ["agent-bridge-historyd"]` and the healthcheck `["CMD", "agent-bridge-historyd", "--health"]`,
then run `docker compose up -d historyd`. Both open the same `history.db` in the `history` volume, and with
`compose.override.yaml` both import the same `/import/history.jsonl`, so nothing is imported twice. Both write the
database's `schema` row (1) and refuse, before writing anything, a database whose schema is another version, so the
rollback ends at the first migration.

Compose runs both services with `init: true`. As PID 1 on an empty image they would ignore SIGTERM, so every stop
would wait 10 s for the kill; docker-init forwards it and they stop at once, which is safe at any point: every request
and message is one SQLite transaction, acked to the broker only after it commits.

The Rust tests, then the ones that need Docker (the services' arguments, health and takeover, the CLI replaying its
golden transcript on a disposable broker, a relay ending with its `claude` while the broker is down, worktree locks
from the shell and the relay, a main-worktree lease still enforced with ledgerd down, and the auth modes on a
broker started through the entrypoint, historyd serving under `enforce` included):

```bash
cargo test --manifest-path rust/Cargo.toml
```

```bash
cargo test --manifest-path rust/Cargo.toml --workspace -- --ignored
```

Measured on one machine against the Python implementation of v0.1.0 (median of 20 calls, the Python CLI from its
virtualenv):

| | Python | Rust |
|---|---|---|
| `agents`, `dm`, `recv` on an empty queue | 68-69 ms | 2 ms |
| relay memory (RSS) | 23 MB | 4 MB |
| ledgerd memory (RSS) | 26 MB | 5 MB |
| ledgerd image | 91 MB | 4.8 MB |
| historyd image | 91 MB | 4.7 MB |

Known differences from v0.1.0's Python services, which matter only for the rollback images above, are all in requests
both refuse, and the golden files pin them: a request nested deeper than 128 levels gets `payload must be UTF-8 JSON`
from Rust where Python parses it and refuses it by its fields; an integer longer than 4300 digits is the other way round
(Python's integer limit refuses the whole request, Rust names the first bad field); and an error that echoes a NaN or an
infinity shows it as `{}` where Python shows `nan` or `inf`. The ledgerd changes since, none of which touch the
database's layout, go away with a rollback: the `cancel` op (the rollback refuses it as an unknown op, so an interrupted
command-line acquire stays queued until its deadline), the waiter cutoff at the request's expiry, the per-owner pending
cap, the refusal of a lease you already hold, the reply window beyond 300 s, and the reworded refusals and DMs; since
0.3.0, the `leases`, `lease_history` and `ping` ops (a client is told the ledgerd is older than 0.3.0), the lease
history and the expiry DM; and since 0.3.1, the `claim`, `unclaim`, `claims` and `holders` ops (claims made before a
rollback are kept and come back with 0.3.1); and since 0.3.2, the `worktrees` op and `acquire`'s `path` and
`git_common_dir` (a client is told the ledgerd is older than 0.3.2 and has no worktrees op, or no worktree locks),
`on_behalf` and the auth checks. Worktree specs are kept and come back with 0.3.2, but a rollback's stale sweep can drop
admin's ledger, with its worktrees and any live leases on them, once admin has had no presence for the stale period, so
keep a rollback short.

## Roadmap

What exists is described above; this is what is planned and not built yet.

- **PyPI.** Releases are on GitHub: a `v*` tag builds manylinux wheels for x86_64 and aarch64 and attaches them,
  with the sdist, to a [GitHub Release](https://github.com/PrithviTM-glitch/Agent-Networking/releases)
  (v0.1.0 was the first). Publishing to PyPI is wired but off until the PyPI project and its trusted publisher are set
  up.
- **Retiring the rollback images.** The v0.1.0 Python ledgerd and historyd images stay valid rollbacks until the first
  schema change; that migration retires them, together with the parity tests and CI's rollback job.
- **The auth phase** ([docs/EXTERNAL.md](docs/EXTERNAL.md)): broker credentials and per-agent ACLs on the request,
  reply and inbox topics, an explicit admin path for the console's ledger actions, and only then agents beyond one
  machine.

## Contributing

Issues and pull requests are welcome. Keep changes small and in the style of the code around them: the standard
library first, no speculative abstractions, and every input from another process treated as hostile. Add or extend
an end-to-end test for any behaviour you change, add its entry under `## [Unreleased]` in
[CHANGELOG.md](CHANGELOG.md) in the same change, and run `ruff check`, `ruff format`, `cargo fmt`, `cargo clippy` and
the test suites before opening a pull request.

## License

MIT, see [LICENSE](LICENSE).
