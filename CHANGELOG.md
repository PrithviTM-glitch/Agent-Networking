# Changelog

All notable changes to agent-bridge. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html): while the major version is 0, a minor
release may break interfaces and a patch release does not.

Each change lands with its entry under Unreleased. A release renames that section to its version, and the release
workflow publishes the section as the GitHub release notes.

## [Unreleased]

## [0.3.0] - 2026-11-12

### Added
- `--version` (and `-V`) on every command, native and Python alike: it prints `<command> <version>` and exits 0
  without connecting or reading any settings. Python code reads the same version as `agent_bridge.__version__`.
- `agent-bridge doctor [--json]` checks this machine's bridge, read-only: the broker, ledgerd and historyd (and their
  versions), the state and config directories (ownership and modes), `cli.lock`, version skew between the CLI, the
  services and the installed wheel, relays still running a replaced binary or outliving their session, and whether each
  agent-bridge hook in `~/.claude/settings.json` runs an existing program. Each check is OK, WARN or FAIL with what to
  do, and any FAIL exits 1.
- `agent-bridge relay restart` stops this session's relay and starts it again on the installed binary, for after an
  upgrade.
- `relay status` (and `stop`) also show the relay's `pid`, `started`, `uptime_s`, `binary` and `version`; the binary
  ends in ` (deleted)` when an upgrade replaced it. The relay lock file gains the version as its third line.
- `agent-bridge watch ROOM [--last N] [--text]` follows a room from the terminal: its last messages from history,
  then each new one live, until Ctrl-C. It needs no agent or membership, and it never takes over a session.
- `--text` on `recv`, `history`, `dms` and `watch` prints one readable line per message (`14:02:31 #ops alice-1a2b3c:
  hello`) instead of JSON. A body's carriage returns and bidi or zero-width marks are shown escaped.
- History paging: `history ROOM --before SEQ` and `dms MY_ADDR --before SEQ` page back past the newest 1000. Each entry
  shown by these commands carries its `seq`. historyd returns `seq` only when a request asks for it, so 0.2.x clients
  get the same replies as before.
- `say` and `dm` read the text from stdin when it is `-`, or from a file with `--file PATH`; the one newline that ends
  the input is dropped, so `echo` and heredocs send what you typed.
- `describe` sets a one-line `about` (up to 200 bytes; `""` removes it) next to the name and caps. Like the MCP
  server's `mcp`, it is an optional presence field: a 0.2.x client that rewrites the record drops both until they are
  set again.
- `agent-bridge acquire ... --exec CMD [ARGS...]` runs a command under a lease: the fencing token and lease are in its
  environment (`AGENT_BRIDGE_FENCE_TOKEN`, `AGENT_BRIDGE_LEASE_ID`, `_RESOURCE`, `_LEVEL`), the lease is renewed while
  it runs and released when it exits, signals sent to agent-bridge reach it, and the exit status is the command's,
  or 75 when it never got the lease or lost it while running (it is then stopped).
- `agent-bridge leases` lists the leases you hold.
- `agent-bridge request ... --wait [S]` waits for the owner's answer.
- Lease history: ledgerd keeps the last 200 lease events of each resource (acquire, renew, release, break, revoke,
  expire, drop, with who did it), for its owner (`agent-bridge lease-history`, the `lease_history` op). The database
  stays schema 1, so both rollback ledgerds still open it.
- A lease that expires without renewal gets its holder a DM from ledgerd.
- ledgerd answers a `ping` op with its version, for `agent-bridge doctor`; a client asking an older ledgerd for one of
  the new ops gets "ledgerd is older than 0.3.0".
- historyd: `room`, `dms`, `topic` and `broadcasts` take `before_seq` (and `seq: true`) to page in storage order, and
  `{"op": "ping", "version": true}` answers with its version. Without them its replies are unchanged.
- MCP tools `leases` (the leases you hold) and `lease_history` (a resource's lease events, owner only; a bare name is
  one of yours, as in the CLI), and `request` takes `wait` to block up to an hour for the owner's answer. A request
  whose resource is removed while it waits ends at once with status `gone`.
- MCP tool `describe` (name, caps and `about` in place), and MCP `history` and `dms` entries carry their `seq`, with
  `before` to page back. The MCP server's presence record carries its version as `mcp`, which the console shows per
  session (unknown for a server from before 0.3.0).
- The console's views update themselves: the ledger, capacity, leftovers and agent views refresh on their own while
  shown and the tab is visible, show when they last updated, and back off (up to a minute) with the reason when a
  load fails. The Refresh buttons are gone.
- The console shows its own version in the page header, in `agent-bridge-admin status` and in `/metrics` as
  `agent_bridge_console_build_info{version, native}`.

### Changed
- `history` and `dms`, in the CLI and the MCP server, ask historyd for each entry's `seq`, so they need historyd 0.3.0:
  upgrade the services before the commands (README, Upgrading).
- CLI JSON output writes text as UTF-8 instead of `\uXXXX` escapes, so non-ASCII text is readable. Control
  characters and bidi or zero-width marks stay escaped, and the wire format is unchanged.

### Fixed
- A Bus whose session was taken over (by a CLI command acting as the agent, for example) could keep an agent that had
  left in its presence cache until its next reconnect, so mentions and DMs still resolved to it. On reconnect it now
  ignores the presence and room records that the broker had queued for the old connection, and rebuilds both caches
  from the fresh replay.
- `agent-bridge --help` and `--version` work whatever the environment holds; a bad `AGENT_BRIDGE_PORT` no longer
  stops them.
- The MCP server posts as the session's own agent (the one its relay delivers for) instead of a second derived
  address that could not receive replies. A running server picks this up when restarted.
- Console message tables no longer split short tokens mid-word in a narrow column (`!1432` showed as `!14` over
  `32`): table cells wrap only between words, and cells of up to 12 characters never wrap.

### Documentation
- The README gains an Everyday commands section and `doctor` in the Quick start, and Upgrading now updates the
  services before the commands and rolls both back together.

## [0.2.3] - 2026-11-04

### Added
- The console renders Markdown tables and fenced code blocks in messages: a pipe table (a header row, then a
  `|---|` separator) becomes a real table, for tables up to 50 columns and 500 rows, and a ``` block becomes
  monospace, in rooms, DMs and broadcasts, at most 200 blocks per message. Both are built from text only, so a
  message cannot inject markup, and anything malformed or over the caps stays plain text.

### Fixed
- Releases publish only the wheels and the sdist, plus `THIRD_PARTY_LICENSES.md` on the GitHub release by name.
  Every artifact of the run used to land in the upload directory (the licenses since 0.1.0, the release notes in
  0.2.2), which PyPI would have refused; `scripts/check-dist.sh` now guards the GitHub release and the PyPI upload.

## [0.2.2] - 2026-11-02

### Added
- `CHANGELOG.md`; the release workflow takes each version's section as its GitHub release notes and refuses a tag
  without one, before anything is built (`scripts/release-notes.sh`).

### Changed
- While a session's relay is live, the CLI's `say`, `dm` and `pub` post over a connection of their own instead of
  taking the relay's session over, so posting from the shell no longer makes the relay reconnect.
- Every `relay.log` line names its relay: its address, or its session before it has one.

### Fixed
- The relay no longer logs `broker unreachable` when a CLI command for its address briefly takes its session over.
- `recv` (CLI, Bus, MCP) no longer returns the agent's own posts made over another of its connections (a
  publish-only Bus, such as the MCP server's or the CLI's while a relay is live). They are dropped when they arrive,
  so they never count as pending or wake `wait_arrival`.
- `agent-bridge relay --help` says that `relay stop` prints the relay's status after stopping it.

### Documentation
- EXTERNAL.md's auth phase separates an agent's own takeovers of its session, which client-id pinning allows, from
  another identity's (kick, the console's stop and wipe), and requires a way for helper connections with their own
  client ids (publish-only Buses, one-shot ledger and history calls, the console's Observer) to connect.
- The README's Upgrading section points at the changelog and explains that `relay stop` prints `"live": false`.

## [0.2.1] - 2026-10-30

Fixes from a QA round on the live 0.2.0 bus.

### Fixed
- Fresh-machine Quick start: `docker compose up` no longer makes `~/.local/state/agent-bridge` root-owned. The legacy
  history import mount moved to `compose.override.example.yaml`, and the CLI names the directory and the fix when it
  cannot write there.
- A ledger waiter whose client is gone no longer receives the lease. A waiter's deadline is capped by its request's
  expiry, and the new `cancel` op, sent by the CLI on SIGINT or SIGTERM, drops a queued waiter.
- ledgerd keeps each reply for its request's whole lifetime, so a late retry is answered, not run twice.
- At most 256 pending access requests per owner.
- Empty or whitespace-only text is refused for `say`, `dm` and broadcasts. A DM to yourself is refused.
- The console tightens `~/.config/agent-bridge` to 0700 on every start, and the tokenless `/metrics` no longer
  carries session names (they need the admin token).
- An agent registered without a session no longer records, or is shown as owning, a Claude process.
- With the broker down, a relay exits within a second of its session ending.
- Tests no longer leak service processes or temp directories, including after a failure.

### Changed
- An `@word` that matches no room member is sent as plain text and reported (a CLI notice, an MCP result field), and is
  no longer refused. Mention matching ignores case.
- Ledger messages:
  - a busy lease names its holders;
  - re-acquiring your own lease says so;
  - revocations say "revoked", and a broken lease DMs its holder;
  - the owner's request DM shows the current commands.
- Clearer errors: "unknown recipient", "X is not registered", the same `--health` wording for both services, and
  `hook -h` prints help.
- The console shows mosquitto's dropped-message count, and admin `audit` takes command names.

### Documentation
- The trust model: 127.0.0.1 is reachable by every local user and by host-network containers.
- Delivery limits: a connected agent that doesn't read its messages loses the excess silently.
- An Upgrading section, and the auth-phase caveats in EXTERNAL.md.

## [0.2.0] - 2026-10-29

### Removed
- The Python implementations of the bus, CLI, relay, ledger and history services. Python keeps the console, the MCP
  server and the Claude host adapter, all on the Rust core.

### Changed
- The wheel ships the native `agent-bridge`, `agent-bridge-ledgerd` and `agent-bridge-historyd`, with shims so
  `agent-bridge-relay-hook`, `agent-bridge-broadcast-hook` and `agent-bridge-ledger` keep working. No Rust toolchain
  is needed to install.

### Added
- Golden replies and a CLI transcript recorded from the Python reference, so the Rust code is still checked against
  it.
- A CI job that hands a database between the Rust services and the v0.1.0 Python images.

## [0.1.0] - 2026-10-28

First release.

### Added
- A local MQTT 5 message bus for Claude Code sessions: agents, rooms, DMs, raw topics, broadcasts, @mentions,
  moderation and a room kill switch.
- ledgerd: a resource ledger with leases, FIFO waiters and fencing tokens.
- historyd: a message history service.
- An MCP server, and a session relay that delivers bus messages into Claude Code.
- An admin console (web UI, `/metrics`, headless mode, `agent-bridge-admin`).
- A Rust core with Python bindings, and Rust ports of the CLI, relay, ledgerd and historyd, kept in parity with the
  Python reference by conformance tests.

[Unreleased]: https://github.com/PrithviTM-glitch/Agent-Networking/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/PrithviTM-glitch/Agent-Networking/compare/v0.2.3...v0.3.0
[0.2.3]: https://github.com/PrithviTM-glitch/Agent-Networking/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/PrithviTM-glitch/Agent-Networking/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/PrithviTM-glitch/Agent-Networking/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/PrithviTM-glitch/Agent-Networking/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/PrithviTM-glitch/Agent-Networking/releases/tag/v0.1.0
