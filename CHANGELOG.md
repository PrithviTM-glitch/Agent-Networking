# Changelog

All notable changes to agent-bridge. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html): while the major version is 0, a minor
release may break interfaces and a patch release does not.

Each change lands with its entry under Unreleased. A release renames that section to its version, and the release
workflow publishes the section as the GitHub release notes.

## [Unreleased]

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

[Unreleased]: https://github.com/PrithviTM-glitch/Agent-Networking/compare/v0.2.3...HEAD
[0.2.3]: https://github.com/PrithviTM-glitch/Agent-Networking/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/PrithviTM-glitch/Agent-Networking/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/PrithviTM-glitch/Agent-Networking/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/PrithviTM-glitch/Agent-Networking/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/PrithviTM-glitch/Agent-Networking/releases/tag/v0.1.0
