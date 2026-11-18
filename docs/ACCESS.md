# Lease-enforced access: design

Status: design, awaiting the user's decisions (section 9). Nothing here is built. It depends on
[AUTH.md](AUTH.md): issued credentials travel in ledger replies, so they ship only after AUTH.md's enforce.

Written from five research sections (the issuer, the client path, the guard hook, other resource kinds, and complete
mediation), each backed by experiments on 2026-11-18: `ssh-keygen` in scratch directories, disposable `mysql:8.4` and
`redis:8.6.2` containers from local images, a scratch git remote, and the user's systemd. No real machine, the phone,
the live bus or the services were touched. **This machine has no sshd**, so rows about sshd's own behaviour come from
the OpenSSH documentation and are marked *docs*; a live test is pending the user's approval in the session that would
run it.

## 1. The problem

A lease today is an advisory lock: it holds only if the model decides to take it, and model decisions are stochastic.
An agent can forget, decide a lease is unnecessary, or be talked out of it, and nothing stops the access.

The goal is deterministic: **no lease, no credential, no access**, whatever the model decides. That is Saltzer and
Schroeder's *complete mediation*: every path to the resource passes a check, and no credential that opens the resource
exists outside it. The check lives at the resource's point of access, not in the model.

Threats are AUTH.md's (a) confused, (b) prompt-injected and (c) outside, plus **(d) the model simply doesn't take the
lease**. Out of scope, as in AUTH.md: a process that deliberately sets out to defeat the bridge as the user.

## 2. Measured facts that shape the design

| | Fact | Consequence |
|---|---|---|
| F1 | Every agent can use the user's standing access: a software ed25519 key in `~/.ssh`, an RSA key in the shared ssh-agent (`SSH_AUTH_SOCK` is in every session), `~/.android/adbkey`, and the gh and glab token files | **Until targets stop trusting these, a lease gates nothing.** The human's touch-required ecdsa-sk key also exists, the natural key to keep |
| F2 | `ssh-keygen`'s default certificate extensions grant agent, port and X11 forwarding plus user-rc | The issuer must always pass `-O clear`, then add back only `permit-pty` |
| F3 | A certificate carries the holder, lease id and fencing token in its key id, and the lease id, token and deadline in its own `force-command` argv | The target learns the lease without `ExposeAuthInfo`, which Windows OpenSSH lacks |
| F4 | KRLs revoke by serial and by exact key id and update in place (under 200 bytes). A copied certificate and key work unchanged | Keep the key id stable per lease, so one `id:` line revokes every renewal. Copies are bounded only by validity and the KRL |
| F5 | `ssh-keygen -V` absolute times are local time unless suffixed `Z` | The issuer uses relative or `Z` times; a UTC issuer on this host would be six hours off |
| F6 | A per-session systemd scope with `RuntimeMaxSec` kills setsid, nohup, double-fork and foreground jobs at the deadline, and `systemctl stop` kills them early. But a process can escape a user scope with its own `systemd-run --user` unit | A deterministic kill on the target needs root: `loginctl terminate-user` on a dedicated leased account |
| F7 | MySQL 8.4: `DROP USER` and `ACCOUNT LOCK` both leave open sessions running, still writing. A per-lease `CREATE EVENT ... AT <expiry>` that drops the user and kills its connections ended an open session mid-query, and its queued INSERT never ran | MySQL can enforce the deadline itself, even with the issuer down |
| F8 | Redis 8.6: `ACL SETUSER off` leaves open sessions; `ACL DELUSER` closes them. Redis has no server-side user expiry | Redis revocation works, but its deadline depends on the issuer being alive |
| F9 | A pre-receive hook checking a lease and fencing token refused a push without a lease and with a stale token, and `--no-verify` can't skip it. gitlab.com runs no custom server hooks | Git needs a local gate remote |
| F10 | The adb server on :5037 has no client authentication, and `~/.android/adbkey` is readable by every agent | The phone's trusted-key list is the only gate |
| F11 | A text guard that parses each program's real options, resolves ssh destinations with `ssh -G`, and fails closed on unknown targets blocked 17 direct and wrapped forms of access (`bash -c`, `eval`, `env`, `xargs`, absolute paths...). Ten evasions still passed: variables, `$(...)`, base64 to a shell, scripts, Python subprocess, git aliases, `glab api`, other host names | The hook is a safety net, not the gate |
| F12 | A long-running MySQL container on this workstation, outside the ledger, is published on all interfaces (`0.0.0.0:6603`) | Reachable from the LAN today. Not touched; the user's call |

## 3. The design in one picture

1. A resource declares an **access spec** (kind, target, what "read" permits).
2. On `acquire`, the client sends a **fresh public key**; an **issuer** mints a credential bound to the lease (an SSH
   certificate, a database user, a gate entry) and returns it with the grant. The private key never leaves the client.
3. The **target enforces it**: it trusts only the issuer's credentials, ends sessions at the lease's deadline, and
   applies revocations.
4. On release, expiry, revocation or the holder leaving, the issuer **revokes** and the target **ends what's open**.
5. A deterministic **guard hook** turns the model's common mistake ("I'll just ssh in") into an immediate "take the
   lease first", before the gate refuses it.
6. Every resource is labelled honestly: **`credential`** only where access really needs an issued credential and no
   standing path remains, otherwise **`hook (soft lock)`** with the reason.

## 4. The issuer

- A separate service, `agent-bridge-issuer`, running as **its own OS user**, holds the CA key and the database admin
  credentials. The client asks it for a credential (`issuer/req/<addr> {op: issue, lease_id, pubkey}`); it asks
  ledgerd whether that lease is live and the caller's own, then signs. It follows the lease lifecycle that ledgerd
  publishes (`ledger/events/<resource>`) to revoke. ledgerd stays a pure ledger, and a crashed issuer stops new
  credentials without stopping coordination.
- **The CA key** is never a file agents can read: it lives under the issuer's user, optionally only in that user's
  ssh-agent (measured: signing works that way). A touch-required `-sk` CA can sign for high-value resources and
  break-glass, at one touch per certificate. Targets trust a current and a next CA, so the CA can rotate.
- An adapter per kind: `check_spec`, `issue`, `renew`, `revoke`, and `reconcile`, which prunes, republishes, re-drops,
  and **reports the resource's real enforcement**.
- An additive `credentials` table (serial, lease, holder, kind, key id, public-key hash, validity, revocation) and the
  serial in `lease_events`; the schema stays 1. The target's own logs (key id and serial at every login) join against
  it.

## 5. Per resource kind

### 5.1 SSH (the reference)

**Certificates.**
- Serial: a monotonic counter, one per certificate. Key id, stable for the lease's life:
  `agent-bridge v1 lease=<id> holder=<addr> resource=<owner/name> level=<level> token=<n>`.
- Principal: the resource's configured login, which **must be a non-admin account**. A lease on an admin login is a
  lease on the gate itself: the holder could add a key or edit `sshd_config`. `check_spec` refuses root, Administrator
  or an admin-marked login unless the owner sets `admin: true`, and the console then labels it "advisory only".
- Validity: from now minus 120 s to **min(lease expiry, now + 10 minutes)**, re-issued on renew. A lost revocation is
  then bounded to 10 minutes.
- Options: `-O clear -O permit-pty -O force-command=<lease wrapper with lease, token and deadline>`.
- Public keys: ed25519 or ecdsa, generated per lease, refused if certified in the last 7 days.

**The client** (`acquire <resource> write --exec ...`) generates the key in `$XDG_RUNTIME_DIR/agent-bridge/lease-<id>/`
(tmpfs, 0600), receives the certificate, runs ssh clean (`-F none`, `IdentitiesOnly`, no agent, no ControlMaster, no
forwarding, the target's host key pinned from the resource record), renews before two thirds of the certificate's
life, and deletes the key at release. That is hygiene; enforcement is on the target.

**The target:**

| Setting | Closes |
|---|---|
| A dedicated leased account: `AuthorizedKeysFile none`, `TrustedUserCAKeys` = the lease CA, `AuthorizedPrincipalsFile` | Standing keys for that account |
| `RevokedKeys` with the KRL | Revoked certificates, for new logins (*docs*: re-read per connection, to verify) |
| `DisableForwarding yes`, `MaxSessions 1` | Forwards and ControlMaster sessions that outlive a lease (*docs*) |
| The certificate's forced command: the lease wrapper refuses after the deadline and runs the session inside `systemd-run --scope -p RuntimeMaxSec=<until-now>` | Processes past the deadline (F6, measured) |
| A small **root target agent**: applies the KRL from a retained bus topic, reports the version back, and at release runs `systemctl stop lease-<id>.scope` plus `loginctl terminate-user <leased account>` | Open sessions and escaped processes at release, before the deadline (F6) |

**Windows** OpenSSH lacks `ExposeAuthInfo` and the principals commands; `TrustedUserCAKeys`, `RevokedKeys`,
`AuthorizedPrincipalsFile` and `ForceCommand` are not on Microsoft's not-supported list, and config changes need a
service restart. Until a scratch Windows VM proves them, Windows targets get forced-command-only leases, and the
process kill needs a Job Object instead of a scope.

**Read leases:** SSH has no read mode. A read lease gets a certificate only if the resource defines a read-only forced
command or a separate read account; otherwise it is refused.

### 5.2 MySQL

- Issue: a per-lease user, granted only on the resource's schema (`SELECT` for read), plus a one-shot
  `CREATE EVENT ... AT <expiry> DO CALL end_lease(user)` that drops the user **and kills its connections**.
- Renew: `ALTER EVENT`. Release: `end_lease` now. The deadline holds even with the issuer down (F7).
- The adapter refuses to issue while `event_scheduler` is off, and pins the user's host where it is known.

### 5.3 Redis

- Issue: `ACL SETUSER lease-<id> on ><random> ~<key pattern> <+@read | +@all>`. Release and expiry:
  `ACL DELUSER` (which closes sessions; `off` alone doesn't).
- No server-side deadline (F8): a supervised reaper, run at issuer start and continuously, deletes every `lease-*`
  user without a live lease, and `doctor` fails on any expired one. While the issuer is down, an expired user keeps
  working.

### 5.4 git

- Agents push only to a **local gate remote** whose pre-receive hook asks ledgerd that the lease is live and the
  pusher is its holder (the pushing certificate's key id). Its post-receive forwards accepted pushes upstream with
  the only GitLab push credential, held by the issuer.
- Push options alone are copyable strings, so the hook must check the pusher, not just the token.

### 5.5 adb (the phone)

The device's trusted-key list is the only gate (F10). Enforcement needs the human, once:
- revoke the phone's USB debugging authorizations, and keep wireless debugging off;
- give the phone's USB node to the issuer's user (udev), authorize the issuer's own adb key on the phone;
- agents reach adb only through a proxy that reads `SO_PEERCRED`, walks the process ancestry to the Claude session,
  and forwards only while that session's agent holds a write lease; release closes the connections.

Until then, adb is a soft lock. adb has no read mode, so only write leases.

### 5.6 Worktrees and directories

The user's ask: nobody but the holder (and admin) changes or pushes a worktree without its lease, locally or over
SSH. `agent-bridge worktree acquire|release|status <path>` (and MCP equivalents) resolves the realpath and leases
`admin/wt-<first 16 hex of its sha256>`, with the path in the description; nobody types that id. Measured on git
2.43, a kernel and the user's systemd, in scratch directories only:

| Layer | What it stops | What gets past it |
|---|---|---|
| `git worktree lock` with the lease in the reason, set at acquire | `git worktree remove` refuses even with one `--force`, quoting the lease; the janitor and stray cleanups | commits; `-f -f` overrides it |
| Git's `reference-transaction` hook, installed per repo on the user's confirm ("Protect this repo", chaining any existing hook) | every ref change on the locked worktree's branch, from any worktree: commit (with `--no-verify` too), amend, reset, rebase, merge, cherry-pick, stash, `checkout -B`, `branch -D`, `update-ref`, tag, `worktree add`, fetch into local refs. Hooks are shared, so it scopes by which worktree owns the branch | `-c core.hooksPath=...`, editing `packed-refs` or the ref file directly; file edits, `git add`, `restore`, `clean` (no ref changes) |
| The path guard (section 6): `Edit`, `Write`, `NotebookEdit` and MCP write tools by resolved path, and shell writes (`rm`, `mv`, `cp`, `tee`, `sed -i`, redirects, `git -C`, `make` inside, `find -delete`) | 7 of 7 file-tool writes and 16 of 16 shell writes in the prototype, symlinks, `../` and hard links included; it also protects the branch's ref files and refuses the `core.hooksPath` overrides while a lock is active | interpreters (`python -c`), scripts run from outside, background jobs, other harnesses |
| The git gate remote (5.4) | pushes, deterministically | nothing short of the issuer's credential |
| **bubblewrap per session**, every other worktree bound read-only | any write by any route ("Read-only file system", raw ref writes and Python included); the session can't remount it, and neither can the same user outside | ref writes in the shared common dir (history); a newly leased worktree only on restart or through a root helper; void if the sandbox exposes a host socket (docker, libvirt, the session bus or systemd, another session's messaging socket) |
| SSH targets: the root target agent grants a POSIX ACL on the dir to the leased account and removes it at release | new opens after release | an open file or a working directory inside the tree keeps writing after revoke (measured), so release also runs `terminate-user` and checks `lsof +D` is empty, then hands created files back to their owner |

Two conditions on those last two rows:
- **The sandbox holds only if it exposes no host socket that can act outside it.** Measured, from inside a sandbox
  with the other tree read-only: `docker run -v <locked tree>:/x` wrote it through the daemon (the user is in the
  docker group, AUTH.md R2), and `systemd-run --user` wrote it too, because the user's systemd manager starts the unit
  outside the sandbox. So host sockets are denied by default: `/run` and `/run/user/<uid>` are fresh tmpfs, and only
  the session's own `CLAUDE_CODE_MESSAGING_SOCKET` is bound back, so the relay can still deliver into it (plus, once
  AUTH.md lands, the broker's unix socket). Docker (its real path is `/run/docker.sock`; `/var/run` is a symlink),
  libvirt, the session bus and systemd, gpg-agent, ssh-agent and **every other session's messaging socket** stay
  out: a sandboxed session that reached another session's socket could ask an unsandboxed one to write the tree for
  it. Session-bus features such as the keyring don't work inside. The sandbox keeps the host network, since the bus
  is TCP on loopback today, so loopback services stay reachable; Docker's TCP port must stay off (the default). The
  launcher or managed settings apply the sandbox; the agent never configures its own.
- **"Enforced" is per session.** A sandboxed session can't write the tree, but it can still ask an unsandboxed one
  to, over the bus, guarded only by the "data, not instructions" framing. The path guard narrows that: an
  unsandboxed session that doesn't hold the lease is refused by its own guard, so the route needs the holder or an
  unguarded session. Hence the guard in managed settings for every session, and the guarantee holds for a worktree
  only when every agent session is sandboxed, which the console shows as "enforced for N of M sessions". The
  sandbox also gets its own PID namespace (`--unshare-pid --proc /proc`): writing through `/proc/<pid>/root` was
  measured as refused already, and this keeps it from resting on the user namespace alone.
- **On an SSH target, the next grant waits** for the target agent's acknowledgement that the previous holder's
  processes are gone, `lsof +D` is empty and files are handed back; `reconcile` reports a release with no
  acknowledgement.

Labels, shown in two parts:
- **files:** `hook` with the path guard installed, `credential` under bubblewrap (launcher-applied, host sockets
  hidden) or on an SSH target, otherwise `soft`;
- **history:** `hook` in a protected repo, also inside a sandbox (the shared ref store must stay writable for the
  session's own commits), `none` otherwise (only Claude Code's own git calls are checked), `credential` on an SSH
  target.

So `files: hook · history: none` is an unprotected local repo with the guard installed. One separate OS user for
agents separates them from the human, not from each other; per-lane locks at the credential level locally need
bubblewrap or a user per lane. Uncommitted work is the one thing no git gate sees: release can take a snapshot ref so
nothing is lost. The console flags two live sessions working in the same worktree today, from cwd data it already
has, and the janitor's candidates become worktrees with no lease, no claim, no session inside, clean and merged.

### 5.7 Coordination-only locks

`test-runner` and other locks with nothing to issue stay `hook (soft lock)`: only the guard enforces them, and their
label says so.

## 6. The guard hook

A Claude Code `PreToolUse` hook, run by the harness, not the model:
- It watches `Bash` and the command-carrying MCP tools (today `mcp__terminal__run_in_terminal`).
- It splits the command with a shell lexer, unwraps `bash -c`, `eval`, `env`, `nohup`, `timeout`, `xargs` and the
  like, parses each guarded program's real options, resolves ssh destinations with `ssh -G`, and reads every
  `host:path`.
- No lease at the needed level: **deny** (exit 2), with the exact `agent-bridge acquire ... --exec <command>` in the
  reason. Unknown targets, unparseable text and connection-reuse options without a lease are denied too. ledgerd
  unreachable: guarded commands are denied, everything else is untouched.
- It checks the session agent's leases, cached at most 30 s. Cost: about 30 ms in Python, a few ms as a Rust
  `agent-bridge hook guard`.
- Non-zero exits other than 2, and timeouts, fail open unless `onFailure: "block"` is set, so the guard sets it.
- Settings reload live, and in bypass mode a model can edit `.claude/`. Only **managed settings** (root-owned) with
  deny rules on the settings files hold against that, and they are within the docker group's reach (AUTH.md R2).

It can't see what a script, an interpreter, another client (`glab`, the browser) or another harness does (F11). That
is why the credential is the gate.

## 7. Holes

| Hole | Status |
|---|---|
| The human's standing keys are the agents' keys (F1) | **Open until fixed by the user**: targets keep only the human's touch-required `-sk` key (section 8) |
| A lease on an admin login | Closed: refused, or labelled advisory only |
| An open session outlives the credential | SSH: processes closed by the wrapper's scope; the connection by the root target agent (*docs* until tested). MySQL: closed by the event. Redis: closed at revoke, open while the issuer is down |
| Connection reuse: ControlMaster, forwards, agent forwarding | Closed by `-O clear`, `DisableForwarding` and `MaxSessions 1` (*docs* for the last two) |
| Leftover jobs on the target | Closed by the scope, and by `terminate-user` for deliberate escapes (needs root on the target) |
| A bare `ssh -N` connection with no session | Closed only by the target agent killing the sshd session |
| Copied key and certificate | Bounded: 10-minute validity, KRL by key id, per-lease key deleted at release |
| The CA key, the ledger database | Bounded by the issuer's own OS user; within reach of the docker group (AUTH.md R2) |
| A lagging KRL on a target | Bounded by the 10-minute cap; `doctor` shows the applied version |
| Issuer or ledgerd down | Fail closed for new access; the human's `-sk` key is break-glass; agents have none |
| Clock skew | Backdated start, `Z` or relative times, the deadline enforced in the target's clock, a `doctor` skew check |
| libvirt and kvm reach the VM without SSH | Open while the user is in those groups (decision 10) |
| Other protocols on targets (RDP, WinRM, SMB, Screen Sharing) | Open unless disabled or their credentials are off this user |
| Pure coordination locks | Soft lock, labelled |
| Before AUTH.md's enforce, identities are self-asserted and reply topics readable | Closed by shipping credentials only after enforce |

## 8. What changes for the user

1. **Your standing key becomes the touch-required one.** Targets keep your ecdsa-sk key (with a PIN where supported)
   and drop the software ed25519 key and the ssh-agent's keys. `ssh <host>` still works for you, with a touch; an
   agent trying the same is stopped at the touch.
2. **Per kind, the same:** remove the shared test database users, the agents' git credentials, and the phone's
   workstation authorization.
3. **Agents** run `agent-bridge acquire <resource> write --exec ssh ...` (or the guard tells them to). When the lease
   ends, their sessions end.
4. **Break-glass** is your own key, which works with the issuer down.
5. **The strongest fix, long term:** run agents as their own OS user, without docker and libvirt. Your keys, tokens
   and the CA then sit outside their reach by file permission, and most open rows in section 7 close.

## 9. Decisions for the user

Recommendation first. The major ones:

| # | Decision | Recommended | Alternatives |
|---|---|---|---|
| 1 | Standing access on targets | Only your touch-required `-sk` key; every other key, shared DB user, git credential and phone authorization removed | Keep them, and leases stay advisory |
| 2 | Lease accounts | Non-admin accounts; admin logins advisory only | Admin logins with a drift check before each grant (detection, not prevention) |
| 3 | Ending live sessions | A small root target agent per machine (KRL, scope stop, `terminate-user`) | Certificates and KRL only (open sessions live up to 10 minutes); later, an SSH proxy in the data path (strongest: also mediates forwards and copied keys) |
| 4 | Issuer | A separate `agent-bridge-issuer` service as its own OS user, holding the CA key; a touch-gated `-sk` CA for high-value resources and break-glass | Inside ledgerd (simpler, bigger blast radius) |
| 5 | git | A local gate remote with the only GitLab push credential held by the issuer | Per-lease deploy keys or tokens (day-granular, fail open); branch protection only |
| 6 | The phone | Do the one-time human setup and the adb proxy, if the phone matters | Label adb a soft lock |
| 7 | The guard hook | Deny with the exact command, installed in managed settings, fail closed when ledgerd is down | Rewrite into `acquire` with "ask"; user settings (removable by an agent in bypass mode); fail open |
| 8 | Order | Issued credentials only after AUTH.md's enforce | Earlier, accepting readable reply topics |
| 9 | The live sshd test | Approve the `openssh-server` download in the testing session directly, so the *docs* rows get measured | Accept the docs-based rows |
| 10 | Agents' OS user | A separate user without docker and libvirt, as the long-term fix | Document and accept the residual |
| 11 | Local worktree locks | The path guard plus the git hook in protected repos (labelled `hook`), with the push gate as the hard line; bubblewrap per session where a hard local lock is worth a restart to pick up a new lease | The `hook` label only; or a user per lane |
| 12 | Installing the git hook | Opt-in per repo from the console ("Protect this repo"), chaining any existing hook, audited | Into every registered repo automatically |
| 13 | A locked worktree, without the lease | Only a read-only allowlist runs inside it (reads, `git status`, `log`, `diff`) | Refuse only the known mutating commands |
| 14 | Your own terminal (no session id) in a locked worktree | Allowed, with a warning | Refused for everyone but the holder |

The minor ones, which the team agrees on:
- Certificates capped at 10 minutes and re-issued on renew, plus a KRL.
- A target lagging on KRLs: warn in `doctor` and the console, don't block.
- SSH read leases only with a resource-defined read-only forced command; otherwise refused.
- Windows targets: forced-command-only leases until a scratch VM proves the rest.
- MySQL: the database-side expiry event plus issuer revoke.
- Redis: accept fail-open while the issuer is down, bounded by a supervised reaper and a `doctor` failure.
- A session's lease covers its subagents.
- Shell-capable MCP tools are matched; the browser is left to the credentials.

**Separately, now:** the MySQL container on `0.0.0.0:6603` (F12) is reachable from the LAN. If it doesn't need to be,
bind it to `127.0.0.1`.

## 10. Build order once approved

1. AUTH.md's 0.3.2 lands and reaches enforce.
2. The issuer service, the `credentials` table, the access spec, honest enforcement labels, the guard hook in deny
   mode (useful at once, as a safety net).
3. SSH: certificates, the client path, the target agent and wrapper, starting with one Linux target after the user
   removes standing keys from it.
4. MySQL, then Redis, then the git gate remote.
5. adb after the human setup; Windows after a scratch VM.

Worktree locks don't wait on any of that: `agent-bridge worktree`, `git worktree lock`, the path guard and the opt-in
git hook need no issuer and no AUTH.md enforce, so they can ship first, labelled `hook`, alongside the guard in step 2.
