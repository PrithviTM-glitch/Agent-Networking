//! The command line: the commands and arguments of v0.1.0's Python CLI.

use clap::{Parser, Subcommand, ValueEnum};

const ABOUT: &str = "agent-bridge: the bus from the shell. Commands that return data print JSON, one object per line; \
errors are one line on stderr with a non-zero exit.";

const LONG_ABOUT: &str =
    "agent-bridge: the bus from the shell. Commands that return data print JSON, one object per line (recv one per
message, so possibly none); register prints just the address, and keep, join, part, say, dm, sub, unsub, pub, leave
and relay start print nothing when they succeed. Errors are one line on stderr with a non-zero exit.

Names, addresses, rooms and caps: 1-64 ASCII letters, digits, _ or - (names at most 57, leaving room for the address
suffix), at most 32 caps. TEXT: at most 64 KiB of UTF-8, and not empty or only whitespace for say and dm (a raw pub
payload may be empty); tab, newline and CR are kept, any other control character (C0, DEL, C1) is refused. Raw topics:
at most 256 bytes of UTF-8, no control characters, never starting with $ or the reserved levels agents, inbox, ledger,
broadcast, rooms and history (only the dedicated commands touch those). + and # work only in sub, as whole levels, #
only last, and not as the first level. Incoming messages that are over 256 KiB, not
UTF-8, or break these rules are dropped; plain text on a raw topic arrives with \"from\": null. \"from\" is checked for
shape only: any local process can claim any address. Room moderation (mode, moderators, mute, ban) is advisory for
the same reason.";

#[derive(Parser)]
#[command(name = "agent-bridge", version, about = ABOUT, long_about = LONG_ABOUT)]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Clone, Copy, ValueEnum)]
pub enum OnOff {
    On,
    Off,
}

#[derive(Clone, Copy, ValueEnum)]
pub enum Change {
    Add,
    Remove,
}

#[derive(Clone, Copy, ValueEnum)]
pub enum Level {
    Read,
    Write,
}

#[derive(Clone, Copy, ValueEnum)]
pub enum Visibility {
    Public,
    Private,
}

#[derive(Clone, Copy, ValueEnum)]
pub enum Mode {
    Open,
    Announce,
}

#[derive(Clone, Copy, PartialEq, ValueEnum)]
pub enum Hook {
    /// UserPromptSubmit and SessionStart: show the admin console's broadcasts this session has not seen
    Broadcast,
    /// SessionStart: start this session's relay in the background
    RelayStart,
}

#[derive(Clone, Copy, PartialEq, ValueEnum)]
pub enum RelayAction {
    Start,
    Stop,
    Status,
    Restart,
    Run,
}

macro_rules! names {
    ($($t:ty),*) => {$(
        impl $t {
            pub fn name(self) -> String {
                self.to_possible_value().map(|v| v.get_name().to_owned()).unwrap_or_default()
            }
        }
    )*};
}
names!(Change, Level, Visibility, Mode);

#[derive(clap::Args)]
pub struct Details {
    #[arg(long)]
    pub description: Option<String>,
    #[arg(long)]
    pub visibility: Option<Visibility>,
    /// highest level granted automatically if public
    #[arg(long = "max-level")]
    pub max_level: Option<Level>,
}

#[derive(Subcommand)]
pub enum Cmd {
    /// join the bus, prints your address
    Register {
        name: String,
        /// comma-separated things you can do for others
        #[arg(long, default_value = "")]
        caps: String,
        /// exempt from expiry after the stale period idle
        #[arg(long)]
        keep: bool,
        /// reattach to this address if it is live (name, caps, keep unchanged), else create it
        #[arg(long)]
        addr: Option<String>,
        /// a subagent's own agent: recorded under its parent session
        #[arg(long)]
        subagent: bool,
    },
    /// delete your ledger, session, queued messages, presence and received DMs
    Leave {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
    },
    /// opt in or out of idle expiry
    Keep {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        state: OnOff,
    },
    /// every agent with caps and topics, one JSON per line
    Agents {
        #[arg(long)]
        room: Option<String>,
    },
    /// every chat room with its members, mode and moderators
    Rooms,
    /// join a chat room
    Join {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        room: String,
    },
    /// leave a chat room
    Part {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        room: String,
    },
    /// post to a chat room; @name / @address of a member and @all mention
    ///
    /// Post to a chat room. @name / @address of a room member and @all mention; any other @word stays plain text
    /// (@@word never mentions), and stderr says which words mentioned nobody. TEXT - reads the text from stdin, and --file PATH from a file (UTF-8, the same limits as TEXT; one final newline
    /// is dropped).
    Say {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        room: String,
        /// the message, or - for stdin
        #[arg(required_unless_present = "file")]
        text: Option<String>,
        /// read the message from this file instead
        #[arg(long, value_name = "PATH", conflicts_with = "text")]
        file: Option<std::path::PathBuf>,
    },
    /// a room's settings: info ROOM, or set MY_ADDR ROOM --mode/--rules
    Room {
        #[command(subcommand)]
        action: RoomCmd,
    },
    /// add or remove a moderator of a room (admin or a moderator)
    Mod {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        room: String,
        change: Change,
        #[arg(value_name = "ADDR")]
        target: String,
    },
    /// stop ADDR posting in a room (admin or a moderator)
    Mute(Moderation),
    /// let ADDR post in a room again (admin or a moderator)
    Unmute(Moderation),
    /// stop ADDR posting in or joining a room (admin or a moderator)
    Ban(Moderation),
    /// lift a ban (admin or a moderator)
    Unban(Moderation),
    /// ban ADDR and drop it from the room (briefly takes its session over, its relay reconnects) (admin or a moderator)
    Kick(Moderation),
    /// a chat room's transcript, to catch up after joining
    ///
    /// Entries come oldest first, each with its "seq"; for the page before, pass the oldest entry's seq to --before.
    History {
        room: String,
        #[arg(long, default_value_t = 20, allow_negative_numbers = true)]
        last: i64,
        /// only entries older than this seq (paging back)
        #[arg(long, value_name = "SEQ", allow_negative_numbers = true)]
        before: Option<i64>,
        /// one readable line per message instead of JSON
        #[arg(long)]
        text: bool,
    },
    /// direct message one agent
    ///
    /// TEXT - reads the text from stdin, and --file PATH from a file (UTF-8, the same limits as TEXT; one final newline
    /// is dropped).
    Dm {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        to: String,
        /// the message, or - for stdin
        #[arg(required_unless_present = "file")]
        text: Option<String>,
        /// read the message from this file instead
        #[arg(long, value_name = "PATH", conflicts_with = "text")]
        file: Option<std::path::PathBuf>,
    },
    /// your direct messages, both directions
    ///
    /// Entries come oldest first, each with its "seq"; for the page before, pass the oldest entry's seq to --before.
    Dms {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        #[arg(long = "with")]
        peer: Option<String>,
        #[arg(long, default_value_t = 20, allow_negative_numbers = true)]
        last: i64,
        /// only entries older than this seq (paging back)
        #[arg(long, value_name = "SEQ", allow_negative_numbers = true)]
        before: Option<i64>,
        /// one readable line per message instead of JSON
        #[arg(long)]
        text: bool,
    },
    /// subscribe to a raw topic, + and # wildcards work
    Sub {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        topic: String,
    },
    /// unsubscribe from a raw topic
    Unsub {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        topic: String,
    },
    /// publish to a raw topic
    Pub {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        topic: String,
        text: String,
    },
    /// new room, DM and topic messages except your own, one JSON per line
    Recv {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        /// block up to N seconds (max 3600) for a message
        #[arg(long, default_value_t = 0.0, allow_negative_numbers = true)]
        wait: f64,
        /// one readable line per message instead of JSON
        #[arg(long)]
        text: bool,
    },
    /// follow a room live until Ctrl-C, after its last few messages
    ///
    /// It reads over a connection of its own, so it needs no address and never takes a session or relay over; muted
    /// and banned senders are left out, as recv leaves them out.
    Watch {
        room: String,
        /// how many past messages to show first
        #[arg(long, default_value_t = 20, allow_negative_numbers = true)]
        last: i64,
        /// one readable line per message instead of JSON
        #[arg(long)]
        text: bool,
    },
    /// change your display name, caps or self-description
    ///
    /// The address never changes. @name mentions follow the new name. --about "" clears the description, --caps ""
    /// the caps.
    Describe {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        /// a new display name
        #[arg(long)]
        name: Option<String>,
        /// comma-separated things you can do for others
        #[arg(long)]
        caps: Option<String>,
        /// a one-line description of yourself, at most 200 bytes
        #[arg(long)]
        about: Option<String>,
    },
    /// check this machine's bridge: broker, services, directories, versions, relays and hooks
    ///
    /// One line per check, OK, WARN or FAIL, with what to do; exit 1 if any check fails. It only reads: it never
    /// creates the state directory, starts or stops a relay, or edits settings. Checks: broker (reachable), ledgerd
    /// and historyd (answer, and their versions), state_dir and config_dir (exist, yours, modes), cli_lock
    /// (writable), versions (this CLI, the services and the installed wheel agree), relays (a live relay on a
    /// replaced or different binary, or whose session ended) and hooks (each agent-bridge hook in
    /// ~/.claude/settings.json runs an existing program).
    Doctor {
        /// one JSON object per check: {"check", "status": "ok"|"warn"|"fail", "detail", ...}
        #[arg(long)]
        json: bool,
    },
    /// this Claude Code session's relay into its inbox: start (the SessionStart hook), stop, status, restart; run PID
    /// START is what start launches
    ///
    /// status prints {"session", "live", "lock", "pid", "started", "uptime_s", "binary", "version"}; the last five
    /// are null while no relay is live, version is null for a relay older than 0.3.0, and binary ends in " (deleted)"
    /// when the relay still runs a file an upgrade replaced: restart it. stop prints that same line once the relay
    /// has stopped, so its "live" is false: the state after the stop, not before it. restart stops this session's
    /// relay, starts it again on the installed binary and prints the line once it is live; it runs only inside the
    /// session, without --session. start prints nothing.
    Relay {
        #[arg(default_value = "start")]
        action: RelayAction,
        #[arg(hide = true, num_args = 0..=2, allow_negative_numbers = true)]
        run: Vec<String>,
        /// session id, default CLAUDE_CODE_SESSION_ID
        #[arg(long)]
        session: Option<String>,
    },
    /// a Claude Code hook: broadcast or relay-start. It prints nothing on failure and always exits 0
    Hook { which: Hook },
    /// one raw ledgerd request: OP [--as ADDR] [--timeout S] [--FIELD VALUE ...], the reply as JSON (what
    /// agent-bridge-ledger runs)
    #[command(name = "ledger-raw", disable_help_flag = true)]
    LedgerRaw {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true, num_args = 0..)]
        args: Vec<String>,
    },
    /// ledger: add a resource you own
    Add {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        /// resource name, your resource id is MY_ADDR/NAME
        name: String,
        #[command(flatten)]
        details: Details,
    },
    /// ledger: change one of your resources
    Update {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        /// resource name, your resource id is MY_ADDR/NAME
        name: String,
        #[command(flatten)]
        details: Details,
    },
    /// ledger: delete one of your resources
    Remove {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        /// resource name, your resource id is MY_ADDR/NAME
        name: String,
    },
    /// ledger: your resources, or another agent's public ones
    Ledger {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        #[arg(long)]
        owner: Option<String>,
    },
    /// ledger: every public resource of every agent
    Public {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
    },
    /// ledger: ask for access to OWNER/NAME
    ///
    /// With --wait it blocks until the owner answers, checking every second: granted prints the request (exit 0);
    /// denied, or still pending after S seconds, is an error (exit 1) and a pending request stays pending.
    Request {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        resource: String,
        level: Level,
        /// wait up to S seconds (default 300, max 3600) for the owner to grant or deny it
        #[arg(long, value_name = "S", num_args = 0..=1, default_missing_value = "300", allow_negative_numbers = true)]
        wait: Option<f64>,
    },
    /// ledger: grant a pending request on your resource
    Grant {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        request_id: String,
    },
    /// ledger: deny or revoke a request on your resource
    Deny {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        request_id: String,
    },
    /// ledger: requests you made and requests on your resources
    Grants {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
    },
    /// ledger: take a lease, read is shared, write exclusive with a fencing token
    ///
    /// With --exec CMD [ARGS...] (last on the line: everything after it is the command) it takes the lease, runs CMD
    /// with AGENT_BRIDGE_LEASE_ID, AGENT_BRIDGE_LEASE_RESOURCE, AGENT_BRIDGE_LEASE_LEVEL and, for write,
    /// AGENT_BRIDGE_FENCE_TOKEN in its environment, renews the lease every ttl/3 while CMD runs, releases it when CMD
    /// exits, and exits with CMD's status (128+N if signal N killed it). SIGINT, SIGTERM and SIGHUP are passed on to
    /// CMD. Exit 75 means CMD did not run to the end with the lease: it was refused (busy, no grant) or lost while CMD
    /// ran (CMD then gets SIGTERM, and SIGKILL 10 s later); the stderr line says which. Exit 127: CMD could not start.
    Acquire {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        resource: String,
        level: Level,
        /// lease seconds, default 60
        #[arg(long, allow_negative_numbers = true)]
        ttl: Option<f64>,
        /// seconds to queue if busy, default 0
        #[arg(long, allow_negative_numbers = true)]
        wait: Option<f64>,
        /// run CMD [ARGS...] while holding the lease
        #[arg(long, value_name = "CMD", num_args = 1.., allow_hyphen_values = true)]
        exec: Option<Vec<String>>,
    },
    /// ledger: extend a lease
    Renew {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        lease_id: String,
        /// lease seconds, default 60
        #[arg(long, allow_negative_numbers = true)]
        ttl: Option<f64>,
    },
    /// ledger: give a lease back
    Release {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        lease_id: String,
    },
    /// ledger: the leases you hold (resource, owner, level, token for write, expires_at), soonest expiry first
    Leases {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
    },
    /// ledger: the last lease events of one of your resources (acquire, renew, release, break, revoke, expire, drop)
    #[command(name = "lease-history")]
    LeaseHistory {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        /// one of your resources, by name or OWNER/NAME
        resource: String,
        /// how many events, 1-500, default 50
        #[arg(long, allow_negative_numbers = true)]
        last: Option<i64>,
    },
}

#[derive(clap::Args)]
pub struct Moderation {
    #[arg(value_name = "MY_ADDR")]
    pub addr: String,
    pub room: String,
    #[arg(value_name = "ADDR")]
    pub target: String,
}

#[derive(Subcommand)]
pub enum RoomCmd {
    /// mode, moderators, rules, muted and banned; open defaults without settings
    Info { room: String },
    /// change mode or rules (admin or a moderator), the room is told
    Set {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        room: String,
        /// open: anyone posts; announce: only moderators
        #[arg(long)]
        mode: Option<Mode>,
        /// short text, at most 2048 bytes
        #[arg(long)]
        rules: Option<String>,
    },
}
