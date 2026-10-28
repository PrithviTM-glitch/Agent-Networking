//! The command line, as the Python CLI defines it.

use clap::{Parser, Subcommand, ValueEnum};

const ABOUT: &str = "agent-bridge: the bus from the shell. Every command prints JSON, one object per line; errors are one \
line on stderr with a non-zero exit.";

const LONG_ABOUT: &str =
    "agent-bridge: the bus from the shell. Every command prints JSON, one object per line; errors \
are one line on stderr with a non-zero exit.

Names, addresses, rooms and caps: 1-64 ASCII letters, digits, _ or - (names at most 57, leaving room for the address
suffix), at most 32 caps. TEXT: at most 64 KiB of UTF-8; tab, newline and CR are kept, any other control character
(C0, DEL, C1) is refused. Raw topics: at most 256 bytes of UTF-8, no control characters, never starting with $ or the
reserved levels agents, inbox, ledger, broadcast and rooms (only the dedicated commands touch those). + and # work
only in sub, as whole levels, # only last, and not as the first level. Incoming messages that are over 256 KiB, not
UTF-8, or break these rules are dropped; plain text on a raw topic arrives with \"from\": null. \"from\" is checked for
shape only: any local process can claim any address. Room moderation (mode, moderators, mute, ban) is advisory for
the same reason.";

#[derive(Parser)]
#[command(name = "agent-bridge", about = ABOUT, long_about = LONG_ABOUT, disable_version_flag = true)]
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
pub enum RelayAction {
    Start,
    Stop,
    Status,
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
    /// post to a chat room; @name, @address and @all mention
    Say {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        room: String,
        text: String,
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
    History {
        room: String,
        #[arg(long, default_value_t = 20, allow_negative_numbers = true)]
        last: i64,
    },
    /// direct message one agent
    Dm {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        to: String,
        text: String,
    },
    /// your direct messages, both directions
    Dms {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        #[arg(long = "with")]
        peer: Option<String>,
        #[arg(long, default_value_t = 20, allow_negative_numbers = true)]
        last: i64,
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
    },
    /// this Claude Code session's relay into its inbox: start (the SessionStart hook), stop, status; run PID START
    /// is what start launches
    Relay {
        #[arg(default_value = "start")]
        action: RelayAction,
        #[arg(hide = true, num_args = 0..=2, allow_negative_numbers = true)]
        run: Vec<String>,
        /// session id, default CLAUDE_CODE_SESSION_ID
        #[arg(long)]
        session: Option<String>,
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
    Request {
        #[arg(value_name = "MY_ADDR")]
        addr: String,
        resource: String,
        level: Level,
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
