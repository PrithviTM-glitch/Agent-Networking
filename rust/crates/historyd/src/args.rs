//! The command line. No arguments runs the service; nothing else ever does, so a stray flag cannot start a second
//! historyd that takes the live one's session over.

use agent_bridge::history::client::call;
use agent_bridge::json::Map;

pub const USAGE: &str = "usage: agent-bridge-historyd [--health]

Records the agent-bridge bus's messages and serves them over MQTT 5 request/response on history/req.

  (no arguments)  run the service (AGENT_BRIDGE_HOST, AGENT_BRIDGE_PORT, HISTORY_DB, HISTORY_IMPORT)
  --health        ping the running historyd; exit 0 on an ok reply within 5 s, else 1
  -h, --help      show this help
";

const HEALTH_TIMEOUT: f64 = 5.0;

#[derive(Debug, PartialEq)]
pub enum Mode {
    Serve,
    Health,
    Help,
    Bad(String),
}

pub fn mode(args: &[String]) -> Mode {
    match args {
        [] => Mode::Serve,
        [a] if a == "--health" => Mode::Health,
        [a] if a == "--help" || a == "-h" => Mode::Help,
        other => Mode::Bad(format!("unrecognized arguments: {}", other.join(" "))),
    }
}

/// A round trip through the broker over a throwaway client, so it fails when historyd is disconnected or wedged, not
/// just when it exits.
pub fn health() -> i32 {
    match call("ping", None, Map::new(), HEALTH_TIMEOUT) {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("historyd: {e}");
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn of(args: &[&str]) -> Mode {
        mode(&args.iter().map(|a| a.to_string()).collect::<Vec<_>>())
    }

    #[test]
    fn only_no_arguments_serve() {
        assert_eq!(of(&[]), Mode::Serve);
        assert_eq!(of(&["--health"]), Mode::Health);
        assert_eq!(of(&["-h"]), Mode::Help);
        assert_eq!(of(&["--bogus"]), Mode::Bad("unrecognized arguments: --bogus".into()));
        assert_eq!(of(&["--health", "x"]), Mode::Bad("unrecognized arguments: --health x".into()));
    }
}
