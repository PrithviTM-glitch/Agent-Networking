//! The command line. No arguments runs the service; nothing else ever does, so a stray flag cannot start a second
//! ledgerd that takes the live one's session over.

use agent_bridge::json::Map;
use agent_bridge::ledger;

pub const USAGE: &str = "usage: agent-bridge-ledgerd [--health]

Serves the agent-bridge resource ledger over MQTT 5 request/response on ledger/req.

  (no arguments)  run the service (AGENT_BRIDGE_HOST, AGENT_BRIDGE_PORT, LEDGER_DB)
  --health        ask the running ledgerd for a dump as health-0; exit 0 on an ok reply within 5 s, else 1
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

/// A round trip through the broker over a throwaway client, so it fails when ledgerd is disconnected or wedged, not
/// just when it exits.
pub fn health() -> i32 {
    match ledger::call("dump", "health-0", Map::new(), Some(HEALTH_TIMEOUT)) {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("agent-bridge-ledgerd: unhealthy: {e}");
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
        assert_eq!(of(&["--help"]), Mode::Help);
        assert_eq!(of(&["-h"]), Mode::Help);
        assert_eq!(of(&["--bogus"]), Mode::Bad("unrecognized arguments: --bogus".into()));
        assert_eq!(of(&["--health", "x"]), Mode::Bad("unrecognized arguments: --health x".into()));
        assert_eq!(of(&[""]), Mode::Bad("unrecognized arguments: ".into()));
    }
}
