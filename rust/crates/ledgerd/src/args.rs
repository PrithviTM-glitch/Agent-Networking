//! --help, --version and --health; agent_bridge::service parses the arguments.

use agent_bridge::config::{self, Auth};
use agent_bridge::json::Map;
use agent_bridge::ledger;

pub const USAGE: &str = "usage: agent-bridge-ledgerd [-h] [--health] [-V]

Serves the agent-bridge resource ledger over MQTT 5 request/response on ledger/req.

  (no arguments)  run the service (AGENT_BRIDGE_HOST, AGENT_BRIDGE_PORT, LEDGER_DB)
  --health        ask the running ledgerd for its worktrees (as health-0, or as ledgerd with its credentials under
                  auth); exit 0 on an ok reply within 5 s, else 1
  -V, --version   print the version and exit
  -h, --help      show this help
";

const HEALTH_TIMEOUT: f64 = 5.0;

/// A round trip through the broker over a throwaway client, so it fails when ledgerd is disconnected or wedged, not
/// just when it exits. worktrees reads the database and is open to anyone, also under auth enforce, where dump is
/// admin's alone.
pub fn health() -> i32 {
    let creds = match config::get().map(|c| c.auth) {
        Ok(Auth::Off) | Err(_) => None,
        Ok(_) => config::credentials("ledgerd").ok().flatten(),
    };
    let who = if creds.is_some() { "ledgerd" } else { "health-0" };
    match ledger::call_with("worktrees", who, Map::new(), Some(HEALTH_TIMEOUT), creds.as_ref()) {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("agent-bridge-ledgerd: unhealthy: {e}");
            1
        }
    }
}
