//! --help and --health; agent_bridge::service parses the arguments.

use agent_bridge::json::Map;
use agent_bridge::ledger;

pub const USAGE: &str = "usage: agent-bridge-ledgerd [-h] [--health]

Serves the agent-bridge resource ledger over MQTT 5 request/response on ledger/req.

  (no arguments)  run the service (AGENT_BRIDGE_HOST, AGENT_BRIDGE_PORT, LEDGER_DB)
  --health        ask the running ledgerd for a dump as health-0; exit 0 on an ok reply within 5 s, else 1
  -h, --help      show this help
";

const HEALTH_TIMEOUT: f64 = 5.0;

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
