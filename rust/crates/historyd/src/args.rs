//! --help and --health; agent_bridge::service parses the arguments.

use agent_bridge::history::client::call;
use agent_bridge::json::Map;

pub const USAGE: &str = "usage: agent-bridge-historyd [-h] [--health]

Records the agent-bridge bus's messages and serves them over MQTT 5 request/response on history/req.

  (no arguments)  run the service (AGENT_BRIDGE_HOST, AGENT_BRIDGE_PORT, HISTORY_DB, HISTORY_IMPORT)
  --health        ping the running historyd; exit 0 on an ok reply within 5 s, else 1
  -h, --help      show this help
";

const HEALTH_TIMEOUT: f64 = 5.0;

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
