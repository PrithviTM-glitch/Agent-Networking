//! The guard's shell lexer and its decision on any text: never a panic, and every Bash call gets a decision.

#![no_main]

#[path = "common.rs"]
mod common;

use agent_bridge::guard::{ToolCall, decide, shell};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else { return };
    let _ = shell::commands(text);
    let f = common::fixture();
    let mut input = serde_json::Map::new();
    input.insert("command".into(), text.into());
    for cwd in [&f.free, &f.lane, &f.repo] {
        let call = ToolCall { tool: "Bash".into(), input: input.clone(), cwd: cwd.clone() };
        let _ = decide(&call, &common::Mock);
    }
});
