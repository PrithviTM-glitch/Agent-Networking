//! agent-bridge in Rust: the wire protocol, validation, history log, MQTT session layer and the Bus, wire-compatible
//! with the Python package, so agents of both kinds share one broker.

pub mod broker;
pub mod bus;
pub mod config;
pub mod history;
pub mod identity;
pub mod json;
pub mod ledger;
pub mod mqtt;
pub mod presence;
pub mod protocol;
pub mod rooms;
pub mod stamp;
pub mod validation;

pub use validation::{Error, Result};
