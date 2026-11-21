//! ledgerd's request decode on any bytes (ledgerd's request.rs, compiled in here: ledgerd has no library). It never
//! panics, and what it accepts acts as a well-formed address: the request topic's when there is one.

#![no_main]

#[allow(dead_code)]
#[path = "../../crates/ledgerd/src/request.rs"]
mod request;

use agent_bridge::json::loads;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Some(req) = loads(data) else { return };
    for via in [None, Some("al-1"), Some("bad/addr")] {
        if let Ok((op, me, _fields)) = request::parse(&req, via) {
            assert!(request::OPS.contains(&op), "{op}");
            assert!(request::is_addr(&me), "accepted as {me:?}");
            if let Some(via) = via {
                assert_eq!(me, via, "the request topic fixes the address");
            }
        }
    }
});
