//! What the admin console needs from the core: the Observer's view of every topic, the admin identity and its
//! broadcast, and describe. Needs Docker, so it is ignored by default: cargo test -p agent-bridge-core -- --ignored

mod common;

use agent_bridge::bus::Bus;
use agent_bridge::observer::{Event, Observer};
use agent_bridge::presence;
use std::time::{Duration, Instant};

/// Events from obs until one satisfies found, failing after 10 s.
fn until(obs: &Observer, what: &str, found: impl Fn(&Event) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(Instant::now() < deadline, "the observer never saw {what}");
        if obs.next(Duration::from_millis(200)).is_some_and(|e| found(&e)) {
            return;
        }
    }
}

#[test]
#[ignore = "needs Docker"]
fn observer_admin_and_describe() {
    let _broker = common::broker("console");
    let obs = Observer::open().expect("the observer starts");
    until(&obs, "its connect", |e| matches!(e, Event::Connected));

    let a = Bus::register("watched", &[], false, None, false).expect("an agent registers");
    until(&obs, "the agent's presence", |e| matches!(e, Event::Presence { addr, rec: Some(_) } if *addr == a.addr));

    let admin = Bus::register_admin().expect("the console's identity");
    assert_eq!(admin.addr, "admin");
    let rec = presence::find("admin").unwrap().expect("admin is live");
    assert!(rec["keep"] == true && !rec.contains_key("session") && !rec.contains_key("pid"), "{rec:?}");

    admin.room_set("watched-room", Some("announce"), None).expect("admin sets a room");
    until(
        &obs,
        "the room record",
        |e| matches!(e, Event::Room { room, rec: Some(r) } if room == "watched-room" && r["mode"] == "announce"),
    );
    until(
        &obs,
        "the room note",
        |e| matches!(e, Event::Message { msg, .. } if msg["body"] == "[mode #watched-room] announce by admin"),
    );

    admin.broadcast("to everyone").expect("admin broadcasts");
    let got = a.recv(5.0, None, None).expect("recv");
    assert_eq!((got[0]["topic"].as_str(), got[0]["from"].as_str()), (Some("broadcast"), Some("admin")));
    assert_eq!(a.broadcast("x").unwrap_err().0, "only the admin console can broadcast");

    a.describe(Some("renamed"), Some(&["triage".to_string()])).expect("describe");
    let rec = presence::find(&a.addr).unwrap().expect("still live");
    assert!(rec["name"] == "renamed" && rec["caps"] == serde_json::json!(["triage"]), "{rec:?}");
    until(
        &obs,
        "the new name",
        |e| matches!(e, Event::Presence { addr, rec: Some(r) } if *addr == a.addr && r["name"] == "renamed"),
    );
    assert_eq!(a.describe(Some("Admin"), None).unwrap_err().0, "name 'Admin' is reserved for the admin console");
    assert_eq!(obs.dropped(), 0);
}
