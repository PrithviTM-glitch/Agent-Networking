//! One Bus shared by threads: no call waits behind another's wait, and concurrent presence writers lose nothing.
//! Needs Docker, so it is ignored by default: cargo test -p agent-bridge-core -- --ignored

mod common;

use agent_bridge::bus::Bus;
use agent_bridge::json::Map;
use agent_bridge::presence;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn quick(what: &str, f: impl FnOnce() -> agent_bridge::Result<()>) {
    let t = Instant::now();
    f().unwrap_or_else(|e| panic!("{what}: {e}"));
    assert!(t.elapsed() < Duration::from_secs(1), "{what} took {:?} while another thread waited", t.elapsed());
}

fn bodies(msgs: &[Map]) -> Vec<String> {
    msgs.iter().map(|m| m["body"].as_str().unwrap_or("").to_owned()).collect()
}

#[test]
#[ignore = "needs Docker"]
fn one_bus_many_threads() {
    let _broker = common::broker("concurrency");
    let a = Arc::new(Bus::register("conc-a", &[], false, None, false).expect("a registers"));
    let b = Bus::register("conc-b", &[], false, None, false).expect("b registers");

    // A recv blocked for up to 30 s does not hold up say, dm or join from another thread.
    let waiting = {
        let a = a.clone();
        std::thread::spawn(move || a.recv(30.0, None, None))
    };
    std::thread::sleep(Duration::from_millis(300));
    quick("join", || a.join("conc-room"));
    quick("say", || a.say("conc-room", "while recv waits"));
    quick("dm", || a.dm(&b.addr, "while recv waits"));
    b.dm(&a.addr, "wake").expect("b's dm");
    let got = waiting.join().expect("the waiting thread").expect("recv");
    assert_eq!(bodies(&got), ["wake"]);

    // Ten concurrent joins on one Bus: presence ends up with all ten rooms, and each room reaches it.
    let rooms: Vec<String> = (0..10).map(|i| format!("conc-r{i}")).collect();
    let joins: Vec<_> = rooms
        .iter()
        .map(|r| {
            let (a, r) = (a.clone(), r.clone());
            std::thread::spawn(move || a.join(&r))
        })
        .collect();
    for j in joins {
        j.join().expect("a joining thread").expect("join");
    }
    let rec = presence::find(&a.addr).expect("lookup").expect("a is live");
    let topics: Vec<String> = presence::topics(&rec).map(str::to_owned).collect();
    for r in &rooms {
        assert!(topics.contains(&format!("room/{r}")), "{r} missing from {topics:?}");
        b.join(r).expect("b joins");
        b.say(r, &format!("hello {r}")).expect("b says");
    }
    let mut heard = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while heard.iter().filter(|b: &&String| b.starts_with("hello ")).count() < rooms.len() {
        assert!(Instant::now() < deadline, "heard only {heard:?}");
        heard.extend(bodies(&a.recv(1.0, None, None).expect("recv")));
    }
}
