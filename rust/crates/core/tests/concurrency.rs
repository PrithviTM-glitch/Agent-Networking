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
    let a = Arc::new(Bus::register("conc-a", &[], false, None, false, true).expect("a registers"));
    let b = Bus::register("conc-b", &[], false, None, false, true).expect("b registers");

    // A recv blocked for up to 30 s does not hold up say, dm or join from another thread.
    let waiting = {
        let a = a.clone();
        std::thread::spawn(move || a.recv(30.0, None, None))
    };
    std::thread::sleep(Duration::from_millis(300));
    quick("join", || a.join("conc-room"));
    quick("say", || a.say("conc-room", "while recv waits").map(drop));
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

    // wait_arrival: idle, it waits out its timeout; a new arrival wakes it at once; unread ones never do.
    let t = Instant::now();
    let (pending, seq) = a.wait_arrival(Duration::from_millis(500), u64::MAX).expect("open");
    assert!(pending == 0 && t.elapsed() >= Duration::from_millis(400));
    for i in 0..3 {
        b.dm(&a.addr, &format!("burst {i}")).expect("b's dm");
    }
    let (pending, _) = a.wait_arrival(Duration::from_secs(5), seq).expect("open");
    assert!(pending >= 1);
    std::thread::sleep(Duration::from_millis(500));
    let (_, seq) = a.wait_arrival(Duration::ZERO, seq).expect("open");
    let t = Instant::now();
    assert_eq!(a.wait_arrival(Duration::from_millis(500), seq).ok(), Some((3, seq)), "unread messages do not wake it");
    assert!(t.elapsed() >= Duration::from_millis(400));
    assert_eq!(bodies(&a.recv(1.0, None, None).expect("recv")), ["burst 0", "burst 1", "burst 2"]);
    a.close();
    let t = Instant::now();
    assert_eq!(a.wait_arrival(Duration::from_secs(5), seq).unwrap_err().0, "not connected to the broker");
    assert!(t.elapsed() < Duration::from_secs(1), "a closed Bus fails at once");

    // A Bus closed while it waits to redial never comes back to kick the session's new owner.
    let old = Bus::register("redial", &[], false, None, false, true).expect("an agent");
    let owner = Bus::attach(&old.addr).expect("a second connection takes the session over");
    std::thread::sleep(Duration::from_millis(200));
    old.close();
    let mut stamps = Vec::new();
    for i in 0..6 {
        b.dm(&owner.addr, &format!("steady {i}")).expect("b's dm");
        let got = owner.recv(2.0, None, None).expect("the owner keeps receiving");
        assert_eq!(bodies(&got), [format!("steady {i}")]);
        stamps.push(Instant::now());
        std::thread::sleep(Duration::from_millis(500));
    }
    let worst = stamps.windows(2).map(|w| w[1] - w[0]).max().unwrap_or_default();
    assert!(worst < Duration::from_millis(800), "a takeover cost the owner {worst:?}");
}
