//! Ledger calls over an agent's own Bus: replies are acked and never buffered (more calls than the receive window, and
//! messages still arrive), they come back on ledger/resp/<address>, the console can act as an owner, and a call that
//! waits stalls nothing else on the Bus. Needs Docker, so it is ignored by default:
//! cargo test -p agent-bridge-ledgerd -- --ignored

use agent_bridge::bus::Bus;
use agent_bridge::json::Map;
use agent_bridge::mqtt::{Conn, Handler, Message, Options, filter};
use serde_json::{Value, json};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct World {
    broker: String,
    ledgerd: Child,
}

impl Drop for World {
    fn drop(&mut self) {
        let _ = self.ledgerd.kill();
        let _ = self.ledgerd.wait();
        let _ = Command::new("docker").args(["rm", "-f", &self.broker]).output();
    }
}

fn start() -> (World, u16) {
    let port = std::net::TcpListener::bind("127.0.0.1:0").and_then(|l| l.local_addr()).expect("a free port").port();
    let broker = format!("agent-bridge-ledgerd-over-bus-{}", std::process::id());
    let conf = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../config/mosquitto.conf");
    let started = Command::new("docker")
        .args(["run", "-d", "--name", &broker, "-p", &format!("127.0.0.1:{port}:1883"), "-v"])
        .arg(format!("{}:/mosquitto/config/mosquitto.conf:ro", conf.canonicalize().expect("the config").display()))
        .arg("eclipse-mosquitto:2.0.22")
        .output()
        .expect("docker runs");
    assert!(started.status.success(), "{}", String::from_utf8_lossy(&started.stderr));
    let dir = std::env::temp_dir().join(&broker);
    std::fs::create_dir_all(&dir).expect("a temp dir");
    // SAFETY: set once, before any thread of this test reads the environment.
    unsafe {
        std::env::set_var("AGENT_BRIDGE_PORT", port.to_string());
        std::env::set_var("AGENT_BRIDGE_LOG", dir.join("history.jsonl"));
        std::env::set_var("XDG_STATE_HOME", dir.join("state"));
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    while agent_bridge::presence::agents(None).is_err() {
        assert!(Instant::now() < deadline, "the broker never came up");
        std::thread::sleep(Duration::from_millis(100));
    }
    let ledgerd = Command::new(env!("CARGO_BIN_EXE_agent-bridge-ledgerd"))
        .env("LEDGER_DB", dir.join("ledger.db"))
        .stderr(Stdio::null())
        .spawn()
        .expect("ledgerd runs");
    while agent_bridge::ledger::call("init", "probe-0", Map::new(), Some(1.0)).is_err() {
        assert!(Instant::now() < deadline, "ledgerd never answered");
    }
    (World { broker, ledgerd }, port)
}

/// Every topic a raw subscriber sees on ledger/resp/#.
#[derive(Default)]
struct Watch(Mutex<Vec<String>>);

impl Handler for Watch {
    fn message(&self, _conn: &Conn, msg: Message) {
        self.0.lock().unwrap().extend(msg.topic);
    }
}

fn fields(v: Value) -> Map {
    v.as_object().cloned().unwrap_or_default()
}

fn quick<T>(what: &str, f: impl FnOnce() -> T) -> T {
    let t = Instant::now();
    let out = f();
    assert!(t.elapsed() < Duration::from_secs(1), "{what} took {:?} while other calls waited", t.elapsed());
    out
}

#[test]
#[ignore = "needs Docker"]
fn ledger_over_the_bus() {
    let (_world, port) = start();
    let watch = Arc::new(Watch::default());
    let opts = Options {
        client_id: String::new(),
        clean_start: true,
        expiry: 0,
        receive_max: None,
        manual_ack: false,
        reconnect: None,
    };
    let watcher = Conn::open("127.0.0.1", port, opts, watch.clone()).ok().expect("a raw watcher");
    let _ = watcher.subscribe(vec![filter("ledger/resp/#", true, false)]);

    let a = Arc::new(Bus::register("over-a", &[], false, None, false, true).expect("a registers"));
    let b = Bus::register("over-b", &[], false, None, false, true).expect("b registers");

    // More calls than the Bus's receive window of 100: unacked replies would stop all delivery to a.
    a.ledger("init", None, Map::new(), None).expect("init");
    for _ in 0..150 {
        a.ledger("ledger", None, Map::new(), None).expect("a ledger call");
    }
    b.dm(&a.addr, "after 150 calls").expect("b's dm");
    let got = a.recv(5.0, None, None).expect("recv");
    assert_eq!(
        got.iter().map(|m| m["body"].clone()).collect::<Vec<_>>(),
        [json!("after 150 calls")],
        "no reply leaks into recv"
    );
    let seen = watch.0.lock().unwrap().clone();
    assert!(seen.len() >= 151 && seen.iter().all(|t| *t == format!("ledger/resp/{}", a.addr)), "{seen:?}");

    // The console's case: a call over a's connection acting as b, the owner.
    b.ledger("init", None, Map::new(), None).expect("b init");
    b.ledger("add", None, fields(json!({"name": "hot", "visibility": "public", "max_level": "write"})), None)
        .expect("add");
    let lease = b
        .ledger("acquire", None, fields(json!({"resource": format!("{}/hot", b.addr), "level": "write"})), None)
        .unwrap();
    let id = lease["lease"]["lease_id"].clone();
    a.ledger("release", Some(&b.addr), fields(json!({"lease_id": id})), None).expect("released as the owner");

    // One thread waits for a busy lease and another in recv; the third's say and release go through at once.
    a.join("over-room").expect("join");
    a.ledger("add", None, fields(json!({"name": "own"})), None).expect("a's own resource");
    let own = a
        .ledger("acquire", None, fields(json!({"resource": format!("{}/own", a.addr), "level": "write"})), None)
        .unwrap();
    let hot = format!("{}/hot", b.addr);
    a.ledger("request", None, fields(json!({"resource": hot, "level": "write"})), None).expect("granted: public");
    let held = b.ledger("acquire", None, fields(json!({"resource": hot, "level": "write"})), None).unwrap();
    let waiting = {
        let (a, hot) = (a.clone(), hot.clone());
        std::thread::spawn(move || {
            a.ledger("acquire", None, fields(json!({"resource": hot, "level": "write", "wait": 20})), None)
        })
    };
    let receiving = {
        let a = a.clone();
        std::thread::spawn(move || a.recv(30.0, None, None))
    };
    std::thread::sleep(Duration::from_millis(300));
    quick("say", || a.say("over-room", "while two calls wait").expect("say"));
    quick("release", || {
        a.ledger("release", None, fields(json!({"lease_id": own["lease"]["lease_id"]})), None).expect("release")
    });
    b.dm(&a.addr, "wake").expect("b's dm");
    let got = receiving.join().unwrap().expect("recv");
    assert!(got.iter().any(|m| m["body"] == "wake"), "{got:?}");
    b.ledger("release", None, fields(json!({"lease_id": held["lease"]["lease_id"]})), None).expect("b releases");
    let lease = waiting.join().unwrap().expect("a gets the lease once b releases");
    assert_eq!(lease["lease"]["holder"], json!(a.addr));

    // leave drops the ledger over the Bus before closing.
    // No historyd runs here, so only the DMs warning may come back.
    let warning = a.leave().expect("leave").unwrap_or_default();
    assert!(!warning.contains("ledger") && warning.starts_with("DMs not dropped: history: no reply"), "{warning}");
    let dump = agent_bridge::ledger::call("dump", "probe-0", Map::new(), None).expect("dump");
    let mine = |list: &str, key: &str| dump[list].as_array().unwrap().iter().any(|r| r[key] == json!(a.addr));
    assert!(!mine("ledgers", "owner") && !mine("leases", "holder") && !mine("waiters", "holder"), "{dump:?}");
    watcher.close();
}
