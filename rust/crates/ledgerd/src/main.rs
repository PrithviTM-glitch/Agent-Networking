//! agent-bridge-ledgerd: serves the resource ledger over MQTT 5 request/response.
//!
//! Requests on ledger/req/<addr> (or plain ledger/req, where the body's "as" names the agent), JSON
//! {"op": ..., "as": "<addr>", ...} with ResponseTopic (under ledger/resp/) and CorrelationData. Every request, lease
//! expiry and presence update is handled in order by the main thread, one at a time, each in its own SQLite
//! transaction.

mod args;
mod request;
mod store;

use agent_bridge::config;
use agent_bridge::mqtt::{Ack, Conn, Handler, Message, Options, filter};
use agent_bridge::protocol::NEVER;
use agent_bridge::service::{Mode, mode};
use request::is_addr;
use rumqttc::v5::mqttbytes::v5::PublishProperties;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use store::{Ledger, Outgoing, log};

/// unacked QoS 1 deliveries the broker may push at once, bounds the in-memory queue
const INFLIGHT: u16 = 32;
const QUEUE_QOS0: usize = 1000;
const REPLY_EXPIRY: u32 = 300;
const SESSION_TAKEN_OVER: u8 = 142;
const RECONNECT_MAX: Duration = Duration::from_secs(10);

/// One message for the main thread, or None when another ledgerd took the session over.
type Item = Option<Message>;

struct Service {
    tx: Mutex<Sender<Item>>,
    queued: AtomicUsize,
    db: String,
    broker: String,
}

impl Handler for Service {
    fn connected(&self, conn: &Conn, _session_present: bool) {
        let topics = ["ledger/req", "ledger/req/+", "agents/+"];
        let _ = conn.subscribe(topics.iter().map(|t| filter(t, true, false)).collect());
        log(&format!("connected to {}, db {}", self.broker, self.db));
    }

    fn disconnected(&self, reason: Option<u8>) {
        log(&format!("disconnected {}", reason.map_or("(connection lost)".into(), |r| format!("reason {r}"))));
        // another ledgerd connected: stepping aside keeps a single writer
        if reason == Some(SESSION_TAKEN_OVER) {
            let _ = self.tx.lock().unwrap_or_else(|e| e.into_inner()).send(None);
        }
    }

    fn message(&self, conn: &Conn, msg: Message) {
        if msg.qos == 0 && self.queued.load(Ordering::SeqCst) >= QUEUE_QOS0 {
            return;
        }
        let ack = msg.ack;
        self.queued.fetch_add(1, Ordering::SeqCst);
        if self.tx.lock().unwrap_or_else(|e| e.into_inner()).send(Some(msg)).is_err() {
            conn.ack(ack);
        }
    }
}

fn main() {
    let argv: Vec<String> = std::env::args_os().skip(1).map(|a| a.to_string_lossy().into_owned()).collect();
    match mode("agent-bridge-ledgerd", &argv) {
        Mode::Serve => {}
        Mode::Health => {
            config_or_exit();
            std::process::exit(args::health());
        }
        Mode::Help => {
            print!("{}", args::USAGE);
            return;
        }
        Mode::Bad(text) => {
            eprint!("{text}");
            std::process::exit(2);
        }
    }
    let cfg = config_or_exit();
    let db = std::env::var_os("LEDGER_DB")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| cfg.state_dir.join("ledger.db"));
    let mut ledger = match Ledger::open(&db, cfg.stale_days) {
        Ok(ledger) => ledger,
        Err(store::Open::Schema(e)) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
        Err(store::Open::Failed(e)) => {
            eprintln!("cannot open the ledger at {}: {e}", db.display());
            std::process::exit(1);
        }
    };
    let (tx, rx) = channel();
    let service = Arc::new(Service {
        tx: Mutex::new(tx),
        queued: AtomicUsize::new(0),
        db: db.display().to_string(),
        broker: format!("{}:{}", cfg.host, cfg.port),
    });
    let opts = Options {
        client_id: "ledgerd".into(),
        clean_start: false,
        expiry: NEVER,
        receive_max: Some(INFLIGHT),
        manual_ack: true,
        reconnect: Some(RECONNECT_MAX),
    };
    let conn = Conn::start(&cfg.host, cfg.port, opts, service.clone()).unwrap_or_else(|e| {
        eprintln!("cannot start the MQTT client: {e}");
        std::process::exit(1);
    });
    serve(&mut ledger, &conn, &service, &rx);
}

/// The settings, read once: a bad one ends ledgerd with its one-line message.
fn config_or_exit() -> config::Config {
    config::get().unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(1);
    })
}

fn serve(ledger: &mut Ledger, conn: &Conn, service: &Service, rx: &Receiver<Item>) {
    let mut publish = |(topic, payload, corr): Outgoing| {
        let properties = corr.map(|c| PublishProperties {
            correlation_data: Some(c.into()),
            message_expiry_interval: Some(REPLY_EXPIRY),
            ..Default::default()
        });
        if let Err(e) = conn.publish(&topic, payload, true, false, properties, false) {
            log(&format!("publish failed {topic} {e}"));
        }
    };
    loop {
        let wait = (ledger.next_deadline() - agent_bridge::json::now()).clamp(0.0, 60.0);
        let item = match rx.recv_timeout(Duration::from_secs_f64(wait)) {
            Ok(Some(m)) => Some(m),
            Ok(None) => {
                eprintln!("another client connected as ledgerd, exiting");
                std::process::exit(1);
            }
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => return,
        };
        if let Err(store::Fail::Bad(e) | store::Fail::Internal(e)) = ledger.run(&mut publish, Ledger::tick) {
            log(&format!("tick failed {e}"));
        }
        let Some(m) = item else { continue };
        service.queued.fetch_sub(1, Ordering::SeqCst);
        handle(ledger, &mut publish, &m);
        // Acked only after the transaction committed, so a crash before this point means redelivery, not loss.
        let ack: Ack = m.ack;
        conn.ack(ack);
    }
}

fn handle(ledger: &mut Ledger, publish: &mut dyn FnMut(Outgoing), m: &Message) {
    let Some(topic) = m.topic.as_deref() else { return };
    if topic == "ledger/req" || topic.starts_with("ledger/req/") {
        let via = topic.strip_prefix("ledger/req/").filter(|v| !v.is_empty());
        ledger.handle(publish, &m.payload, m.response_topic.as_deref(), m.correlation.as_deref(), via);
    } else if let Some(who) = topic.strip_prefix("agents/").filter(|a| is_addr(a)) {
        let payload = m.payload.clone();
        if let Err(store::Fail::Bad(e) | store::Fail::Internal(e)) = ledger.run(publish, |l| l.presence(who, &payload))
        {
            log(&format!("failed on {} {e}", &topic[..topic.len().min(80)]));
        }
    }
}
