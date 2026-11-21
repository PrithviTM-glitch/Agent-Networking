//! agent-bridge-historyd: records the bus's messages and serves them over MQTT 5 request/response.
//!
//! It subscribes to # and keeps rooms, inboxes, broadcasts and raw topics (store::stored). Requests on
//! history/req/<addr> (or plain history/req for ops that name nobody), JSON {"op": ..., "as": "<addr>", ...} with
//! ResponseTopic (under history/resp/) and CorrelationData. Messages, requests and pruning are handled in order by the
//! main thread, each in its own SQLite transaction, and a message is acked only after its row committed. On its first
//! start it imports the legacy history log (HISTORY_IMPORT, default the bridge's history.jsonl) once and leaves the
//! file in place.

mod args;
mod request;
mod store;

use agent_bridge::config::{self, Auth};
use agent_bridge::mqtt::{Acked, Conn, Handler, Message, Options, SESSION_TAKEN_OVER, filter};
use agent_bridge::protocol::NEVER;
use agent_bridge::service::{Mode, mode, version_line};
use rumqttc::v5::mqttbytes::v5::{Login, PublishProperties, RetainForwardRule};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use store::{History, Outgoing, log};

/// unacked QoS 1 deliveries the broker may push at once, bounds the in-memory queue
const INFLIGHT: u16 = 32;
const QUEUE_QOS0: usize = 1000;
const REPLY_EXPIRY: u32 = 300;
const RECONNECT_MAX: Duration = Duration::from_secs(10);
/// without a broker it imports anyway after this long
const IMPORT_WAIT: Duration = Duration::from_secs(30);

/// One message for the main thread, or None when another historyd took the session over.
type Item = Option<Message>;

struct Service {
    tx: Mutex<Sender<Item>>,
    queued: AtomicUsize,
    subscribed: (Mutex<bool>, Condvar),
    /// The pkid of the latest subscription to #, so main can tell a refusal from an ack.
    subscription: Mutex<Option<u16>>,
    db: String,
    broker: String,
}

impl Handler for Service {
    fn connected(&self, conn: &Conn, _session_present: bool) {
        // No Local: never its own replies. Retain handling 2: a (re)subscribe must not replay retained records as
        // history.
        let mut everything = filter("#", true, true);
        everything.retain_forward_rule = RetainForwardRule::Never;
        match conn.subscribe(vec![everything]) {
            Ok(pkid) => *self.subscription.lock().unwrap_or_else(|e| e.into_inner()) = Some(pkid),
            Err(e) => log(&format!("cannot subscribe to #: {e}")),
        }
        log(&format!("connected to {}, db {}", self.broker, self.db));
    }

    fn disconnected(&self, reason: Option<u8>) {
        log(&format!("disconnected {}", reason.map_or("(connection lost)".into(), |r| format!("reason {r}"))));
        // another historyd connected: stepping aside keeps a single writer
        if reason == Some(SESSION_TAKEN_OVER) {
            #[allow(clippy::let_underscore_must_use, reason = "a closed channel means main has already stopped")]
            let _ = self.tx.lock().unwrap_or_else(|e| e.into_inner()).send(None);
        }
    }

    fn acked(&self, _pkid: u16) {
        *self.subscribed.0.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.subscribed.1.notify_all();
    }

    fn message(&self, conn: &Conn, msg: Message) {
        if msg.topic.is_none() || (msg.qos == 0 && self.queued.load(Ordering::SeqCst) >= QUEUE_QOS0) {
            return conn.ack(msg.ack);
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
    match mode("agent-bridge-historyd", &argv) {
        Mode::Serve => {}
        Mode::Health => {
            config_or_exit();
            std::process::exit(args::health());
        }
        Mode::Help => {
            print!("{}", args::USAGE);
            return;
        }
        Mode::Version => {
            println!("{}", version_line("agent-bridge-historyd"));
            return;
        }
        Mode::Bad(text) => {
            eprint!("{text}");
            std::process::exit(2);
        }
    }
    let cfg = config_or_exit();
    let env_path = |name: &str| std::env::var_os(name).filter(|v| !v.is_empty()).map(PathBuf::from);
    let db = env_path("HISTORY_DB").unwrap_or_else(|| cfg.state_dir.join("history.db"));
    let import = env_path("HISTORY_IMPORT").unwrap_or_else(|| cfg.log.clone());
    let mut history = match History::open(&db, cfg.stale_s) {
        Ok(history) => history,
        Err(store::Open::Schema(e)) => {
            eprintln!("historyd: {e}");
            std::process::exit(1);
        }
        Err(store::Open::Failed(e)) => {
            eprintln!("cannot open the history at {}: {e}", db.display());
            std::process::exit(1);
        }
    };
    let (tx, rx) = channel();
    let service = Arc::new(Service {
        tx: Mutex::new(tx),
        queued: AtomicUsize::new(0),
        subscribed: (Mutex::new(false), Condvar::new()),
        subscription: Mutex::new(None),
        db: db.display().to_string(),
        broker: format!("{}:{}", cfg.host, cfg.port),
    });
    let (client_id, login) = login_or_exit(cfg.auth);
    let opts = Options {
        client_id,
        clean_start: false,
        expiry: NEVER,
        receive_max: Some(INFLIGHT),
        manual_ack: true,
        reconnect: Some(RECONNECT_MAX),
    };
    let conn = Conn::start_as(&cfg.host, cfg.port, opts, login, service.clone()).unwrap_or_else(|e| {
        eprintln!("cannot start the MQTT client: {e}");
        std::process::exit(1);
    });
    // After subscribing, so nothing sent meanwhile is missed (digests skip what arrives both ways).
    {
        let (flag, cv) = &service.subscribed;
        let guard = flag.lock().unwrap_or_else(|e| e.into_inner());
        #[allow(clippy::let_underscore_must_use, reason = "the wait only bounds how long the import is held back")]
        let _ = cv.wait_timeout_while(guard, IMPORT_WAIT, |done| !*done);
    }
    // a broker that refuses # would leave historyd connected and recording nothing: say so and stop
    let subscription = *service.subscription.lock().unwrap_or_else(|e| e.into_inner());
    if subscription.is_some_and(|pkid| conn.wait_refused(pkid, Duration::ZERO) == Some(Acked::Refused)) {
        eprintln!(
            "historyd: the broker refused its subscription to #; check its credentials (AGENT_BRIDGE_CREDENTIALS)"
        );
        std::process::exit(1);
    }
    if let Err(e) = history.migrate(&import) {
        eprintln!("cannot import {}: {e}", import.display());
        std::process::exit(1);
    }
    serve(&mut history, &conn, &service, &rx);
}

/// The client id and login historyd connects with: "historyd" and none when auth is off; under warn or enforce, the
/// secret client id and login of its credentials (AGENT_BRIDGE_CREDENTIALS, username historyd). Two historyds with
/// the same credentials still share an id, so the newer one takes over and the older steps aside.
fn login_or_exit(auth: Auth) -> (String, Option<Login>) {
    if auth == Auth::Off {
        return ("historyd".into(), None);
    }
    match agent_bridge::provision::credentials_for("historyd") {
        Ok(Some(c)) => {
            log(&format!("auth {}: logging in as {}", auth.as_str(), c.username));
            (c.client_id, Some(Login::new(c.username, c.password)))
        }
        Ok(None) if auth == Auth::Warn => {
            log("auth warn: no credentials for historyd (AGENT_BRIDGE_CREDENTIALS), connecting anonymously");
            ("historyd".into(), None)
        }
        Ok(None) => {
            eprintln!("auth enforce: historyd needs its credentials file in AGENT_BRIDGE_CREDENTIALS");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    }
}

/// The settings, read once: a bad one ends historyd with its one-line message.
fn config_or_exit() -> config::Config {
    config::get().unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(1);
    })
}

fn serve(history: &mut History, conn: &Conn, service: &Service, rx: &Receiver<Item>) {
    let mut publish = |(topic, payload, corr): Outgoing| {
        let properties = PublishProperties {
            correlation_data: corr.map(Into::into),
            message_expiry_interval: Some(REPLY_EXPIRY),
            ..Default::default()
        };
        if let Err(e) = conn.publish(&topic, payload, true, false, Some(properties), false) {
            log(&format!("reply failed {topic} {e}"));
        }
    };
    loop {
        let item = match rx.recv_timeout(Duration::from_secs(60)) {
            Ok(Some(m)) => Some(m),
            Ok(None) => {
                eprintln!("another client connected as historyd, exiting");
                std::process::exit(1);
            }
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => return,
        };
        if let Err(e) = history.prune() {
            log(&format!("prune failed {e}"));
        }
        let Some(m) = item else { continue };
        service.queued.fetch_sub(1, Ordering::SeqCst);
        let topic = m.topic.as_deref().unwrap_or_default();
        if topic == "history/req" || topic.starts_with("history/req/") {
            let via = topic.strip_prefix("history/req/").filter(|v| !v.is_empty());
            history.handle(&mut publish, &m.payload, m.response_topic.as_deref(), m.correlation.as_deref(), via);
        } else if let Err(e) = history.record(topic, &m.payload) {
            log(&format!("failed on {} {e}", topic.chars().take(80).collect::<String>()));
        }
        // Acked only after the row committed, so a crash before this point means redelivery, not loss.
        conn.ack(m.ack);
    }
}
