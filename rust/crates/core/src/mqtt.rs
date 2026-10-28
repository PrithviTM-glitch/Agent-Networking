//! A synchronous MQTT 5 client on rumqttc's packet codec. Packet ids are chosen here, so each publish, subscribe and
//! unsubscribe is confirmed on its own even across reconnects; rumqttc's own event loop hides them. One reader thread
//! per connection dispatches what arrives, keeps the connection alive and, if asked, reconnects with session resume.

use bytes::{Bytes, BytesMut};
use rumqttc::v5::mqttbytes::v5::{
    ConnAck, Connect, ConnectProperties, ConnectReturnCode, Disconnect, DisconnectReasonCode, Filter, Packet, PingReq,
    PubAck, Publish, PublishProperties, Subscribe, SubscribeReasonCode, UnsubAckReason, Unsubscribe,
};
use rumqttc::v5::mqttbytes::{Error as CodecError, QoS};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub const KEEP_ALIVE: u16 = 60;
pub const TIMEOUT: Duration = Duration::from_secs(5);
const TICK: Duration = Duration::from_secs(5);

pub struct Options {
    pub client_id: String,
    pub clean_start: bool,
    pub expiry: u32,
    pub receive_max: Option<u16>,
    /// QoS 1 messages are acked only through Conn::ack, never on arrival.
    pub manual_ack: bool,
    /// Reconnect after a lost connection, backing off from 1 s up to this.
    pub reconnect: Option<Duration>,
}

/// What Conn::ack needs to ack one message, on the connection it arrived on only.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Ack {
    generation: u64,
    pkid: u16,
}

pub struct Message {
    /// None when the topic is not UTF-8.
    pub topic: Option<String>,
    pub payload: Bytes,
    pub qos: u8,
    pub ack: Ack,
    pub response_topic: Option<String>,
    pub correlation: Option<Bytes>,
}

pub trait Handler: Send + Sync {
    /// Runs on the reader thread after each CONNACK, before anything else is read or resent.
    fn connected(&self, _conn: &Conn, _session_present: bool) {}
    /// The server's DISCONNECT reason code, if it sent one.
    fn disconnected(&self, _reason: Option<u8>) {}
    /// A SUBACK or UNSUBACK arrived for pkid.
    fn acked(&self, _pkid: u16) {}
    fn message(&self, conn: &Conn, msg: Message);
}

pub enum DialError {
    Unreachable(io::Error),
    NotAccepted,
}

#[derive(Clone)]
pub struct Conn(Arc<Inner>);

struct Inner {
    host: String,
    port: u16,
    opts: Options,
    handler: Arc<dyn Handler>,
    writer: Mutex<Writer>,
    state: Mutex<State>,
    cv: Condvar,
    reader: Mutex<Option<JoinHandle<()>>>,
}

struct Writer {
    stream: Option<TcpStream>,
    last_sent: Instant,
}

struct Outgoing {
    publish: Publish,
    in_flight: bool,
    sent: bool,
}

struct State {
    attached: bool,
    up: bool,
    closed: bool,
    generation: u64,
    next_pkid: u16,
    server_max: usize,
    in_flight: usize,
    outgoing: VecDeque<Outgoing>,
    waiting: HashSet<u16>,
    acks: HashMap<u16, bool>,
    ping_sent: Option<Instant>,
}

type Dialed = (TcpStream, ConnAck, BytesMut);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Conn {
    /// Connect now, then keep the connection in a reader thread.
    pub fn open(host: &str, port: u16, opts: Options, handler: Arc<dyn Handler>) -> Result<Conn, DialError> {
        let first = dial(host, port, &opts)?;
        let conn = Conn::new(host, port, opts, handler);
        conn.spawn(Some(first)).map_err(DialError::Unreachable)?;
        if !conn.wait_up(TIMEOUT) {
            conn.close();
            return Err(DialError::NotAccepted);
        }
        Ok(conn)
    }

    /// Connect in the background, retrying until the broker answers; for services that start before the broker.
    pub fn start(host: &str, port: u16, opts: Options, handler: Arc<dyn Handler>) -> io::Result<Conn> {
        let conn = Conn::new(host, port, opts, handler);
        conn.spawn(None)?;
        Ok(conn)
    }

    fn new(host: &str, port: u16, opts: Options, handler: Arc<dyn Handler>) -> Conn {
        Conn(Arc::new(Inner {
            host: host.to_owned(),
            port,
            opts,
            handler,
            writer: Mutex::new(Writer { stream: None, last_sent: Instant::now() }),
            state: Mutex::new(State {
                attached: false,
                up: false,
                closed: false,
                generation: 0,
                next_pkid: 0,
                server_max: 65535,
                in_flight: 0,
                outgoing: VecDeque::new(),
                waiting: HashSet::new(),
                acks: HashMap::new(),
                ping_sent: None,
            }),
            cv: Condvar::new(),
            reader: Mutex::new(None),
        }))
    }

    fn spawn(&self, first: Option<Dialed>) -> io::Result<()> {
        let me = self.clone();
        let handle = thread::Builder::new().name("mqtt".into()).spawn(move || me.run(first))?;
        *lock(&self.0.reader) = Some(handle);
        Ok(())
    }

    pub fn is_up(&self) -> bool {
        lock(&self.0.state).up
    }

    pub fn wait_up(&self, timeout: Duration) -> bool {
        let st = lock(&self.0.state);
        let (st, _) =
            self.0.cv.wait_timeout_while(st, timeout, |s| !s.up && !s.closed).unwrap_or_else(|e| e.into_inner());
        st.up
    }

    /// Publish; with qos1 the message is kept and resent until the broker acks it, and track lets wait_ack see that.
    pub fn publish(
        &self,
        topic: &str,
        payload: impl Into<Bytes>,
        qos1: bool,
        retain: bool,
        properties: Option<PublishProperties>,
        track: bool,
    ) -> io::Result<u16> {
        let mut publish =
            Publish::new(topic, if qos1 { QoS::AtLeastOnce } else { QoS::AtMostOnce }, payload, properties);
        publish.retain = retain;
        if !qos1 {
            return self.send(&Packet::Publish(publish));
        }
        let mut st = lock(&self.0.state);
        let pkid = alloc(&mut st);
        publish.pkid = pkid;
        if track {
            st.waiting.insert(pkid);
        }
        st.outgoing.push_back(Outgoing { publish, in_flight: false, sent: false });
        self.flush(&mut st);
        Ok(pkid)
    }

    pub fn subscribe(&self, filters: Vec<Filter>) -> io::Result<u16> {
        self.request(|pkid| Packet::Subscribe(Subscribe { pkid, filters, properties: None }))
    }

    pub fn unsubscribe(&self, filters: Vec<String>) -> io::Result<u16> {
        self.request(|pkid| Packet::Unsubscribe(Unsubscribe { pkid, filters, properties: None }))
    }

    fn request(&self, packet: impl FnOnce(u16) -> Packet) -> io::Result<u16> {
        let mut st = lock(&self.0.state);
        if !st.attached {
            return Err(io::ErrorKind::NotConnected.into());
        }
        let pkid = alloc(&mut st);
        st.waiting.insert(pkid);
        self.send(&packet(pkid))?;
        Ok(pkid)
    }

    /// Whether the broker accepted the tracked request pkid, or None if no answer came within timeout.
    pub fn wait_ack(&self, pkid: u16, timeout: Duration) -> Option<bool> {
        let st = lock(&self.0.state);
        let (mut st, _) = self
            .0
            .cv
            .wait_timeout_while(st, timeout, |s| !s.acks.contains_key(&pkid) && !s.closed)
            .unwrap_or_else(|e| e.into_inner());
        let got = st.acks.remove(&pkid);
        if got.is_none() {
            st.waiting.remove(&pkid);
        }
        got
    }

    /// Ack a QoS 1 message, unless the connection it came on is gone (the broker then resends it, or never will).
    pub fn ack(&self, ack: Ack) {
        let st = lock(&self.0.state);
        if ack.pkid != 0 && st.attached && st.generation == ack.generation {
            let _ = self.send(&Packet::PubAck(PubAck::new(ack.pkid, None)));
        }
    }

    /// Disconnect cleanly and stop the reader thread; the session and anything not yet acked stay with the broker.
    pub fn close(&self) {
        lock(&self.0.state).closed = true;
        self.0.cv.notify_all();
        if let Some(stream) = lock(&self.0.writer).stream.take() {
            disconnect(&stream);
        }
        let handle = lock(&self.0.reader).take();
        if let Some(h) = handle.filter(|h| h.thread().id() != thread::current().id()) {
            let _ = h.join();
        }
    }

    fn send(&self, packet: &Packet) -> io::Result<u16> {
        let mut buf = BytesMut::new();
        packet.write(&mut buf, None).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
        let mut w = lock(&self.0.writer);
        let stream = w.stream.as_ref().ok_or(io::ErrorKind::NotConnected)?;
        (&*stream).write_all(&buf)?;
        w.last_sent = Instant::now();
        Ok(0)
    }

    /// Send queued QoS 1 publishes while the broker's receive maximum allows.
    fn flush(&self, st: &mut State) {
        if !st.attached {
            return;
        }
        for i in 0..st.outgoing.len() {
            if st.in_flight >= st.server_max {
                break;
            }
            let out = &mut st.outgoing[i];
            if out.in_flight {
                continue;
            }
            out.publish.dup = out.sent;
            out.in_flight = true;
            out.sent = true;
            let packet = Packet::Publish(out.publish.clone());
            st.in_flight += 1;
            if self.send(&packet).is_err() {
                return;
            }
        }
    }

    fn run(self, first: Option<Dialed>) {
        let max = self.0.opts.reconnect;
        // Started without a connection (Conn::start): dial at once; after a lost one, wait first.
        let mut wait = first.is_some();
        let mut next = first;
        loop {
            let (mut stream, ack, buf) = match next.take() {
                Some(d) => d,
                None => match self.redial(max.unwrap_or(Duration::ZERO), wait) {
                    Some(d) => d,
                    None => return,
                },
            };
            wait = true;
            if !self.attach(&stream, &ack) {
                return;
            }
            let reason = self.read_loop(&mut stream, buf);
            self.detach(reason);
            if max.is_none() || lock(&self.0.state).closed {
                return;
            }
        }
    }

    /// Dial until it works, waiting first if wait, backing off between failures; None once closed.
    fn redial(&self, max: Duration, wait: bool) -> Option<Dialed> {
        let mut delay = if wait { Duration::from_secs(1) } else { Duration::ZERO };
        loop {
            let st = lock(&self.0.state);
            let (st, _) = self.0.cv.wait_timeout_while(st, delay, |s| !s.closed).unwrap_or_else(|e| e.into_inner());
            if st.closed {
                return None;
            }
            drop(st);
            if let Ok(d) = dial(&self.0.host, self.0.port, &self.0.opts) {
                return Some(d);
            }
            delay = (delay * 2).clamp(Duration::from_secs(1), max.max(Duration::from_secs(1)));
        }
    }

    /// false if the Conn was closed while it was redialling: the session it just took is let go at once, so a
    /// closed Conn never holds it against the client that owns it now.
    fn attach(&self, stream: &TcpStream, ack: &ConnAck) -> bool {
        if let Ok(clone) = stream.try_clone() {
            let mut w = lock(&self.0.writer);
            w.stream = Some(clone);
            w.last_sent = Instant::now();
        }
        // The writer is set first and close() sets closed first: one of the two sees the other.
        if lock(&self.0.state).closed {
            if let Some(s) = lock(&self.0.writer).stream.take() {
                disconnect(&s);
            }
            disconnect(stream);
            return false;
        }
        {
            let mut st = lock(&self.0.state);
            st.attached = true;
            st.generation += 1;
            st.in_flight = 0;
            st.ping_sent = None;
            st.server_max = ack.properties.as_ref().and_then(|p| p.receive_max).unwrap_or(65535).max(1) as usize;
            let st = &mut *st;
            for out in st.outgoing.iter_mut() {
                out.in_flight = false;
            }
            let outgoing = &st.outgoing;
            st.waiting.retain(|p| outgoing.iter().any(|o| o.publish.pkid == *p));
        }
        self.0.handler.connected(self, ack.session_present);
        let mut st = lock(&self.0.state);
        self.flush(&mut st);
        st.up = true;
        self.0.cv.notify_all();
        true
    }

    fn detach(&self, reason: Option<u8>) {
        if let Some(s) = lock(&self.0.writer).stream.take() {
            let _ = s.shutdown(Shutdown::Both);
        }
        {
            let mut st = lock(&self.0.state);
            st.attached = false;
            st.up = false;
        }
        self.0.cv.notify_all();
        self.0.handler.disconnected(reason);
    }

    fn read_loop(&self, stream: &mut TcpStream, mut buf: BytesMut) -> Option<u8> {
        let _ = stream.set_read_timeout(Some(TICK));
        let mut chunk = vec![0u8; 64 * 1024];
        loop {
            loop {
                match Packet::read(&mut buf, None) {
                    Ok(packet) => {
                        if let Some(reason) = self.dispatch(packet) {
                            return reason;
                        }
                    }
                    Err(CodecError::InsufficientBytes(_)) => break,
                    Err(_) => return None,
                }
            }
            match stream.read(&mut chunk) {
                Ok(0) => return None,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {
                    if !self.keep_alive() {
                        return None;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(_) => return None,
            }
            if lock(&self.0.state).closed {
                return None;
            }
        }
    }

    /// Ping when idle for half the keep-alive; false once a ping went unanswered for a whole one.
    fn keep_alive(&self) -> bool {
        let mut st = lock(&self.0.state);
        let period = Duration::from_secs(KEEP_ALIVE as u64);
        if st.ping_sent.is_some_and(|t| t.elapsed() > period) {
            return false;
        }
        if st.ping_sent.is_none() && lock(&self.0.writer).last_sent.elapsed() >= period / 2 {
            st.ping_sent = Some(Instant::now());
            let _ = self.send(&Packet::PingReq(PingReq));
        }
        true
    }

    /// Some(reason) ends the connection.
    fn dispatch(&self, packet: Packet) -> Option<Option<u8>> {
        match packet {
            Packet::Publish(p) => {
                let generation = lock(&self.0.state).generation;
                let qos = p.qos as u8;
                let ack = Ack { generation, pkid: if qos > 0 { p.pkid } else { 0 } };
                let props = p.properties.unwrap_or_default();
                let msg = Message {
                    topic: String::from_utf8(p.topic.to_vec()).ok(),
                    payload: p.payload,
                    qos,
                    ack,
                    response_topic: props.response_topic,
                    correlation: props.correlation_data,
                };
                self.0.handler.message(self, msg);
                if !self.0.opts.manual_ack {
                    self.ack(ack);
                }
            }
            Packet::PubAck(a) => {
                let mut st = lock(&self.0.state);
                if let Some(i) = st.outgoing.iter().position(|o| o.publish.pkid == a.pkid && o.in_flight) {
                    st.outgoing.remove(i);
                    st.in_flight -= 1;
                }
                if st.waiting.remove(&a.pkid) {
                    st.acks.insert(a.pkid, true);
                }
                self.flush(&mut st);
                self.0.cv.notify_all();
            }
            Packet::SubAck(s) => {
                let ok = s.return_codes.iter().all(|c| matches!(c, SubscribeReasonCode::Success(_)));
                self.settle(s.pkid, ok);
            }
            Packet::UnsubAck(u) => {
                let ok = u
                    .reasons
                    .iter()
                    .all(|r| matches!(r, UnsubAckReason::Success | UnsubAckReason::NoSubscriptionExisted));
                self.settle(u.pkid, ok);
            }
            Packet::PingResp(_) => lock(&self.0.state).ping_sent = None,
            Packet::Disconnect(d) => return Some(Some(d.reason_code as u8)),
            _ => {}
        }
        None
    }

    fn settle(&self, pkid: u16, ok: bool) {
        {
            let mut st = lock(&self.0.state);
            if st.waiting.remove(&pkid) {
                st.acks.insert(pkid, ok);
            }
        }
        self.0.cv.notify_all();
        self.0.handler.acked(pkid);
    }
}

/// End a connection cleanly: DISCONNECT, then shut the socket for every clone of it.
fn disconnect(stream: &TcpStream) {
    let mut buf = BytesMut::new();
    if Packet::Disconnect(Disconnect::new(DisconnectReasonCode::NormalDisconnection)).write(&mut buf, None).is_ok() {
        let _ = (&*stream).write_all(&buf);
    }
    let _ = stream.shutdown(Shutdown::Both);
}

fn alloc(st: &mut State) -> u16 {
    loop {
        st.next_pkid = st.next_pkid % u16::MAX + 1;
        let p = st.next_pkid;
        if !st.waiting.contains(&p) && !st.outgoing.iter().any(|o| o.publish.pkid == p) {
            st.acks.remove(&p);
            return p;
        }
    }
}

pub fn filter(path: &str, qos1: bool, no_local: bool) -> Filter {
    let mut f = Filter::new(path, if qos1 { QoS::AtLeastOnce } else { QoS::AtMostOnce });
    f.nolocal = no_local;
    f
}

fn dial(host: &str, port: u16, opts: &Options) -> Result<Dialed, DialError> {
    let mut last = io::Error::new(io::ErrorKind::NotFound, "no address to connect to");
    let mut stream = None;
    for addr in (host, port).to_socket_addrs().map_err(DialError::Unreachable)? {
        match TcpStream::connect_timeout(&addr, TIMEOUT) {
            Ok(s) => {
                stream = Some(s);
                break;
            }
            Err(e) => last = e,
        }
    }
    let mut stream = stream.ok_or(DialError::Unreachable(last))?;
    // Without it every request/response round trip waits on Nagle plus delayed ACK (~40 ms each way).
    let _ = stream.set_nodelay(true);
    let _ = stream.set_read_timeout(Some(TIMEOUT));
    let _ = stream.set_write_timeout(Some(TIMEOUT));
    let properties = ConnectProperties {
        session_expiry_interval: (opts.expiry > 0).then_some(opts.expiry),
        receive_maximum: opts.receive_max,
        ..ConnectProperties::new()
    };
    let connect = Connect {
        keep_alive: KEEP_ALIVE,
        client_id: opts.client_id.clone(),
        clean_start: opts.clean_start,
        properties: Some(properties),
    };
    let mut buf = BytesMut::new();
    Packet::Connect(connect, None, None).write(&mut buf, None).map_err(|_| DialError::NotAccepted)?;
    stream.write_all(&buf).map_err(DialError::Unreachable)?;
    buf.clear();
    let mut chunk = [0u8; 4096];
    loop {
        match Packet::read(&mut buf, None) {
            Ok(Packet::ConnAck(ack)) if ack.code == ConnectReturnCode::Success => return Ok((stream, ack, buf)),
            Err(CodecError::InsufficientBytes(_)) => {}
            _ => return Err(DialError::NotAccepted),
        }
        match stream.read(&mut chunk) {
            Ok(n) if n > 0 => buf.extend_from_slice(&chunk[..n]),
            _ => return Err(DialError::NotAccepted),
        }
    }
}
