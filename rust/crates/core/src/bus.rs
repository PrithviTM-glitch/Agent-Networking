//! The Bus: one agent's persistent MQTT connection, with client id = its address, so it owns the agent's session.
//!
//! Incoming messages are buffered by the reader thread and acked only when recv hands them out, so a takeover or a
//! crash never loses them. Every failure is an Error with a one-line message.

use crate::broker::{self, TIMEOUT};
use crate::json::{Map, dumps_map, float, now};
use crate::mqtt::{Ack, Conn, Handler, Message, filter};
use crate::protocol::{BROADCAST, parse_message, parse_presence};
use crate::validation::{
    ADMIN, Error, ID_MAX, NAME_MAX, Result, TOPICS_MAX, check_body, check_caps, check_ident, check_ident_cap,
    check_topic, check_wait, fail, is_ident, not_admin,
};
use crate::{config, history, identity, ledger, presence, rooms};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

const TOUCH: Duration = Duration::from_secs(60);
const BUFFER_MAX: usize = 1000;

type Deliver<'a> = &'a mut dyn FnMut(&[Map]) -> Result<()>;

/// One agent on the bus. Bus::attach attaches to a live address; Bus::register joins the bus.
pub struct Bus {
    pub addr: String,
    pub rec: Map,
    conn: Conn,
    shared: Arc<Shared>,
    touched: Option<Instant>,
}

/// What the reader thread maintains for the Bus.
struct Shared {
    addr: String,
    st: Mutex<Cache>,
    cv: Condvar,
    on_arrival: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}

#[derive(Default)]
struct Cache {
    /// room -> record, kept current by the rooms/+ subscription
    rooms: HashMap<String, Map>,
    /// address -> presence record, kept current by the agents/+ subscription
    agents: HashMap<String, Map>,
    buf: VecDeque<(Option<Ack>, Map)>,
    /// the agent's topics, subscribed afresh when the broker lost the session
    topics: Vec<String>,
    up: bool,
    /// set once both caches hold every retained record of this connection
    synced: bool,
    barrier: Option<u16>,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, Cache> {
        self.st.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Handler for Shared {
    fn connected(&self, conn: &Conn, session_present: bool) {
        // The ack of the next request (the barrier) arrives after every retained record, filling both caches afresh.
        let topics = {
            let mut c = self.lock();
            c.rooms.clear();
            c.agents.clear();
            c.synced = false;
            // A surviving session resends everything not yet acked; a lost one never will, so keep those unacked.
            c.buf = std::mem::take(&mut c.buf)
                .into_iter()
                .filter(|(ack, _)| ack.is_none() || !session_present)
                .map(|(_, m)| (None, m))
                .collect();
            c.topics.clone()
        };
        let _ = conn.subscribe(vec![filter(rooms::TOPICS, false, false), filter(presence::TOPICS, false, false)]);
        // broadcast on every connect, so agents whose sessions predate it pick it up on their next attach.
        let mut wanted = vec![BROADCAST.to_owned()];
        if !session_present {
            wanted.push(format!("inbox/{}", self.addr));
            wanted.extend(topics);
        }
        let barrier = conn.subscribe(wanted.iter().map(|t| filter(t, true, true)).collect()).ok();
        let mut c = self.lock();
        c.barrier = barrier;
        c.up = true;
        self.cv.notify_all();
    }

    fn disconnected(&self, _reason: Option<u8>) {
        let mut c = self.lock();
        c.up = false;
        c.synced = false;
        self.cv.notify_all();
    }

    fn acked(&self, pkid: u16) {
        let mut c = self.lock();
        if c.barrier == Some(pkid) {
            c.synced = true;
            self.cv.notify_all();
        }
    }

    fn message(&self, conn: &Conn, m: Message) {
        let Some(topic) = m.topic else { return conn.ack(m.ack) };
        for (prefix, is_room) in [("rooms/", true), ("agents/", false)] {
            if let Some(key) = topic.strip_prefix(prefix) {
                if is_ident(key, ID_MAX) {
                    let rec = if is_room { rooms::parse_room(&m.payload) } else { parse_presence(&m.payload) };
                    let mut c = self.lock();
                    let cache = if is_room { &mut c.rooms } else { &mut c.agents };
                    match rec {
                        Some(rec) => cache.insert(key.to_owned(), rec),
                        None => cache.remove(key),
                    };
                }
                return conn.ack(m.ack);
            }
        }
        if let Some(msg) = parse_message(&topic, &m.payload) {
            let mut c = self.lock();
            // Unacked QoS 1 messages are already capped by ReceiveMaximum, only QoS 0 floods need BUFFER_MAX.
            if m.qos > 0 || c.buf.len() < BUFFER_MAX {
                c.buf.push_back(((m.qos > 0).then_some(m.ack), msg));
                self.cv.notify_all();
                drop(c);
                if let Some(f) = self.on_arrival.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
                    f();
                }
                return;
            }
        }
        conn.ack(m.ack)
    }
}

fn topics_of(rec: &Map) -> Vec<String> {
    presence::topics(rec).map(str::to_owned).collect()
}

fn str_of<'a>(rec: &'a Map, key: &str) -> Option<&'a str> {
    rec.get(key).and_then(Value::as_str).filter(|s| !s.is_empty())
}

impl Bus {
    fn open(addr: &str, rec: Map) -> Result<Bus> {
        let shared = Arc::new(Shared {
            addr: addr.to_owned(),
            st: Mutex::new(Cache { topics: topics_of(&rec), ..Default::default() }),
            cv: Condvar::new(),
            on_arrival: Mutex::new(None),
        });
        // mosquitto only writes a session to disk if its latest connect had clean_start=False, even under MQTT 5.
        let conn = broker::connect(addr, false, presence::expiry(&rec), true, shared.clone())?;
        Ok(Bus { addr: addr.to_owned(), rec, conn, shared, touched: None })
    }

    /// Attach to a live address.
    pub fn attach(addr: &str) -> Result<Bus> {
        let addr = check_ident(addr, "address")?;
        let rec = presence::need(addr)?;
        let mut bus = Bus::open(addr, rec)?;
        if bus.fresh_pid().iter().any(|(k, v)| bus.rec.get(k) != Some(v)) {
            bus.touch(false)?;
        }
        Ok(bus)
    }

    /// Join the bus. With addr, reattach to it if it is live (its record stays as is), else create it.
    /// subagent marks a subagent's own agent: "kind": "subagent" and the creating session as "parent" instead of
    /// "session", so neither the session's relay nor the console mistakes it for the session itself.
    pub fn register(name: &str, caps: &[String], keep: bool, addr: Option<&str>, subagent: bool) -> Result<Bus> {
        not_admin(Some(name), "name")?;
        not_admin(addr, "address")?;
        let cap = if addr.is_some_and(|a| !a.is_empty()) { ID_MAX } else { NAME_MAX };
        let mut rec = Map::new();
        rec.insert("name".into(), check_ident_cap(name, "name", cap)?.into());
        rec.insert("caps".into(), check_caps(caps)?.into());
        rec.insert("topics".into(), Value::Array(vec![]));
        rec.insert("keep".into(), keep.into());
        // Origin fields are written here, once; later attaches only refresh pid and pid_start (see fresh_pid).
        let mut here = identity::origin().clone();
        if subagent {
            rec.insert("kind".into(), "subagent".into());
            if let Some(session) = here.shift_remove("session") {
                rec.insert("parent".into(), session);
            }
        }
        rec.extend(here);
        let addr = match addr {
            Some(a) => {
                match presence::find(a)? {
                    Some(live) => rec = live,
                    None => presence::wipe(a)?,
                }
                a.to_owned()
            }
            None => format!("{name}-{}", ledger::random_hex(3)),
        };
        history::prune(None)?;
        let mut bus = Bus::open(&addr, rec)?;
        bus.touch(false)?;
        Ok(bus)
    }

    /// Called on the reader thread after each buffered message.
    pub fn on_arrival(&self, f: impl Fn() + Send + Sync + 'static) {
        *self.shared.on_arrival.lock().unwrap_or_else(|e| e.into_inner()) = Some(Box::new(f));
    }

    fn set_rec(&mut self, rec: Map) {
        self.shared.lock().topics = topics_of(&rec);
        self.rec = rec;
    }

    fn ready(&self) -> Result<()> {
        let c = self.shared.lock();
        let (c, _) = self.shared.cv.wait_timeout_while(c, TIMEOUT, |c| !c.up).unwrap_or_else(|e| e.into_inner());
        let (c, _) = self.shared.cv.wait_timeout_while(c, TIMEOUT, |c| !c.synced).unwrap_or_else(|e| e.into_inner());
        if c.up && c.synced { Ok(()) } else { fail("not connected to the broker") }
    }

    /// addr's presence record, from this connection's cache: complete once connected, then kept current.
    fn seen(&self, addr: &str) -> Result<Option<Map>> {
        self.ready()?;
        Ok(self.shared.lock().agents.get(addr).cloned())
    }

    fn current(&self) -> Result<Map> {
        Ok(self.seen(&self.addr)?.unwrap_or_else(|| self.rec.clone()))
    }

    fn touch(&mut self, refresh: bool) -> Result<()> {
        if refresh {
            let rec = self.current()?;
            self.set_rec(rec);
        }
        for (k, v) in self.fresh_pid() {
            self.rec.insert(k, v);
        }
        self.rec.insert("seen".into(), float(now()));
        self.ready()?;
        let expire = (!presence::keep(&self.rec)).then(|| config::get().stale_s.min(u32::MAX as u64) as u32);
        broker::publish(&self.conn, &format!("agents/{}", self.addr), dumps_map(&self.rec), true, expire)?;
        // The broker echoes this publish before confirming it, so no older echo can overwrite the entry afterwards.
        self.shared.lock().agents.insert(self.addr.clone(), self.rec.clone());
        self.touched = Some(Instant::now());
        Ok(())
    }

    /// pid and pid_start of this process's claude, when it runs in the very session the record names (its session,
    /// or a subagent's parent); else nothing. Any other process acting as the agent, a moderator's kick among them,
    /// must not rewrite who the agent belongs to.
    fn fresh_pid(&self) -> Map {
        let here = identity::origin();
        let owner = str_of(&self.rec, "session").or_else(|| str_of(&self.rec, "parent"));
        let mut out = Map::new();
        if let Some(owner) = owner
            && here.contains_key("pid")
            && str_of(here, "session").unwrap_or("").to_lowercase() == owner.to_lowercase()
        {
            out.insert("pid".into(), here["pid"].clone());
            out.insert("pid_start".into(), here["pid_start"].clone());
        }
        out
    }

    /// Subscribe or unsubscribe; true if that changed the agent's topics.
    fn subscribe(&mut self, topic: &str, on: bool) -> Result<bool> {
        // the whole record: another process may have changed any of it
        let rec = self.current()?;
        self.set_rec(rec);
        let mut topics: BTreeSet<String> = topics_of(&self.rec).into_iter().collect();
        let had = topics.contains(topic);
        if on && !had && topics.len() >= TOPICS_MAX {
            return fail(format!("at most {TOPICS_MAX} subscriptions"));
        }
        self.ready()?;
        let pkid = if on {
            self.conn.subscribe(vec![filter(topic, true, true)])
        } else {
            self.conn.unsubscribe(vec![topic.to_owned()])
        };
        if pkid.ok().and_then(|p| self.conn.wait_ack(p, TIMEOUT)) != Some(true) {
            return fail(format!("broker refused or did not confirm {} {topic}", if on { "sub" } else { "unsub" }));
        }
        if on {
            topics.insert(topic.to_owned());
        } else {
            topics.remove(topic);
        }
        let mut rec = self.rec.clone();
        rec.insert("topics".into(), topics.into_iter().collect::<Vec<_>>().into());
        self.set_rec(rec);
        self.touch(false)?;
        Ok(had != on)
    }

    fn room_rec(&self, room: &str) -> Option<Map> {
        self.shared.lock().rooms.get(room).cloned()
    }

    fn announce(&self, room: &str, verb: &str) -> Result<()> {
        if rooms::refusal(self.room_rec(room).as_ref(), &self.addr).is_some() {
            return Ok(());
        }
        let caps: Vec<&str> = self.rec["caps"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
        let name = self.rec["name"].as_str().unwrap_or("");
        let mut body = format!("[{verb} #{room}] {name}");
        if !caps.is_empty() {
            body += &format!(", caps: {}", caps.join(", "));
        }
        self.send(&format!("room/{room}"), &body, None)
    }

    fn send(&self, topic: &str, body: &str, mentions: Option<Vec<String>>) -> Result<()> {
        let mut entry = Map::new();
        entry.insert("from".into(), self.addr.clone().into());
        entry.insert("body".into(), check_body(body)?.into());
        entry.insert("ts".into(), float(now()));
        if let Some(m) = mentions.filter(|m| !m.is_empty()) {
            entry.insert("mentions".into(), m.into());
        }
        self.ready()?;
        broker::publish(&self.conn, topic, dumps_map(&entry), false, None)?;
        history::append(topic, &entry)
    }

    /// Delete this agent: its ledger, session, queued messages, presence and received DMs in the log. Its rooms get
    /// a "[left #room]" message. Returns a warning if ledgerd could not drop the ledger; the agent leaves regardless.
    pub fn leave(mut self) -> Result<Option<String>> {
        let warning = match ledger::call("drop", &self.addr, Map::new(), Some(3.0)) {
            Err(e) if !e.to_string().starts_with("no ledger for") => Some(format!("ledger not dropped: {e}")),
            _ => None,
        };
        for topic in topics_of(&self.current()?) {
            if let Some(room) = topic.strip_prefix("room/") {
                let _ = self.announce(room, "left");
            }
        }
        self.close();
        presence::wipe(&self.addr)?;
        history::prune(Some(&format!("inbox/{}", self.addr)))?;
        Ok(warning)
    }

    /// on exempts the agent from idle expiry, off opts back in.
    pub fn keep(&mut self, on: bool) -> Result<()> {
        let mut rec = self.current()?;
        rec.insert("keep".into(), on.into());
        self.set_rec(rec);
        self.touch(false)?;
        self.close();
        // the session expiry interval only changes on connect
        self.conn = broker::connect(&self.addr, false, presence::expiry(&self.rec), true, self.shared.clone())?;
        Ok(())
    }

    /// Join a room; its members get a "[joined #room]" message unless this agent was already in it.
    pub fn join(&mut self, room: &str) -> Result<()> {
        self.ready()?;
        let room = check_ident(room, "room")?;
        if self.room_rec(room).is_some_and(|r| rooms::listed(&r, "banned", &self.addr)) {
            return fail(format!("you are banned from #{room}"));
        }
        if self.subscribe(&format!("room/{room}"), true)? {
            self.announce(room, "joined")?;
        }
        Ok(())
    }

    /// Leave a room; its members get a "[left #room]" message unless this agent was not in it.
    pub fn part(&mut self, room: &str) -> Result<()> {
        let room = check_ident(room, "room")?;
        if self.subscribe(&format!("room/{room}"), false)? {
            self.announce(room, "left")?;
        }
        Ok(())
    }

    pub fn sub(&mut self, topic: &str) -> Result<()> {
        self.subscribe(check_topic(topic, true)?, true).map(drop)
    }

    pub fn unsub(&mut self, topic: &str) -> Result<()> {
        self.subscribe(check_topic(topic, true)?, false).map(drop)
    }

    /// Post to a room. @address or @name (every member with that name) mentions members: their relays wake for it,
    /// other relays keep it quiet. @all mentions nobody in particular, like no mention.
    pub fn say(&self, room: &str, body: &str) -> Result<()> {
        self.ready()?;
        let room = check_ident(room, "room")?;
        if let Some(why) = rooms::refusal(self.room_rec(room).as_ref(), &self.addr) {
            return fail(format!("you may not post in #{room}: {why}"));
        }
        let mut mentions = None;
        if !rooms::mention_tokens(check_body(body)?).is_empty() {
            let members: Vec<(String, Map)> = {
                let c = self.shared.lock();
                c.agents
                    .iter()
                    .filter(|(_, r)| presence::in_room(r, room))
                    .map(|(a, r)| (a.clone(), r.clone()))
                    .collect()
            };
            mentions = rooms::mentions(room, body, &members)?;
        }
        self.send(&format!("room/{room}"), body, mentions)
    }

    pub fn dm(&self, to: &str, body: &str) -> Result<()> {
        check_body(body)?;
        if self.seen(check_ident(to, "address")?)?.is_none() {
            return fail(format!("unknown address: {to}"));
        }
        self.send(&format!("inbox/{to}"), body, None)
    }

    pub fn publish(&self, topic: &str, body: &str) -> Result<()> {
        self.send(check_topic(topic, false)?, body, None)
    }

    /// Drain up to limit buffered messages, blocking up to wait seconds for the first. They are acked once returned,
    /// or with deliver once deliver(messages) returns; if it fails they go back to the buffer.
    pub fn recv(&mut self, wait: f64, limit: Option<usize>, deliver: Option<Deliver>) -> Result<Vec<Map>> {
        check_wait(wait)?;
        // while reconnecting, buffered messages may still be resent, so wait until connected() sorted them
        self.ready()?;
        if self.touched.is_none_or(|t| t.elapsed() > TOUCH) {
            self.touch(true)?;
        }
        let got: Vec<(Option<Ack>, Map)> = {
            let c = self.shared.lock();
            let (mut c, _) = self
                .shared
                .cv
                .wait_timeout_while(c, Duration::from_secs_f64(wait), |c| c.buf.is_empty())
                .unwrap_or_else(|e| e.into_inner());
            let n = limit.unwrap_or(c.buf.len()).min(c.buf.len());
            c.buf.drain(..n).collect()
        };
        let msgs: Vec<Map> = got.iter().filter(|(_, m)| self.allowed(m)).map(|(_, m)| self.for_me(m)).collect();
        if let Some(deliver) = deliver
            && !msgs.is_empty()
            && let Err(e) = deliver(&msgs)
        {
            let mut c = self.shared.lock();
            for item in got.into_iter().rev() {
                c.buf.push_front(item);
            }
            return Err(e);
        }
        for (ack, _) in got {
            if let Some(ack) = ack {
                self.conn.ack(ack);
            }
        }
        Ok(msgs)
    }

    fn for_me(&self, m: &Map) -> Map {
        let mut m = m.clone();
        if let Some(mentions) = m.get("mentions").and_then(Value::as_array) {
            let mine = mentions.iter().any(|a| *a == *self.addr);
            m.insert("for_me".into(), mine.into());
        }
        m
    }

    /// False for a room message its room's record forbids: from a banned or muted address, or a non-moderator in an
    /// announce room. Spoofed or raw publishes are dropped too, unless they claim a moderator's address.
    fn allowed(&self, m: &Map) -> bool {
        let topic = m["topic"].as_str().unwrap_or("");
        let Some(room) = topic.strip_prefix("room/") else {
            return true;
        };
        rooms::refusal(self.room_rec(room).as_ref(), m["from"].as_str().unwrap_or("")).is_none()
    }

    /// Apply change(record) -> (fields, notes) to room's record as this agent, if it may, then post each note to the
    /// room as "<note> by <this address>". Returns the room's new settings.
    fn moderate(&self, room: &str, change: impl FnOnce(&Map) -> Result<(Map, Vec<String>)>) -> Result<Map> {
        let room = check_ident(room, "room")?;
        self.ready()?;
        let current = self.room_rec(room);
        if !rooms::is_moderator(current.as_ref(), &self.addr) {
            let first = if current.is_some() { "" } else { ", admin names the first" };
            return fail(format!("only admin or a moderator of #{room} may change it{first}"));
        }
        let mut d = rooms::open();
        d.extend(current.unwrap_or_default());
        let (fields, notes) = change(&d)?;
        d.extend(fields);
        d.insert("by".into(), self.addr.clone().into());
        d.insert("ts".into(), float(now()));
        let rec = rooms::check_record(&d)?;
        broker::publish(&self.conn, &format!("rooms/{room}"), dumps_map(&rec), true, None)?;
        self.shared.lock().rooms.insert(room.to_owned(), rec.clone());
        for note in notes {
            self.send(&format!("room/{room}"), &format!("{note} by {}", self.addr), None)?;
        }
        Ok(rooms::view(room, Some(&rec)))
    }

    /// Change a room's mode ("open" or "announce") and/or its rules text; admin or a moderator only.
    pub fn room_set(&self, room: &str, mode: Option<&str>, rules: Option<&str>) -> Result<Map> {
        if mode.is_none() && rules.is_none() {
            return fail("nothing to change: give mode or rules");
        }
        self.moderate(room, |_| {
            let (mut fields, mut notes) = (Map::new(), Vec::new());
            if let Some(mode) = mode {
                fields.insert("mode".into(), rooms::check_mode(&mode.into())?.into());
                notes.push(format!("[mode #{room}] {mode}"));
            }
            if let Some(rules) = rules {
                fields.insert("rules".into(), rooms::check_rules(&rules.into())?.into());
                notes.push(format!("[rules #{room}] updated"));
            }
            Ok((fields, notes))
        })
    }

    /// action "add" or "remove" addr as a moderator of room.
    pub fn moderator(&self, room: &str, action: &str, addr: &str) -> Result<Map> {
        match action {
            "add" => self.listed(room, addr, "moderators", true, "moderator", "+"),
            "remove" => self.listed(room, addr, "moderators", false, "moderator", "-"),
            _ => fail("action must be add or remove"),
        }
    }

    /// mute, unmute, ban, unban or kick addr in room. A kick bans addr and drops its subscription, through a brief
    /// connection with addr's client id that takes its session over: its relay or MCP server reconnects a second later.
    pub fn moderate_member(&self, verb: &str, room: &str, addr: &str) -> Result<Map> {
        let (field, on) = match verb {
            "mute" => ("muted", true),
            "unmute" => ("muted", false),
            "ban" | "kick" => ("banned", true),
            "unban" => ("banned", false),
            _ => return fail(format!("unknown moderation {verb}")),
        };
        let verb = match verb {
            "mute" | "unmute" => format!("{verb}d"),
            "ban" | "unban" => format!("{verb}ned"),
            _ => "kicked".into(),
        };
        let settings = self.listed(room, addr, field, on, &verb, "")?;
        if verb == "kicked" && self.seen(addr)?.is_some() {
            let mut target = Bus::attach(addr)?;
            target.subscribe(&format!("room/{room}"), false)?;
        }
        Ok(settings)
    }

    fn listed(&self, room: &str, addr: &str, field: &str, on: bool, verb: &str, sign: &str) -> Result<Map> {
        let target = check_ident(addr, "address")?;
        if target.to_lowercase() == ADMIN {
            return fail("admin moderates every room and cannot be moderated");
        }
        self.moderate(room, |rec| {
            let mut set: BTreeSet<String> =
                rec[field].as_array().into_iter().flatten().filter_map(Value::as_str).map(str::to_owned).collect();
            if on {
                set.insert(target.to_owned());
            } else {
                set.remove(target);
            }
            let mut fields = Map::new();
            fields.insert(field.into(), set.into_iter().collect::<Vec<_>>().into());
            Ok((fields, vec![format!("[{verb} #{room}] {sign}{target}")]))
        })
    }

    /// Disconnect; the session and anything not yet received stay with the broker.
    pub fn close(&mut self) {
        self.conn.close();
        self.shared.lock().up = false;
    }
}

impl Drop for Bus {
    fn drop(&mut self) {
        self.close();
    }
}

impl From<ledger::LedgerError> for Error {
    fn from(e: ledger::LedgerError) -> Error {
        Error(e.to_string())
    }
}
