//! Each command, on the core library.

use crate::args::{Cmd, Details, Hook, Moderation, OnOff, RoomCmd};
use crate::relay;
use agent_bridge::bus::Bus;
use agent_bridge::json::{Map, dumps_map, float};
use agent_bridge::validation::{check_ident, check_wait, not_admin, os_error};
use agent_bridge::{Error, Result, config, history, ledger, presence, rooms};
use serde_json::Value;
use std::fs::{File, OpenOptions};
use std::io::Write;

const QUIET: f64 = 0.2;

/// Print one line; a closed stdout ends the command quietly with exit 1, like Python's BrokenPipeError handling.
pub fn out(line: &str) {
    let mut stdout = std::io::stdout().lock();
    if writeln!(stdout, "{line}").and_then(|_| stdout.flush()).is_err() {
        std::process::exit(1);
    }
}

pub fn show_all(items: &[Map]) {
    for item in items {
        out(&dumps_map(item));
    }
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for b in data {
        crc ^= *b as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 { (crc >> 1) ^ 0xEDB8_8320 } else { crc >> 1 };
        }
    }
    !crc
}

/// Make this machine's CLI commands for one address take turns instead of taking over each other's session. Held
/// until the returned file closes.
fn turn(addr: &str) -> Result<File> {
    let addr = check_ident(addr, "address")?;
    let path = config::data_dir()?.join("cli.lock");
    let f = OpenOptions::new().append(true).create(true).open(&path).map_err(|e| os_error(&e, Some(&path)))?;
    // One byte per address hash: a single lock file serves every address, collisions only cost waiting.
    let byte = libc::flock {
        l_type: libc::F_WRLCK as libc::c_short,
        l_whence: libc::SEEK_SET as libc::c_short,
        l_start: crc32(addr.as_bytes()) as libc::off_t,
        l_len: 1,
        l_pid: 0,
    };
    nix::fcntl::fcntl(&f, nix::fcntl::FcntlArg::F_SETLKW(&byte)).map_err(|e| os_error(&e.into(), Some(&path)))?;
    Ok(f)
}

fn attached<T>(addr: &str, take_turn: bool, f: impl FnOnce(&Bus) -> Result<T>) -> Result<T> {
    not_admin(Some(addr), "address")?;
    let _turn = if take_turn { Some(turn(addr)?) } else { None };
    f(&Bus::attach(addr)?)
}

fn shown(addr: &str, f: impl FnOnce(&Bus) -> Result<Map>) -> Result<()> {
    attached(addr, true, |b| f(b).map(|m| show_all(&[m])))
}

fn register(name: &str, caps: &str, keep: bool, addr: Option<&str>, subagent: bool) -> Result<()> {
    let _turn = addr.map(turn).transpose()?;
    let caps: Vec<String> = caps.split(',').filter(|c| !c.is_empty()).map(str::to_owned).collect();
    let bus = Bus::register(name, &caps, keep, addr, subagent, true)?;
    let addr = bus.addr.clone();
    drop(bus);
    out(&addr);
    Ok(())
}

fn leave(addr: &str) -> Result<()> {
    not_admin(Some(addr), "address")?;
    let _turn = turn(addr)?;
    if let Some(warning) = Bus::attach(addr)?.leave()? {
        eprintln!("warning: {warning}");
    }
    Ok(())
}

fn recv(addr: &str, wait: f64) -> Result<()> {
    attached(addr, wait == 0.0, |b| {
        // Queued messages come right after CONNACK, ahead of the Bus's connect barrier, so recv(0) already sees them.
        let mut msgs = b.recv(check_wait(wait)?, None, None)?;
        while !msgs.is_empty() {
            show_all(&msgs);
            msgs = b.recv(QUIET, None, None)?;
        }
        Ok(())
    })
}

fn moderation(verb: &str, m: &Moderation) -> Result<()> {
    shown(&m.addr, |b| b.moderate_member(verb, &m.room, &m.target))
}

/// A ledger command: init MY_ADDR's ledger, send op with the given fields, print the reply without "ok".
fn ledger_op(addr: &str, op: &str, fields: &[(&str, Option<Value>)]) -> Result<()> {
    not_admin(Some(addr), "address")?;
    presence::need(addr)?;
    let args: Map = fields.iter().filter_map(|(k, v)| Some(((*k).to_owned(), v.clone()?))).collect();
    let plain = |e: ledger::LedgerError| {
        Error(format!("ledger: {}", e.to_string().split_whitespace().collect::<Vec<_>>().join(" ")))
    };
    ledger::call("init", addr, Map::new(), None).map_err(plain)?;
    let mut reply = ledger::call(op, addr, args, None).map_err(plain)?;
    reply.shift_remove("ok");
    show_all(&[reply]);
    Ok(())
}

/// Python sends NaN and infinities as JSON literals, which ledgerd refuses as numbers; {} gets the same answer.
fn number(v: f64) -> Value {
    if v.is_finite() { float(v) } else { Value::Object(Map::new()) }
}

fn text(v: &Option<String>) -> Option<Value> {
    v.clone().map(Value::String)
}

fn details(d: &Details) -> [(&'static str, Option<Value>); 3] {
    [
        ("description", text(&d.description)),
        ("visibility", d.visibility.map(|v| v.name().into())),
        ("max_level", d.max_level.map(|v| v.name().into())),
    ]
}

pub fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Register { name, caps, keep, addr, subagent } => register(&name, &caps, keep, addr.as_deref(), subagent),
        Cmd::Leave { addr } => leave(&addr),
        Cmd::Keep { addr, state } => attached(&addr, true, |b| b.keep(matches!(state, OnOff::On))),
        Cmd::Agents { room } => presence::agents(room.as_deref()).map(|a| show_all(&a)),
        Cmd::Rooms => rooms::listing().map(|r| show_all(&r)),
        Cmd::Join { addr, room } => attached(&addr, true, |b| b.join(&room)),
        Cmd::Part { addr, room } => attached(&addr, true, |b| b.part(&room)),
        Cmd::Say { addr, room, text } => attached(&addr, true, |b| b.say(&room, &text)),
        Cmd::Room { action: RoomCmd::Info { room } } => rooms::info(&room).map(|r| show_all(&[r])),
        Cmd::Room { action: RoomCmd::Set { addr, room, mode, rules } } => {
            let mode = mode.map(|m| m.name());
            shown(&addr, |b| b.room_set(&room, mode.map(Value::from), rules.map(Value::from)))
        }
        Cmd::Mod { addr, room, change, target } => shown(&addr, |b| b.moderator(&room, &change.name(), &target)),
        Cmd::Mute(m) => moderation("mute", &m),
        Cmd::Unmute(m) => moderation("unmute", &m),
        Cmd::Ban(m) => moderation("ban", &m),
        Cmd::Unban(m) => moderation("unban", &m),
        Cmd::Kick(m) => moderation("kick", &m),
        Cmd::History { room, last } => history::client::room(&room, last, None).map(|h| show_all(&h)),
        Cmd::Dm { addr, to, text } => attached(&addr, true, |b| b.dm(&to, &text)),
        Cmd::Dms { addr, peer, last } => history::client::dms(&addr, peer.as_deref(), last).map(|h| show_all(&h)),
        Cmd::Sub { addr, topic } => attached(&addr, true, |b| b.sub(&topic)),
        Cmd::Unsub { addr, topic } => attached(&addr, true, |b| b.unsub(&topic)),
        Cmd::Pub { addr, topic, text } => attached(&addr, true, |b| b.publish(&topic, &text)),
        // A waiting recv can be long, so it does not take a turn: other commands take its session over and it
        // reconnects.
        Cmd::Recv { addr, wait } => recv(&addr, wait),
        Cmd::Relay { action, run, session } => relay::command(action, &run, session),
        Cmd::Hook { which: Hook::Broadcast } => {
            crate::hooks::broadcast();
            Ok(())
        }
        Cmd::Hook { which: Hook::RelayStart } => {
            crate::hooks::relay_start();
            Ok(())
        }
        Cmd::Add { addr, name, details: d } => {
            ledger_op(&addr, "add", &[[("name", Some(name.into()))].as_slice(), &details(&d)].concat())
        }
        Cmd::Update { addr, name, details: d } => {
            ledger_op(&addr, "update", &[[("name", Some(name.into()))].as_slice(), &details(&d)].concat())
        }
        Cmd::Remove { addr, name } => ledger_op(&addr, "remove", &[("name", Some(name.into()))]),
        Cmd::Ledger { addr, owner } => ledger_op(&addr, "ledger", &[("owner", text(&owner))]),
        Cmd::Public { addr } => ledger_op(&addr, "public", &[]),
        Cmd::Request { addr, resource, level } => {
            ledger_op(&addr, "request", &[("resource", Some(resource.into())), ("level", Some(level.name().into()))])
        }
        Cmd::Grant { addr, request_id } => ledger_op(&addr, "grant", &[("request_id", Some(request_id.into()))]),
        Cmd::Deny { addr, request_id } => ledger_op(&addr, "deny", &[("request_id", Some(request_id.into()))]),
        Cmd::Grants { addr } => ledger_op(&addr, "grants", &[]),
        Cmd::Acquire { addr, resource, level, ttl, wait } => ledger_op(
            &addr,
            "acquire",
            &[
                ("resource", Some(resource.into())),
                ("level", Some(level.name().into())),
                ("ttl", ttl.map(number)),
                ("wait", wait.map(number)),
            ],
        ),
        Cmd::Renew { addr, lease_id, ttl } => {
            ledger_op(&addr, "renew", &[("lease_id", Some(lease_id.into())), ("ttl", ttl.map(number))])
        }
        Cmd::Release { addr, lease_id } => ledger_op(&addr, "release", &[("lease_id", Some(lease_id.into()))]),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn crc32_is_zlibs() {
        assert_eq!(super::crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(super::crc32(b""), 0);
    }
}
