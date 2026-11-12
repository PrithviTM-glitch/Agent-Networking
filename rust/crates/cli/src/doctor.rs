//! `agent-bridge doctor`: read-only checks of this machine's bridge, each OK, WARN or FAIL. It connects to the broker
//! and asks ledgerd and historyd their versions, and only reads files: it never creates the state directory, starts a
//! relay or edits settings.

use crate::commands::{out, show_all};
use agent_bridge::json::Map;
use agent_bridge::relay::{binary, live};
use agent_bridge::{config, history, host, ledger};
use serde_json::{Value, json};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const VERSION: &str = env!("CARGO_PKG_VERSION");
/// how long ledgerd and historyd get to answer
const ANSWER_S: f64 = 3.0;

#[derive(Clone, Copy, PartialEq)]
enum Status {
    Ok,
    Warn,
    Fail,
}

struct Check {
    name: &'static str,
    status: Status,
    detail: String,
    extra: Map,
}

fn check(name: &'static str, status: Status, detail: impl Into<String>) -> Check {
    Check { name, status, detail: detail.into(), extra: Map::new() }
}

impl Check {
    fn with(mut self, key: &str, value: Value) -> Check {
        self.extra.insert(key.into(), value);
        self
    }
}

/// Run every check, print them (plain, or one JSON object per line), and exit 1 if any failed.
pub fn run(as_json: bool) -> agent_bridge::Result<()> {
    let cfg = config::get()?;
    let broker = broker(&cfg.host, cfg.port);
    let up = broker.status == Status::Ok;
    let ledgerd = if up { ledgerd() } else { check("ledgerd", Status::Fail, "not checked: no broker") };
    let historyd = if up { historyd() } else { check("historyd", Status::Fail, "not checked: no broker") };
    // None: not reached, so not compared; Some(None): answers without a version (older than 0.3.0)
    let services = [&ledgerd, &historyd]
        .map(|c| (c.status != Status::Fail).then(|| c.extra.get("version").and_then(Value::as_str).map(str::to_owned)));
    let data = config::data_path(&[])?;
    let checks = vec![
        broker,
        ledgerd,
        historyd,
        state_dir(&data),
        config_dir(&config::config_dir()),
        cli_lock(&data.join("cli.lock")),
        versions(&services),
        relays(&data.join("relay")),
        hooks(),
    ];
    for c in &checks {
        if as_json {
            let mut row = Map::new();
            row.insert("check".into(), c.name.into());
            let status = match c.status {
                Status::Ok => "ok",
                Status::Warn => "warn",
                Status::Fail => "fail",
            };
            row.insert("status".into(), status.into());
            row.insert("detail".into(), c.detail.clone().into());
            row.extend(c.extra.clone());
            show_all(&[row]);
        } else {
            let status = match c.status {
                Status::Ok => "OK",
                Status::Warn => "WARN",
                Status::Fail => "FAIL",
            };
            out(&format!("{status:<5} {:<11}{}", c.name, c.detail));
        }
    }
    if checks.iter().any(|c| c.status == Status::Fail) {
        std::process::exit(1);
    }
    Ok(())
}

fn broker(host: &str, port: u16) -> Check {
    let started = Instant::now();
    let addrs = std::net::ToSocketAddrs::to_socket_addrs(&(host, port)).map(|a| a.collect::<Vec<_>>());
    let reached = addrs
        .ok()
        .unwrap_or_default()
        .iter()
        .any(|a| std::net::TcpStream::connect_timeout(a, Duration::from_secs(2)).is_ok());
    if reached {
        let ms = started.elapsed().as_millis();
        check("broker", Status::Ok, format!("{host}:{port} accepts connections ({ms} ms)"))
    } else {
        check("broker", Status::Fail, format!("nothing answers at {host}:{port}; start it with: docker compose up -d"))
    }
}

/// A service's ping answer: its version, or why there is none.
fn service(name: &'static str, answer: Result<Map, String>) -> Check {
    match answer {
        Ok(reply) => match reply.get("version").and_then(Value::as_str) {
            Some(v) => check(name, Status::Ok, format!("answers, version {v}")).with("version", v.into()),
            None => {
                check(name, Status::Warn, "answers, version unknown (older than 0.3.0)").with("version", Value::Null)
            }
        },
        // a pre-0.3.0 service answers, but refuses the version question
        Err(e) if e.contains("older than 0.3.0") || e.contains("unknown fields for ping") => {
            check(name, Status::Warn, "answers, version unknown (older than 0.3.0)").with("version", Value::Null)
        }
        Err(e) => {
            check(name, Status::Fail, format!("{e}; is it running? docker compose up -d")).with("version", Value::Null)
        }
    }
}

fn ledgerd() -> Check {
    // ping writes nothing and needs no ledger, so any well-formed address will do
    service("ledgerd", ledger::call("ping", "doctor", Map::new(), Some(ANSWER_S)).map_err(|e| e.to_string()))
}

fn historyd() -> Check {
    let fields = Map::from_iter([("version".to_owned(), Value::Bool(true))]);
    service("historyd", history::client::call("ping", None, fields, ANSWER_S).map_err(|e| e.to_string()))
}

fn me() -> u32 {
    nix::unistd::getuid().as_raw()
}

fn owner_name(uid: u32) -> String {
    nix::unistd::User::from_uid(uid.into()).ok().flatten().map_or_else(|| uid.to_string(), |u| u.name)
}

fn writable(path: &Path) -> bool {
    nix::unistd::access(path, nix::unistd::AccessFlags::W_OK).is_ok()
}

fn state_dir(dir: &Path) -> Check {
    let d = dir.display();
    let Ok(meta) = std::fs::metadata(dir) else {
        return check("state_dir", Status::Warn, format!("{d} does not exist yet; the first command creates it"));
    };
    if meta.uid() != me() {
        let whose = owner_name(meta.uid());
        return check(
            "state_dir",
            Status::Fail,
            format!("{d} belongs to {whose}, not you; take it back with: sudo chown -R \"$(id -un)\" {d}"),
        );
    }
    if !writable(dir) {
        return check("state_dir", Status::Fail, format!("{d} is not writable; fix it with: chmod u+w {d}"));
    }
    let mode = meta.permissions().mode() & 0o777;
    // group write is the norm under a umask of 002 with a per-user group; only other users' write access matters
    if mode & 0o002 != 0 {
        return check(
            "state_dir",
            Status::Warn,
            format!("{d} is {mode:o}: anyone can write it; fix it with: chmod o-w {d}"),
        );
    }
    check("state_dir", Status::Ok, format!("{d}, yours, {mode:o}"))
}

fn config_dir(dir: &Path) -> Check {
    let d = dir.display();
    let Ok(meta) = std::fs::metadata(dir) else {
        return check("config_dir", Status::Ok, format!("{d} not created yet; the console creates it"));
    };
    if meta.uid() != me() {
        let whose = owner_name(meta.uid());
        return check(
            "config_dir",
            Status::Fail,
            format!("{d} belongs to {whose}, not you; take it back with: sudo chown -R \"$(id -un)\" {d}"),
        );
    }
    let mode = meta.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        // the console's start fixes it, so this is a warning, not a failure
        return check("config_dir", Status::Warn, format!("{d} is {mode:o}, not 700; fix it with: chmod 700 {d}"));
    }
    check("config_dir", Status::Ok, format!("{d}, yours, 700"))
}

fn cli_lock(path: &Path) -> Check {
    let p = path.display();
    if path.exists() {
        return match writable(path) {
            true => check("cli_lock", Status::Ok, format!("{p} is writable")),
            false => check("cli_lock", Status::Fail, format!("{p} is not writable by you; see the state_dir check")),
        };
    }
    match path.parent().filter(|d| d.exists()) {
        Some(dir) if writable(dir) => {
            check("cli_lock", Status::Ok, format!("{p} not created yet; its directory is writable"))
        }
        Some(_) => check("cli_lock", Status::Fail, format!("{p} cannot be created; see the state_dir check")),
        None => check("cli_lock", Status::Ok, format!("{p} not created yet")),
    }
}

/// The version of the wheel this binary came from: the agent_bridge-VERSION.dist-info in the environment around its
/// bin directory, or None for a binary outside a wheel (a cargo build).
fn wheel_version() -> Option<String> {
    let exe = std::env::current_exe().ok()?.canonicalize().ok()?;
    let env = exe.parent()?.parent()?;
    let lib = std::fs::read_dir(env.join("lib")).ok()?;
    lib.flatten()
        .filter(|d| d.file_name().to_string_lossy().starts_with("python"))
        .filter_map(|d| std::fs::read_dir(d.path().join("site-packages")).ok())
        .flat_map(|entries| entries.flatten())
        .find_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            name.strip_prefix("agent_bridge-")?.strip_suffix(".dist-info").map(str::to_owned)
        })
}

fn versions(services: &[Option<Option<String>>; 2]) -> Check {
    let wheel = wheel_version();
    let mut parts = vec![format!("cli {VERSION}")];
    let mut skew = Vec::new();
    let found = [("ledgerd", services[0].clone()), ("historyd", services[1].clone()), ("wheel", Some(wheel.clone()))];
    for (name, v) in found {
        match v {
            Some(Some(v)) => {
                parts.push(format!("{name} {v}"));
                if v != VERSION {
                    skew.push(name);
                }
            }
            Some(None) if name == "wheel" => parts.push("no wheel around this binary".into()),
            Some(None) => {
                parts.push(format!("{name} older than 0.3.0"));
                skew.push(name);
            }
            None => parts.push(format!("{name} not reached")),
        }
    }
    let status = if skew.is_empty() { Status::Ok } else { Status::Warn };
    let detail = match skew.is_empty() {
        true => parts.join(", "),
        false => format!("{}; {} differ from this CLI: upgrade them together", parts.join(", "), skew.join(" and ")),
    };
    check("versions", status, detail)
        .with("cli", VERSION.into())
        .with("ledgerd", services[0].clone().flatten().into())
        .with("historyd", services[1].clone().flatten().into())
        .with("wheel", wheel.into())
}

/// The claude process a relay serves, from its command line (relay run PID START), if it still lives.
fn serving(pid: i32) -> Option<bool> {
    let cmdline = std::fs::read(format!("/proc/{pid}/cmdline")).ok()?;
    let args: Vec<String> = cmdline.split(|b| *b == 0).map(|a| String::from_utf8_lossy(a).into_owned()).collect();
    let at = args.iter().position(|a| a == "run")?;
    let (claude, start) = (args.get(at + 1)?.parse::<i64>().ok()?, args.get(at + 2)?.parse::<u64>().ok()?);
    Some(host::stat(claude).is_some_and(|p| p.start == start))
}

fn relays(dir: &Path) -> Check {
    let current = std::env::current_exe().ok().and_then(|p| p.canonicalize().ok());
    let Ok(entries) = std::fs::read_dir(dir) else { return check("relays", Status::Ok, "no relay has run yet") };
    let (mut fine, mut leftover, mut stale) = (0, 0, Vec::new());
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(session) = name.strip_suffix(".lock") else { continue };
        if !live(session) {
            leftover += 1;
            continue;
        }
        let Some(pid) = agent_bridge::relay::pid(session) else { continue };
        let exe = binary(pid).unwrap_or_default();
        let why = if exe.ends_with(" (deleted)") {
            Some("its binary was replaced")
        } else if current.as_ref().is_some_and(|c| Path::new(&exe) != c.as_path()) {
            Some("it runs another binary than this CLI")
        } else if serving(pid) == Some(false) {
            Some("its session has ended")
        } else {
            None
        };
        match why {
            None => fine += 1,
            Some(why) => stale.push(json!({"session": session, "pid": pid, "binary": exe, "why": why})),
        }
    }
    let counts =
        format!("{fine} current{}", if leftover > 0 { format!(", {leftover} old lock files") } else { String::new() });
    if stale.is_empty() {
        return check("relays", Status::Ok, counts).with("stale", json!([]));
    }
    let sessions: Vec<String> =
        stale.iter().map(|s| format!("{} (pid {})", s["session"].as_str().unwrap_or(""), s["pid"])).collect();
    check(
        "relays",
        Status::Warn,
        format!("{counts}, {} stale: {}; run agent-bridge relay restart in each", stale.len(), sessions.join(", ")),
    )
    .with("stale", stale.into())
}

/// The program a hook command runs: its first word, found on PATH unless it is a path.
fn program(command: &str) -> Option<PathBuf> {
    let first = command.split_whitespace().next()?.trim_matches(|c| c == '"' || c == '\'');
    if first.contains('/') {
        let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_default();
        return Some(match first.strip_prefix("~/") {
            Some(rest) => home.join(rest),
            None => PathBuf::from(first),
        });
    }
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path).map(|d| d.join(first)).find(|p| p.is_file()).or(Some(PathBuf::from(first)))
}

fn executable(path: &Path) -> bool {
    std::fs::metadata(path).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
}

fn hooks() -> Check {
    let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_default();
    let settings = home.join(".claude").join("settings.json");
    let Ok(text) = std::fs::read_to_string(&settings) else {
        return check("hooks", Status::Warn, format!("no {}: no hooks, so no relay or broadcasts", settings.display()));
    };
    let Ok(parsed) = serde_json::from_str::<Value>(&text) else {
        return check("hooks", Status::Fail, format!("{} is not valid JSON", settings.display()));
    };
    let mut commands = Vec::new();
    if let Some(events) = parsed.get("hooks").and_then(Value::as_object) {
        for groups in events.values().filter_map(Value::as_array) {
            for hook in groups.iter().filter_map(|g| g.get("hooks")).filter_map(Value::as_array).flatten() {
                if let Some(c) = hook.get("command").and_then(Value::as_str).filter(|c| c.contains("agent-bridge")) {
                    commands.push(c.to_owned());
                }
            }
        }
    }
    if commands.is_empty() {
        return check(
            "hooks",
            Status::Warn,
            "no agent-bridge hooks in ~/.claude/settings.json: the relay never starts on its own",
        );
    }
    let mut missing: Vec<String> = Vec::new();
    for p in commands.iter().filter_map(|c| program(c)) {
        if !executable(&p) && !missing.contains(&p.display().to_string()) {
            missing.push(p.display().to_string());
        }
    }
    match missing.is_empty() {
        true => {
            check("hooks", Status::Ok, format!("{} agent-bridge hooks, each runs an existing program", commands.len()))
        }
        false => check("hooks", Status::Fail, format!("missing: {}", missing.join(", "))),
    }
}
