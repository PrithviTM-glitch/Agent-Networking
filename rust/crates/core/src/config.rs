//! Settings read from the environment once, and where the bridge keeps its files.

use crate::validation::py_repr;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

pub struct Config {
    pub host: String,
    pub port: u16,
    pub stale_days: f64,
    pub stale_s: u64,
    pub state_dir: PathBuf,
    pub log: PathBuf,
}

static CONFIG: OnceLock<Config> = OnceLock::new();

/// The settings; a bad AGENT_BRIDGE_PORT or AGENT_BRIDGE_STALE_DAYS exits with a one-line message, as Python does
/// at import.
pub fn get() -> &'static Config {
    CONFIG.get_or_init(|| match load() {
        Ok(c) => c,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(1);
        }
    })
}

/// A numeric environment variable within [lo, hi].
pub fn env_number(name: &str, default: &str, lo: f64, hi: f64) -> Result<f64, String> {
    let raw = env::var(name).unwrap_or_else(|_| default.to_owned());
    match raw.trim().parse::<f64>() {
        Ok(v) if (lo..=hi).contains(&v) => Ok(v),
        _ => Err(format!("{name} must be a number from {lo} to {hi}, got {}", py_repr(&raw))),
    }
}

fn load() -> Result<Config, String> {
    let port = env_number("AGENT_BRIDGE_PORT", "1883", 1.0, 65535.0)? as u16;
    let stale_days = env_number("AGENT_BRIDGE_STALE_DAYS", "3", 0.0, 36500.0)?;
    let state_dir = xdg("XDG_STATE_HOME", ".local/state");
    let log = match env::var_os("AGENT_BRIDGE_LOG").filter(|v| !v.is_empty()) {
        Some(v) => PathBuf::from(v),
        None => state_dir.join("history.jsonl"),
    };
    Ok(Config {
        host: env::var("AGENT_BRIDGE_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
        port,
        stale_days,
        stale_s: ((stale_days * 86400.0) as u64).max(1),
        state_dir,
        log,
    })
}

fn xdg(variable: &str, fallback: &str) -> PathBuf {
    let base =
        env::var_os(variable).filter(|v| !v.is_empty()).map(PathBuf::from).unwrap_or_else(|| home().join(fallback));
    base.join("agent-bridge")
}

fn home() -> PathBuf {
    if let Some(h) = env::var_os("HOME") {
        return PathBuf::from(h);
    }
    nix::unistd::User::from_uid(nix::unistd::getuid()).ok().flatten().map_or_else(|| PathBuf::from("/"), |u| u.dir)
}

/// A path in the directory of the history log, which may not exist yet. The bridge's other host files live there:
/// cli.lock, relay/, hook-state/, prune.stamp and admin-audit.jsonl.
pub fn data_path(parts: &[&str]) -> PathBuf {
    let dir = get().log.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
    parts.iter().fold(dir.to_path_buf(), |p, part| p.join(part))
}

/// The directory of the history log, created if missing.
pub fn data_dir() -> crate::Result<PathBuf> {
    let path = data_path(&[]);
    std::fs::create_dir_all(&path).map_err(|e| crate::validation::os_error(&e, Some(&path)))?;
    Ok(path)
}
