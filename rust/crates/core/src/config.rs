//! Settings from the environment, re-read on every call so a process (or a test) can change them, and where the
//! bridge keeps its files. Long-lived services read them once at start.

use crate::validation::{Error, py_repr};
use std::env;
use std::path::{Path, PathBuf};

pub struct Config {
    pub host: String,
    pub port: u16,
    pub stale_days: f64,
    pub stale_s: u64,
    pub state_dir: PathBuf,
    pub log: PathBuf,
}

/// The settings now; a bad AGENT_BRIDGE_PORT or AGENT_BRIDGE_STALE_DAYS is an error with a one-line message.
pub fn get() -> crate::Result<Config> {
    load().map_err(Error)
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
pub fn data_path(parts: &[&str]) -> crate::Result<PathBuf> {
    let log = get()?.log;
    let dir = log.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
    Ok(parts.iter().fold(dir.to_path_buf(), |p, part| p.join(part)))
}

/// The directory of the history log, created if missing.
pub fn data_dir() -> crate::Result<PathBuf> {
    let path = data_path(&[])?;
    std::fs::create_dir_all(&path).map_err(|e| data_error(&e, &path))?;
    Ok(path)
}

/// An error creating or writing path in the data directory. Permission denied says which directory refused and how to
/// fix it: Docker creates a missing bind-mount source as root, which once made the CLI's lock file unwritable.
pub fn data_error(e: &std::io::Error, path: &Path) -> Error {
    if e.kind() != std::io::ErrorKind::PermissionDenied {
        return crate::validation::os_error(e, Some(path));
    }
    // the nearest existing ancestor is the one that refused
    let dir = path.ancestors().skip(1).find(|d| d.exists()).unwrap_or(path);
    let owner = std::fs::metadata(dir).map_or(0, |m| std::os::unix::fs::MetadataExt::uid(&m));
    denied(path, dir, owner, nix::unistd::getuid().as_raw())
}

fn denied(path: &Path, dir: &Path, owner: u32, me: u32) -> Error {
    if owner == me {
        return Error(format!(
            "cannot write {}: {} is not writable; fix it with: chmod u+w {}",
            path.display(),
            dir.display(),
            dir.display()
        ));
    }
    let whose = match nix::unistd::User::from_uid(owner.into()) {
        Ok(Some(u)) => u.name,
        _ => owner.to_string(),
    };
    Error(format!(
        "cannot write {}: {} belongs to {whose}, not you (Docker makes a missing bind-mount source root's); \
         take it back with: sudo chown -R \"$(id -un)\" {}",
        path.display(),
        dir.display(),
        dir.display()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn an_unwritable_data_dir_says_how_to_fix_it() {
        if nix::unistd::getuid().is_root() {
            return; // root writes anywhere
        }
        let dir = std::env::temp_dir().join(format!("agent-bridge-config-perm-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("a temp dir");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).expect("read-only");
        let path = dir.join("agent-bridge");
        let err = std::fs::create_dir_all(&path).map_err(|e| data_error(&e, &path)).unwrap_err();
        let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700));
        let _ = std::fs::remove_dir_all(&dir);
        let (p, d) = (path.display(), dir.display());
        assert_eq!(err.0, format!("cannot write {p}: {d} is not writable; fix it with: chmod u+w {d}"));
        // what a root-owned dir (Docker's doing) says, worded for a user who is not root
        let root = denied(Path::new("/s/agent-bridge/cli.lock"), Path::new("/s/agent-bridge"), 0, 1000).0;
        assert_eq!(
            root,
            "cannot write /s/agent-bridge/cli.lock: /s/agent-bridge belongs to root, not you (Docker makes a missing \
             bind-mount source root's); take it back with: sudo chown -R \"$(id -un)\" /s/agent-bridge"
        );
    }
}
