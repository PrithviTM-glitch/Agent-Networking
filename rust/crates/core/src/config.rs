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
    pub auth: Auth,
}

/// The broker's authentication mode (docs/AUTH.md section 4.8): off is 0.3.1's anonymous bus; warn lets anonymous
/// clients in while credentialed ones are checked; enforce refuses anonymous clients.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Auth {
    Off,
    Warn,
    Enforce,
}

impl Auth {
    pub fn as_str(self) -> &'static str {
        match self {
            Auth::Off => "off",
            Auth::Warn => "warn",
            Auth::Enforce => "enforce",
        }
    }

    fn parse(raw: &str, source: &str) -> Result<Auth, String> {
        match raw.trim() {
            "off" => Ok(Auth::Off),
            "warn" => Ok(Auth::Warn),
            "enforce" => Ok(Auth::Enforce),
            _ => Err(format!("{source} must be off, warn or enforce, got {}", py_repr(raw.trim()))),
        }
    }
}

/// An identity's broker login: username is its address (or a service's or admin's name), and client_id is the secret
/// id its owning connection uses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Credentials {
    pub username: String,
    pub password: String,
    pub client_id: String,
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
        auth: load_auth()?,
    })
}

/// AGENT_BRIDGE_AUTH (compose passes it to the services), else the auth-mode file the broker's entrypoint writes, else
/// off.
fn load_auth() -> Result<Auth, String> {
    if let Ok(raw) = env::var("AGENT_BRIDGE_AUTH") {
        return Auth::parse(&raw, "AGENT_BRIDGE_AUTH");
    }
    let path = config_dir().join("auth-mode");
    match std::fs::read_to_string(&path) {
        Ok(raw) => Auth::parse(&raw, &path.display().to_string()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Auth::Off),
        Err(e) => Err(format!("cannot read {}: {e}", path.display())),
    }
}

/// The broker's authentication mode now.
pub fn auth_mode() -> crate::Result<Auth> {
    load_auth().map_err(Error)
}

/// addr's credentials: the AGENT_BRIDGE_CREDENTIALS file (a service's, mounted into its container) when its username
/// is addr, else agents/<addr>.json in the config dir, or admin.json for "admin". None when there is no file; a file
/// that is unreadable, malformed, or readable by anyone but its owner is an error naming it.
pub fn credentials(addr: &str) -> crate::Result<Option<Credentials>> {
    if let Some(path) = env::var_os("AGENT_BRIDGE_CREDENTIALS").filter(|v| !v.is_empty())
        && let Some(c) = read_credentials(Path::new(&path))?.filter(|c| c.username == addr)
    {
        return Ok(Some(c));
    }
    let path = if addr == "admin" {
        config_dir().join("admin.json")
    } else {
        crate::validation::check_ident(addr, "address")?;
        config_dir().join("agents").join(format!("{addr}.json"))
    };
    read_credentials(&path)
}

fn read_credentials(path: &Path) -> crate::Result<Option<Credentials>> {
    use std::os::unix::fs::MetadataExt;
    let meta = match std::fs::metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(Error(format!("cannot read credentials {}: {e}", path.display()))),
    };
    if meta.mode() & 0o077 != 0 {
        return Err(Error(format!(
            "credentials {} are readable by others (mode {:o}); fix it with: chmod 600 {}",
            path.display(),
            meta.mode() & 0o777,
            path.display()
        )));
    }
    let raw = std::fs::read(path).map_err(|e| Error(format!("cannot read credentials {}: {e}", path.display())))?;
    let value: serde_json::Value = serde_json::from_slice(&raw).unwrap_or_default();
    let field = |k: &str| value.get(k).and_then(serde_json::Value::as_str).map(str::to_owned);
    match (field("username"), field("password"), field("client_id")) {
        (Some(username), Some(password), Some(client_id)) => Ok(Some(Credentials { username, password, client_id })),
        _ => Err(Error(format!(
            "credentials {} are not JSON with string username, password and client_id",
            path.display()
        ))),
    }
}

/// The broker's unix socket: AGENT_BRIDGE_SOCKET; else, unless AGENT_BRIDGE_HOST or AGENT_BRIDGE_PORT picks a TCP
/// broker, socket_dir()/mqtt.sock when it exists (compose.auth.yaml). None means TCP.
pub fn socket() -> Option<PathBuf> {
    if let Some(path) = env::var_os("AGENT_BRIDGE_SOCKET").filter(|v| !v.is_empty()) {
        return Some(PathBuf::from(path));
    }
    if env::var_os("AGENT_BRIDGE_HOST").is_some() || env::var_os("AGENT_BRIDGE_PORT").is_some() {
        return None;
    }
    let path = socket_dir().join("mqtt.sock");
    std::os::unix::fs::FileTypeExt::is_socket(&std::fs::metadata(&path).ok()?.file_type()).then_some(path)
}

/// $XDG_STATE_HOME/agent-bridge/broker (default ~/.local/state/agent-bridge/broker), the broker's socket directory.
/// Not $XDG_RUNTIME_DIR: that tmpfs is gone at boot, when Docker restarts the broker before anyone logs in.
pub fn socket_dir() -> PathBuf {
    xdg("XDG_STATE_HOME", ".local/state").join("broker")
}

/// $XDG_CONFIG_HOME/agent-bridge (default ~/.config/agent-bridge): the console's admin token; 0700 from the console.
pub fn config_dir() -> PathBuf {
    xdg("XDG_CONFIG_HOME", ".config")
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
    fn auth_modes_parse_and_a_bad_one_names_its_source() {
        assert_eq!(Auth::parse("warn\n", "f"), Ok(Auth::Warn), "the entrypoint's file ends in a newline");
        assert_eq!(Auth::parse("enforce", "f"), Ok(Auth::Enforce));
        assert_eq!(Auth::parse("off", "f").map(Auth::as_str), Ok("off"));
        assert_eq!(
            Auth::parse("on", "AGENT_BRIDGE_AUTH"),
            Err("AGENT_BRIDGE_AUTH must be off, warn or enforce, got 'on'".into())
        );
    }

    #[test]
    fn credentials_files_must_be_private_and_complete() {
        let dir = std::env::temp_dir().join(format!("agent-bridge-config-creds-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("a temp dir");
        let path = dir.join("a.json");
        assert_eq!(read_credentials(&path), Ok(None), "no file, no credentials");
        let write = |body: &str, mode: u32| {
            std::fs::write(&path, body).expect("write");
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).expect("chmod");
        };
        write(r#"{"username": "a", "password": "p", "client_id": "c", "session": "s"}"#, 0o600);
        let want = Credentials { username: "a".into(), password: "p".into(), client_id: "c".into() };
        assert_eq!(read_credentials(&path), Ok(Some(want)));
        write(r#"{"username": "a", "password": "p", "client_id": "c"}"#, 0o640);
        let p = path.display();
        assert_eq!(
            read_credentials(&path).unwrap_err().0,
            format!("credentials {p} are readable by others (mode 640); fix it with: chmod 600 {p}")
        );
        write(r#"{"username": "a", "password": 5}"#, 0o600);
        assert_eq!(
            read_credentials(&path).unwrap_err().0,
            format!("credentials {p} are not JSON with string username, password and client_id")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

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
