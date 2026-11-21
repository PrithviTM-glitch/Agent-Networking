//! The service binaries (ledgerd, historyd) cargo builds into the target dir, found from CARGO_TARGET_TMPDIR, which is
//! <target dir>/tmp in every build-dir layout. The test executable itself is not a guide: cargo 1.100 moved it from
//! <target>/<profile>/deps/ to <target>/<profile>/build/<package>/<hash>/.

use std::path::{Component, Path, PathBuf};

/// name's binary: the env override, else the one in this test's profile dir.
pub fn service(name: &str, env: &str) -> PathBuf {
    if let Some(p) = std::env::var_os(env) {
        return PathBuf::from(p);
    }
    let exe = std::env::current_exe().expect("this test's path");
    let path = service_in(Path::new(env!("CARGO_TARGET_TMPDIR")), &exe, name);
    assert!(path.exists(), "{} missing: cargo build -p {name}, or set {env}", path.display());
    path
}

/// name in the target dir (tmp's parent), under the profile dir exe runs from; debug or release when exe lives
/// outside the target dir (a separate build-dir).
pub fn service_in(tmp: &Path, exe: &Path, name: &str) -> PathBuf {
    let target = tmp.parent().unwrap_or(tmp);
    let profile = match exe.strip_prefix(target).ok().and_then(|rest| rest.components().next()) {
        Some(Component::Normal(dir)) => PathBuf::from(dir),
        _ => PathBuf::from(if cfg!(debug_assertions) { "debug" } else { "release" }),
    };
    target.join(profile).join(name)
}

#[test]
fn services_are_found_in_both_build_dir_layouts() {
    let tmp = Path::new("/t/target/tmp");
    let want = Path::new("/t/target/debug/agent-bridge-ledgerd");
    for exe in ["/t/target/debug/deps/auth-0a1b2c3d4e5f6a7b", "/t/target/debug/build/agent-bridge-core/0a1b2c3d/auth"] {
        assert_eq!(service_in(tmp, Path::new(exe), "agent-bridge-ledgerd"), want, "{exe}");
    }
    let release = Path::new("/t/target/release/build/agent-bridge-core/0a1b2c3d/auth");
    assert_eq!(service_in(tmp, release, "h"), Path::new("/t/target/release/h"));
}
