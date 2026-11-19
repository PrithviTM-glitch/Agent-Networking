"""The test harness and release tooling: what conftest promises about leftovers from killed runs, the release notes
the release workflow takes from CHANGELOG.md, what it publishes, and the version every command reports."""

import os
import subprocess
import sys

from conftest import BIN, ROOT, TMP_PREFIX, _isolate, _sweep_stale_dirs


def test_a_killed_runs_directory_is_swept():
    """A run directory named after a dead pid is removed; a live run's and anything not ours to judge are kept."""
    gone = subprocess.Popen([sys.executable, "-c", "pass"])
    gone.wait()  # its pid is now dead
    root = os.environ["AGENT_BRIDGE_LOG"].rsplit("/", 1)[0]  # this run's own directory, a safe place to try it
    parent = os.path.join(root, "sweep")
    os.makedirs(parent)
    dead, live = f"{TMP_PREFIX}{gone.pid}-abc123", f"{TMP_PREFIX}{os.getpid()}-abc123"
    for name in (dead, live, f"{TMP_PREFIX}xyz", "unrelated-123-abc"):
        os.makedirs(os.path.join(parent, name, "state"))
    _sweep_stale_dirs(parent)
    assert sorted(os.listdir(parent)) == sorted([live, f"{TMP_PREFIX}xyz", "unrelated-123-abc"])


def test_a_users_auth_setup_never_reaches_a_test(tmp_path):
    """A user's auth setup (the mode, a credentials file and a socket path exported in the shell, and an enforce
    auth-mode and an admin.json in the real config dir) never reaches a test: through conftest's isolation a register
    still goes to the disposable broker and stays anonymous. Without it, the same register fails."""
    real = tmp_path / "home" / ".config" / "agent-bridge"
    real.mkdir(parents=True, mode=0o700)
    admin = real / "admin.json"
    admin.write_text('{"username": "admin", "password": "p", "client_id": "c"}')
    admin.chmod(0o600)
    (real / "auth-mode").write_text("enforce\n")
    users = {
        **os.environ,
        "HOME": str(tmp_path / "home"),
        "XDG_CONFIG_HOME": str(tmp_path / "home" / ".config"),
        "AGENT_BRIDGE_AUTH": "enforce",
        "AGENT_BRIDGE_CREDENTIALS": str(admin),
        "AGENT_BRIDGE_SOCKET": str(tmp_path / "live.sock"),
    }
    register = [str(BIN / "agent-bridge"), "register", "harness"]
    leaked = subprocess.run(register, env=users, capture_output=True, text=True)
    assert leaked.returncode != 0 and "live.sock" in leaked.stderr, "the control: unisolated, it reaches the socket"
    isolated = dict(users)
    _isolate(isolated, str(tmp_path / "run"))
    got = subprocess.run(register, env=isolated, capture_output=True, text=True)
    assert got.returncode == 0 and got.stdout.startswith("harness-"), got.stderr
    assert not (real / "agents").exists(), "nothing provisioned with the real admin.json"
    assert not (tmp_path / "run" / "config" / "agent-bridge" / "agents").exists(), "and anonymous: auth is off"
    assert not {"AGENT_BRIDGE_AUTH", "AGENT_BRIDGE_CREDENTIALS", "AGENT_BRIDGE_SOCKET"} & isolated.keys()


CHANGELOG = """# Changelog

## [Unreleased]

## [1.2.0] - 2030-01-02

Intro.

### Fixed
- one
  continued

## [1.1.0] - 2030-01-01

### Added
- two

[1.2.0]: https://example.invalid/compare/v1.1.0...v1.2.0
"""


def release_notes(version, path):
    script = str(ROOT / "scripts" / "release-notes.sh")
    return subprocess.run([script, version, str(path)], capture_output=True, text=True)


def test_release_notes_are_the_tags_changelog_section(tmp_path):
    """A tag gets exactly its section plus its compare link; a missing or empty section fails the release."""
    changelog = tmp_path / "CHANGELOG.md"
    changelog.write_text(CHANGELOG)
    got = release_notes("v1.2.0", changelog)
    assert got.returncode == 0, got.stderr
    assert got.stdout == (
        "Intro.\n\n### Fixed\n- one\n  continued\n\n**Full changes:** https://example.invalid/compare/v1.1.0...v1.2.0\n"
    )
    assert release_notes("1.1.0", changelog).stdout == "### Added\n- two\n", "the link definitions are not part of it"
    for missing in ("1.3.0", "1.2", "Unreleased"):  # absent, a prefix of a real version, and an empty section
        refused = release_notes(missing, changelog)
        assert refused.returncode == 1 and "has no section" in refused.stderr, (missing, refused)


def check_dist(path, version=""):
    script = str(ROOT / "scripts" / "check-dist.sh")
    return subprocess.run([script, str(path), *([version] if version else [])], capture_output=True, text=True)


def test_only_the_distributions_are_published(tmp_path):
    """dist must hold one wheel per architecture and one sdist, of the tag's version, and nothing else, so neither the
    GitHub release nor the PyPI upload picks up a stray file (release notes, licenses)."""
    good = [
        "agent_bridge-1.2.0-cp312-abi3-manylinux_2_28_x86_64.whl",
        "agent_bridge-1.2.0-cp312-abi3-manylinux_2_28_aarch64.whl",
        "agent_bridge-1.2.0.tar.gz",
    ]
    for name in good:
        (tmp_path / name).touch()
    for version in ("", "1.2.0", "v1.2.0"):
        assert check_dist(tmp_path, version).returncode == 0, version
    assert "not a wheel or sdist of agent-bridge 1.1.0" in check_dist(tmp_path, "1.1.0").stderr
    for stray, why in (
        ("THIRD_PARTY_LICENSES.md", "not a wheel or sdist"),
        (".release-notes.md", "not a wheel or sdist"),
        ("agent_bridge-1.2.0-cp312-abi3-manylinux_2_34_x86_64.whl", "2 x86_64 and 1 aarch64 wheels"),
        ("agent_bridge-1.1.0.tar.gz", "2 sdists"),
    ):
        (tmp_path / stray).touch()
        refused = check_dist(tmp_path)
        assert refused.returncode == 1 and why in refused.stderr, (stray, refused.stderr)
        (tmp_path / stray).unlink()
    (tmp_path / good[1]).unlink()
    assert "1 x86_64 and 0 aarch64 wheels" in check_dist(tmp_path).stderr


COMMANDS = (
    "agent-bridge",
    "agent-bridge-ledgerd",
    "agent-bridge-historyd",
    "agent-bridge-mcp",
    "agent-bridge-console",
    "agent-bridge-admin",
)


def test_every_command_reports_the_package_version():
    """Each command under test, the native binaries and the Python entry points alike, prints "<command> <version>"
    for -V and --version and exits 0 without connecting to anything, the version being the installed package's."""
    import agent_bridge

    for command in COMMANDS:
        for flag in ("--version", "-V"):
            got = subprocess.run([str(BIN / command), flag], capture_output=True, text=True, timeout=10, env={})
            assert (got.returncode, got.stdout, got.stderr) == (0, f"{command} {agent_bridge.__version__}\n", ""), (
                command,
                flag,
                got,
            )
