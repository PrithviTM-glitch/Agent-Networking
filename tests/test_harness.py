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


def script(name):
    return str(ROOT / "scripts" / name)


FLAKY_TEST = """
import pathlib

def test_steady():
    pass

def test_flaky(tmp_path_factory):
    mark = pathlib.Path(__file__).with_name("ran-once")
    if not mark.exists():
        mark.write_text("1")
        assert False, "fails the first time only"

def test_broken():
    {broken}
"""


def run_rerun_pytest(tmp_path, broken="pass", *args):
    (tmp_path / "suite").mkdir(exist_ok=True)
    (tmp_path / "suite" / "test_some.py").write_text(FLAKY_TEST.format(broken=broken))
    env = {
        **os.environ,
        "PYTHON": sys.executable,
        "FLAKES_FILE": str(tmp_path / "flakes.txt"),
        "GITHUB_STEP_SUMMARY": str(tmp_path / "summary.md"),
        "GITHUB_RUN_ID": "4242",
    }
    cmd = [script("rerun-pytest.sh"), "-q", *(args or [str(tmp_path / "suite")])]
    return subprocess.run(cmd, cwd=tmp_path, env=env, capture_output=True, text=True)


def test_a_flaky_pytest_test_passes_on_its_rerun_and_is_logged(tmp_path):
    """The CI test job reruns only the failed tests once: green on the rerun passes the job, but every rerun test is
    named in a FLAKE line, in flakes.txt and in the step summary, with the run id."""
    got = run_rerun_pytest(tmp_path)
    assert got.returncode == 0, got.stdout + got.stderr
    [line] = (tmp_path / "flakes.txt").read_text().splitlines()
    assert line.startswith("FLAKE ") and line.endswith("test_some.py::test_flaky run 4242"), line
    assert line in (tmp_path / "summary.md").read_text()


def test_a_test_that_fails_twice_fails_the_job(tmp_path):
    """A test that fails again on its rerun is a real failure: the job fails, and nothing is logged as a flake."""
    got = run_rerun_pytest(tmp_path, "assert False, 'always'")
    assert got.returncode == 1, got.stdout + got.stderr
    assert not (tmp_path / "flakes.txt").exists()


def test_a_pytest_run_with_no_failed_test_is_not_rerun(tmp_path):
    """A failure with no failed test to rerun (a usage or collection error) fails at once, rather than passing on an
    empty rerun."""
    got = run_rerun_pytest(tmp_path, "pass", str(tmp_path / "no-such-dir"))
    assert got.returncode != 0
    assert "not rerunning" in got.stdout + got.stderr
    assert not (tmp_path / "flakes.txt").exists()


FAKE_CARGO = """#!/bin/sh
# A stand-in for cargo test: each call appends its arguments to calls.txt and prints the next canned output.
here=$(dirname "$0")
echo "$*" >> "$here/calls.txt"
n=$(wc -l < "$here/calls.txt")
cat "$here/out$n.txt"
exit $(cat "$here/code$n.txt")
"""

RUNNING = "     Running unittests src/lib.rs (target/debug/deps/core-abc)\n"
OK = "test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n"
ONE_FAILED = (
    "test bus::reconnects ... FAILED\n"
    "test result: FAILED. 2 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s\n"
)


def run_rerun_cargo(tmp_path, *outputs):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    (bin_dir / "cargo").write_text(FAKE_CARGO)
    (bin_dir / "cargo").chmod(0o755)
    for i, (out, code) in enumerate(outputs, 1):
        (bin_dir / f"out{i}.txt").write_text(out)
        (bin_dir / f"code{i}.txt").write_text(str(code))
    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "FLAKES_FILE": str(tmp_path / "flakes.txt"),
        "GITHUB_RUN_ID": "77",
    }
    env.pop("GITHUB_STEP_SUMMARY", None)
    cmd = [script("rerun-cargo.sh"), "--manifest-path", "rust/Cargo.toml", "--workspace", "--", "--ignored"]
    got = subprocess.run(cmd, cwd=tmp_path, env=env, capture_output=True, text=True)
    calls = (bin_dir / "calls.txt").read_text().splitlines() if (bin_dir / "calls.txt").exists() else []
    return got, calls


def test_a_flaky_cargo_test_is_rerun_by_name_and_logged(tmp_path):
    """cargo test runs with --no-fail-fast, so every binary runs; a failure reruns just the failed test names, exactly,
    once. Green on the rerun passes and logs a FLAKE line."""
    got, calls = run_rerun_cargo(tmp_path, (RUNNING + ONE_FAILED, 101), (RUNNING + OK, 0))
    assert got.returncode == 0, got.stdout + got.stderr
    assert calls[0] == "test --manifest-path rust/Cargo.toml --workspace --no-fail-fast -- --ignored"
    assert (
        calls[1]
        == "test --manifest-path rust/Cargo.toml --workspace --no-fail-fast -- --ignored --exact bus::reconnects"
    )
    assert (tmp_path / "flakes.txt").read_text().splitlines() == ["FLAKE bus::reconnects run 77"]


TWO_FAILED = (
    RUNNING
    + "test bus::reconnects ... FAILED\n"
    + "test result: FAILED. 2 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s\n"
    + "     Running tests/auth.rs (target/debug/deps/auth-def)\n"
    + "test warn_provision_refusals_socket_then_enforce ... FAILED\n"
    + "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 1 filtered out; finished in 11.59s\n"
)


def test_every_failed_cargo_test_is_rerun(tmp_path):
    """Two tests failing in two binaries are both rerun, in one call, each by its exact name (staging run 44863292653
    reran only the first: the second name, on its own line, ran as a shell command). libtest names are paths of
    identifiers (a::b), so no name can carry a shell or regex character; --exact matches them literally."""
    got, calls = run_rerun_cargo(tmp_path, (TWO_FAILED, 101), (RUNNING + OK + RUNNING + OK, 0))
    assert got.returncode == 0, got.stdout + got.stderr
    assert "not found" not in got.stderr, got.stderr
    assert calls[1] == (
        "test --manifest-path rust/Cargo.toml --workspace --no-fail-fast -- --ignored "
        "--exact bus::reconnects warn_provision_refusals_socket_then_enforce"
    )
    assert (tmp_path / "flakes.txt").read_text().splitlines() == [
        "FLAKE bus::reconnects run 77",
        "FLAKE warn_provision_refusals_socket_then_enforce run 77",
    ]


def test_a_cargo_test_that_fails_twice_fails_the_job(tmp_path):
    got, calls = run_rerun_cargo(tmp_path, (RUNNING + ONE_FAILED, 101), (RUNNING + ONE_FAILED, 101))
    assert got.returncode == 1 and len(calls) == 2
    assert not (tmp_path / "flakes.txt").exists()


def test_a_crashed_or_unbuilt_test_binary_is_not_rerun(tmp_path):
    """A test binary that never reported a result (a crash, or a build error) has no test names to rerun, and rerunning
    the others would hide it: the job fails at once."""
    crashed = RUNNING + "error: test failed, to rerun pass `-p core --lib`\n" + RUNNING + ONE_FAILED
    got, calls = run_rerun_cargo(tmp_path, (crashed, 101))
    assert got.returncode == 1 and len(calls) == 1, got.stdout + got.stderr
    assert "not rerunning" in got.stdout + got.stderr
    (tmp_path / "unbuilt").mkdir()
    got, calls = run_rerun_cargo(tmp_path / "unbuilt", ("error[E0425]: cannot find value `x` in this scope\n", 101))
    assert got.returncode == 1 and len(calls) == 1


def gate(env_extra, ignore_int):
    env = {k: v for k, v in os.environ.items() if k != "GATE_ALLOW_BG"}
    env.update(GATE_CHECK_ONLY="1", **env_extra)
    launch = "trap '' INT; exec \"$0\"" if ignore_int else 'exec "$0"'
    return subprocess.run(["sh", "-c", launch, script("gate.sh")], env=env, capture_output=True, text=True)


def test_the_gate_refuses_to_run_with_sigint_ignored():
    """Started as a shell background job, the gate would inherit SIGINT ignored: Ctrl-C couldn't stop it, and every test
    child inherits it (0.3.2's first gate run failed that way). It refuses, saying why, unless GATE_ALLOW_BG=1."""
    got = gate({}, ignore_int=True)
    assert got.returncode == 2, got.stdout + got.stderr
    assert "SIGINT ignored" in got.stderr and "GATE_ALLOW_BG=1" in got.stderr
    allowed = gate({"GATE_ALLOW_BG": "1"}, ignore_int=True)
    assert allowed.returncode == 0, allowed.stderr
    assert "SIGINT ignored" in allowed.stderr, "allowed, but still said"
    normal = gate({}, ignore_int=False)
    assert normal.returncode == 0 and normal.stderr == "", normal.stderr


def verify_paths(files, *flags, root=None):
    env = {**os.environ, "VERIFY_ROOT": str(root or ROOT)}
    cmd = [script("verify-paths.sh"), *flags]
    got = subprocess.run(cmd, input="\n".join(files) + "\n", env=env, capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return dict(line.split("=", 1) for line in got.stdout.split())


def test_the_verify_jobs_run_only_for_their_paths(tmp_path):
    """verify.yml runs each formal-methods job only when its spec, its script or the code it checks changed (a plain git
    diff of the whole push, read by scripts/verify-paths.sh); --all runs everything, for the weekly and manual runs.
    Kani harnesses sit next to their code under cfg(kani), so a changed Rust file that mentions kani:: runs kani."""
    jobs = {"tlc-ledger", "tlc-worktree", "tlc-relay", "fuzz-smoke", "kani"}
    on = lambda files, root=None: {k for k, v in verify_paths(files, root=root).items() if v == "true"}  # noqa: E731
    assert set(verify_paths(["README.md"])) == jobs
    assert on(["README.md", "src/agent_bridge/mcp_server.py"]) == set()
    assert on(["rust/crates/ledgerd/src/store.rs"]) == on(["rust/crates/ledgerd/src/main.rs"]) == {"tlc-ledger"}
    assert on(["specs/ledger/Ledger.tla", "scripts/verify-tlc-ledger.sh"]) == {"tlc-ledger"}
    assert on(["rust/crates/core/src/guard/paths.rs"]) == {"tlc-worktree", "fuzz-smoke", "kani"}
    assert on(["rust/crates/core/src/worktree.rs"]) == {"tlc-worktree"}
    assert on(["rust/crates/cli/src/relay.rs", "specs/relay/MC.cfg"]) == {"tlc-relay"}
    assert on(["rust/crates/core/src/bus.rs", "rust/crates/core/src/mqtt.rs"]) == {"tlc-relay"}
    assert on(["rust/fuzz/fuzz_targets/shell.rs", "scripts/verify-fuzz.sh"]) == {"fuzz-smoke"}
    assert on(["rust/crates/core/src/json.rs"]) == {"fuzz-smoke"}
    assert on(["rust/crates/core/src/validation.rs", "rust/crates/core/src/protocol.rs"]) == {"fuzz-smoke", "kani"}
    assert on(["rust/crates/core/src/identity.rs"]) == {"kani"}
    assert on(["scripts/verify-kani.sh"]) == {"kani"}
    assert on([".github/workflows/verify.yml"]) == jobs
    assert on(["scripts/verify.sh"]) == on(["scripts/verify-changes.sh"]) == jobs
    assert on(["scripts/tla2tools.sh"]) == {"tlc-ledger", "tlc-worktree", "tlc-relay"}
    harness = tmp_path / "rust" / "crates" / "core" / "src"
    harness.mkdir(parents=True)
    (harness / "proofs.rs").write_text("#[cfg(kani)]\nfn p() { let x: u8 = kani::any(); }\n")
    (harness / "plain.rs").write_text("fn p() {}\n")
    assert on(["rust/crates/core/src/proofs.rs"], root=tmp_path) == {"kani"}
    assert on(["rust/crates/core/src/plain.rs"], root=tmp_path) == set()
    assert set(verify_paths([], "--all").values()) == {"true"}


def test_a_verify_job_without_its_script_says_so():
    """scripts/verify.sh runs a job's own script (written by the spec's owner); a job whose script doesn't exist yet
    passes with a notice, so the workflow can land before the specs do."""
    got = subprocess.run(
        [script("verify.sh"), "no-such-job"],
        env={**os.environ, "VERIFY_ROOT": "/nonexistent"},
        capture_output=True,
        text=True,
    )
    assert got.returncode == 0 and "no check yet for no-such-job" in got.stdout, got.stdout + got.stderr


def test_tlc_gets_only_the_pinned_tla2tools(tmp_path):
    """The TLC jobs use one tla2tools.jar, pinned by version and sha256 and checked before use: a jar whose hash isn't
    the pinned one is refused, and so is any jar while no hash is pinned."""
    fake = tmp_path / "tla2tools.jar"
    fake.write_bytes(b"not the pinned jar")
    got = subprocess.run(
        [script("tla2tools.sh"), str(tmp_path)],
        env={**os.environ, "TLA2TOOLS_JAR": str(fake)},
        capture_output=True,
        text=True,
    )
    assert got.returncode != 0 and "sha256" in got.stderr, got.stdout + got.stderr


def test_verify_paths_reads_a_list_of_many_files():
    """A push carries many files at once; every job any of them touches runs, and no other."""
    files = ["README.md", "specs/relay/MC.cfg", "rust/crates/ledgerd/src/store.rs", "src/agent_bridge/mcp_server.py"]
    assert {k for k, v in verify_paths(files).items() if v == "true"} == {"tlc-relay", "tlc-ledger"}


def git(repo, *args):
    out = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=True)
    return out.stdout.strip()


def verify_changes(repo, base, head):
    env = {**os.environ, "VERIFY_ROOT": str(repo)}
    got = subprocess.run([script("verify-changes.sh"), base, head], env=env, capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return {line.split("=", 1)[0] for line in got.stdout.split() if line.endswith("=true")}


def test_verify_diffs_the_whole_push_and_runs_everything_when_it_cant(tmp_path):
    """verify.yml diffs the whole push (before..sha) or pull request (base..head), so a spec changed in an earlier
    commit of the push still runs its job. A new branch (before is all zeros), a commit the clone doesn't have (after a
    force push) or a failed diff runs every job."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main")
    git(repo, "config", "user.email", "t@example.com")
    git(repo, "config", "user.name", "t")
    (repo / "README.md").write_text("one\n")
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "base")
    base = git(repo, "rev-parse", "HEAD")
    (repo / "specs" / "ledger").mkdir(parents=True)
    (repo / "specs" / "ledger" / "Ledger.tla").write_text("---- MODULE Ledger ----\n====\n")
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "the spec, in the push's first commit")
    (repo / "README.md").write_text("two\n")
    git(repo, "commit", "-qam", "docs, in its last")
    head = git(repo, "rev-parse", "HEAD")
    every = {"tlc-ledger", "tlc-worktree", "tlc-relay", "fuzz-smoke", "kani"}
    assert verify_changes(repo, base, head) == {"tlc-ledger"}
    assert verify_changes(repo, "0" * 40, head) == every
    assert verify_changes(repo, "1234567" * 5 + "12345", head) == every
    assert verify_changes(repo, "", head) == every
