"""The test harness and release tooling: what conftest promises about leftovers from killed runs, and the release notes
the release workflow takes from CHANGELOG.md."""

import os
import subprocess
import sys

from conftest import ROOT, TMP_PREFIX, _sweep_stale_dirs


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
