"""The test harness itself: what conftest promises about leftovers from killed runs."""

import os
import subprocess
import sys

from conftest import TMP_PREFIX, _sweep_stale_dirs


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
