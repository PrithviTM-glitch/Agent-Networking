"""historyd stays a single writer: a second historyd on the same broker takes the session over and the first exits,
instead of the two taking it from each other. A module of its own, since every historyd shares the client id and this
one would take the session from test_history's service. Runs whichever historyd AGENT_BRIDGE_HISTORYD names.
"""

import os
import time

from conftest import start_historyd, wait_for

from agent_bridge import _native


def test_the_first_historyd_steps_aside(fresh_broker, state_dir):
    db = os.path.join(state_dir, "history.db")
    logs = [os.path.join(state_dir, f"historyd-{n}.log") for n in (1, 2)]
    first = start_historyd(db, logs[0])
    second = start_historyd(db, logs[1])
    try:
        assert wait_for(lambda: first.poll() is not None, 10), (
            "the first historyd kept the session: " + open(logs[0]).read()
        )
        assert first.returncode != 0
        time.sleep(3)  # the redial delay: a second takeover would come in this window
        assert second.poll() is None, open(logs[1]).read()
        assert _native.history_call("ping", timeout=2) == {"ok": True}, "the one left answers"
    finally:
        for proc in (first, second):
            proc.kill()
            proc.wait()
