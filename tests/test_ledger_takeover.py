"""ledgerd stays a single writer: a second ledgerd on the same broker takes the session over and the first exits,
instead of the two taking it from each other. A module of its own, since every ledgerd shares the client id and this
one would take the session from test_ledger's service. Runs whichever ledgerd AGENT_BRIDGE_LEDGERD names.
"""

import os
import time

from conftest import start_ledgerd, wait_for

from agent_bridge import _native


def test_the_first_ledgerd_steps_aside(fresh_broker, state_dir):
    db = os.path.join(state_dir, "ledger.db")
    logs = [os.path.join(state_dir, f"ledgerd-{n}.log") for n in (1, 2)]
    first = start_ledgerd(db, logs[0])
    second = start_ledgerd(db, logs[1])
    try:
        assert wait_for(lambda: first.poll() is not None, 10), (
            "the first ledgerd kept the session: " + open(logs[0]).read()
        )
        assert first.returncode != 0
        time.sleep(3)  # the redial delay: a second takeover would come in this window
        assert second.poll() is None, open(logs[1]).read()
        assert _native.ledger_call("init", "after-takeover-a1", timeout=2)["created"] is True, "the one left serves"
    finally:
        for proc in (first, second):
            proc.kill()
            proc.wait()
