"""Claude Code hooks. Both print nothing on failure and always exit 0, so they can never block a session.

broadcast (UserPromptSubmit, SessionStart): shows this session the admin console's broadcasts it has not seen. It
asks historyd for them in one request with a 1 s timeout (a few ms while historyd is up, at once if the broker is
down) and shows nothing if that fails. It skips sessions with a live relay, which delivers broadcasts itself. State:
<data dir>/hook-state/<session id>, the timestamp of the newest broadcast already shown to that session; a session
the hook has never seen gets the last FIRST_S seconds.

relay_start (SessionStart): starts this session's relay in the background and returns at once. SessionStart output
becomes context, so it prints nothing.

Only the standard library is imported here, and the bridge's own modules only once inside the error handling.
"""

import json
import os
import sys
import time

FIRST_S = 3600
SHOWN_MAX = 20
EVENTS = ("UserPromptSubmit", "SessionStart")


def _broadcasts(since):
    from ..history import client

    return client.broadcasts(since=since, last=SHOWN_MAX, timeout=1.0)


def _show_broadcasts():
    from .. import config, relay
    from ..validation import UUID

    hook = json.load(sys.stdin)
    session, event = hook.get("session_id"), hook.get("hook_event_name")
    if not isinstance(session, str) or not UUID.fullmatch(session) or event not in EVENTS:
        return
    session = session.lower()
    if relay.live(session):
        return
    state = config.data_path("hook-state", session)
    try:
        with open(state) as f:
            since = float(f.read())
    except (OSError, ValueError):
        since = time.time() - FIRST_S
    new = _broadcasts(since)
    if not new:
        return
    os.makedirs(os.path.dirname(state), mode=0o700, exist_ok=True)
    with open(f"{state}.tmp", "w") as f:
        f.write(repr(max(e["ts"] for e in new)))
    os.replace(f"{state}.tmp", state)
    lines = [f"[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(e['ts']))}] {e['body']}" for e in new[-SHOWN_MAX:]]
    text = (
        "Broadcast from the user's agent-bridge admin console (the user's own admin page for the local agent bus, "
        f"sent to every Claude session on this machine). {len(new)} new since this session last checked:\n"
        + "\n".join(lines)
    )
    print(json.dumps({"hookSpecificOutput": {"hookEventName": event, "additionalContext": text}}))


def broadcast():
    try:
        _show_broadcasts()
    except BaseException:  # noqa: BLE001  a broken hook must never block the user's prompt
        pass


def relay_start():
    devnull = os.open(os.devnull, os.O_RDWR)
    for fd in (0, 1, 2):
        os.dup2(devnull, fd)
    try:
        from .. import relay

        relay.start()
    except BaseException:  # noqa: BLE001  the session must start whatever happens here
        pass
