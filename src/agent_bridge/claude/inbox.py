"""The session inbox: a Unix socket Claude Code opens for each session, which takes one user message per connection
after an auth line. Its path and token are in the session's environment."""

import json
import os
import socket

SOCKET, TOKEN = "CLAUDE_CODE_MESSAGING_SOCKET", "CLAUDE_CODE_MESSAGING_TOKEN"


def available():
    return bool(os.environ.get(SOCKET))


def post(text):
    """Write one user message to this session's inbox socket; raises OSError if it is not there."""
    path = os.environ.get(SOCKET)
    if not path:
        raise OSError(f"{SOCKET} is not set: not inside a Claude Code session with an inbox")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.settimeout(5)
        s.connect(path)
        lines = (
            {"type": "auth", "token": os.environ.get(TOKEN, "")},
            {"type": "user", "message": {"role": "user", "content": text}},
        )
        s.sendall("".join(json.dumps(x) + "\n" for x in lines).encode())
        s.shutdown(socket.SHUT_WR)
