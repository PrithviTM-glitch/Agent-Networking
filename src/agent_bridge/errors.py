"""The bridge's exceptions, defined once: the Python modules and the Rust extension (_native) raise these very classes,
so one except clause catches both. Standard library only, and nothing of the bridge imported: _native imports this
module."""

import math
import time


class BridgeError(Exception):
    """Bad input, an unknown address or a broker problem; the message is one line."""


class LedgerError(Exception):
    """An ok:false reply, a timeout or a malformed reply; .reply holds the reply dict. A busy reply's message names
    the holders, as the Rust client's does."""

    def __init__(self, reply):
        message = reply.get("error", "ledger error")
        holders = reply.get("holders") if message == "busy" else None
        if holders and isinstance(holders, list):
            now = time.time()
            held = []
            for h in holders:
                h = h if isinstance(h, dict) else {}
                left = h.get("expires_at")
                left = max(0, math.ceil(left - now)) if isinstance(left, (int, float)) else 0
                held.append(f"{h.get('holder', '?')} ({h.get('level', '?')}, {left} s left)")
            message = "busy, held by " + ", ".join(held)
        super().__init__(message)
        self.reply = reply


class HistoryError(BridgeError):
    """An ok:false reply, a timeout, a malformed reply or no broker; .reply holds the reply dict."""

    def __init__(self, reply):
        super().__init__("history: " + " ".join(str(reply.get("error", "error")).split()))
        self.reply = reply
