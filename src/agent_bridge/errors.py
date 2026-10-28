"""The bridge's exceptions, defined once: the Python modules and the Rust extension (_native) raise these very classes,
so one except clause catches both. Standard library only, and nothing imported: _native imports this module."""


class BridgeError(Exception):
    """Bad input, an unknown address or a broker problem; the message is one line."""


class LedgerError(Exception):
    """An ok:false reply, a timeout or a malformed reply; .reply holds the reply dict."""

    def __init__(self, reply):
        super().__init__(reply.get("error", "ledger error"))
        self.reply = reply


class HistoryError(BridgeError):
    """An ok:false reply, a timeout, a malformed reply or no broker; .reply holds the reply dict."""

    def __init__(self, reply):
        super().__init__("history: " + " ".join(str(reply.get("error", "error")).split()))
        self.reply = reply
