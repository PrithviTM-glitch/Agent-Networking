"""Input rules shared by every boundary. Each check returns its value or raises BridgeError with a one-line message.

Names, addresses, rooms and caps: 1-64 ASCII letters, digits, _ or - (names at most 57, leaving room for the address
suffix), at most 32 caps. Text: at most 64 KiB of UTF-8; tab, newline and CR are kept, any other control character
(C0, DEL, C1) is refused. Raw topics: at most 256 bytes of UTF-8, no control characters, never starting with $ or the
reserved levels agents, inbox, ledger and broadcast. + and # work only in subscription filters, as whole levels, # only
last, and not as the first level.
"""

import re

ID_MAX, NAME_MAX = 64, 57
CAPS_MAX, TOPICS_MAX, TOPIC_MAX = 32, 100, 256
BODY_MAX = 64 * 1024
WAIT_MAX, LAST_MAX = 3600, 1000
RESERVED = ("agents", "inbox", "ledger", "broadcast")
ADMIN = "admin"
IDENT = re.compile(r"[A-Za-z0-9_-]+")
CTRL = re.compile(r"[\x00-\x1f\x7f-\x9f]")
BODY_CTRL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")
UUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")


class BridgeError(Exception):
    """Bad input, an unknown address or a broker problem; the message is one line."""


def show(value):
    return repr(value)[:60]


def is_ident(value, cap=ID_MAX):
    return isinstance(value, str) and len(value) <= cap and IDENT.fullmatch(value) is not None


def is_count(value, cap):
    return isinstance(value, int) and not isinstance(value, bool) and 0 < value <= cap


def check_ident(value, what="name", cap=ID_MAX):
    if not is_ident(value, cap):
        raise BridgeError(f"bad {what} {show(value)}: use 1-{cap} ASCII letters, digits, _ or -")
    return value


def not_admin(value, what):
    if isinstance(value, str) and value.lower() == ADMIN:
        raise BridgeError(f"{what} {value!r} is reserved for the admin console")
    return value


def check_session(value):
    if not isinstance(value, str) or not UUID.fullmatch(value):
        raise BridgeError(f"bad session id {show(value)}: expected a UUID")
    return value


def _utf8_len(value, what):
    if not isinstance(value, str):
        raise BridgeError(f"{what} must be a string, got {type(value).__name__}")
    try:
        return len(value.encode())
    except UnicodeEncodeError:
        raise BridgeError(f"{what} is not valid UTF-8") from None


def check_text(value, what="text"):
    if (size := _utf8_len(value, what)) > BODY_MAX:
        raise BridgeError(f"{what} is {size} bytes, the limit is {BODY_MAX}")
    if BODY_CTRL.search(value):
        raise BridgeError(f"{what} contains a control character other than tab, newline or CR")
    return value


def check_topic(value, filter=False):
    """A raw topic (or with filter=True a subscription filter) outside the bridge's reserved namespaces."""
    if not (size := _utf8_len(value, "topic")) or size > TOPIC_MAX:
        raise BridgeError(f"topic must be 1-{TOPIC_MAX} bytes")
    if CTRL.search(value):
        raise BridgeError("topic contains a control character")
    levels = value.split("/")
    if value.startswith("$") or levels[0] in RESERVED:
        raise BridgeError(f"topic {show(value)} is reserved: $, {', '.join(RESERVED)} belong to the bridge")
    if not filter:
        if "+" in value or "#" in value:
            raise BridgeError("+ and # are wildcards, they only work in sub")
    elif levels[0] in ("+", "#"):
        raise BridgeError("a filter's first level must be literal, a wildcard there would reach reserved topics")
    elif any(
        ("#" in level and (level != "#" or i < len(levels) - 1)) or ("+" in level and level != "+")
        for i, level in enumerate(levels)
    ):
        raise BridgeError("+ must be a whole level and # a whole last level")
    return value


def check_caps(values):
    if not isinstance(values, (list, tuple)) or len(values) > CAPS_MAX:
        raise BridgeError(f"caps must be a list of at most {CAPS_MAX}")
    return [check_ident(c, "cap") for c in values]


def check_wait(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= value <= WAIT_MAX:
        raise BridgeError(f"wait must be 0-{WAIT_MAX} seconds")
    return value


def check_last(value):
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= LAST_MAX:
        raise BridgeError(f"last must be 1-{LAST_MAX}")
    return value


def check_ts(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= value < 1e10:
        raise BridgeError("bad timestamp")
    return float(value)
