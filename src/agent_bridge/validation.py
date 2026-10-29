"""Input rules shared by every boundary. Each check returns its value or raises BridgeError with a one-line message.

Names, addresses, rooms and caps: 1-64 ASCII letters, digits, _ or - (names at most 57, leaving room for the address
suffix), at most 32 caps. Text: at most 64 KiB of UTF-8; tab, newline and CR are kept, any other control character
(C0, DEL, C1) is refused. Raw topics: at most 256 bytes of UTF-8, no control characters, never starting with $ or the
reserved levels agents, inbox, ledger, broadcast, rooms and history. + and # work only in subscription filters, as
whole levels, # only last, and not as the first level.

The limits and the checks are the native core's own, re-exported here so there is one definition; UUID and is_count
are host-side helpers with no native counterpart.
"""

import re

from . import _native
from .errors import BridgeError  # noqa: F401  defined once, with the other errors, raised by _native too

ID_MAX, NAME_MAX = _native.ID_MAX, _native.NAME_MAX
CAPS_MAX, TOPICS_MAX, TOPIC_MAX, MENTIONS_MAX = (
    _native.CAPS_MAX,
    _native.TOPICS_MAX,
    _native.TOPIC_MAX,
    _native.MENTIONS_MAX,
)
BODY_MAX = _native.BODY_MAX
WAIT_MAX, LAST_MAX = _native.WAIT_MAX, _native.LAST_MAX
RESERVED = tuple(_native.RESERVED)  # tuples, as str.startswith needs
SERVICES = tuple(_native.SERVICES)  # the services' request/response levels: RPC traffic, never shown as messages
ADMIN = _native.ADMIN
check_ident, is_ident, check_topic = _native.check_ident, _native.is_ident, _native.check_topic
check_caps, check_wait, check_last = _native.check_caps, _native.check_wait, _native.check_last
check_text, check_session = _native.check_text, _native.check_session
UUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")


def is_count(value, cap):
    return isinstance(value, int) and not isinstance(value, bool) and 0 < value <= cap
