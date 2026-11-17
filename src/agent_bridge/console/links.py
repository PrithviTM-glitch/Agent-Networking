"""Auto-link rules for the console page, from an optional file the operator writes: a token in a message that matches
a rule's pattern becomes a link built from the rule's URL template. No URL is built in; the repository ships none.

The page runs the patterns in the browser's JavaScript engine, so a pattern must mean the same in Python, where it is
checked, and in JavaScript, where it runs. Only a small common subset is accepted (literals, classes, the escapes
\\d \\w \\s and escaped punctuation, ( and (?: groups, quantifiers), and only in a shape that matches in time linear in
the token (see check_pattern), against single tokens of at most TOKEN_MAX characters, anchored. Message text fills
only the URL's path, query or fragment, percent-encoded, so it can never choose the scheme or host."""

import json
import os
import re
import sys

from .. import config

MAX_RULES = 20
PATTERN_MAX = 200
URL_MAX = 500
TOKEN_MAX = 200  # as links.js: longer tokens are never matched
MAX_OPTIONAL = 3
FILE_MAX = 64 * 1024
NAME = re.compile(r"[a-z][a-z0-9_-]{0,31}")
PLACEHOLDER = re.compile(r"\{([1-3])\}")
URL = re.compile(r"https?://[^/?#{}\s]+(?P<rest>[/?#][^\s]*)?")
CLASS_ESCAPES = set("dDwWsS")
PUNCT_ESCAPES = set("\\^$.|?*+()[]{}/-")
BOUND = re.compile(r"\{(\d{1,3})(?:(,)(\d{0,3}))?\}")

current = {"rules": [], "error": None}


def path():
    return os.environ.get("AGENT_BRIDGE_LINKS") or os.path.join(config.CONFIG_DIR, "console-links.json")


class Bad(ValueError):
    pass


def check_pattern(p):
    """The number of capture groups in p, or Bad naming what is not allowed.

    The shape rules keep matching linear in the token on any backtracking engine:
    - a repeat (+, *, {n,} or {n,m} with m > n) must be followed at once by one plain character it cannot match, or by
      the end, so it has exactly one place to stop; a repeat that ends a group, or a branch of one, is followed by
      what comes after the group;
    - a group may be optional (?) but never repeated, and only one group (the whole pattern counts as one) holds |;
    - at most MAX_OPTIONAL parts are optional (? or {0,n}), so their combinations stay few;
    - an exact {n} neither repeats nor separates."""
    if not isinstance(p, str) or not 0 < len(p) <= PATTERN_MAX:
        raise Bad(f"pattern must be 1 to {PATTERN_MAX} characters")
    if not all(" " < c <= "~" for c in p):
        raise Bad("pattern must be printable ASCII without spaces")
    # Per open group: the repeats waiting when it opened, those ending its branches so far, and whether it holds |.
    frames = [{"open": [], "ends": [], "alt": False}]
    atom = None  # what a quantifier here applies to: {"kind": "lit" | "class" | "group", "src", "ch"}
    need = []  # repeats that may end just here, until one character none of them can match follows
    groups = optional = alternations = i = 0

    def refuse_repeat():
        raise Bad(
            f"repeated {' or '.join(need)} must be followed by one plain character it cannot match, or by the end"
        )

    def settle(a):
        """a, with no quantifier after it: a literal none of the waiting repeats can match ends the wait."""
        if a is None or a["kind"] == "group" or not need:
            return
        if a["kind"] == "lit" and not any(re.fullmatch(r, a["ch"], re.ASCII) for r in need):
            need.clear()
            return
        refuse_repeat()

    def close(frame):
        nonlocal alternations
        if frame["alt"]:
            alternations += 1
            if alternations > 1:
                raise Bad("only one group may contain |")
        need[:] = list(dict.fromkeys(frame["ends"] + need))  # every branch's trailing repeat waits after the group

    while i < len(p):
        c = p[i]
        if c in "*+?{":
            if c == "{":
                if not (m := BOUND.match(p, i)) or (m[3] and int(m[3]) < int(m[1])):
                    raise Bad("{ must be a {n}, {n,} or {n,m} quantifier")
                lo, hi, i = int(m[1]), (int(m[3]) if m[3] else None) if m[2] else int(m[1]), m.end()
            else:
                lo, hi, i = {"?": (0, 1), "*": (0, None), "+": (1, None)}[c] + (i + 1,)
            if p[i : i + 1] == "?":
                i += 1  # lazy: the same shape
            if atom is None:
                raise Bad("a quantifier needs something to repeat")
            if c == "?" or (lo == 0 and hi is not None):
                optional += 1
                if optional > MAX_OPTIONAL:
                    raise Bad(f"at most {MAX_OPTIONAL} optional parts (? or {{0,n}})")
            if atom["kind"] == "group":
                if (lo, hi) != (0, 1):
                    raise Bad("a group may only be optional (?), never repeated")
            elif hi is None or hi > max(lo, 1):  # a repeat: a variable count past one
                if need:
                    refuse_repeat()
                need.append(atom["src"])
            elif need:  # an exact count or an optional part does not end the wait
                refuse_repeat()
            atom = None
            continue
        settle(atom)
        atom = None
        if c == "\\":
            if i + 1 == len(p) or (p[i + 1] not in CLASS_ESCAPES and p[i + 1] not in PUNCT_ESCAPES):
                raise Bad(f"escape {p[i : i + 2]!r} is not allowed (only \\d \\w \\s, their capitals, and punctuation)")
            kind = "class" if p[i + 1] in CLASS_ESCAPES else "lit"
            atom, i = {"kind": kind, "src": p[i : i + 2], "ch": p[i + 1]}, i + 2
        elif c == "[":
            j = i + 1 + (p[i + 1 : i + 2] == "^")
            if p[j : j + 1] == "]":
                raise Bad("a class may not be empty or start with ]")
            while j < len(p) and p[j] != "]":
                if p[j] == "[":
                    raise Bad("[ inside a class must be escaped")
                if p[j] == "\\":
                    if j + 1 == len(p) or (p[j + 1] not in CLASS_ESCAPES and p[j + 1] not in PUNCT_ESCAPES):
                        raise Bad(f"escape {p[j : j + 2]!r} is not allowed in a class")
                    j += 1
                j += 1
            if j == len(p):
                raise Bad("unclosed [")
            atom, i = {"kind": "class", "src": p[i : j + 1]}, j + 1
        elif c == "(":
            if p.startswith("(?:", i):
                i += 3
            elif p.startswith("(?", i):
                raise Bad("only ( and (?: groups are allowed")
            else:
                groups, i = groups + 1, i + 1
            frames.append({"open": list(need), "ends": [], "alt": False})
        elif c == ")":
            if len(frames) == 1:
                raise Bad("unbalanced )")
            close(frames.pop())
            atom, i = {"kind": "group"}, i + 1
        elif c == "|":
            f = frames[-1]
            f["alt"], f["ends"] = True, f["ends"] + need
            need[:] = f["open"]  # the next branch starts where the group did
            i += 1
        elif c in "^$":
            raise Bad("no ^ or $: a pattern always matches a whole token")
        elif c in "]}":
            raise Bad(f"{c} must be escaped")
        else:
            atom, i = {"kind": "class" if c == "." else "lit", "src": c, "ch": c}, i + 1
    settle(atom)
    if len(frames) > 1:
        raise Bad("unclosed (")
    close(frames[0])
    if not 1 <= groups <= 3:
        raise Bad("a pattern needs 1 to 3 capture groups")
    try:
        re.compile(f"^(?:{p})$", re.ASCII)
    except re.error as e:
        raise Bad(f"pattern: {e}") from None
    return groups


def check_url(url, groups):
    if not isinstance(url, str) or not 0 < len(url) <= URL_MAX or not url.isascii():
        raise Bad(f"url must be ASCII, 1 to {URL_MAX} characters")
    if not (m := URL.fullmatch(url)):
        raise Bad("url must be http(s)://host/..., with no placeholder or space in the scheme or host")
    rest = m["rest"] or ""
    if "{" in PLACEHOLDER.sub("", rest) or "}" in PLACEHOLDER.sub("", rest):
        raise Bad("the only braces allowed in url are the placeholders {1}, {2} and {3}")
    if any(int(n) > groups for n in PLACEHOLDER.findall(rest)):
        raise Bad(f"url uses a placeholder past the pattern's {groups} group(s)")


def check(rules):
    """The validated rules, as the page gets them, or Bad naming the first problem."""
    if not isinstance(rules, list):
        raise Bad("the file must hold a JSON list of rules")
    if len(rules) > MAX_RULES:
        raise Bad(f"at most {MAX_RULES} rules")
    out = []
    for n, r in enumerate(rules, 1):
        try:
            if not isinstance(r, dict) or set(r) != {"name", "pattern", "url"}:
                raise Bad('each rule is {"name", "pattern", "url"} and nothing else')
            if not isinstance(r["name"], str) or not NAME.fullmatch(r["name"]):
                raise Bad("name must be 1 to 32 of a-z, 0-9, _ and -, starting with a letter")
            check_url(r["url"], check_pattern(r["pattern"]))
        except Bad as e:
            raise Bad(f"rule {n}: {e}") from None
        out.append({"name": r["name"], "pattern": r["pattern"], "url": r["url"]})
    return out


def load():
    """Read the rules file once, at start. A missing file means no links; a bad one is refused whole with a one-line
    reason (logged, and shown by status), and the console runs without links."""
    p = path()
    try:
        with open(p, encoding="utf-8") as f:
            raw = f.read(FILE_MAX + 1)
        if len(raw) > FILE_MAX:
            raise Bad(f"larger than {FILE_MAX} bytes")
        current.update(rules=check(json.loads(raw)), error=None)
    except FileNotFoundError:
        current.update(rules=[], error=None)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, Bad) as e:
        reason = e.strerror if isinstance(e, OSError) and e.strerror else str(e)
        current.update(rules=[], error=f"{p}: {reason}")
        print(f"links: {current['error']}; running without links", file=sys.stderr, flush=True)
    return current
