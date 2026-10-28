"""HTTP for the console: static page, token-protected JSON API and the live event stream."""

import argparse
import json
import os
import queue
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from .. import config, rooms
from ..history import dms
from ..validation import BridgeError
from . import actions, admin, auth, capacity, events, kill, leaks, metrics, processes, sessions, usage
from .processes import Refused

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
STATIC = {
    "/": ("index.html", "text/html"),
    "/app.js": ("app.js", "text/javascript"),
    "/app.css": ("app.css", "text/css"),
}
CSP = (
    "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; "
    "base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
)
REQUEST_MAX = 256 * 1024
DMS_SHOWN = 200


def agent_detail(query):
    v = sessions.find(query)
    out = {"agent": v, "activity": sessions.activity(v["session"]) if v["session"] else [], "dms": [], "ledger": None}
    if v["addr"]:
        out["dms"] = dms(v["addr"], last=DMS_SHOWN)
        try:
            out["ledger"] = admin.ledger_of(v["addr"], admin.dump())
        except Refused as e:
            out["ledger"] = {"error": str(e)}
    return out


def state(_query):
    return {
        "agents": sessions.entities(),
        "messages": admin.backlog(),
        "audit": actions.audit_tail(),
        "jobs": processes.job_list(),
        "connected": admin.held["bus"] is not None,
        "rooms": admin.settings,
    }


def kill_preview(query):
    """What a room kill or a single agent's stop would do, with the same options the POST takes."""
    room, addr = query.get("room"), query.get("addr")
    targets = kill.plan(room, query.get("signal", "terminate"), query.get("kill_sessions") == "true", addr=addr)
    return {"room": room, "addr": addr, "targets": [kill.public(t) for t in targets]}


def audit(query):
    def number(key):
        try:
            return float(query[key]) if key in query else None
        except ValueError:
            raise Refused(f"{key} must be a unix time") from None

    return actions.audit_query(query.get("action"), query.get("target"), number("since"), number("until"))


READS = {
    "/api/state": state,
    "/api/agent": agent_detail,
    "/api/room": lambda q: rooms.info(q.get("room")),
    "/api/room/kill": kill_preview,
    "/api/agent/stop": lambda q: kill_preview({**q, "room": None}),
    "/api/ledger": lambda _q: admin.dump(),
    "/api/capacity": lambda _q: capacity.snapshot(),
    "/api/leaks": lambda _q: leaks.report(),
    "/api/audit": audit,
    "/api/usage": lambda _q: usage.snapshot(),
}


class Handler(BaseHTTPRequestHandler):
    token = None
    headless = False

    def do_GET(self):
        url = urlsplit(self.path)
        if not auth.host_ok(self.headers):
            return self.send_error(403)
        if url.path == "/metrics":
            # Read-only and loopback-only like the page itself, so a scraper needs no token.
            return self.reply(
                200, metrics.exposition(metrics.collect()).encode(), "text/plain; version=0.0.4; charset=utf-8"
            )
        if url.path in STATIC and self.headless:
            return self.json(404, {"error": "headless console: no web page, use the API or agent-bridge-admin"})
        if url.path in STATIC:
            name, ctype = STATIC[url.path]
            with open(os.path.join(STATIC_DIR, name), "rb") as f:
                return self.reply(200, f.read(), f"{ctype}; charset=utf-8")
        if not auth.token_ok(self.headers, self.token):
            return self.json(401, {"error": "admin token required"})
        query = {k: v[0] for k, v in parse_qs(url.query).items()}
        try:
            if url.path == "/api/stream":
                return self.stream()
            if url.path in READS:
                return self.json(200, READS[url.path](query))
        except (Refused, BridgeError) as e:
            return self.json(400, {"error": str(e)})
        self.json(404, {"error": "not found"})

    def do_POST(self):
        peer, action = f"{self.client_address[0]}:{self.client_address[1]}", self.path.removeprefix("/api/")
        if not auth.host_ok(self.headers):
            return self.send_error(403)
        if self.path not in actions.ROUTES:
            return self.json(404, {"error": "not found"})
        if not auth.origin_ok(self.headers):
            actions.audit("unknown", peer, action, None, "refused: bad Origin")
            return self.json(403, {"error": "cross-origin request refused"})
        if not auth.token_ok(self.headers, self.token):
            actions.audit("unknown", peer, action, None, "refused: bad admin token")
            return self.json(401, {"error": "admin token required"})
        try:
            size = int(self.headers.get("Content-Length", ""))
            if not 0 < size <= REQUEST_MAX or self.headers.get_content_type() != "application/json":
                raise ValueError
            body = json.loads(self.rfile.read(size))
            if not isinstance(body, dict):
                raise ValueError
        except (ValueError, RecursionError):
            return self.json(400, {"error": f"send a JSON object of at most {REQUEST_MAX} bytes"})
        keys = ("addr", "session", "target", "room", "lease_id", "request_id")
        target = next((body[k] for k in keys if k in body), None)
        target = target[:200] if isinstance(target, str) else None
        try:
            target, result, *extra = actions.ROUTES[self.path](body, peer)
        except (Refused, BridgeError) as e:
            actions.audit("admin", peer, action, target, f"refused: {e}")
            return self.json(400, {"error": str(e)})
        except Exception as e:  # noqa: BLE001  any failure still lands in the audit log
            actions.audit("admin", peer, action, target, f"error: {type(e).__name__}: {e}")
            return self.json(500, {"error": f"{type(e).__name__}: {e}"})
        actions.audit("admin", peer, action, target, result)
        self.json(200, {"ok": True, "result": result, **(extra[0] if extra else {})})

    def do_OPTIONS(self):
        self.send_error(405)

    def head(self, code, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Content-Security-Policy", CSP)

    def reply(self, code, body, ctype):
        self.head(code, ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def json(self, code, obj):
        self.reply(code, json.dumps(obj).encode(), "application/json")

    def stream(self):
        q = events.subscribe()
        self.head(200, "text/event-stream")
        self.end_headers()
        try:
            while events.subscribed(q):
                try:
                    chunk = f"data: {q.get(timeout=15)}\n\n".encode()
                except queue.Empty:
                    chunk = b": ping\n\n"
                self.wfile.write(chunk)
                self.wfile.flush()
        except OSError:
            pass
        finally:
            events.unsubscribe(q)

    def log_message(self, *_):
        pass


def main():
    p = argparse.ArgumentParser(description="The agent bus admin console on 127.0.0.1 (MONITOR_PORT, default 10000).")
    p.add_argument("--headless", action="store_true", help="serve the API and /metrics only, no web page")
    a = p.parse_args()
    Handler.token, Handler.headless = auth.load_token(), a.headless
    server = ThreadingHTTPServer(("127.0.0.1", auth.PORT), Handler)
    admin.observe()
    for fn in (admin.hold_admin, sessions.watch):
        threading.Thread(target=fn, daemon=True).start()
    print(f"admin console for broker {config.HOST}:{config.PORT}", flush=True)
    if a.headless:
        print(
            f"headless: API on http://127.0.0.1:{auth.PORT}/api, token in {auth.TOKEN_FILE}; use agent-bridge-admin",
            flush=True,
        )
    else:
        print(f"login: http://127.0.0.1:{auth.PORT}/#token={Handler.token}", flush=True)
    server.serve_forever()
