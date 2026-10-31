"""Who may use the console: loopback Host, same-origin writes, and the admin token."""

import hmac
import os
import re
import secrets

from .. import config

PORT = int(config.env_number("MONITOR_PORT", "10000", 1, 65535))
# Only these Host headers are served, so a DNS-rebinding page cannot reach the API through the browser.
HOSTS = {f"{h}:{PORT}" for h in ("localhost", "127.0.0.1")}
ORIGINS = {f"http://{h}" for h in HOSTS}
CONFIG = config.CONFIG_DIR
TOKEN_FILE = os.path.join(CONFIG, "admin-token")


def load_token():
    """The admin token, created on first run in a 0600 file under a 0700 directory."""
    os.makedirs(CONFIG, mode=0o700, exist_ok=True)
    os.chmod(CONFIG, 0o700)  # makedirs leaves an existing directory's mode alone
    try:
        fd = os.open(TOKEN_FILE, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        os.chmod(TOKEN_FILE, 0o600)
        with open(TOKEN_FILE) as f:
            token = f.read().strip()
        if not re.fullmatch(r"[A-Za-z0-9_-]{32,128}", token):
            raise SystemExit(f"{TOKEN_FILE} does not hold a valid token, delete it to make a new one") from None
        return token
    with os.fdopen(fd, "w") as f:
        f.write(token := secrets.token_urlsafe(32))
    return token


def host_ok(headers):
    return headers.get("Host") in HOSTS


def origin_ok(headers):
    return headers.get("Origin") in ORIGINS


def token_ok(headers, token):
    return hmac.compare_digest(headers.get("X-Admin-Token", "").encode(), token.encode())


def scraper_ok(headers, token):
    """The token as X-Admin-Token or as a bearer credential, the form a Prometheus scrape config can send."""
    bearer = headers.get("Authorization", "").removeprefix("Bearer ").strip()
    return token_ok(headers, token) or hmac.compare_digest(bearer.encode(), token.encode())
