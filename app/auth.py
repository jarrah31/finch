import hashlib
import hmac
import os
import base64
import secrets
from pathlib import Path

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, RedirectResponse
from urllib.parse import quote

DATA_DIR = Path(os.getenv("DATA_DIR", Path(__file__).parent.parent / "data"))

# ── Secret key ────────────────────────────────────────────────────────────────

def _load_or_create_secret() -> str:
    env_key = os.environ.get("SECRET_KEY")
    if env_key:
        return env_key
    secret_file = DATA_DIR / ".secret"
    if secret_file.exists():
        return secret_file.read_text().strip()
    key = secrets.token_hex(32)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    # Write with restricted permissions atomically
    fd = os.open(str(secret_file), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, key.encode())
    finally:
        os.close(fd)
    return key


SECRET_KEY: str = _load_or_create_secret()


# ── Password hashing (stdlib scrypt) ──────────────────────────────────────────

def hash_password(password: str) -> str:
    """Hash a password using scrypt. Returns a base64-encoded string (salt + hash)."""
    salt = os.urandom(16)
    dk = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=16384, r=8, p=1)
    return base64.b64encode(salt + dk).decode("ascii")


def verify_password(password: str, stored: str) -> bool:
    """Verify a password against a stored hash. Constant-time comparison."""
    try:
        data = base64.b64decode(stored.encode("ascii"))
        salt = data[:16]
        dk_stored = data[16:]
        dk_check = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=16384, r=8, p=1)
        return hmac.compare_digest(dk_check, dk_stored)
    except Exception:
        return False


# ── Auth middleware ────────────────────────────────────────────────────────────

# Paths that do NOT require authentication
_EXEMPT_PREFIXES = ("/static/",)
_EXEMPT_EXACT = {"/login", "/setup", "/logout"}


class AuthMiddleware(BaseHTTPMiddleware):
    """Redirect unauthenticated page requests to /login; return 401 for API requests."""

    async def dispatch(self, request, call_next):
        path = request.url.path

        # Always allow static files and auth routes through
        if path in _EXEMPT_EXACT or any(path.startswith(p) for p in _EXEMPT_PREFIXES):
            return await call_next(request)

        # If first-run setup is required, redirect everything to /setup
        if getattr(request.app.state, "setup_required", False):
            if path.startswith("/api/"):
                return JSONResponse({"detail": "Application not yet configured"}, status_code=503)
            return RedirectResponse("/setup")

        # Check authentication
        if not request.session.get("authenticated"):
            # HTMX requests and API calls get a 401 with HX-Redirect header
            if path.startswith("/api/") or request.headers.get("HX-Request"):
                response = JSONResponse({"detail": "Not authenticated"}, status_code=401)
                response.headers["HX-Redirect"] = "/login"
                return response
            # Regular page requests get a redirect to /login?next=<path>
            next_path = request.url.path
            if request.url.query:
                next_path += "?" + request.url.query
            return RedirectResponse(f"/login?next={quote(next_path)}")

        return await call_next(request)
