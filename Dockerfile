# syntax=docker/dockerfile:1
# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 — install Python dependencies into an isolated virtualenv.
# Keeping deps in their own stage means the final image contains no build
# tooling and the layer is only rebuilt when requirements.txt changes.
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

# Create a virtualenv so we can COPY just /opt/venv into the runtime image
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt


# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 — minimal runtime image
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim

# gosu: tiny, well-audited setuid binary used to drop from root → app user.
# It correctly propagates signals (unlike `su` or `sudo`).
RUN apt-get update \
 && apt-get install -y --no-install-recommends gosu \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# ── App user / group ──────────────────────────────────────────────────────────
# Built-in defaults match a typical Linux desktop user (UID/GID 1000).
# Override at build time with --build-arg, or at run time via PUID/PGID envs
# which the entrypoint re-maps dynamically without rebuilding the image.
ARG APP_UID=1000
ARG APP_GID=1000
RUN groupadd -g "${APP_GID}" appgroup \
 && useradd  -u "${APP_UID}" -g appgroup \
             -s /sbin/nologin -M \
             -c "Finance app service account" \
             appuser

# ── Python environment ────────────────────────────────────────────────────────
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# ── Application code ──────────────────────────────────────────────────────────
# Owned by root; app user has read-execute access but cannot modify app files.
WORKDIR /app
COPY app/ ./app/

# Verify the bundled data package (merchant dictionary, MCC codes) is present.
# This fails the build immediately if .dockerignore accidentally excluded it.
RUN test -f /app/app/data/merchant_dictionary.py \
 && test -f /app/app/data/mcc_codes.json \
 && echo "✓ app/data package verified"

# ── Logo cache ────────────────────────────────────────────────────────────────
# logos.py writes cached PNGs to app/static/logos/ at runtime.
# This is the only sub-directory inside /app that the app user needs to write.
# Logos are ephemeral (re-downloaded if the container is recreated); add a
# named volume here if you want them to persist across restarts.
RUN mkdir -p /app/app/static/logos \
 && chown appuser:appgroup /app/app/static/logos

# ── Persistent data volume ────────────────────────────────────────────────────
# The database (finance.db), secret key file, and any other runtime-mutable
# files all live under /data.  Mount a host directory or named volume here.
RUN mkdir /data && chown appuser:appgroup /data
VOLUME ["/data"]

# ── Entrypoint ────────────────────────────────────────────────────────────────
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 755 /usr/local/bin/docker-entrypoint.sh

# ── Runtime environment ───────────────────────────────────────────────────────
ENV DATA_DIR=/data \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1

EXPOSE 8000

# TCP-based health check — succeeds as soon as uvicorn is accepting connections.
# Does not require a dedicated /health endpoint and is unaffected by auth.
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD python -c \
    "import socket; s=socket.create_connection(('localhost',8000),timeout=5); s.close()"

ENTRYPOINT ["docker-entrypoint.sh"]
# Single worker: SQLite + aiosqlite is designed for one process with async
# concurrency; multiple workers would each open the DB and fight over WAL locks.
CMD ["uvicorn", "app.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1", \
     "--no-access-log"]
