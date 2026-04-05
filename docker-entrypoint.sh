#!/bin/sh
# docker-entrypoint.sh — re-map the app user to the host's UID/GID at runtime.
#
# Usage (docker run):
#   -e PUID=1000 -e PGID=1000   (defaults; match a typical Linux desktop user)
#
# This lets host-mounted volumes be readable/writable without chmod hacks on
# the host side.  gosu is used instead of su/sudo so signals propagate cleanly
# to the child process (important for graceful uvicorn shutdown).
set -e

PUID=${PUID:-1000}
PGID=${PGID:-1000}

echo "Starting Finance with UID=${PUID} GID=${PGID}"

# ── Re-map group ──────────────────────────────────────────────────────────────
# -o / --non-unique allows mapping to an already-existing GID (e.g. when the
# host GID happens to match another group inside the container).
if [ "$(id -g appgroup)" != "${PGID}" ]; then
    groupmod -o -g "${PGID}" appgroup
fi

# ── Re-map user ───────────────────────────────────────────────────────────────
if [ "$(id -u appuser)" != "${PUID}" ]; then
    usermod -o -u "${PUID}" appuser
fi

# ── Fix data directory ownership ──────────────────────────────────────────────
# Essential on first run against a fresh (empty) volume, and when PUID/PGID
# differ from the previous run.
chown -R appuser:appgroup /data

# ── Drop privileges and exec the CMD ─────────────────────────────────────────
exec gosu appuser "$@"
