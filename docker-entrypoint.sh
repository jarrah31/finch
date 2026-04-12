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
# getent group returns "name:x:GID:members"; field 3 is the numeric GID.
# -o / --non-unique allows reusing a GID already owned by another group.
CURRENT_GID=$(getent group appgroup | cut -d: -f3)
if [ "${CURRENT_GID}" != "${PGID}" ]; then
    groupmod -o -g "${PGID}" appgroup
fi

# ── Re-map user ───────────────────────────────────────────────────────────────
if [ "$(id -u appuser)" != "${PUID}" ]; then
    usermod -o -u "${PUID}" appuser
fi

# ── Fix data directory ownership ──────────────────────────────────────────────
# Best-effort: requires CAP_CHOWN (add to cap_add in your compose file if
# needed).  Failures are non-fatal — the app can still run if the mounted
# volume is already accessible by the mapped UID/GID.
chown -R appuser:appgroup /data 2>/dev/null || \
    echo "Warning: could not chown /data (missing CAP_CHOWN?) — continuing anyway"

# ── Drop privileges and exec the CMD ─────────────────────────────────────────
exec gosu appuser "$@"
