#!/bin/bash
DEFAULT_TILT_PORT=10352

set -euo pipefail

cd "$(dirname "$0")"

missing=()
for cmd in tilt yq docker; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        missing+=("$cmd")
    fi
done
if ! docker compose version >/dev/null 2>&1; then
    missing+=("docker compose")
fi
if [ ${#missing[@]} -gt 0 ]; then
    echo "Missing required dependencies: ${missing[*]}" >&2
    exit 1
fi

cleanup() {
    echo "Shutting down PeerDB..."
    tilt down
}
trap cleanup EXIT

if [ -n "${TILT_PORT:-}" ]; then
    tilt up --port="$TILT_PORT"
else
    tilt up --port="$DEFAULT_TILT_PORT"
fi