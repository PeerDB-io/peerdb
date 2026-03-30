#!/bin/bash
DEFAULT_TILT_PORT=10352

set -euo pipefail

cd "$(dirname "$0")"

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