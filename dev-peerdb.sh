#!/bin/bash
set -Eeuo pipefail

echo "Checking for Docker installation..."
if ! command -v docker &>/dev/null; then
  echo "Docker could not be found on PATH."
  exit 1
fi

mode=""
if [[ $# -gt 0 ]]; then
  if [[ $1 == "--detach" ]]; then
    mode="-d"
    echo "Running Docker Compose in detached mode..."
  else
    echo "Invalid argument: $1. Usage: $0 [--detach]"
    exit 1
  fi
fi

docker compose -f docker-compose-dev.yml up --build $mode
