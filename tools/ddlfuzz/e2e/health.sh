#!/bin/sh
set -eu
cd "$(dirname "$0")"

status_json="$(docker compose -p ddlfuzz-e2e -f compose.yml ps --format json)"
printf '%s\n' "$status_json" | awk '
  BEGIN { mysql=0; mariadb=0 }
  /"Service":"mysql"/ && /"Health":"healthy"/ { mysql=1 }
  /"Service":"mariadb"/ && /"Health":"healthy"/ { mariadb=1 }
  END { if (!mysql || !mariadb) exit 1 }
'

for port in 13306 13307; do
  PORT="$port" python3 - <<'PY'
import os
import socket
import sys

port = int(os.environ["PORT"])
s = socket.socket()
s.settimeout(1)
try:
    s.connect(("127.0.0.1", port))
finally:
    s.close()
PY
done
