#!/usr/bin/env bash
# Runs fault-injection tests, then reruns failed tests up to twice.
#
# gotestsum --rerun-fails reruns each failed test in its own go test process,
# one after another. A red fault case takes 12-15 minutes, so a shard with ten
# reds would rerun for hours. This script reruns all of a package's failures in
# one go test process instead, in parallel.
#
# Run from flow/. Environment:
#   FAULT_PACKAGES  space-separated package:parallelism pairs; each package runs
#                   in its own go test process, concurrently with the others
#   FAULT_RUN       go test -run pattern for the first attempt
#   FAULT_OUT       output directory for attempt<N>-<package>.xml and rerun.txt
#   FAULT_ATTEMPTS  total attempts, default 3
#   FAULT_ATTEMPT_TIMEOUT        go test -timeout of each attempt, default 55m
#   FAULT_RERUN_DEADLINE_MINUTES no rerun starts after this, default 60, so a
#                   rerun cannot outlast the step timeout
set -uo pipefail

: "${FAULT_PACKAGES:?}" "${FAULT_RUN:?}" "${FAULT_OUT:?}"
attempts="${FAULT_ATTEMPTS:-3}"
attempt_timeout="${FAULT_ATTEMPT_TIMEOUT:-55m}"
rerun_deadline="${FAULT_RERUN_DEADLINE_MINUTES:-60}"
results="$(cd "$(dirname "$0")" && pwd)/fault_results.py"
mkdir -p "$FAULT_OUT"
: > "$FAULT_OUT/rerun.txt"
read -ra specs <<< "$FAULT_PACKAGES"

run_package() {
  local attempt="$1" pkg="$2" parallel="$3" run="$4" name
  name="$(basename "$pkg")"
  gotestsum --format standard-quiet --no-color \
    --junitfile "$FAULT_OUT/attempt$attempt-$name.xml" --packages "$pkg" -- \
    -tags tilt -count=1 -run "$run" -parallel "$parallel" -timeout "$attempt_timeout" 2>&1 |
    sed -u "s|^|[$name #$attempt] |"
  echo "$?" > "$FAULT_OUT/attempt$attempt-$name.rc"
}

for attempt in $(seq 1 "$attempts"); do
  if [ "$attempt" -gt 1 ] && [ $((SECONDS / 60)) -ge "$rerun_deadline" ]; then
    echo "attempt $attempt: skipped, ${rerun_deadline}-minute rerun deadline passed" | tee -a "$FAULT_OUT/rerun.txt"
    break
  fi
  started=0
  for spec in "${specs[@]}"; do
    pkg="${spec%:*}"
    parallel="${spec##*:}"
    name="$(basename "$pkg")"
    run="$FAULT_RUN"
    if [ "$attempt" -gt 1 ]; then
      prev="$FAULT_OUT/attempt$((attempt - 1))-$name.xml"
      [ -f "$prev" ] || continue
      run="$(python3 "$results" rerun-pattern "$prev")"
      [ -n "$run" ] || continue
      python3 "$results" failed "$prev" | sed "s|^|attempt $attempt reruns $name: |" | tee -a "$FAULT_OUT/rerun.txt"
    fi
    run_package "$attempt" "$pkg" "$parallel" "$run" &
    started=1
  done
  [ "$started" -eq 1 ] || break
  wait
done

# Red if any package's last attempt failed.
rc=0
for spec in "${specs[@]}"; do
  name="$(basename "${spec%:*}")"
  last="$(find "$FAULT_OUT" -name "attempt*-$name.rc" | sort | tail -n 1)"
  if [ -z "$last" ] || [ "$(cat "$last")" != 0 ]; then
    rc=1
  fi
done
exit "$rc"
