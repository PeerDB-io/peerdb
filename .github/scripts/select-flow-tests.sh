#!/usr/bin/env bash
# Select tilt-only tests for the flow and pkg modules concurrently.
# Inputs: TEST_PACKAGES, PKG_TEST_PACKAGES, and RUNNER_TEMP.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../../flow"

# Keep discovery outputs outside Tilt's watched source tree. The next
# step reads the selected package/test pairs from this directory.
selectiondir="$RUNNER_TEMP/test-selection"
mkdir -p "$selectiondir"
COVERFLAGS_FLOW=(-cover -coverpkg github.com/PeerDB-io/peerdb/flow/...)
COVERFLAGS_PKG=(-cover -coverpkg github.com/PeerDB-io/peerdb/flow/pkg/...)

# List all requested packages in one invocation. Keep package names in the
# output so subtraction cannot confuse identically named tests in different
# packages. A build failure must fail selection, not silently drop tests.
list_tests() {
  local label="$1" output="$2" rc=0
  shift 2
  go test -json -list '^Test' -p 32 "$@" > "$output.json" 2> "$output.stderr" || rc=$?
  if [ "$rc" -ne 0 ]; then
    printf '[%s] go test failed (exit %s)\n' "$label" "$rc" >&2
    {
      jq -j '.Output // empty' "$output.json" || true
      cat "$output.stderr"
    } 2>&1 | sed "s/^/[$label] /" >&2
    return "$rc"
  fi
  sed "s/^/[$label] /" "$output.stderr" >&2
  # Emit package/test-name pairs, ignoring other test output.
  jq -r '
    select(.Action == "output")
    | .Package as $package
    | (.Output | split("\n")[]) as $test_name
    | select($test_name | test("^Test[^[:space:]]*$"))
    | [$package, $test_name]
    | @tsv
  ' "$output.json" 2> "$output-parse.stderr" | LC_ALL=C sort -u > "$output.tsv" || rc=$?
  sed "s/^/[$label] /" "$output-parse.stderr" >&2
  return "$rc"
}

# Skip go test when no packages have untagged tests. Passing only
# unmatched patterns would fail with "no packages to test".
list_untagged_tests() {
  local label="$1" patterns="$2" expanded package rc=0
  local output="$selectiondir/$label-untagged"
  local -a packages untagged_packages
  shift 2
  read -ra packages <<< "$patterns"
  expanded=$(go list -e \
    -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' \
    "${packages[@]}" 2> "$output-list.stderr") || rc=$?
  sed "s/^/[$label untagged] /" "$output-list.stderr" >&2
  if [ "$rc" -ne 0 ]; then
    return "$rc"
  fi
  untagged_packages=()
  while IFS= read -r package; do
    if [ -n "$package" ]; then
      untagged_packages+=("$package")
    fi
  done <<< "$expanded"
  if [ ${#untagged_packages[@]} -eq 0 ]; then
    : > "$output.tsv"
    return 0
  fi
  list_tests "$label untagged" "$output" "$@" "${untagged_packages[@]}"
}

select_tests() (
  local label="$1" patterns="$2" untagged_pid tagged_pid rc=0
  local -a packages
  shift 2
  read -ra packages <<< "$patterns"

  list_untagged_tests "$label" "$patterns" "$@" &
  untagged_pid=$!
  list_tests "$label tagged" "$selectiondir/$label-tagged" -tags tilt "$@" "${packages[@]}" &
  tagged_pid=$!
  wait "$untagged_pid" || rc=1
  wait "$tagged_pid" || rc=1
  if [ "$rc" -ne 0 ]; then
    return "$rc"
  fi

  # Both lists contain sorted package/test-name pairs. Keep only
  # tests added by the tilt tag within each package.
  LC_ALL=C comm -13 \
    "$selectiondir/$label-untagged.tsv" \
    "$selectiondir/$label-tagged.tsv" > "$selectiondir/$label-selected.tsv"
)

# Run (flow, pkg) x (untagged, tagged) concurrently. Wait for every
# listing and fail this step if any listing or parsing fails.
select_tests flow "$TEST_PACKAGES" "${COVERFLAGS_FLOW[@]}" &
selection_pids=("$!")
if [ -n "$PKG_TEST_PACKAGES" ]; then
  (
    cd pkg || exit 1
    select_tests pkg "$PKG_TEST_PACKAGES" "${COVERFLAGS_PKG[@]}"
  ) &
  selection_pids+=("$!")
fi
rc=0
for pid in "${selection_pids[@]}"; do
  wait "$pid" || rc=1
done

exit "$rc"
