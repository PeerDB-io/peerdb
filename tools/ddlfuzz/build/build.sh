#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
# -covermode=atomic: runtime/coverage.WriteCounters (the parser-coverage
# feedback poll) refuses other modes. cmd/ddlfuzz (a thin main shim) must be in
# -coverpkg too: the compiler only injects the coverage init hook into an
# instrumented main package, and without it WriteCounters always fails.
# Keep supervisor/build.go ddlfuzzCoverBuildArgs in sync with these flags.
go build -cover -covermode=atomic -coverpkg=github.com/PeerDB-io/peerdb/flow/connectors/mysql,github.com/PeerDB-io/peerdb/tools/ddlfuzz/cmd/ddlfuzz -o build/ddlfuzz ./cmd/ddlfuzz
go build -o build/ddlfuzz-e2e ./cmd/ddlfuzz-e2e
