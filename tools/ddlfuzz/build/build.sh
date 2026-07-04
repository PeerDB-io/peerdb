#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
go build -cover -coverpkg=github.com/PeerDB-io/peerdb/flow/connectors/mysql -o build/ddlfuzz ./cmd/ddlfuzz
go build -o build/ddlfuzz-e2e ./cmd/ddlfuzz-e2e
