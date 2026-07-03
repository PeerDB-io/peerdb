#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
go build -tags ddlfuzz -o build/ddlfuzz-e2e ./cmd/ddlfuzz-e2e
e2e/up.sh
trap 'e2e/down.sh' EXIT
./build/ddlfuzz-e2e --state state --cases 100 --smoke
