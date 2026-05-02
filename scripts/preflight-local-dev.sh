#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

GO_VER=$(grep -oE 'golang:[0-9.]+' stacks/flow.Dockerfile | head -1 | cut -d: -f2)
RUST_VER=$(grep -oE 'rust-[0-9.]+' stacks/peerdb-server.Dockerfile | head -1 | cut -d- -f2)
NODE_VER=$(grep -oE 'node:[0-9]+' stacks/peerdb-ui.Dockerfile | head -1 | cut -d: -f2)

fail=0
check_cmd() {
  command -v "$1" >/dev/null || { echo "MISSING: $1 — $2"; fail=1; }
}

check_cmd go     "install Go ${GO_VER}+ (https://go.dev/dl)"
check_cmd cargo  "install rustup, then 'rustup install ${RUST_VER}' (https://rustup.rs)"
check_cmd protoc "brew install protobuf"
check_cmd buf    "brew install bufbuild/buf/buf"
check_cmd node   "brew install node@${NODE_VER%%.*}"
check_cmd npm    "ships with node"
check_cmd pkg-config "brew install pkg-config"

if command -v pkg-config >/dev/null; then
  pkg-config --exists geos || { echo "MISSING: geos — brew install geos"; fail=1; }
fi

vercmp_ge() {
  [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | head -1)" = "$1" ]
}

if command -v go >/dev/null; then
  go_have=$(go version | awk '{print $3}' | sed 's/^go//')
  vercmp_ge "$GO_VER" "$go_have" || { echo "Go too old: have $go_have, need >=$GO_VER"; fail=1; }
fi

if command -v node >/dev/null; then
  node_have=$(node -v | sed 's/^v//')
  vercmp_ge "$NODE_VER" "$node_have" || { echo "Node too old: have $node_have, need >=$NODE_VER"; fail=1; }
fi

if command -v rustc >/dev/null; then
  rust_have=$(rustc --version | awk '{print $2}')
  vercmp_ge "$RUST_VER" "$rust_have" || { echo "Rust too old: have $rust_have, need >=$RUST_VER"; fail=1; }
fi

if [ "$fail" -eq 0 ]; then
  echo "preflight OK (go>=$GO_VER, rust>=$RUST_VER, node>=$NODE_VER)"
fi
exit $fail
