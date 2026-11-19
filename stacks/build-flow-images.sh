#!/usr/bin/env bash
# Build one flow image from stacks/flow.Dockerfile. The Tiltfile's custom_build
# and flow.yml's flow_images job both call this, so local and CI images use the
# same targets and build args.
#
# usage: build-flow-images.sh <flow-api|flow-worker|flow-snapshot-worker> [tag]
# DOCKER_GO_DEBUG_FLOW_{API,WORKER,SNAPSHOT_WORKER}=1|true build the Delve-enabled
# -debug target instead.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

image="$1"
tag="${2:-$1}"
case "$image" in
  flow-api)             debug="${DOCKER_GO_DEBUG_FLOW_API:-}" ;;
  flow-worker)          debug="${DOCKER_GO_DEBUG_FLOW_WORKER:-}" ;;
  flow-snapshot-worker) debug="${DOCKER_GO_DEBUG_FLOW_SNAPSHOT_WORKER:-}" ;;
  *) echo "unknown image $image" >&2; exit 1 ;;
esac
target="$image"
case "$debug" in 1|true) target="$image-debug" ;; esac

docker build --file stacks/flow.Dockerfile --target "$target" --tag "$tag" \
  --build-arg DEBUG_BUILD="$debug" \
  --build-arg PEERDB_VERSION_SHA_SHORT="${PEERDB_VERSION_SHA_SHORT:-unknown}" \
  .
