#!/bin/bash
# Cleanup script for Tilt resources
# Based on Tiltfile from /home/pablo/engineering/clickhouse/peerdb

set -e

echo "=== Tilt Resource Cleanup Script ==="
echo ""

# Clean up ancillary volumes (from line 206-219 in Tiltfile)
echo "Cleaning up ancillary volumes..."
for v in $(yq '.volumes | keys[]' ancillary-docker-compose.yml); do
    if docker volume rm peerdb_$v 2>/dev/null; then
        echo "✓ Removed volume: $v"
    else
        echo "⚠ Volume $v not found or not removable (may be in use)"
    fi
done

echo ""
echo "Cleaning up test code caches (from line 221-228 in Tiltfile)..."
cd flow && go clean -cache
echo "✓ Cleared Go cache"

echo ""
echo "=== Additional Tilt cleanup options ==="
echo "To stop Tilt resources:"
echo "  tilt down"
echo ""
echo "To disable specific resources:"
echo "  tilt disable <resource-name>"
echo ""
echo "To clear Tilt's build cache:"
echo "  ~/.cache/tilt/build_cache/prune"
echo ""
echo "To clean Tilt layer caches:"
echo "  rm -rf ~/.cache/tilt/layers/prune"

echo ""
echo "=== Cleanup complete ==="
