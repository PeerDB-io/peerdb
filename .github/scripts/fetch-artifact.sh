#!/usr/bin/env bash
# Download an unarchived artifact (upload-artifact with `archive: false`) of
# this repository to a file, and verify it against its upload digest.
#
# download-artifact fetches over a single stream (~30 MB/s); resolving the
# artifact's blob URL and letting azcopy fetch it in parallel ranges is
# several times faster.
#
# usage: fetch-artifact.sh <artifact-id> <sha256-digest or ''> <output-file>
# Needs GH_TOKEN with actions: read.
set -euo pipefail

artifact_id="$1"
digest="$2"
out="$3"

url="$(curl -fsS -o /dev/null -w '%{redirect_url}' \
  -H "Authorization: Bearer $GH_TOKEN" \
  -H 'Accept: application/vnd.github+json' \
  "$GITHUB_API_URL/repos/$GITHUB_REPOSITORY/actions/artifacts/$artifact_id/zip")"
if [ -z "$url" ]; then
  echo "artifact $artifact_id has no download URL" >&2
  exit 1
fi

start=$SECONDS
azcopy copy "$url" "$out" --log-level=ERROR --output-level=quiet
echo "fetched $(du -h "$out" | cut -f1) in $((SECONDS - start))s"
if [ -n "$digest" ]; then
  echo "${digest#sha256:}  $out" | sha256sum -c --quiet -
fi
