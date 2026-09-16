#!/bin/sh
set -eu

image_id_file="$1"
shift
mkdir -p "$(dirname "$image_id_file")"

# Publish atomically so Tilt only deploys successfully built images.
docker build --iidfile "$image_id_file.tmp" "$@" .
mv "$image_id_file.tmp" "$image_id_file"
