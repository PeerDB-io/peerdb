#!/bin/sh
cd "$(dirname "$0")"
. ./env
cd ../flow
exec go run . snapshot-worker
