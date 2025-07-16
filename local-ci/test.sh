#!/bin/sh
cd "$(dirname "$0")"
. ./env
cd ../flow
exec go test -v -parallel 1 -p 1 ./e2e/clickhouse/...
