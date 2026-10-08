#!/usr/bin/env bash
# Run from any directory. Requires a pre-token baseline revision (default HEAD).
# PEERDB_BENCH_S3=1 enables real uploads to MinIO configured by ../.env.
# BENCH_OUTPUT optionally selects a persistent results directory.
set -euo pipefail
cd "$(dirname "$0")/../flow"
bench_dir="${BENCH_OUTPUT:-$(mktemp -d)}"
mkdir -p "$bench_dir"
bench_dir="$(cd "$bench_dir" && pwd)"
base_revision="${1:-HEAD}"
git show "$base_revision:flow/model/record_items.go" > "$bench_dir/record_items.baseline.go"
printf 'package model\n' > "$bench_dir/empty.go"
python3 - "$PWD" "$bench_dir" <<'PY'
import json,sys
from pathlib import Path
flow,out=map(Path,sys.argv[1:])
replacements={
    flow/'model/record_items.go':out/'record_items.baseline.go',
    flow/'model/record_items_json.go':out/'empty.go',
    flow/'model/record_items_legacy_test.go':out/'empty.go',
}
(out/'baseline.json').write_text(json.dumps({'Replace':{str(k):str(v) for k,v in replacements.items()}}))
PY
go test -overlay="$bench_dir/baseline.json" -c -o "$bench_dir/stream-baseline.test" ./connectors/utils
go test -c -o "$bench_dir/stream-candidate.test" ./connectors/utils
# Alternate versions; never run benchmarks concurrently. Each op drains 32,768
# prefabricated records. Record construction/channel prefill are outside timing.
for round in 1 2 3 4 5; do
    for variant in baseline candidate; do
        "$bench_dir/stream-$variant.test" -test.run '^$' \
            -test.bench '^BenchmarkRecordStreamToS3$' -test.cpu 4 \
            -test.benchtime 3x -test.benchmem \
            > "$bench_dir/stream-$variant-$round.txt" 2>&1
    done
done
printf 'Results: %s\n' "$bench_dir"
