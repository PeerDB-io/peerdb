#!/usr/bin/env bash
# Build benchmark-only workers with instantaneous ClickHouse normalization.
# Never use these binaries for real replication: they acknowledge normalization
# without inserting anything into ClickHouse. S3 sync/checkpointing stays real.
set -euo pipefail
cd "$(dirname "$0")/../flow"
bench_dir="${BENCH_OUTPUT:?set BENCH_OUTPUT to the benchmark artifact directory}"
mkdir -p "$bench_dir"
bench_dir="$(cd "$bench_dir" && pwd)"
git show "${1:-HEAD}:flow/model/record_items.go" > "$bench_dir/record_items.baseline.go"
printf 'package model\n' > "$bench_dir/empty.go"
python3 - "$PWD" "$bench_dir" <<'PY'
import json, sys
from pathlib import Path
flow, out = map(Path, sys.argv[1:])
source = flow / 'connectors/clickhouse/normalize.go'
text = source.read_text()
signature = '''func (c *ClickHouseConnector) NormalizeRecords(
\tctx context.Context,
\treq *model.NormalizeRecordsRequest,
) (model.NormalizeResponse, error) {'''
assert text.count(signature) == 1
text = text.replace(signature, signature + '''
\t// BENCHMARK ONLY: model infinitely fast, independent normalization.
\treturn model.NormalizeResponse{StartBatchID: req.SyncBatchID, EndBatchID: req.SyncBatchID}, nil
''')
replacement = out / 'normalize.instant.go'
replacement.write_text(text)
common = {str(source): str(replacement)}
for variant in ('baseline', 'candidate'):
    replacements = dict(common)
    if variant == 'baseline':
        for filename, target in [('record_items.go', 'record_items.baseline.go'),
                                 ('record_items_json.go', 'empty.go'),
                                 ('record_items_legacy_test.go', 'empty.go')]:
            replacements[str(flow / 'model' / filename)] = str(out / target)
    (out / (variant + '.json')).write_text(json.dumps({'Replace': replacements}))
PY
for variant in baseline candidate; do
    go build -overlay="$bench_dir/$variant.json" -o "$bench_dir/peer-flow-$variant" .
done
go test -c -o "$bench_dir/e2e.test" ./e2e
printf 'Workers: %s\nUse PEERDB_BENCH_BACKLOG=1 with an isolated deployment/catalog.\n' "$bench_dir"
