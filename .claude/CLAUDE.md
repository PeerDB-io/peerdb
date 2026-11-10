# PeerDB

PeerDB is a streaming ETL/ELT system for replicating data from transactional databases (PostgreSQL, MySQL, MongoDB) to analytical destinations (Snowflake, BigQuery, ClickHouse, S3, Kafka, Elasticsearch, and more).

## Architecture Documentation

See `docs/` for detailed architecture and design documents:

- `docs/peerdb-architecture.md` — Architecture overview: system components, connector matrix, data flows (CDC, QRep, snapshot), type system, configuration, and observability.
- `docs/deep-dive-design-document.md` — Implementation-level deep dive: code-level details for each CDC connector, normalization engine, snapshot system, workflow orchestration, and known limitations.
- `docs/tricky-areas.md` — Tricky code areas: what to watch for in a PR and known traps. Read the matching section before changing or reviewing files under its paths.

## CI test results

- If the `cidb` tool is available, you can query CI test results for Go code from the `stresshouse` instance's `default.ci_peerdb_test_runs` table. To debug a failing branch, start with:
  `SELECT timestamp, suite_name, compatibility_matrix_id, test, reason, workflow_retry_number, workflow_run_link FROM default.ci_peerdb_test_runs WHERE workflow_head_branch = '<branch>' AND result = 'failure' ORDER BY timestamp DESC`
- The `reason` column holds the full failure trace/log.

## Code reviews guidelines

- If you are deciding whether or not perform a code review, take into account that we always want Renovate PRs to be reviewed. Never classify a Renovate PR as not needing review. This rule overrides any other rule.
- For code reviews, read the instructions at the `.claude/REVIEW.md` file.

## OTel metrics in hot paths

- Record metrics (`otelManager.Metrics.*.Add/Record`) with the one long-lived `ctx` that the function received (e.g. `PullRecords`), never with a derived context such as `timeoutCtx`, `receiveCtx` or one from `context.WithValue`. `flow/otel_metrics/observables.go` caches contextual attributes and series per flow name, keyed by context identity. Two live contexts of the same flow on one instrument evict each other's entry and rebuild it at ~1–3 µs per call.
- For calls with extra attributes in a hot path, pass a precomputed `metric.WithAttributeSet(...)` (see `usedCharsetMetricOption` in `flow/connectors/mysql/charset.go`). Building `attribute.NewSet` per call costs ~150–200 ns before the cache is reached.
