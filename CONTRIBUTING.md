# Contributing to PeerDB

Thanks for your interest in contributing to PeerDB! Bug reports, feature requests, and pull requests are all welcome. If you have a question, feel free to drop by our [Slack](https://slack.peerdb.io/).

## Deprecated connectors

Several destination connectors (Snowflake, BigQuery, ElasticSearch, Kafka including Confluent and Redpanda, Azure Event Hubs, Google Pub/Sub, and S3) are deprecated and no longer actively maintained. They remain fully functional, and no code is currently being removed. (BigQuery is deprecated only as a destination — it remains a supported source.)

If you depend on one of these connectors, see the [deprecated connectors migration guide](docs/deprecated-connectors.md) for how to pin to a release or fork the relevant connector code.

## Testing

Flow tests have two build modes. Run commands from `flow/` unless a command says otherwise.

| Tier | What it needs | Example |
|---|---|---|
| Unit | No Docker or external services | `go test ./...` |
| Tilt (E2E + integration) | Tilt services; files have `//go:build tilt` | `go test -tags tilt ./connectors/mysql/...` or `go test -tags tilt ./e2e/mysql_clickhouse/ -run TestGenericCH_MySQL` |

Run the nested `flow/pkg` module separately: `cd pkg && go test ./...` for unit tests, or add `-tags tilt` when its service tests are needed. Name files containing only service tests `*_integration_test.go`. Any test that opens a connection to a service belongs in a `tilt` tagged file, including e2e entry points. The unit CI job starts no Docker services, so an untagged service connection will fail there. Tests that start an in-process test server can remain unit tests.

To get diagnostics for tagged files in gopls, add this to your editor settings:

```json
"gopls": {"buildFlags": ["-tags=tilt"]}
```

CI selects Tilt-tagged tests by package. E2e packages map to source jobs as follows:

| E2E package | CI job |
|---|---|
| `postgres_clickhouse`, `postgres_postgres`, `switchboard_postgres` | `postgres_clickhouse` |
| `postgres_other` | `postgres_other` |
| `mysql_clickhouse`, `switchboard_mysql` | `mysql_clickhouse` |
| `mongo_clickhouse`, `switchboard_mongo` | `mongo_clickhouse` |
| `cockroachdb_clickhouse` | `cockroachdb_clickhouse` |
| `bigquery_clickhouse` | `bigquery_clickhouse` |
