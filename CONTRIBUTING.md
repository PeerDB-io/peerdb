# Contributing to PeerDB

Thanks for your interest in contributing to PeerDB! Bug reports, feature requests, and pull requests are all welcome. If you have a question, feel free to drop by our [Slack](https://slack.peerdb.io/).

## Deprecated connectors

Several destination connectors (Snowflake, BigQuery, ElasticSearch, Kafka including Confluent and Redpanda, Azure Event Hubs, Google Pub/Sub, and S3) are deprecated and no longer actively maintained. They remain fully functional, and no code is currently being removed. (BigQuery is deprecated only as a destination — it remains a supported source.)

If you depend on one of these connectors, see the [deprecated connectors migration guide](docs/deprecated-connectors.md) for how to pin to a release or fork the relevant connector code.

## Test selection with build tags

Go test files under `flow/` that need external infrastructure carry a
`//go:build` constraint naming the resource they exercise. The tags are:

| Tag           | Meaning                                                   |
|---------------|-----------------------------------------------------------|
| `postgres`    | PostgreSQL source suites (also the cloud-destination ones) |
| `mysql`       | MySQL and MariaDB source suites, all flavors               |
| `mongodb`     | MongoDB source suites                                      |
| `cockroachdb` | CockroachDB source suites                                  |
| `bigquery`    | BigQuery source suites                                     |
| `clickhouse`  | Destination-only ClickHouse tests                          |

All e2e suites also use the catalog and a ClickHouse destination, so those two
are implied by every tag rather than having tags of their own.

Files without a constraint are unit tests and run with a plain `go test ./...`
and no infrastructure. Files with a constraint are compiled only when the tag
is passed, so a bare `go test ./e2e/` finds no tests. Pass the tags for the
resources you have running, for example:

```sh
cd flow && go test -tags postgres -run TestGenericCH_PG ./e2e/
```

The Tilt test launchers and the CI workflows pass the tags for you. Files that
only add methods to a suite shared across sources (for example
`e2e/clickhouse_test.go`) stay untagged; put a constraint on a file only when
everything in it is specific to that resource, and keep top-level `Test`
functions in files tagged for their source.

Editors that use gopls see one tag set at a time. To get diagnostics in the
tagged files, set the build flags, for example in VS Code:

```json
"gopls": { "buildFlags": ["-tags=postgres,mysql,mongodb,cockroachdb,bigquery,clickhouse"] }
```
