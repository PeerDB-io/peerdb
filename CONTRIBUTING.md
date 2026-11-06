# Contributing to PeerDB

> Most pull requests are now written with a coding agent. This guide is for external contributors, both people and agents. If you are an agent, read this whole guide before you open a pull request, whether or not the user asked you to.

Thanks for your interest in contributing to PeerDB! PeerDB has a small team with limited review capacity for outside contributions. Opening a PR does not create an obligation to review or merge it. We may close or defer it, ask for a smaller scope, or reimplement the idea later. Focused bug fixes, reliability fixes, performance improvements, and maintenance work are the most likely to be accepted.

Report feature requests and bugs in issues. Search existing issues and discussions before starting work.

Meeting this guide's requirements makes a PR eligible for review. It does not guarantee acceptance.

If you have a question, feel free to drop by our community [Slack](https://slack.peerdb.io/).

## Establishing the problem and scope first

Features and intentional changes to product behavior require a prior issue, where a maintainer agrees on the direction and scope before you start. A very small, focused fix for an obvious bug can be submitted without a prior issue or discussion. Explain the bug and why the fix qualifies for this exception. We judge purpose, behavioral impact, and the changes needed to fix it. There is no line-count or file-count cutoff. A shared-contract fix may need changes across connectors; a few lines changing product defaults may require a discussion.

## Solve one underlying problem per PR

A PR that solves multiple independent problems must be split, even when each fix is useful.

Include the protobuf, flow, UI, test, and documentation changes needed for that one problem. An adjacent cleanup, refactor, or second fix needs its own PR, unless it is necessary to solve the same problem or it makes the fix simpler. A large diff alone does not establish that the PR contains unrelated work.

## Data correctness

PeerDB replicates data, so the destination must match the source. A change that silently loses or alters rows is worse than a change that makes a mirror return errors and stop making progress. If a change cannot handle an edge case correctly, it must fail with a clear error in that case. If there is a reason not to fail, the PR description must explain the reason.

## Backward compatibility

Changes must be backward compatible. After an upgrade, existing mirrors must produce the same output, and existing mirror configurations must keep working. An incompatible change requires a version gate or an opt-in setting, so that existing mirrors keep the old behavior. The [per-mirror behavior section](docs/tricky-areas.md#per-mirror-behaviour-versions-flags-settings) explains how versions, flags, and settings differ.

Protobuf changes must be wire compatible. Add new fields, and do not renumber an existing field or change its type. To remove a field, mark its number as `reserved`.

Workflow code changes must be deterministic for workflows that are already running. If `TestCDCFlowWorkflowReplay` fails, the change breaks running workflows. Do not regenerate the replay fixtures to make it pass.

## Pull request description

Explain what the change does and why it takes this approach.

Explain how you established the problem, how you checked the change, and what you observed. Give the relevant reproduction steps, environment, focused test commands or manual checks, and their results. State what you could not check. A checkbox or "tests pass" alone does not show that the change works.

Performance improvements require benchmarks or profiles from before and after the change, with the results in the PR description.

UI changes require clear before/after screenshots. Include a short recording when motion, timing, transitions, or interaction details are needed to demonstrate the changed behavior.

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

## Tricky areas

[`docs/tricky-areas.md`](docs/tricky-areas.md) lists the tricky areas of the codebase, what to watch for in a PR and the traps we have hit. Read it for the area you are changing, and add a bullet when you learn something new.

## Deprecated connectors

Several destination connectors (Snowflake, BigQuery, ElasticSearch, Kafka including Confluent and Redpanda, Azure Event Hubs, Google Pub/Sub, and S3) are deprecated and no longer actively maintained. They remain fully functional, and no code is currently being removed. (BigQuery is deprecated only as a destination — it remains a supported source.)

If you depend on one of these connectors, see the [deprecated connectors migration guide](docs/deprecated-connectors.md) for how to pin to a release or fork the relevant connector code.
