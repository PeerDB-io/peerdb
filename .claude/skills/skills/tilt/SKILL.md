---
name: tilt
description: Interact with the PeerDB Tilt dev environment. Use when the user wants to check service status, enable/disable resources, trigger builds or tests, view logs, or manage the local development infrastructure.
argument-hint: "[action] [resource...]"
allowed-tools: Bash(tilt *) Bash(./tilt.sh) Bash(docker ps *) Bash(tail *) Bash(python3 *) Read Monitor ScheduleWakeup
---

You are managing PeerDB's local Tilt dev environment via the CLI.

## Tilt connection

All `tilt` CLI commands MUST use `--port 10352`:
```
tilt --port 10352 <subcommand> [args...]
```

## Starting and stopping Tilt

Use the `tilt.sh` script at the project root to start Tilt, keep it running until you choose to stop it.
To top, kill the script execution.

If the script was stopped for some reason but Tilt is still running, use `tilt down` to stop.

## Quick reference: tilt subcommands

| Action | Command |
|--------|---------|
| List all resources with status | `tilt --port 10352 get uiresource` |
| Get JSON resource detail | `tilt --port 10352 get uiresource -o json` |
| Enable a resource | `tilt --port 10352 enable <name>` |
| Disable a resource | `tilt --port 10352 disable <name>` |
| Trigger (run) a resource | `tilt --port 10352 trigger <name>` (accepts exactly one resource; for multiple, issue one command per resource, e.g. `for r in a b c; do tilt --port 10352 trigger "$r"; done`) |
| Stream logs | `tilt --port 10352 logs -f <name>` |
| Recent logs | `tilt --port 10352 logs --since 5m <name>` |
| Tail N lines | `tilt --port 10352 logs --tail 200 <name>` |
| Wait for ready | `tilt --port 10352 wait --for=condition=Ready uiresource/<name>` |

NOTE: For wait commands, use `--timeout 10` to avoid waiting indefinitely. It's better to iterate over several wait attempts with shorter timeouts than to risk getting stuck on a single long wait.

## Architecture

Tilt manages Docker Compose services defined in `docker-compose-dev.yml` (core PeerDB) and `ancillary-docker-compose.yml` (test databases + infra). Environment variables for tests are in `.env` at the project root and are auto-loaded by Go tests.

### Resource labels

| Label | Purpose | Examples |
|-------|---------|---------|
| `PeerDB` | Core services (always running) | flow-api, flow-worker, flow-snapshot-worker, catalog, temporal, minio |
| `Ancillary-DB` | Data stores (manual start) | postgres, clickhouse, mongodb, mysql-gtid, mysql-pos, mariadb |
| `Ancillary-DB-Provisioning` | DB setup scripts (auto after DB starts) | provision-postgres, provision-clickhouse, provision-mongodb, provision-mysql-gtid, provision-mysql-pos, provision-mariadb |
| `Ancillary-TestInfra` | Test infrastructure (manual start, needed by SSH keepalive and chaos tests) | toxiproxy, openssh |
| `Test` | Test launchers (manual trigger) | e2e_postgres, e2e_mysql-gtid, connector_clickhouse, ... |
| `Monitoring` | Observability | dozzle |

### Test resources and their dependencies

**E2E tests** (run `go test ./e2e/` with a specific `-run` pattern):

| Resource | Test pattern | Required ancillary DBs |
|----------|-------------|----------------------|
| `e2e_postgres` | `TestGenericCH_PG` | postgres, clickhouse |
| `e2e_mysql-gtid` | `TestGenericCH_MySQL` | mysql-gtid, clickhouse |
| `e2e_mysql-pos` | `TestGenericCH_MySQL` | mysql-pos, clickhouse |
| `e2e_mariadb` | `TestGenericCH_MySQL` | mariadb, clickhouse |
| `e2e_mongodb` | `TestMongoClickhouseSuite` | mongodb, clickhouse |
| `e2e_switchboard-postgres` | `TestSwitchboardPostgres` | postgres, clickhouse |
| `e2e_switchboard-mysql-gtid` | `TestSwitchboardMySQL` | mysql-gtid, clickhouse |
| `e2e_switchboard-mysql-pos` | `TestSwitchboardMySQL` | mysql-pos, clickhouse |
| `e2e_switchboard-mariadb` | `TestSwitchboardMySQL` | mariadb, clickhouse |
| `e2e_switchboard-mongodb` | `TestSwitchboardMongo` | mongodb, clickhouse |
| `e2e_peer-flow-postgres` | `^TestPeerFlowE2ETestSuitePG_CH$` | postgres, clickhouse |
| `e2e_peer-flow-mysql-gtid` | `^TestPeerFlowE2ETestSuiteMySQL_CH$` | mysql-gtid, clickhouse |
| `e2e_peer-flow-mysql-pos` | `^TestPeerFlowE2ETestSuiteMySQL_CH$` | mysql-pos, clickhouse |
| `e2e_peer-flow-mariadb` | `^TestPeerFlowE2ETestSuiteMySQL_CH$` | mariadb, clickhouse |
| `e2e_api-postgres` | `TestApiPg` | postgres, clickhouse |
| `e2e_api-mysql-gtid` | `TestApiMy` | mysql-gtid, postgres, clickhouse |
| `e2e_api-mysql-pos` | `TestApiMy` | mysql-pos, postgres, clickhouse |
| `e2e_api-mariadb` | `TestApiMy` | mariadb, postgres, clickhouse |
| `e2e_api-mongodb` | `TestApiMongo` | mongodb, clickhouse |

All e2e tests also depend on core PeerDB services: `flow-api`, `flow-worker`, `catalog`, and `provision-clickhouse`.

**Connector tests** (run `go test ./connectors/<connector>/...`):

| Resource | Package | Required ancillary DBs |
|----------|---------|----------------------|
| `connector_postgres` | `connectors/postgres` | postgres |
| `connector_mongo` | `connectors/mongo` | mongodb |
| `connector_mysql` | `connectors/mysql` | mysql-gtid |
| `connector_clickhouse` | `connectors/clickhouse` | clickhouse |

Connector tests only depend on `catalog` + their specific DB.

### MySQL flavor overrides

MySQL tests use env var overrides to select flavor:

| Flavor | `CI_MYSQL_PORT` | `CI_MYSQL_VERSION` |
|--------|----------------|-------------------|
| mysql-gtid | from `CI_MYSQL_GTID_PORT` in .env | from `CI_MYSQL_GTID_VERSION` |
| mysql-pos | from `CI_MYSQL_POS_PORT` in .env | from `CI_MYSQL_POS_VERSION` |
| mariadb | from `CI_MARIADB_PORT` in .env | from `CI_MARIADB_VERSION` |

### Toxiproxy and OpenSSH (test infrastructure)

Some tests require **toxiproxy** and/or **openssh** from the `Ancillary-TestInfra` label. These are manually started like ancillary DBs.

**Toxiproxy** (network chaos proxy, admin port `18474`):
- Used by SSH keepalive tests in `connectors/postgres/ssh_keepalive_test.go` and `connectors/mysql/ssh_keepalive_test.go` to simulate network failures (down, latency, reset_peer)
- Used by some e2e tests in `e2e/api_test.go` and `e2e/cancel_table_addition_test.go` for latency injection during CDC flows
- Proxy ports: `9902`, `9904`, `42001-42005` (MySQL), `49001-49003` (Postgres)

**OpenSSH** (SSH tunnel server, port `2222`, user `testuser` / password `testpass`):
- Used by SSH keepalive tests: toxiproxy sits in front of the openssh server to simulate tunnel failures
- The SSH proxy chain is: test -> toxiproxy proxy port -> openssh:2222 -> target DB

Examples of tests requiring these services:

| Test file | Needs |
|-----------|-------|
| `connectors/postgres/ssh_keepalive_test.go` | toxiproxy + openssh + postgres |
| `connectors/mysql/ssh_keepalive_test.go` | toxiproxy + openssh + mysql-gtid |
| `e2e/api_test.go` (toxiproxy tests) | toxiproxy + postgres + clickhouse |
| `e2e/cancel_table_addition_test.go` (toxiproxy tests) | toxiproxy + postgres + clickhouse |

**Note:** Tests that exercise toxiproxy or openssh can leave these services in an inconsistent state (e.g., lingering proxies, injected toxics, stuck SSH sessions) after failures or interruptions. If subsequent tests using them behave strangely (unexpected timeouts, refused connections, latency that shouldn't be there), recycle the affected resource before re-running:

```bash
tilt --port 10352 disable toxiproxy
tilt --port 10352 enable toxiproxy
tilt --port 10352 disable openssh
tilt --port 10352 enable openssh
tilt --port 10352 wait --for=condition=Ready uiresource/toxiproxy --timeout=60s
tilt --port 10352 wait --for=condition=Ready uiresource/openssh --timeout=60s
```

## Getting a status overview

```bash
tilt --port 10352 get uiresource -o json | python3 -c "
import json, sys
data = json.load(sys.stdin)
for item in data.get('items', []):
    name = item['metadata']['name']
    labels = item.get('metadata', {}).get('labels', {})
    runtime = item.get('status', {}).get('runtimeStatus', 'unknown')
    update = item.get('status', {}).get('updateStatus', 'unknown')
    enabled = not item.get('status', {}).get('disableStatus', {}).get('disabled', False)
    state = 'enabled' if enabled else 'disabled'
    print(f'{name}: runtime={runtime} update={update} ({state}) labels={list(labels.keys())}')
"
```

## Enabling resources and waiting for readiness

Ancillary databases are NOT auto-started. Enable them before running tests:
```bash
tilt --port 10352 enable postgres clickhouse
```

Wait for them and their provisioning to complete:
```bash
tilt --port 10352 wait --for=condition=Ready uiresource/postgres --timeout=120s
tilt --port 10352 wait --for=condition=Ready uiresource/provision-postgres --timeout=120s
```

For test infrastructure:
```bash
tilt --port 10352 enable toxiproxy openssh
tilt --port 10352 wait --for=condition=Ready uiresource/toxiproxy --timeout=60s
tilt --port 10352 wait --for=condition=Ready uiresource/openssh --timeout=60s
```

## Interpreting arguments

When the user provides `$ARGUMENTS`:

- If it's a **status/overview request** (e.g., `status`, `list`, `overview`): show a summary of all resources grouped by label with their health.
- If it's a **specific action** (e.g., `logs e2e_postgres`, `enable postgres`, `trigger e2e_mysql-gtid`): execute that tilt command.
- If it's a **resource name** (e.g., `e2e_postgres`, `connector_clickhouse`, `postgres`): show its status, logs, and health.
- If it's a **clean up request** (e.g., `cleanup`, `destroy`): run the cleanup commands to stop containers, teardown Tilt environment, and remove volumes.
- If empty or ambiguous: show the status overview and ask what the user needs.
- if it's a **start tilt** request** (e.g., `start`, `up`): start Tilt according to the instructions above.
- If it's a **stop tilt** request** (e.g., `stop`, `shutdown`): stop Tilt gracefully according to the instructions above.

## Cleaning up the environment

To wipe all PeerDB Docker volumes (for a fresh start), first stop containers then remove volumes:
```bash
tilt --port 10352 disable --all
docker compose -f docker-compose-dev.yml -f ancillary-docker-compose.yml down
docker volume ls | awk '{print $2}' | grep peerdb | xargs docker volume rm -f
```

The Tilt UI also has a "Wipe ancillary volumes" button in the nav bar that removes ancillary DB volumes only (without touching core PeerDB volumes).

## Important notes

- Always check that required databases are enabled and healthy before triggering tests.
- E2e tests need core PeerDB services (flow-api, flow-worker, catalog) running -- these auto-start with tilt.
- When streaming logs with `-f`, use a timeout or `--since` to avoid blocking indefinitely.
- If a test fails with "connection refused" or similar, the most likely cause is that the required database wasn't enabled/provisioned.
- Always one resource per tilt command for actions like `trigger`. For multiple, run separate commands or use a loop in bash.
- When starting resources, it is not enough with enabling them, you need to trigger them.