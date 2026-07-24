---
name: local-debug
description: Debug failing PeerDB e2e and connector tests locally. Use when the user wants to investigate test failures, run specific tests, isolate flaky tests, or troubleshoot test issues. Uses the /tilt skill for infrastructure management.
argument-hint: "[test-name-or-resource]"
allowed-tools: Skill(tilt) Bash(awk *) Bash(tilt *) Bash(./tilt.sh) Bash(docker ps *) Bash(go test *) Bash(go vet *) Bash(go build *) Bash(go clean *) Bash(dlv *) Bash(git log *) Bash(git diff *) Bash(git show *) Bash(git status *) Bash(git rev-parse *) Bash(git checkout *) Bash(git stash *) Bash(cd ./*) Bash(tail *) Bash(python3 *) Read Grep Glob Monitor ScheduleWakeup
---

You are debugging PeerDB e2e or connector test failures locally. For this you'll need Tilt. Please, use `/tilt` skill (see `.claude/skills/tilt/SKILL.md`) to interact with Tilt, including managing its life cycle.

## Tilt lifecycle: start it if it isn't running, record prior state

**Debugging requires Tilt. If it isn't running, start it — don't ask first.**. Pausing to confirm wastes a turn.

## Step 0: Ensure `.env` exists

Tests auto-load environment variables from `.env` at the project root. If it's missing, copy from the template:

```bash
cp .env.example .env
```

## Step 1: Ensure infrastructure is ready

Before debugging any test, verify its dependencies are healthy. Use the `/tilt` skill reference to:
1. Check resource status.
2. Find which data stores and services the test requires (e.g., `e2e_postgres` needs `postgres` and `clickhouse`), you can use './Tiltfile' as a reference for resource dependencies. If not enough, you might inspect the test Go code.
3. Using the list of required services from (2), enable **and trigger** required databases. Make sure all them are triggered and their provision steps completed before proceeding. Be active, don't wait for the user to do or confirm this step.
4. Wait for provisioning to complete. Checks for this each 30 seconds, if provisioning is waitinng on the service to come up, check the service tilt resource status to make sure it was triggered.

NOTE: When starting dependencies, trigger them all without waiting for the confirmation of each one to be ready so they start in parallel. Then wait for all of them to be ready before moving to the next step.

### Core PeerDB services can fail to initialize — re-trigger them

Services under the `PeerDB` label (notably `flow-api`, `flow-worker`, `flow-snapshot-worker`) sometimes fail their initial build/start and land in `update=error` or `runtime=error`. This is a known flakiness of the local stack; the fix is to re-trigger the failed resource so Tilt rebuilds/restarts it, not to tear the whole environment down.

If the readiness poll above exits with `ERROR … :update-error` for one of these services, or you see a core service stuck in an error state, re-trigger it and resume waiting:

If a service fails to come up after **two** re-triggers, stop retrying and inspect its logs. More failures usually indicates a real problem (bad build, port conflict, misconfig) rather than init flakiness, and blindly re-triggering will just loop.

## Step 2: Reproduce the failure

Direct `go test`:

```bash
cd flow && go test -count=1 -v -run '<TestPattern>/<SubTest>' ./e2e/
```

or

```bash
cd flow && go test -count=1 -v -run '<TestPattern>/<SubTest>' ./connectors/
```

Note: For MySQL tests, set the flavor overrides:

```bash
CI_MYSQL_PORT=3306 CI_MYSQL_VERSION=mysql-gtid go test -count=1 -v -run TestGenericCH_MySQL ./e2e/
```

## Step 3: Diagnose the failure

### Common failure patterns and what to check

| Symptom | Likely cause | Action |
|---------|-------------|--------|
| "connection refused" / "dial tcp" | Required database not running | Trigger its resource |
| "context deadline exceeded" | Service slow to start or test timeout too short | Check DB logs: `tilt --port 10352 logs --since 5m <db>` |
| "catalog" errors | Catalog postgres not ready | Check: `tilt --port 10352 logs --since 2m catalog` |
| "temporal" errors | Temporal not ready | Check: `tilt --port 10352 logs --since 2m temporal` |
| Flaky pass/fail | Stale test cache or race condition | Clear cache: `cd flow && go clean -cache` |
| "table already exists" | Leftover state from prior run | Check DB for stale `e2e_test_*` databases/tables |
| "unexpected mysql version" / `CI_MYSQL_VERSION=None` / other "var is unset" | Local `.env` drift from `.env.example` | Diff: `diff <(sort .env.example) <(sort .env)`. Add missing keys to `.env` (don't blindly overwrite — `.env` is user-local). If you change `.env`, restart the test environment. |

### Check dependency health
```bash
# Core services
tilt --port 10352 logs --since 2m flow-api flow-worker catalog

# Database being tested
tilt --port 10352 logs --since 5m <database-resource>
```

### Read the test code

Test code lives in:
- `flow/e2e/` -- e2e test files (`*_test.go`) and helpers (`clickhouse.go`, `pg.go`, `mysql.go`, `mongo.go`, `test_utils.go`)
- `flow/connectors/*/` -- connector-specific tests
- `flow/e2eshared/` -- shared test utilities (`RunSuite`, `CheckQRecordEquality`)

Use `Grep` and `Read` to find the failing test function, understand what it does, and trace the failure.

## Step 4: Isolate and fix

1. **Narrow the test pattern** to run only the failing subtest:
   ```bash
   cd flow && go test -count=1 -v -run 'TestGenericCH_PG/TestSpecificSubtest' ./e2e/
   ```

2. **Clear test cache** if you suspect stale results:
   ```bash
   cd flow && go clean -cache
   ```

3. **Check compilation** after code changes:
   ```bash
   cd flow && go vet ./e2e/ ./connectors/...
   ```

4. **Re-run the test** to verify the fix. Use `-count=1` to bypass caching.

5. Use `dlv` to step through the test code if the failure is hard to diagnose from logs and code reading alone.

## Step 5: Cleanup (if you started Tilt)

The user might have indicated to stop the Tilt environment after debugging. If they haven't, ask if they want to stop it now that debugging is done. If yes, stop Tilt gracefully following the `/tilt` skill instructions for **stop tilt** request in that skill.

## Interpreting arguments

When the user provides `$ARGUMENTS`:

- If it matches a **tilt test resource** (e.g., `e2e_postgres`, `connector_clickhouse`): check its dependencies, trigger it, and analyze the output for failures.
- If it matches a **Go test name** (e.g., `TestGenericCH_PG`, `TestPeerFlowE2ETestSuitePG_CH`): find the test code, ensure infrastructure is ready, run it, and analyze failures.
- If it's a **connector name** (e.g., `postgres`, `clickhouse`, `mysql`, `mongo`): debug the corresponding `connector_<name>` test resource.
- If it describes a **symptom** (e.g., "connection refused", "timeout", "flaky"): follow the diagnostic patterns above.
- If empty or ambiguous: check overall infrastructure health and ask which test to debug.