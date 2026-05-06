---
name: peerdb-snowpipe-test
description: |
  Run a quick end-to-end Postgres → Snowflake CDC smoke test for PeerDB's Snowpipe
  Streaming v2 path on the local `dev-peerdb.sh` stack. Covers preflight (Docker,
  credentials), bootstrap, source seeding, mirror creation, the standard CDC
  workload (insert / update / delete / TOAST update / ALTER TABLE ADD COLUMN),
  Snowflake-side verification, log assertions on flow-worker, and cleanup.
  Use when: user says "test peerdb snowpipe streaming", "postgres to snowflake
  quickstart", "verify snowpipe streaming locally", "smoke test snowflake
  streaming mirror", "dev-peerdb snowflake streaming", or asks to validate
  changes to `flow/connectors/snowflake/streaming_*.go` against a real Snowflake
  account.
license: MIT
metadata:
  author: Gustavo
  version: "1.1.0"
allowed-tools: Read, Grep, Glob, Bash, AskUserQuestion, TodoWrite
---

# PeerDB Snowpipe Streaming — Postgres → Snowflake Quickstart

You are a focused operator helping the user validate the PeerDB Snowpipe
Streaming v2 CDC path against a **real Snowflake account** using the local
`dev-peerdb.sh` Docker stack. Your job is to drive the test deterministically,
verify each stage, and surface failures with a known root-cause matrix.

## When to Apply

Invoke this skill when the user wants to:
- Smoke-test Snowpipe Streaming v2 end-to-end against their own Snowflake account.
- Validate a change in `flow/connectors/snowflake/streaming_*.go` without writing Go tests.
- Reproduce one of the LEARNINGS.md gotchas (TOAST carry-forward, schema evolution, idempotency on worker restart, soft-delete-as-first-event).

Do **not** invoke for unit tests (`go test ./connectors/snowflake/...`) or for
the Avro/COPY path — both have their own existing flows.

## Preflight (run all checks before any mutation)

1. **Workspace** — confirm CWD is the PeerDB repo root (file `dev-peerdb.sh` exists). If not, ask the user for the path.
2. **Docker** — `docker info > /dev/null 2>&1`. If it fails, abort with "Start Docker Desktop and re-run."
3. **jq** — `command -v jq`. If missing, ask the user to install it (`brew install jq`) before continuing — it is required for the Snowflake peer creation step.
4. **Credentials file** — check `~/sf_streaming_creds.json` exists. If missing, print the template below and ask the user to populate it before continuing. Never fabricate credentials and never commit this file.

   ```json
   {
     "account_id": "<ORG>-<ACCOUNT>",
     "username": "<SNOWFLAKE_USER>",
     "private_key": "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n",
     "database": "PEERDB_STREAMING_TEST",
     "warehouse": "<WAREHOUSE>",
     "role": "ACCOUNTADMIN",
     "query_timeout": 300,
     "enable_streaming": true
   }
   ```

   Helper to convert a PEM file to a single-line `private_key` value:
   ```bash
   awk 'NF {sub(/\r/, ""); printf "%s\\n",$0;}' ~/path/to/snowflake_rsa_key.p8
   ```

5. **Snowflake grants reminder** — show the user once (do not execute):
   ```sql
   GRANT CREATE PIPE         ON SCHEMA <DB>.<SCHEMA> TO ROLE <ROLE>;
   GRANT EVOLVE SCHEMA       ON SCHEMA <DB>.<SCHEMA> TO ROLE <ROLE>;
   GRANT USAGE               ON DATABASE <DB>          TO ROLE <ROLE>;
   ```
6. **Port collisions** — `lsof -nP -iTCP:3000 -iTCP:9900 -iTCP:9901 -iTCP:8085 -sTCP:LISTEN`. If anything is bound, ask whether to stop it before continuing.

If any preflight fails, **stop and report**; do not move to bootstrap.

## Process

### Step 1 — Bootstrap PeerDB

```bash
./dev-peerdb.sh
```

Wait for healthy. Poll, do not `sleep` blindly:
```bash
for i in {1..60}; do
  curl -fsS http://localhost:3000 >/dev/null 2>&1 && \
  nc -z localhost 9900 && \
  nc -z localhost 9901 && \
  nc -z localhost 8085 && echo "ALL_UP" && break
  echo "Attempt $i/60 — waiting 5s..."
  sleep 5
done
```

If after 5 minutes a port is still down, run:
```bash
docker compose -f docker-compose-dev.yml ps
docker compose -f docker-compose-dev.yml logs --tail 100 flow-api flow-worker temporal catalog
```
and surface the first error to the user.

### Step 2 — Seed the source Postgres

The catalog Postgres on `localhost:9901` doubles as the local source DB for testing.
Create the `source` database, the test table, configure it for logical replication,
create the publication, and insert initial rows — **all before the mirror is created**
so the initial snapshot captures them.

```bash
psql "postgres://postgres:postgres@localhost:9901/postgres" \
  -c "CREATE DATABASE source;" 2>/dev/null || true

psql "postgres://postgres:postgres@localhost:9901/source" <<'SQL'
CREATE TABLE IF NOT EXISTS test_streaming (
  id          SERIAL PRIMARY KEY,
  name        TEXT NOT NULL,
  amount      NUMERIC(10,2),
  is_active   BOOLEAN DEFAULT true,
  metadata    JSONB,
  created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Required for CDC: emit full old row in WAL on UPDATE/DELETE
ALTER TABLE test_streaming REPLICA IDENTITY FULL;

-- Required for logical replication: must exist before mirror is created
CREATE PUBLICATION streaming_smoke_pub FOR TABLE test_streaming;

-- Seed rows for the initial snapshot
INSERT INTO test_streaming (name, amount, is_active, metadata) VALUES
  ('alice',   100.50, true,  '{"tier":"gold"}'),
  ('bob',     200.75, true,  '{"tier":"silver"}'),
  ('charlie', 300.00, false, '{"tier":"bronze"}'),
  ('diana',   450.00, true,  '{"tier":"platinum"}'),
  ('eve',     75.25,  true,  NULL);
SQL
```

After this step, Snowflake should eventually receive all 5 rows via the initial
snapshot once the mirror is created.

For richer test data, the repo provides `scripts/test-snowpipe-streaming.sh --reset`
which seeds a `streaming_test.orders` schema with TOAST-eligible columns. Use that
script when the user wants the full multi-stage workload.

### Step 3 — Create the Postgres peer

```bash
psql "postgres://peerdb:peerdb@localhost:9900/peerdb" <<'SQL'
CREATE PEER pg_source FROM POSTGRES WITH (
  host     = 'catalog',
  port     = 5432,
  user     = 'postgres',
  password = 'postgres',
  database = 'source'
);
SQL
```

> **Note:** `host = 'catalog'` is the Docker-internal hostname for the catalog
> Postgres container. PeerDB's flow-worker runs inside the same Docker network,
> so it resolves `catalog` correctly. Do not use `localhost` here — that would
> resolve to the flow-worker container itself.

### Step 4 — Create the Snowflake peer

Ask the user to create the Snowflake peer. Two options:

**Option A — UI (recommended, avoids shell escaping issues):**
Direct the user to <http://localhost:3000> → **Peers → Create → Snowflake**.
Toggle **Enable Streaming = ON** before saving.

**Option B — SQL proxy (reproducible, requires jq):**
```bash
# jq expands BEFORE psql sees the heredoc (note: <<SQL not <<'SQL')
psql "postgres://peerdb:peerdb@localhost:9900/peerdb" <<SQL
CREATE PEER sf_streaming FROM SNOWFLAKE WITH (
  $(jq -r 'to_entries | map("\(.key) = \(.value | @json)") | join(",\n  ")' ~/sf_streaming_creds.json)
);
SQL
```

Ask the user what they named the peer — you need it for the mirror creation command.

### Step 5 — Create the mirror and wait for snapshot

Replace `<SF_PEER_NAME>` with the name the user chose in Step 4:

```bash
psql "postgres://peerdb:peerdb@localhost:9900/peerdb" <<SQL
CREATE MIRROR streaming_smoke
FROM pg_source TO <SF_PEER_NAME>
FOR TABLES (test_streaming)
WITH (
  do_initial_snapshot    = true,
  publication_name       = 'streaming_smoke_pub',
  replication_slot_name  = 'streaming_smoke_slot'
);
SQL
```

**Verify the mirror started:** open <http://localhost:3000> → **Mirrors** and confirm
`streaming_smoke` is listed as **Running** (not Failed or Paused).

**Watch the snapshot in Temporal:** open
<http://localhost:8085/namespaces/default/workflows> and find the workflow named
after your mirror. A successful snapshot shows the snapshot child workflow completing
before the CDC workflow takes over. If it stays stuck or fails, check:

```bash
docker compose -f docker-compose-dev.yml logs --tail 100 flow-worker
```

**Do not proceed to Step 6 until the initial snapshot is complete.** The Temporal
workflow will transition from snapshot to CDC mode when done. You can also confirm
by querying Snowflake — the 5 seeded rows should be present:

```sql
-- Snowflake
SELECT id, name, amount, is_active FROM <SF_DB>.<SF_SCHEMA>.test_streaming ORDER BY id;
```

Expected:

| id | name    | amount | is_active |
|----|---------|--------|-----------|
| 1  | alice   | 100.50 | true      |
| 2  | bob     | 200.75 | true      |
| 3  | charlie | 300.00 | false     |
| 4  | diana   | 450.00 | true      |
| 5  | eve     | 75.25  | true      |

### Step 6 — Drive the CDC workload (one stage at a time, verify between each)

After each stage, **wait 10–15 s** then run the verification SQL on Snowflake
(have the user run it; check `command -v snowsql` if they want CLI access).

#### 6a. Update / Delete (happy path CDC)

```sql
-- Postgres (run via psql)
UPDATE test_streaming SET amount = 150.00, metadata = '{"tier":"platinum"}' WHERE name = 'alice';
DELETE FROM test_streaming WHERE name = 'charlie';
```

Expected Snowflake state:

| id | name  | amount | metadata             |
|----|-------|--------|----------------------|
| 1  | alice | 150.00 | {"tier":"platinum"}  |
| 2  | bob   | 200.75 | {"tier":"silver"}    |
| 4  | diana | 450.00 | {"tier":"platinum"}  |
| 5  | eve   | 75.25  | null                 |

(Row 3 / charlie deleted. If `_PEERDB_IS_DELETED` is configured on the mirror, it will be `true` for that row instead of disappearing.)

#### 6b. TOAST carry-forward

The TOAST scenario only kicks in when the omitted column actually lives in
TOAST storage (>~2KB after compression). The default `metadata = '{"tier":"gold"}'`
seed values are too small. Use the bundled script — it inflates `alice.metadata`
to a TOAST-eligible size, waits for that to sync, then triggers the omission update:

```bash
psql "postgres://postgres:postgres@localhost:9901/source" \
     -f ~/.claude/skills/peerdb-snowpipe-test/scripts/toast_carry_forward.sql
```

Stages (the script runs all three; you can also split them manually):
1. Inflate `alice.metadata` with a ~6KB JSONB containing `marker = 'toast-test-stage-1'`.
2. Wait 180s for the setup batch to MERGE into Snowflake.
3. `UPDATE test_streaming SET amount = 999.99 WHERE name = 'alice'` — Postgres
   marks `metadata` as unchanged-TOAST in the WAL.

Verify in Snowflake:

```sql
SELECT name,
       amount,
       LENGTH(TO_VARCHAR(metadata)) AS metadata_chars,
       metadata:marker::STRING      AS marker
FROM <SF_DB>.<SF_SCHEMA>.test_streaming
WHERE name = 'alice';
-- expected: amount=999.99, metadata_chars > 6000, marker='toast-test-stage-1'
```

If `marker` is NULL or `metadata_chars` is small, see the troubleshooting matrix
row for `metadata column NULL after TOAST update` (LEARNINGS §2.1).

#### 6c. Schema evolution

```sql
-- Postgres
ALTER TABLE test_streaming ADD COLUMN score INT DEFAULT 0;
INSERT INTO test_streaming (name, amount, score) VALUES ('dan', 50.00, 99);
```

The streaming pipe is **not** recreated for source-schema changes. New columns are
absorbed by the JSON `_PEERDB_DATA` blob on the raw table, then projected into the
destination table by MERGE after `ALTER TABLE … ADD COLUMN IF NOT EXISTS` runs on
the destination side.

Expected log evidence (no `CREATE … PIPE` line should appear; instead, look for
the destination ALTER and a successful MERGE):

```bash
docker compose -f docker-compose-dev.yml logs flow-worker 2>&1 \
  | grep -E "ADD COLUMN|merged records into|schema delta" | tail -20
```

Verify in Snowflake that the new column exists and was populated:

```sql
SELECT id, name, amount, score FROM <SF_DB>.<SF_SCHEMA>.test_streaming WHERE name = 'dan';
-- expected: score = 99
```

Also confirm in Temporal (<http://localhost:8085/namespaces/default/workflows>)
that the mirror workflow did not error out — schema evolution should be handled
transparently.

#### 6d. Idempotency on worker restart (LEARNINGS §Temporal heartbeat timeouts)

```bash
# Insert a large batch in Postgres, then immediately restart flow-worker
docker compose -f docker-compose-dev.yml restart flow-worker
```

After recovery, query Snowflake row count: it must equal the Postgres row count.
Duplicates indicate the `GetLastSyncBatchID` guard regressed.

### Step 7 — Verify the streaming path was used

```bash
docker compose -f docker-compose-dev.yml logs flow-worker 2>&1 \
  | grep "\[streaming\]" | tail -40
```

If nothing matches, first check the raw tail to confirm the log prefix format:
```bash
docker compose -f docker-compose-dev.yml logs flow-worker 2>&1 | tail -50
```

The expected sequence per batch:
```
[streaming] flushing buffer       — table=…, pipe=…, channel=…, rows=N
[streaming] executing CREATE PIPE — only on first flush or after schema change
[streaming] pipe created
[streaming] channel ready
[streaming] rows sent to Snowpipe Streaming API
[streaming] channel status        — rowsInserted:0, rowsErrors:0
[streaming] channel status        — rowsInserted:N, rowsErrors:0, latencyMs:NNN
[streaming] ingestion confirmed   — rowsInserted:N, baseline:M
```

Confirm the raw table name is `_PEERDB_INTERNAL._PEERDB_STREAMING_<JOB>` (not
`_PEERDB_RAW_<JOB>`) — the job name is uppercased from the mirror name:
```sql
-- Snowflake
SELECT COUNT(*) FROM _PEERDB_INTERNAL._PEERDB_STREAMING_STREAMING_SMOKE;
```

### Step 8 — Cleanup

Always confirm with the user before running destructive cleanup. Default to
**preserve-by-default** so the user can re-inspect failures.

```bash
# Stop PeerDB (preserves volumes)
docker compose -f docker-compose-dev.yml down

# Hard reset (drops volumes — ASK BEFORE RUNNING)
docker compose -f docker-compose-dev.yml down -v
```

Snowflake side (run via the user's `snowsql` or UI; do not assume credentials):
```sql
DROP DATABASE IF EXISTS PEERDB_STREAMING_TEST;
```

## Troubleshooting matrix

Map symptoms to the LEARNINGS.md root cause **before** the user starts guessing.
Also check the Temporal workflow at <http://localhost:8085/namespaces/default/workflows>
for structured error messages — it often shows the failure reason more clearly than raw Docker logs.

| Symptom in `flow-worker` logs / Snowflake | Likely root cause | Fix / next step |
|------------------------------------------|-------------------|-----------------|
| `401 Unauthorized` from `/oauth/token` | Clock skew or stale JWT | Check NTP (`chronyc tracking`); restart flow-worker to force token refresh |
| `STALE_CONTINUATION_TOKEN_SEQUENCER` | Channel reopen race | Expected on first retry — should self-heal in `sendChunkWithRetry` |
| `pendingFileCount` stuck at 1, no `[streaming] ingestion confirmed` | Channel state lost mid-batch | Inspect Temporal workflow for retries; check `sendChunkWithRetry` self-heal logic and `STALE_CONTINUATION_TOKEN_SEQUENCER` handling |
| Rows missing in Snowflake but no error | Positional pipe DDL fallback (LEARNINGS §1.2) | Confirm `CREATE PIPE IF NOT EXISTS` SQL in `createStreamingPipe` contains explicit column list `("COL1","COL2",…)` |
| `CREATE OR REPLACE PIPE` line in flow-worker logs | Regression — streaming path should use `CREATE PIPE IF NOT EXISTS` | Inspect `createStreamingPipe` in `streaming_sync.go`; OR-REPLACE invalidates channels and breaks offsetToken idempotency |
| `metadata` column NULL after TOAST update | `processStreamingDelete` did not populate `columnNames` (LEARNINGS §2.1) | Confirm `buildColumnNames` is called in both `processStreamingRecord` and `processStreamingDelete` |
| Duplicate rows after `restart flow-worker` | `GetLastSyncBatchID` idempotency guard regressed (LEARNINGS §2.3) | Check `syncRecordsViaStreaming` early-exit when `syncBatchID <= lastBatchID` |
| `NumRecordsSynced` reports 3 when batch had 1000 rows across 3 tables | Counting tables, not rows (LEARNINGS §2.2) | Sum `Insert+Update+Delete` across `tableNameRowsMapping` |
| `RelationRecord` log spam in record loop | Reading deltas from records channel instead of `req.Records.SchemaDeltas` (LEARNINGS §2.5) | Use `req.Records.SchemaDeltas` like every other connector |
| `EVOLVE SCHEMA` permission error | Missing grant | Run grants from preflight step 5 |
| Hostname `myaccount_us_east_1...` in error messages | Underscore→hyphen conversion missed (LEARNINGS §1.4) | Confirm `streaming_auth.go` strips quotes and replaces `_` with `-` in `GetIngestHost` |
| Mirror shows Failed in UI immediately after creation | Publication missing or replication slot conflict | Confirm `streaming_smoke_pub` publication exists; check for duplicate slot names |
| Snapshot workflow stuck in Temporal | flow-worker OOM or Temporal heartbeat timeout | Check `docker stats`; reduce snapshot parallelism via PeerDB config |

## Operating constraints

- **Never** modify `~/sf_streaming_creds.json`; only read it.
- **Never** run `docker compose down -v` without explicit user confirmation — it drops the catalog Postgres volume and loses replication slot state.
- **Never** run destructive Snowflake DDL (`DROP DATABASE`, `DROP PIPE`) yourself; print the SQL for the user to execute.
- Always poll service readiness; do **not** sleep blindly.
- If preflight fails, stop. Do not attempt to recover by guessing.
- Reference assessment files at `~/Desktop/new_feature_peerdb/{TESTING.md,LEARNINGS.md}` if the user asks for deeper context.
