-- TOAST carry-forward test for PeerDB Snowpipe Streaming
--
-- Verifies that when an UPDATE doesn't touch a TOAST'd column, PeerDB preserves
-- the prior value on the destination side rather than overwriting it with NULL.
--
-- Background
-- ----------
-- PostgreSQL stores oversized variable-length values (>~2KB) out-of-line in a
-- TOAST table. With REPLICA IDENTITY FULL, an UPDATE that does not change the
-- TOAST'd column omits its value from the logical-replication stream — the
-- WAL marks it as "unchanged TOAST" instead of carrying the bytes.
--
-- A naïve CDC consumer would interpret the absence as NULL and overwrite the
-- destination column. PeerDB carries the prior value forward via the
-- _PEERDB_UNCHANGED_TOAST_COLUMNS metadata column on the raw table. The MERGE
-- step preserves the existing destination value for any column listed there.
--
-- Test stages
-- -----------
--   1. Seed alice's metadata with a JSONB blob large enough to be TOAST'd (>~6KB).
--   2. Wait for that batch to sync to Snowflake (so destination has the large value).
--   3. Trigger TOAST omission by updating only `amount` on alice's row.
--   4. Verify (in Snowflake): alice.metadata is unchanged — still the large blob.
--
-- Usage
-- -----
--   psql "postgres://postgres:postgres@localhost:9901/source" \
--        -f scripts/toast_carry_forward.sql
--
-- Or split into two runs if you want to inspect logs between stages:
--   - run lines through Stage 1 manually
--   - watch flow-worker for `[streaming] ingestion confirmed` + `merged records into`
--   - then run Stage 2 manually

-- ============================================================
-- Stage 1 — seed alice with a TOAST-eligible metadata blob
-- ============================================================

UPDATE test_streaming
SET metadata = (
  SELECT jsonb_object_agg('key_' || g, repeat('x', 200))
  FROM generate_series(1, 40) g
) || '{"tier":"platinum","marker":"toast-test-stage-1"}'::jsonb
WHERE name = 'alice';

SELECT
  name,
  pg_column_size(metadata) AS metadata_bytes,
  metadata->>'marker'      AS marker
FROM test_streaming
WHERE name = 'alice';

-- ------------------------------------------------------------
-- Wait 180s for the setup batch to sync to Snowflake
-- (PeerDB idle-flush is roughly 2-3 minutes by default)
-- ------------------------------------------------------------
SELECT pg_sleep(180);

-- ============================================================
-- Stage 2 — trigger TOAST omission (UPDATE only amount)
-- PostgreSQL marks metadata as unchanged-TOAST in the WAL;
-- PeerDB must carry the prior value forward in the destination MERGE.
-- ============================================================

UPDATE test_streaming
SET amount = 999.99
WHERE name = 'alice';

-- ============================================================
-- Stage 3 — verification (run separately in Snowflake)
-- ============================================================
-- SELECT name,
--        amount,
--        LENGTH(TO_VARCHAR(metadata)) AS metadata_chars,
--        metadata:marker::STRING      AS marker
-- FROM <SF_DB>.<SF_SCHEMA>.test_streaming
-- WHERE name = 'alice';
--
-- Expected:
--   amount         = 999.99
--   metadata_chars > 6000
--   marker         = 'toast-test-stage-1'  (NOT NULL — carry-forward worked)
