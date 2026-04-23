#!/usr/bin/env bash
# test-snowpipe-streaming.sh
#
# Exercises the Snowpipe Streaming CDC path end-to-end.
# Runs against the local dev Postgres (localhost:5436) and prints
# the expected Snowflake state after each stage so you can verify manually.
#
# Usage:
#   ./scripts/test-snowpipe-streaming.sh [--reset]
#
#   --reset   Drop and recreate the test schema before running.
#
# Prerequisites:
#   - peerdb-postgres container healthy (tilt up)
#   - A PeerDB mirror already created pointing at streaming_test.orders
#     (see output of --reset for the CREATE MIRROR template)

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5436}"
PG_USER="${PG_USER:-postgres}"
PG_PASSWORD="${PG_PASSWORD:-postgres}"
PG_DATABASE="${PG_DATABASE:-postgres}"

SCHEMA="streaming_test"
TABLE="orders"

# ── Helpers ───────────────────────────────────────────────────────────────────
PSQL="psql -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DATABASE --no-psqlrc -v ON_ERROR_STOP=1"

pg() { PGPASSWORD="$PG_PASSWORD" $PSQL -c "$1"; }
pg_file() { PGPASSWORD="$PG_PASSWORD" $PSQL -f "$1"; }

step() { echo; echo "────────────────────────────────────────"; echo "▶ $*"; echo "────────────────────────────────────────"; }
expect() { echo "  EXPECT in Snowflake → $*"; }

# ── Reset (optional) ──────────────────────────────────────────────────────────
if [[ "${1:-}" == "--reset" ]]; then
    step "Resetting schema $SCHEMA"
    pg "DROP SCHEMA IF EXISTS $SCHEMA CASCADE;"
    pg "CREATE SCHEMA $SCHEMA;"

    pg "
CREATE TABLE $SCHEMA.$TABLE (
    id            BIGSERIAL        PRIMARY KEY,
    order_uuid    UUID             NOT NULL DEFAULT gen_random_uuid(),
    customer_id   INT              NOT NULL,
    product_name  TEXT             NOT NULL,
    quantity      INT              NOT NULL DEFAULT 1,
    unit_price    NUMERIC(12,4)    NOT NULL,
    is_fulfilled  BOOLEAN          NOT NULL DEFAULT FALSE,
    status        VARCHAR(32)      NOT NULL DEFAULT 'pending',
    notes         TEXT,
    metadata      JSONB,
    created_at    TIMESTAMPTZ      NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ      NOT NULL DEFAULT now()
);
ALTER TABLE $SCHEMA.$TABLE REPLICA IDENTITY FULL;
"
    echo "Schema ready."
    echo
    echo "Create the PeerDB mirror now, then rerun without --reset:"
    echo
    echo "  CREATE MIRROR streaming_orders_mirror"
    echo "  FROM pg_source TO sf_streaming"
    echo "  FOR TABLES ($SCHEMA.$TABLE)"
    echo "  WITH ("
    echo "      do_initial_snapshot   = true,"
    echo "      publication_name      = 'streaming_orders_pub',"
    echo "      replication_slot_name = 'streaming_orders_slot'"
    echo "  );"
    echo
    exit 0
fi

# ── Stage 0: initial snapshot seed ────────────────────────────────────────────
step "Stage 0 — Seed rows (initial snapshot baseline)"
pg "
TRUNCATE $SCHEMA.$TABLE RESTART IDENTITY;
INSERT INTO $SCHEMA.$TABLE (customer_id, product_name, quantity, unit_price, status, notes, metadata)
VALUES
    (101, 'Laptop Pro 16\"',        1, 1899.9900, 'pending',    NULL,                  '{\"source\":\"web\",\"promo\":\"SAVE10\"}'),
    (102, 'Wireless Mouse',          2,   29.9900, 'shipped',   'Handle with care',    '{\"source\":\"mobile\",\"gift\":true}'),
    (103, 'USB-C Hub',               3,   49.9900, 'pending',    NULL,                  NULL),
    (104, 'Mechanical Keyboard',     1,  129.9900, 'pending',   'Cherry MX Blue',      '{\"source\":\"web\"}'),
    (105, 'Monitor 4K 27\"',         2,  499.9900, 'processing', NULL,                 '{\"source\":\"api\",\"batch_id\":42}');
"
expect "5 rows, ids 1-5, all statuses as seeded"

# ── Stage 1: plain inserts ─────────────────────────────────────────────────────
step "Stage 1 — INSERT new rows (CDC INSERT path)"
pg "
INSERT INTO $SCHEMA.$TABLE (customer_id, product_name, quantity, unit_price, status, notes, metadata)
VALUES
    (201, 'Thunderbolt Dock',           1, 199.9900, 'pending',    NULL,                 '{\"source\":\"web\"}'),
    (202, 'Webcam 4K',                  1,  89.9900, 'pending',    NULL,                 '{\"source\":\"mobile\"}'),
    (203, 'NVMe SSD 2TB',               1, 149.9900, 'processing', 'Expedited shipping', '{\"source\":\"api\",\"batch_id\":1}'),
    (204, 'Noise-Cancelling Headset',   1, 249.9900, 'pending',    NULL,                 NULL),
    (205, 'Portable Charger 20000mAh',  2,  39.9900, 'pending',    NULL,                 '{\"source\":\"web\",\"promo\":\"BUNDLE5\"}');
"
expect "10 rows total, ids 1-10"

# ── Stage 2: updates (TOAST carry-forward) ────────────────────────────────────
step "Stage 2 — UPDATE status only (TOAST carry-forward: notes must survive)"
pg "
UPDATE $SCHEMA.$TABLE SET status = 'shipped',   updated_at = now() WHERE id = 1;
UPDATE $SCHEMA.$TABLE SET status = 'delivered', updated_at = now() WHERE id = 2;
"
expect "id=1: status='shipped',   notes still NULL"
expect "id=2: status='delivered', notes still 'Handle with care'"

# ── Stage 3: multi-column update ──────────────────────────────────────────────
step "Stage 3 — UPDATE multiple columns (boolean + text)"
pg "
UPDATE $SCHEMA.$TABLE
SET    is_fulfilled = TRUE, notes = 'Delivered to locker', updated_at = now()
WHERE  id = 2;
"
expect "id=2: is_fulfilled=TRUE, notes='Delivered to locker'"

# ── Stage 4: hard delete ──────────────────────────────────────────────────────
step "Stage 4 — DELETE (hard-delete path in executeHardDeletes)"
pg "DELETE FROM $SCHEMA.$TABLE WHERE id = 3;"
expect "9 rows total; id=3 ABSENT"

# ── Stage 5: batch update ─────────────────────────────────────────────────────
step "Stage 5 — Batch UPDATE multiple rows"
pg "
UPDATE $SCHEMA.$TABLE
SET    quantity = quantity + 1, updated_at = now()
WHERE  customer_id IN (104, 105);
"
expect "id=4 (customer 104): quantity=2"
expect "id=5 (customer 105): quantity=3"

# ── Stage 6: special characters in PK-hashed text (Fix 7) ─────────────────────
step "Stage 6 — INSERT with pipe/quote chars in text (PK hash collision guard)"
pg "
INSERT INTO $SCHEMA.$TABLE (customer_id, product_name, quantity, unit_price, status, notes)
VALUES (301, 'Cable | Adapter \"combo\"', 1, 15.9900, 'pending', 'SKU: A|B\"C');
"
expect "id=11: product_name='Cable | Adapter \"combo\"', notes='SKU: A|B\"C' — distinct from any collision"

# ── Stage 7: upsert collision (duplicate PK update) ──────────────────────────
step "Stage 7 — UPDATE then re-UPDATE same row (idempotency)"
pg "
UPDATE $SCHEMA.$TABLE SET status = 'processing', updated_at = now() WHERE id = 6;
UPDATE $SCHEMA.$TABLE SET status = 'shipped',    updated_at = now() WHERE id = 6;
"
expect "id=6: final status='shipped' (last write wins)"

# ── Stage 8: NULL → value and value → NULL ────────────────────────────────────
step "Stage 8 — NULL <-> value transitions"
pg "
UPDATE $SCHEMA.$TABLE SET notes = 'Now has notes', updated_at = now() WHERE id = 7;  -- NULL → value
UPDATE $SCHEMA.$TABLE SET metadata = NULL,          updated_at = now() WHERE id = 8;  -- value → NULL
"
expect "id=7: notes='Now has notes' (was NULL)"
expect "id=8: metadata=NULL (was JSON object)"

# ── Stage 9: second delete ────────────────────────────────────────────────────
step "Stage 9 — DELETE another row"
pg "DELETE FROM $SCHEMA.$TABLE WHERE id = 9;"
expect "8 rows total; ids 3 and 9 ABSENT"

# ── Final source state ────────────────────────────────────────────────────────
step "Final Postgres state"
PGPASSWORD="$PG_PASSWORD" $PSQL -c "
SELECT id, customer_id, product_name, quantity, status, is_fulfilled,
       left(notes, 25)    AS notes,
       left(metadata::text, 30) AS metadata
FROM   $SCHEMA.$TABLE
ORDER BY id;
"

echo
echo "════════════════════════════════════════"
echo "All stages applied. Now verify in Snowflake:"
echo
echo "  SELECT id, customer_id, product_name, quantity, status, is_fulfilled, notes"
echo "  FROM   <DB>.STREAMING_TEST.ORDERS"
echo "  ORDER BY id;"
echo
echo "  -- Missing rows (hard-deleted): 3, 9"
echo "  SELECT id FROM <DB>.STREAMING_TEST.ORDERS WHERE id IN (3, 9);"
echo "════════════════════════════════════════"
