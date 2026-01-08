# PeerDB Architecture

This document provides a deep dive into PeerDB's internal architecture, focusing on the CDC (Change Data Capture) workflow.

## System Overview

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Web UI    │     │  SQL (psql) │     │  gRPC API   │
│  (Next.js)  │     │  port 9900  │     │             │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       │           ┌───────▼───────┐           │
       │           │    Nexus      │           │
       │           │(Rust pgwire)  │           │
       │           └───────┬───────┘           │
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
                    ┌──────▼──────┐
                    │  Flow API   │
                    │  (Go gRPC)  │
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
       ┌──────▼──────┐     │     ┌──────▼──────┐
       │   Temporal  │◄────┘     │  Catalog DB │
       │   Server    │           │ (PostgreSQL)│
       └──────┬──────┘           └─────────────┘
              │
    ┌─────────┼─────────┐
    │         │         │
┌───▼───┐ ┌───▼───┐ ┌───▼───┐
│Worker │ │Worker │ │Snapshot│
│       │ │       │ │Worker  │
└───────┘ └───────┘ └────────┘
```

## CDC Workflow Stages

The CDC workflow has three main phases:

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         CDCFlowWorkflow                                   │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────┐      ┌────────────┐      ┌─────────────────────────────┐   │
│  │  SETUP  │ ───▶ │  SNAPSHOT  │ ───▶ │      RUNNING (sync loop)    │   │
│  │ (once)  │      │   (once)   │      │        (forever)            │   │
│  └─────────┘      └────────────┘      └─────────────────────────────┘   │
│       │                 │                           │                    │
│       ▼                 ▼                           ▼                    │
│  - Create slot    - Copy existing         - Pull CDC records            │
│  - Create pub       data to dest          - Push to destination         │
│  - Create tables  - Parallel for          - Normalize raw → final       │
│  - Init metadata    large tables          - Update checkpoints          │
│                                           - ContinueAsNew every N iters │
└──────────────────────────────────────────────────────────────────────────┘
```

## SyncFlow Activity Architecture

The `SyncFlow` activity is the heart of CDC, running three concurrent goroutines:

```
┌─────────────────────────────────────────────────────────────────┐
│                        SyncFlow Activity                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────────┐ │
│  │ normalizeLoop│   │maintainRepl  │   │    sync loop         │ │
│  │  goroutine   │   │   Conn       │   │    (main)            │ │
│  │              │   │  goroutine   │   │                      │ │
│  │ Waits for    │   │              │   │ Pulls records from   │ │
│  │ normalization│   │ Sends        │   │ source, syncs to     │ │
│  │ requests and │   │ keepalives   │   │ destination          │ │
│  │ processes    │   │ to source    │   │                      │ │
│  │ them         │   │ (Postgres)   │   │                      │ │
│  └──────┬───────┘   └──────────────┘   └──────────┬───────────┘ │
│         │                                         │              │
│         └─────────── errgroup.Wait() ─────────────┘              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## syncCore: Pull and Push in Parallel

```
┌─────────────────────────────────────────────────────────────────┐
│                          syncCore                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Setup                                                        │
│     - Get source connector (Postgres/MySQL/MongoDB)              │
│     - Get destination connector (ClickHouse/BigQuery/etc)        │
│     - Initialize batch state                                     │
│                                                                  │
│  2. Pull Records (goroutine 1)                                   │
│     ┌────────────────────────────────────────┐                   │
│     │ srcConn.PullRecords(recordBatch)       │                   │
│     │   - Reads from WAL/binlog/oplog        │                   │
│     │   - Writes to recordBatch channel      │                   │
│     │   - Stops when batch full or timeout   │                   │
│     └────────────────────────────────────────┘                   │
│                           │                                      │
│                           ▼ (records channel)                    │
│  3. Sync Records (goroutine 2)                                   │
│     ┌────────────────────────────────────────┐                   │
│     │ dstConn.SyncRecords(recordBatch)       │                   │
│     │   - Reads from recordBatch channel     │                   │
│     │   - Transforms and writes to dest      │                   │
│     │   - Returns count of synced records    │                   │
│     └────────────────────────────────────────┘                   │
│                                                                  │
│  4. Update Checkpoints                                           │
│     - Save last LSN/GTID to catalog                              │
│     - Record batch statistics                                    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Data Flow Through Connectors

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  PostgreSQL │     │   Channel   │     │ ClickHouse  │
│   (source)  │────▶│  (records)  │────▶│   (dest)    │
└─────────────┘     └─────────────┘     └─────────────┘
      │                                        │
      │ PullRecords()                          │ SyncRecords()
      │                                        │
      ▼                                        ▼
┌─────────────┐                         ┌─────────────┐
│ model.Record│                         │ Raw staging │
│ - Operation │                         │   table     │
│ - Data      │                         │ (avro/json) │
│ - LSN       │                         └─────────────┘
└─────────────┘                                │
                                               │ NormalizeRecords()
                                               ▼
                                        ┌─────────────┐
                                        │ Final table │
                                        │ (typed cols)│
                                        └─────────────┘
```

## syncBatchID: Coordination Between Sync and Normalize

The `syncBatchID` is a monotonically increasing integer that coordinates sync and normalization:

```
Time ─────────────────────────────────────────────────────────▶

Sync:    [Batch 1] [Batch 2] [Batch 3] [Batch 4] [Batch 5]
              │         │         │         │         │
              ▼         ▼         ▼         ▼         ▼
Staging:  ████████████████████████████████████████████████
          syncBatchID: 1→2→3→4→5

Normalize:    [Batch 1] [Batch 2] [Batch 3]  ← catching up
                  │         │         │
                  ▼         ▼         ▼
Final:    ████████████████████████████
          normBatchID: 3

              ↑
              └── Gap is OK! Normalization will catch up.
                  Worker tracks both IDs to manage backpressure.
```

**Why syncBatchID exists:**

1. **Ordering and Progress Tracking**: Uniquely identifies each batch for crash recovery
2. **Decoupling Sync from Normalization**: Sync (fast) can run ahead of normalization (slower)
3. **Crash Recovery**: On restart, resume from last completed batch
4. **Backpressure**: Prevents sync from getting too far ahead of normalization

## Temporal Workflow Concepts Used

### ContinueAsNew
Workflows restart themselves periodically to prevent unbounded history growth:
```go
if iterations >= limit {
    return workflow.NewContinueAsNewError(ctx, CDCFlowWorkflow, cfg, state)
}
```

### Child Workflows
Setup and Snapshot run as child workflows with their own history:
```go
setupFuture := workflow.ExecuteChildWorkflow(setupCtx, SetupFlowWorkflow, cfg, ...)
```

### Signals
External events (pause, resume, terminate) are handled via signals:
```go
workflow.GetSignalChannel(ctx, "PauseSignal").Receive(ctx, &signal)
```

### Selectors
Multiplex between different signal channels:
```go
selector := workflow.NewSelector(ctx)
selector.AddReceive(pauseChan, handlePause)
selector.AddReceive(terminateChan, handleTerminate)
selector.Select(ctx)
```

### Activities
Long-running operations with automatic retry:
```go
workflow.ExecuteActivity(ctx, a.SyncFlow, config, options)
```

### Search Attributes
Queryable workflow metadata:
```go
workflow.UpsertTypedSearchAttributes(ctx,
    shared.MirrorNameSearchAttribute.ValueSet(cfg.Name))
```

## CDC Workflow Pseudocode

```python
# ============================================================
# CDC FLOW - Complete Lifecycle
# ============================================================

def CDCFlowWorkflow(config, state):
    """
    Main CDC orchestrator. Runs as Temporal workflow.
    Uses ContinueAsNew to run indefinitely without memory buildup.
    """

    # --------------------------------------------------------
    # PHASE 1: SETUP (runs once, first time only)
    # --------------------------------------------------------
    if not state.setup_complete:
        SetupFlowWorkflow(config)  # child workflow
        state.setup_complete = True

    # --------------------------------------------------------
    # PHASE 2: SNAPSHOT (runs once, if tables have data)
    # --------------------------------------------------------
    if not state.snapshot_complete:
        SnapshotFlowWorkflow(config)  # child workflow
        state.snapshot_complete = True

    # --------------------------------------------------------
    # PHASE 3: MAIN SYNC LOOP (runs forever)
    # --------------------------------------------------------
    iterations = 0
    while True:
        # Check for signals (pause, resume, terminate)
        signal = check_signals()
        if signal == PAUSE:
            wait_for_resume_signal()
        if signal == TERMINATE:
            return DropFlowWorkflow(config)

        # Execute one sync cycle
        SyncFlow(config, state)  # activity

        iterations += 1

        # Prevent workflow history from growing too large
        if iterations >= 10 or should_continue_as_new():
            return ContinueAsNew(CDCFlowWorkflow, config, state)


# ============================================================
# SETUP WORKFLOW
# ============================================================

def SetupFlowWorkflow(config):
    """
    One-time initialization. Creates everything needed for CDC.
    """

    # 1. Validate source and destination connections
    source = connect(config.source)
    destination = connect(config.destination)

    # 2. Create replication slot on source (Postgres)
    source.create_replication_slot(
        name=f"{config.flow_name}_slot",
        plugin="pgoutput"
    )

    # 3. Create publication for tables we want to replicate
    source.execute(f"""
        CREATE PUBLICATION {config.flow_name}_pub
        FOR TABLE {config.tables}
    """)

    # 4. Get source table schemas
    schemas = source.get_table_schemas(config.tables)

    # 5. Create destination tables
    for table, schema in schemas:
        # Raw/staging table (holds CDC records before normalization)
        destination.create_table(
            name=f"_peerdb_raw_{table}",
            columns=[
                "_peerdb_uid",
                "_peerdb_timestamp",
                "_peerdb_data",
                "_peerdb_record_type",
                "_peerdb_batch_id"
            ]
        )

        # Final destination table (matches source schema)
        destination.create_table(
            name=table,
            columns=schema.columns + [
                "_peerdb_synced_at",
                "_peerdb_is_deleted"
            ]
        )

    # 6. Initialize metadata tracking
    catalog.insert("metadata_last_sync_state", {
        "job_name": config.flow_name,
        "last_offset": 0,
        "sync_batch_id": 0,
        "normalize_batch_id": 0
    })


# ============================================================
# SNAPSHOT WORKFLOW
# ============================================================

def SnapshotFlowWorkflow(config):
    """
    Initial data load. Copies existing rows before CDC starts.
    Runs on snapshot-worker (separate task queue).
    """

    source = connect(config.source)
    destination = connect(config.destination)

    for table in config.tables:
        row_count = source.query(f"SELECT count(*) FROM {table}")

        if row_count > 1_000_000:
            # Large table: parallel snapshot with partitions
            partitions = calculate_partitions(table, num_workers=4)
            parallel_for partition in partitions:
                snapshot_partition(source, destination, table, partition)
        else:
            # Small table: single query
            snapshot_table(source, destination, table)


def snapshot_table(source, destination, table):
    """Copy all rows from source to destination."""
    cursor = source.query(f"SELECT * FROM {table}")

    batch = []
    for row in cursor:
        batch.append(row)
        if len(batch) >= 10_000:
            destination.bulk_insert(table, batch)
            batch = []

    if batch:
        destination.bulk_insert(table, batch)


# ============================================================
# SYNC FLOW (Activity) - The Heart of CDC
# ============================================================

def SyncFlow(config, state):
    """
    Single sync iteration. Pulls changes and writes to destination.
    """

    source = connect(config.source)
    destination = connect(config.destination)

    # Three concurrent goroutines
    parallel:
        maintain_replication_connection(source)
        normalize_loop(destination, state)
        sync_loop(source, destination, state)


def sync_loop(source, destination, state):
    """Pull from source, push to destination."""
    while not should_stop():
        sync_core(source, destination, state)
        heartbeat(state)


def sync_core(source, destination, state):
    """Core sync logic. One batch of records."""

    # 1. Get next batch ID
    sync_batch_id = destination.get_last_sync_batch_id() + 1

    # 2. Create record channel (for streaming)
    records_channel = Channel()

    # 3. Pull and push in parallel (streaming)
    parallel:
        pull_records(source, records_channel, state)
        push_records(destination, records_channel, sync_batch_id)

    # 4. Update checkpoint
    catalog.update("metadata_last_sync_state", {
        "job_name": config.flow_name,
        "last_offset": state.last_lsn,
        "sync_batch_id": sync_batch_id
    })

    # 5. Queue normalization (async)
    normalize_queue.put(sync_batch_id)

    # 6. Backpressure: wait if normalization too far behind
    while normalize_batch_id < sync_batch_id - MAX_BUFFER:
        sleep(100ms)


# ============================================================
# PULL - Read changes from source
# ============================================================

def pull_records(source, records_channel, state):
    """Read CDC records from source WAL/binlog/oplog."""

    replication_stream = source.start_replication(
        slot=f"{config.flow_name}_slot",
        start_lsn=state.last_lsn,
        publication=f"{config.flow_name}_pub"
    )

    batch_size = 0
    start_time = now()

    for message in replication_stream:
        if message.type == "relation":
            state.relation_mapping[message.relation_id] = message.schema
            continue

        if message.type == "insert":
            record = Record(INSERT, message.table, message.new_row, message.lsn)
        elif message.type == "update":
            record = Record(UPDATE, message.table, message.new_row, message.lsn)
        elif message.type == "delete":
            record = Record(DELETE, message.table, message.old_row, message.lsn)

        records_channel.send(record)
        batch_size += 1
        state.last_lsn = message.lsn

        if batch_size >= MAX_BATCH_SIZE or now() - start_time >= BATCH_TIMEOUT:
            break

    records_channel.close()


# ============================================================
# PUSH - Write changes to destination
# ============================================================

def push_records(destination, records_channel, sync_batch_id):
    """Write to destination staging table."""

    if destination.type == CLICKHOUSE:
        writer = AvroWriter(destination.s3_bucket)
    elif destination.type == BIGQUERY:
        writer = AvroWriter(destination.gcs_bucket)
    else:
        writer = DirectWriter(destination)

    for record in records_channel:
        transformed = transform_record(record, sync_batch_id)
        writer.write(transformed)

    writer.flush()

    if writer.type == AVRO:
        catalog.insert("ch_s3_stage", {
            "flow_job_name": config.flow_name,
            "sync_batch_id": sync_batch_id,
            "avro_file": writer.file_path
        })


# ============================================================
# NORMALIZE - Transform staging to final tables
# ============================================================

def normalize_loop(destination, state):
    """Process normalization requests from queue."""
    while not should_stop():
        batch_id = normalize_queue.get()
        normalize_batch(destination, batch_id)
        state.normalize_batch_id = batch_id


def normalize_batch(destination, sync_batch_id):
    """Transform raw CDC records into final table format."""

    last_norm_batch = destination.get_last_normalize_batch_id()

    for batch_id in range(last_norm_batch + 1, sync_batch_id + 1):
        if destination.type == CLICKHOUSE:
            # Load Avro from S3, merge into final table
            avro_file = get_avro_stage(batch_id)
            destination.execute(f"""
                INSERT INTO {table}
                SELECT _peerdb_data.*, ...
                FROM s3('{avro_file}', 'Avro')
            """)
        elif destination.type == POSTGRES:
            # MERGE from raw table to final table
            destination.execute(f"""
                INSERT INTO {table} ...
                FROM _peerdb_raw_{table}
                WHERE _peerdb_batch_id > {last_norm_batch}
                ON CONFLICT (id) DO UPDATE ...
            """)

    destination.update_normalize_batch_id(sync_batch_id)


# ============================================================
# HELPER: Maintain Replication Connection
# ============================================================

def maintain_replication_connection(source):
    """Send periodic keepalives to prevent slot timeout."""
    while not should_stop():
        source.send_standby_status(
            write_lsn=state.last_lsn,
            flush_lsn=state.last_lsn,
            timestamp=now()
        )
        sleep(10s)
```

## Key Source Files

| Component | File | Description |
|-----------|------|-------------|
| CDC Workflow | `flow/workflows/cdc_flow.go` | Main workflow orchestrator |
| Sync Activity | `flow/activities/flowable.go` | SyncFlow entry point |
| Sync Core | `flow/activities/flowable_core.go` | Pull/push/normalize logic |
| Postgres Pull | `flow/connectors/postgres/cdc.go` | WAL reading |
| ClickHouse Push | `flow/connectors/clickhouse/cdc.go` | Avro sync |
| ClickHouse Normalize | `flow/connectors/clickhouse/normalize.go` | S3 → final table |
| Metadata Store | `flow/connectors/external_metadata/store.go` | Checkpoint tracking |

## Common Bottlenecks

1. **WAL Reading Speed**: Limited by disk I/O and WAL decoding
2. **Network Bandwidth**: Between source → worker → destination
3. **Destination Write Speed**: Bulk insert performance varies by destination
4. **Normalization Lag**: If destination is slow to normalize, sync blocks via backpressure
5. **Memory**: Large batches can cause OOM if not streaming properly (channel-based streaming prevents this)
