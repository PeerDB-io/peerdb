# Implementation Plan: `QualifiedTable` struct identifiers throughout PeerDB

Replaces `schema.table` string identifiers with structs everywhere internally.
Supersedes the string-escaping approach of PR #4052.

## Locked requirements

1. Scope: BOTH source and destination identifiers; CDC AND QRep (incl. `watermark_table`).
   Nexus (Rust) stays on legacy string fields. UI updated to send structured names.
2. Proto message: `QualifiedTable { string namespace = 1; string table = 2; }`, namespace
   optional. Connector packing: BigQuery puts `project.dataset` in namespace; EventHub puts
   `eventhub.partition_key_column` in table.
3. API compat: new struct fields added next to deprecated string fields. Boundary
   (gRPC handlers + persisted-config loads + workflow/activity entry) normalizes
   string→struct via first-dot split, then clears strings in memory; internals assume
   structs only. No escaping scheme.
4. Go maps keyed by comparable `common.QualifiedTable`; proto maps needing message keys
   become repeated lists + runtime map construction with uniqueness validation.
5. Catalog: split namespace/table columns + migrations, backfill via first-dot split.

## 0. Global decisions and conventions

1. **Canonical internal type**: `common.QualifiedTable{Namespace, Table}`
   (`flow/pkg/common/identifiers.go:8`) — comparable, the key type for all Go maps
   currently keyed by `schema.table`.
2. **Proto message**: `peerdb_flow.QualifiedTable`; helpers `common.QualifiedTableFromProto`
   / `(common.QualifiedTable).Proto()` bridge the generated pointer message.
3. **Normalization is a pure function** (first-dot split, no catalog lookup):
   `if contains dot → cut at first dot; else {"" , s}`. Safe inside Temporal workflow code
   (deterministic on replay, no new commands). BigQuery 3-part quirk handled inside the
   connector, not the normalizer.
4. **`LegacyDotted()`** (= `Namespace + "." + Table`, or `Table` if namespace empty) used
   ONLY where strings persist outside the catalog or are user-exposed and must not change:
   - `_peerdb_destination_table_name` values in destination raw tables (pending pre-deploy
     rows must still match),
   - Lua script API (`flow/pua/peerdb.go:505-508` — `record.target`/`record.source`),
   - child workflow IDs (`flow/workflows/snapshot_flow.go:120`),
   - API display fields (`CDCTableRowCounts.table_name`, `CloneTableSummary.source_table`).
   Enforce uniqueness of `LegacyDotted()` across a flow's destination tables at validation
   time (collision only possible when a component itself contains a dot).
5. **Connector-specific packing**:
   - **BigQuery**: `convertToDatasetTable` (`flow/connectors/bigquery/bigquery.go:1012`)
     takes `common.QualifiedTable`, tolerates `{"",t}`, `{d,t}`, `{p.d,t}`, and the
     mid-flight-normalized `{p,"d.t"}` (namespace without dot + table with dot ⇒ re-split
     table at first dot).
   - **EventHub**: destination `Table` packs `eventhub.partition_key_column`; first-dot
     normalization of `ns.hub.pkcol` produces `{ns,"hub.pkcol"}`. `NewScopedEventhub`
     (`flow/connectors/eventhub/scoped_eventhub.go:18`) takes `common.QualifiedTable` and
     splits `Table` at the first dot.
   - **ClickHouse / Kafka / PubSub / Elasticsearch / S3**: table-only; `Namespace == ""`.
6. **Logs/metrics/errors**: standardize on `QualifiedTable.String()` (quoted form).
   `LegacyDotted()` never used in logs.
7. **Strings cleared at the boundary** in memory; outbound API responses (`MirrorStatus`)
   populate BOTH struct and legacy strings (via `LegacyDotted()`) for nexus/external readers.
   Rollback-safety variant (recommended): persisted `flows.config_proto` keeps both forms
   for one release (clearing only in memory); strings dropped from persistence in N+1.

## Phase 1 — Proto + codegen + common helpers (one commit)

### 1.1 protos/flow.proto

New messages near `TableNameMapping`:

```proto
message QualifiedTable {
  string namespace = 1; // schema (PG), db (MySQL/Mongo), dataset or project.dataset (BQ), EH namespace; empty for table-only dests
  string table = 2;     // table / collection / topic / index; EventHub packs "eventhub.partition_key_column"
}
message QualifiedTableMapping {
  QualifiedTable source = 1;
  QualifiedTable destination = 2;
}
```

Field additions (legacy string fields get `[deprecated = true]`; nothing reserved/removed):

| Message (line) | New fields |
|---|---|
| `TableMapping` (:34) | `QualifiedTable source_table = 10; QualifiedTable destination_table = 11;` |
| `TableNameMapping` (:20) | `QualifiedTable source = 3; QualifiedTable destination = 4;` (nexus-only; completeness) |
| `QRepConfig` (:366) | `QualifiedTable watermark_table_struct = 33; QualifiedTable destination_table = 34;` |
| `EnsurePullabilityBatchInput` (:198) | `repeated QualifiedTable source_tables = 6;` |
| `EnsurePullabilityBatchOutput` (:209) | `message TableRelId { QualifiedTable table = 1; uint32 rel_id = 2; }` → `repeated TableRelId table_rel_ids = 2;` |
| `SetupReplicationInput` (:213) | `repeated QualifiedTableMapping qualified_table_mappings = 10;` |
| `CreateRawTableInput` (:232) | `repeated QualifiedTableMapping qualified_table_mappings = 5;` |
| `TableSchema` (:241) | `QualifiedTable table = 8;` (field 1 deprecated but still READ — persisted bytea rows) |
| `TableSchemaDelta` (:467) | `QualifiedTable src_table = 6; QualifiedTable dst_table = 7;` |
| `SyncFlowOptions` (:189) | `map<uint32, QualifiedTable> src_table_id_mapping = 8;` (message values in maps are legal) |
| `RenameTableOption` (:154) | `QualifiedTable current_table = 3; QualifiedTable new_table = 4;` |
| `RemoveTablesFromRawTableInput` (:168) | `repeated QualifiedTable destination_tables = 5;` |
| `CreateTablesFromExistingInput` (:179) | `repeated QualifiedTableMapping new_to_existing = 5;` |
| `GetDefaultPartitionKeyForTablesOutput` (:671) | `message TablePartitionKey { QualifiedTable table = 1; string partition_key = 2; }` → `repeated TablePartitionKey table_partition_keys = 2;` |
| `SetupNormalizedTableBatchOutput` (:288) | `message TableExistsEntry { QualifiedTable table = 1; bool exists = 2; }` → `repeated TableExistsEntry tables = 2;` |
| `ChildTableRange` (:432) | `QualifiedTable child_table = 4;` |
| `SetupNormalizedTableOutput` (:283) | `QualifiedTable table = 3;` (verify usage; possibly skip) |

`protos/route.proto`: `CDCTableRowCounts.table_name` (:139) and `CloneTableSummary` (:293)
stay display strings (LegacyDotted). Optionally add `QualifiedTable table = 3;` to
`CDCTableRowCounts` for the UI — recommended.

### 1.2 Codegen

- `./generate-protos.sh` (buf, `buf.gen.yaml`): Go `flow/generated/protos/`, then
  `cd flow && go generate` for `flow/generated/proto_conversions/flow_config_converter.go`.
  `FlowConnectionConfigs`/`Core` gain no new fields ⇒ equality check in
  `flow/cmd/codegen/flow_config_converter.go:120-209` passes.
- Rust `nexus/pt/src/gen/peerdb_flow.rs`: additive; nexus references only string fields
  (`nexus/analyzer/src/lib.rs:187-188`, `nexus/flow-rs/src/grpc.rs:104-105`,
  `nexus/server/src/main.rs:339-352`). Verify with `cargo check`.
- TS `ui/grpc_generated/flow.ts`: additive.

### 1.3 flow/pkg/common/identifiers.go

- Add `LegacyDotted()`, `Proto()`, `QualifiedTableFromProto`, and
  `NormalizeTableIdentifier(s string) QualifiedTable` (first-dot split; no-dot ⇒ `{"",s}`;
  never errors). Strict `ParseTableIdentifier` deleted at end of refactor.
- If keeping `pkg/common` proto-free is preferred, put `Proto()`/`FromProto` in
  `flow/internal/qualified.go` instead (no import cycle either way).
- Unit tests: normalization table tests incl. `"a"`, `"a.b"`, `"a.b.c"`, `"a.b.c.d"`,
  empty, quotes in components.

### 1.4 flow/internal/identifier_normalization.go (new)

Pure, idempotent functions that populate structs and clear legacy strings:
`NormalizeTableMapping(s)`, `NormalizeFlowConfig` (Core + API overload),
`NormalizeQRepConfig`, `NormalizeSyncFlowOptions`, `NormalizeCDCFlowConfigUpdate`,
`NormalizeDropFlowInput`, `NormalizeTableSchema`, `NormalizeEnsurePullabilityInput`,
`NormalizeSetupReplicationInput`, `NormalizeCreateRawTableInput`,
`NormalizeRenameTablesInput`, `NormalizeRemoveTablesFromRawTableInput`,
`NormalizeQRepPartition` (child_table_ranges), plus
`DenormalizeFlowConfigForAPI` (fills legacy strings for outbound responses; wire into
`pconv.FlowConnectionConfigsCoreToAPI` call sites, e.g. `flow/cmd/mirror_status.go`).
Unit tests in `flow/internal/identifier_normalization_test.go`.

## Phase 2 — Normalization boundary wiring (one commit)

### 2.1 gRPC handlers (flow/cmd/)
- `handler.go:158 CreateCDCFlow`: normalize first; validation sees structs only. Add
  uniqueness validation: no duplicate QualifiedTable sources/destinations and no duplicate
  `LegacyDotted()` destination strings.
- `handler.go:249 CreateQRepFlow`: normalize before `createQRepJobEntry`.
- `FlowStateChange` signal path: normalize `CDCFlowConfigUpdate` additional/removed tables
  before signaling.
- `flow/cmd/validate_mirror.go:97,148,183-185`: struct fields.
- `flow/cmd/cancel_table_addition.go`: normalize input.
- `flow/cmd/mirror_status.go:471 getFlowConfigFromCatalog`: normalize after unmarshal;
  denormalize on response path.
- `dropFlow`/`shutdownFlow` covered via `getFlowConfigFromCatalog`.

### 2.2 Catalog config loads
- `flow/internal/flow_configuration_helpers.go:14 FetchConfigFromDB`: normalize before
  return (covers `bigquery/qrep_object_pull.go:328` etc.).
- `flow/activities/flowable_core.go:43 getTableNameSchemaMapping`: reads split columns
  (post-Phase 5); returns `map[common.QualifiedTable]*protos.TableSchema`; normalize
  deserialized `TableSchema`.
- `flow/internal/postgres.go:83,102,287` (LoadTableSchema* / ReadModifyWrite*): take
  `common.QualifiedTable`, query split columns.

### 2.3 Temporal workflows (replay safety)
Normalization at workflow entry is a pure transform of recorded input ⇒ deterministic on
replay, no new commands. Precedent: maintenance-pause + Continue-As-New convention
(`flow/workflows/cdc_state/state.go:18-23`), unconditional `MigratePostgresTableOIDs`
(`flow/workflows/cdc_flow.go:578-591`).
- `cdc_flow.go:475 CDCFlowWorkflow`: normalize cfg; if state != nil, normalize
  `state.SyncFlowOptions`, `state.FlowConfigUpdate`, `state.DropFlowInput`.
- `processCDCFlowConfigUpdate` (cdc_flow.go:94): normalize `state.FlowConfigUpdate` at top
  (signals enqueued pre-deploy, delivered post-deploy).
- Same one-line entry normalization in `setup_flow.go:358`, `snapshot_flow.go`,
  `qrep_flow.go`, `xmin_flow.go`, `drop_flow.go`, `cancel_table_addition_flow.go`.
- Resync suffix on structs: `cdc_flow.go:627-635` (`.Table += "_resync"`), `:765-779`
  rename options; `qrep_flow.go:367-429` (`_peerdb_resync`).
- `setup_flow.go:359-361` / `snapshot_flow.go:56-59` maps become
  `map[common.QualifiedTable]common.QualifiedTable`; populate repeated
  `QualifiedTableMapping` proto fields.
- `setup_flow.go:140-148` ensurePullability: consume `table_rel_ids` into
  `map[uint32]common.QualifiedTable`, validate uniqueness, store via
  `SyncFlowOptions.src_table_id_mapping`.
- `cdc_flow.go:415-426 processTableRemovals`: set keyed by `common.QualifiedTable`.

### 2.4 Activity boundary (defensive)
With the paused-deploy prerequisite no activities should be in flight across a deploy,
so this is belt-and-suspenders (cheap, greppable, and protects against an unpaused
deploy). Add matching `Normalize*` at top of:
- `flow/activities/flowable.go`: `SetupTableSchema` (:206), `CreateNormalizedTable` (:256),
  `SyncFlow`, `AddTablesToPublication`, `RemoveTablesFromPublication`,
  `RemoveTablesFromRawTable`, `RemoveTablesFromCatalog`, `RenameTables`,
  `CreateTablesFromExisting`, `UpdateCDCConfigInCatalogActivity`,
  `MigratePostgresTableOIDs` (:2084).
- `flow/activities/snapshot_activity.go`: `SetupReplication`,
  `GetDefaultPartitionKeyForTables`, `LoadTableSchema`.
- `flow/activities/cancel_table_addition_activity.go` (already uses QualifiedTable :376).
- QRep activities: `NormalizeQRepConfig` at entry of `GetQRepPartitions`,
  `ReplicateQRepPartitions`, `ConsolidateQRepPartitions`, etc.
(Interceptor alternative rejected — explicit calls are greppable/testable.)

## Phase 3 — Model & internal plumbing (one commit; big mechanical one)

`flow/model/model.go`:
- `NameAndExclude.Name` → `common.QualifiedTable` (:16-30).
- `PullRecordsRequest`: `SrcTableIDNameMapping map[uint32]common.QualifiedTable`,
  `TableNameMapping map[common.QualifiedTable]NameAndExclude`,
  `TableNameSchemaMapping map[common.QualifiedTable]*protos.TableSchema` (:66-93).
- `TableWithPkey.TableName` → `common.QualifiedTable` (:114) — still comparable map key.
- `RecToTablePKey` (:120), `RecordsToStreamRequest.TableMapping` (:40),
  `SyncRecordsRequest`/`NormalizeRecordsRequest.TableNameSchemaMapping` (:143-172),
  `SyncResponse.TableNameRowsMapping` (:177).

`flow/model/record.go`: `SourceTableName`/`DestinationTableName` →
`SourceTable`/`DestinationTable common.QualifiedTable`; rename interface method to
`GetDestinationTable()` for compile-driven refactor. Log sites use `.String()`
(`flow/model/cdc_stream.go:86-88`).

`flow/model/conversion_avro.go` + per-destination raw-row writers:
`_peerdb_destination_table_name` = `rec.GetDestinationTable().LegacyDotted()`.

`flow/internal/schema_helpers.go`: `BuildProcessedSchemaMapping` returns
`map[common.QualifiedTable]*protos.TableSchema` (:44-90); `AdditionalTablesHasOverlap`
keys sets by struct (:13-39).

`flow/activities/flowable_core.go`: `tblNameMapping` (:146-149), pull/sync request
population (:238-252, :321-333), `getTableNameSchemaMapping` (:43).
`flow/activities/flowable.go`: `applySchemaDeltas`, `SetupTableSchema` insert writes
`(flow_name, table_namespace, table_name, table_schema)`, resync schema-mapping updates
(:1655-1730), `RemoveTablesFromCatalog` (:1919), DropFlow cleanup (:1941).

`flow/shared/telemetry/activity_logging.go:249-250`: switch inventory queries to new columns.
`flow/pua/peerdb.go:505-508`: push `LegacyDotted()` (Lua compat).

## Phase 4 — Per-connector conversion (one commit per connector)

Order: postgres → mysql → mongo → clickhouse → snowflake → bigquery →
postgres-destination/external_metadata → kafka/pubsub/eventhub/elasticsearch/s3.

1. **postgres source**: `cdc.go` (PostgresCDCConfig maps :72-86,
   `getSourceSchemaForDestinationColumn` :144-157 reads `.Namespace` — delete the parse),
   `postgres_source.go` (:325, :527, :663, :789-793, :1041, :1073), `qrep_source.go`
   (watermark struct :166/:248/:378; partition-child handling :490-510 —
   `ChildTableRange.child_table` becomes QualifiedTable; children keep parent's destination
   mapping), `client.go:526-540`, `validate.go:248,310`, `qrep_partition_test.go`.
2. **mysql**: `mysql.go:554`, `qrep.go:57,222`, `cdc.go:74,575,772`, `validate.go:91`;
   `QualifiedTable.MySQL()` already exists for quoting.
3. **mongo**: `mongo.go:204`, `cdc.go:477-490` (`createPipeline` match on `{db,coll}` from
   struct, delete `SplitN`), `qrep.go:49,103`, `validate.go:26`.
4. **clickhouse**: table-only (`Namespace==""`, validate at mirror validation);
   `normalize.go` (:456/:578 schema lookups, :644 distinct-raw-table query maps strings
   back via LegacyDotted lookup), `cdc.go` (raw table `_peerdb_raw_<flow>` untouched),
   `table_function.go:141` (watermark struct), `validate.go`.
5. **snowflake**: `client.go:19-38` (`SnowflakeIdentifierNormalize` per-component —
   uppercase semantics unchanged), `snowflake.go:309,736,755`, `qrep.go:54,182`,
   `merge_stmt_generator.go:30`, `qrep_avro_consolidate.go:150`. Raw-name comparisons must
   use the same LegacyDotted string sync wrote (case-sensitive today — keep).
6. **bigquery**: `bigquery.go:1012` per decision 5; `qrep.go:25,55,64,79,103`; `source.go`;
   raw table untouched.
7. **postgres destination + pkg/postgres**: `postgres_destination.go:267,285,440,523,598,614`,
   `normalize_stmt_generator.go:111,156`, `postgres.go:373`,
   `pkg/postgres/dest_validation.go:45,109`.
8. **queues/object stores**: `kafka/kafka.go`, `pubsub`, `s3` (Namespace==""; LegacyDotted
   only where strings land in object paths — verify s3 layout stays byte-identical),
   `elasticsearch.go:225-234` (index = Table), `eventhub/eventhub.go:112,285` +
   `scoped_eventhub.go`.
9. **connectors/core.go** interface updates: `SetupNormalizedTable` (:168),
   `GetTableSizeEstimatedBytes` (:359), `RenameTablesWithSoftDeleteConnector` map (:323).

## Phase 5 — Catalog migrations + monitoring queries (one commit; migration deploys before code needing columns)

Next free migration: V54.
- **V54__table_schema_mapping_namespace.sql**: add `table_namespace TEXT NOT NULL DEFAULT ''`;
  backfill `split_part(table_name,'.',1)` / remainder where dot present (idempotent guard);
  PK → `(flow_name, table_namespace, table_name)`. (Keys are DESTINATION identifiers, so
  no-dot rows — CH/Kafka — stay `namespace=''`, matching runtime normalizer.)
- **V55__cdc_table_aggregate_counts_namespace.sql**: add `destination_table_namespace`,
  backfill, PK → `(flow_name, destination_table_namespace, destination_table_name)`,
  recreate dest-table index.
- **V56__qrep_runs_table_namespaces.sql**: add `source_table_namespace`,
  `destination_table_namespace` to `peerdb_stats.qrep_runs`, backfill.
- **V57__drop_flows_legacy_table_identifiers.sql**: drop dead
  `flows.source_table_identifier`/`destination_table_identifier` (re-verify nexus first).
- Not migrated: `cdc_batches`, `cdc_flows`, `qrep_partitions` (ranges only),
  `schema_deltas_audit_log.delta_info` (JSONB debug).

Go query updates: `flow/connectors/utils/monitoring/monitoring.go`
(`AddCDCBatchTablesForFlow` :145 takes `map[common.QualifiedTable]*RecordTypeCounts`,
upsert :188 writes namespace; `InitializeQRepRun` :246 split source/dest),
`flow/cmd/mirror_status.go:662,698`, `flow/activities/flowable.go:1115`,
`flow/shared/telemetry/activity_logging.go:249-250`, all `table_schema_mapping` call sites.

**Ordering**: migrations apply at startup before workers serve traffic. Old pods' INSERT
... ON CONFLICT `(flow_name, table_name)` breaks against new PK during rollout — keep a
unique index on `(flow_name, table_name)` until code deploy completes (drop in V58), or
rely on maintenance pause.

## Phase 6 — UI (one commit)

- New `ui/lib/utils/tableIdentifier.ts`: `qualifiedTableFromParts`,
  `displayQualifiedTable` (dotted join), `parseDestinationInput(text, peerType)`
  (first-dot generic, last-dot for BigQuery, table-only for CH/Kafka/PubSub/ES).
- `ui/app/dto/MirrorsDTO.ts`: `TableMapRow` gains `tableName` component alongside `schema`;
  `source` stays the dotted display/React key.
- `ui/app/mirrors/create/cdc/schemabox.tsx`: stop splitting at :152; pass components;
  record components in `fetchTablesForSchema`.
- `ui/app/mirrors/create/handlers.ts`: `reformattedTableMapping` (:182) /
  `changesTableMapping` (:201) emit structs, clear legacy strings; QRep watermark split
  :279 → `watermarkTableStruct`; `destinationTableIdentifier` :343 → `destinationTable`.
- `ui/app/mirrors/create/qrep/qrep.tsx:124-125`, `helpers/qrep.ts`, `helpers/common.ts`.
- `ui/app/mirrors/[mirrorId]/edit/EditMirror.tsx:96`, `columnDisplayModal.tsx:40`,
  `tablePairs.tsx` (render via `displayQualifiedTable`).
- `eventhubsCallout.tsx`: update destination-format help text.
- Keep legacy-string fallback when rendering old mirror configs (server denormalizes anyway).

## Phase 7 — Test plan

The codebase tests mostly through e2e; per-component unit tests where existing files
already cover the component or where logic is pure and tricky. Coverage splits into four
buckets: (A) regression of the mechanical refactor, (B) new dotted-name behavior,
(C) backward compatibility with pre-refactor state, (D) targeted unit tests.

### Dot-capability matrix (what each system permits — gates what we can test)

| System | Namespace with dot | Table with dot | Notes |
|---|---|---|---|
| Postgres (src+dst) | yes (`"sch.ema"`) | yes (`"ta.ble"`) | primary dotted-source vehicle |
| MySQL (src) | **no** | **no** | MySQL forbids `.` in db/table names — dotted-source tests N/A; refactor still applies (no behavior to test) |
| MongoDB (src) | **no** (db) | yes (collection) | test dotted collection |
| ClickHouse (dst) | n/a (table-only) | yes | **headline regression test**: single-part dest name containing a dot |
| Snowflake (dst) | yes (`"SCH.EMA"`) | yes (`"TA.BLE"`) | quoted identifiers |
| BigQuery (dst) | **no** (datasets: `[A-Za-z0-9_]`) | **no** | only unit-test `project.dataset` packing resolution |
| Kafka (dst) | n/a | yes (topics allow `.`) | dotted topic test |
| PubSub (dst) | n/a | yes (topic IDs allow `.`) | dotted topic test |
| Elasticsearch (dst) | n/a | yes (within index name) | dotted index test |
| S3 (dst) | n/a | yes (path component) | verify path layout for dotless stays byte-identical |
| EventHub (dst) | yes | **unsupported** | EventHub names may legally contain dots, but our `eventhub.partition_key_column` packing keeps them ambiguous — documented limitation, parity with today |

### A. Regression — existing e2e suites (the broad net)

All existing suites updated to build struct mappings and must pass unchanged — this is
the main safety net for the mechanical refactor (maps rekeyed, signatures changed).
Helpers: `flow/e2e/test_utils.go:76 AttachSchema` + mapping builders, `congen.go`,
`pg.go:224`, `postgres.go`, `clickhouse.go:163`. Suites (PR 4052 file list is the floor):
api, bigquery(+qrep,+source), cancel_table_addition, clickhouse(+tls,+mysql),
elasticsearch(+qrep), eventhub, generic, kafka, mongo, switchboard_mongo,
mysql_rds_binlog, postgres(+qrep), pubsub, s3(+qrep), snowflake(+qrep,+schema_delta),
flow_status, pg_schema_dump. These run across the CI compatibility matrix as usual;
check `cidb` for failures per branch.

### B. New dotted-name e2e (the feature)

Write dotted DDL with proper quoting in a dedicated test file rather than retrofitting
every helper; gate per the capability matrix (suite helper, e.g.
`(suite) DottedDestinationName()` returns a dotted name only where legal, so dotted-source
coverage still runs against BQ).

1. **Generic CDC matrix test** (`generic_test.go`, runs on PG-source suites; skips
   MySQL-source): source `"sch.ema"."ta.ble"` → dest per capability (CH `"dst.table"`,
   SF `"SCH.EMA"."TA.BLE"`, PG dotted, BQ plain). Covers: initial snapshot, CDC
   insert/update/delete, toast, soft-delete + synced-at cols, column exclusion.
2. **Schema changes on dotted table**: add/drop column mid-CDC (exercises
   `TableSchemaDelta` structs + `ReplayTableSchemaDeltas` on each destination).
3. **ALTER MIRROR table addition/removal with dotted names** (signal path:
   `CDCFlowConfigUpdate` → publication ALTER, schema setup, raw-table removal). Include
   cancel_table_addition variant.
4. **Resync with dotted names** (`_resync` suffix must land on `.Table`, rename swap on
   destination — SF + CH + PG).
5. **QRep with dotted watermark table** (PG source; `postgres_qrep_test.go` + one
   ClickHouse QRep): full refresh + watermark incremental + ctid partitioning. Include a
   **dotted partitioned parent table** case (exercises `ChildTableRange.child_table`).
6. **Mongo dotted collection** (`mongo_test.go`): CDC + QRep of `db."col.lection"`.
7. **Queue destinations**: Kafka dotted topic, PubSub dotted topic, Elasticsearch dotted
   index (each in its existing suite; one test per suite suffices).
8. **API-level tests** (`api_test.go`):
   - create mirror sending only struct fields (new client path),
   - create mirror sending only legacy strings (nexus path) → verify normalization +
     mirror works,
   - **validation rejections**: duplicate struct destinations; `LegacyDotted` collision
     pair (`{a, b.c}` vs `{a.b, c}`) rejected; empty table component rejected.
9. **Initial-snapshot-only mirror** with dotted names (snapshot/clone workflow path —
   child workflow IDs built from `LegacyDotted` + illegal-char replacement).

### C. Backward compatibility

1. **Normalization unit tests** (`flow/internal/identifier_normalization_test.go`): every
   `Normalize*` — legacy-only input, struct-only input (idempotent no-op), mixed input
   (struct wins), string-clearing asserted; `DenormalizeFlowConfigForAPI` round-trip.
2. **Old persisted artifacts as fixtures**: serialize `FlowConnectionConfigsCore` /
   `TableSchema` / `SyncFlowOptions` bytes with ONLY legacy string fields (as current main
   produces), feed through `FetchConfigFromDB` / schema load paths, assert normalized
   structs. Cheap and pins the wire format.
3. **Raw-table continuity**: ClickHouse `normalize_test.go`-style test seeding raw rows
   whose `_peerdb_destination_table_name` is legacy-dotted, then normalizing with a
   struct-keyed config — rows must land. Plus a unit test pinning that the sync-side
   writer and normalize-side lookup both use `LegacyDotted` (centralized helper).
4. **Catalog migration tests**: Go test against the dev catalog applying V54/V55/V56
   backfills to fixture rows (dotless + pre-existing dotted-by-bug rows), asserting split
   results, new PK uniqueness, and idempotency (running the backfill twice is a no-op).

(No Temporal replay testing: the deploy model guarantees mirrors are paused and signal-free
during deploy, so post-deploy runs start fresh via Continue-As-New with old-format inputs —
covered by the entry-normalization unit tests above.)

### D. Per-component unit tests (existing files where possible)

| Area | File | Cases |
|---|---|---|
| Identifier core | `flow/pkg/common/identifiers_test.go` (new) | `NormalizeTableIdentifier` table-driven (`a`, `a.b`, `a.b.c`, `a.b.c.d`, ``, leading/trailing dot); `LegacyDotted` round-trip property (Normalize∘LegacyDotted = id iff components dot-free); `String()` quoting; collision-detection helper |
| Snowflake SQL | `snowflake/merge_stmt_generator_test.go` | golden MERGE/normalize SQL **byte-identical** for existing dotless cases; new dotted cases; `SnowflakeIdentifierNormalize` per-component casing |
| ClickHouse SQL | `clickhouse/normalize_query_test.go`, `normalize_test.go` | dotted destination table in INSERT…SELECT; raw-name lookup map construction |
| Postgres SQL | `postgres/normalize_stmt_generator_test.go` | dotted schema+table quoting in merge/updates |
| Postgres QRep | `postgres/qrep_partition_test.go` | dotted watermark; `ChildTableRange` struct; child→parent mapping |
| BigQuery | `bigquery/utils_test.go` / `merge_stmt_generator_test.go` | `convertToDatasetTable` matrix: `{"",t}`+default dataset, `{d,t}`, `{p.d,t}`, mid-flight `{p,"d.t"}`, errors; golden SQL unchanged |
| EventHub | `eventhub/scoped_eventhub_test.go` (new) | packing/unpacking `{ns,"hub.pkcol"}`; error cases |
| Lua scripts | `flow/pua/peerdb_test.go` | `record.target`/`record.source` return legacy-dotted strings (never quoted `String()`) for dotted destinations |
| Schema helpers | `flow/internal/schema_helpers_test.go` | `BuildProcessedSchemaMapping` / `AdditionalTablesHasOverlap` keyed by struct, dotted overlap cases |
| Validation | `flow/cmd` validate tests | duplicate/collision/empty-component rejection |

### UI

No automated UI test infra — manual checklist: create CDC + QRep mirror via UI against a
dotted PG schema (schemabox listing, column modal, destination input parsing per peer
type), EditMirror add-tables with dotted table, table pairs rendering, EventHub callout
text. Verify legacy mirrors (string-only configs) still render via the denormalized
response.

### E. Upgrade testing in a local env (old bits → new bits)

Covers live-upgrade semantics no automated test honestly can: old bits write state, new
bits consume it. **Prerequisite assumed by the deploy model: all mirrors are paused and
no signals are sent during deploy** — so mid-flight workflow upgrades, in-flight signals,
and mid-run QRep upgrades are out of scope by design (workflows resume post-deploy via
Continue-As-New with old-format inputs, which entry normalization covers). Matrix:
PG→ClickHouse for most scenarios + one merge-based destination (Snowflake or BigQuery)
for the raw-table round-trip.

1. **Paused-mirror upgrade (the official path)**: mirror created on old bits, paused;
   deploy; resume on new bits — old-format config/state through entry normalization,
   then continued CDC correctness.
2. **Pending un-normalized batches**: old bits sync raw rows, hold normalize back
   (pause landing between sync and normalize); upgrade; normalize must consume rows
   written by old code (LegacyDotted round-trip — the top silent-data-loss risk).
3. **Catalog migrations on real data**: organically-populated table_schema_mapping /
   cdc_table_aggregate_counts / qrep_runs / flows.config_proto through V54-V56; verify
   stats continuity in UI (pre- and post-upgrade rows attribute to the SAME table).
4. **Old mirror, new operations**: resync, drop, edit of an old-bits-created mirror on
   new bits (legacy-format config through rename/_resync, DropFlowInput, edit paths).
5. **Client compat**: old mirrors in new UI; string-only CreateCDCFlow via grpcurl
   (nexus/old-client shape); MirrorStatus responses keep legacy strings; nexus
   CREATE MIRROR via psql; Lua-script mirror routing unchanged across upgrade.

## Backward-compatibility analysis

- **Running CDC mirror across deploy**: deploy model guarantees mirrors are paused +
  Continue-As-New, no signals during deploy (`cdc_state/state.go:18-23`); post-deploy run
  inputs contain legacy strings → entry normalization converts in memory; next
  `uploadConfigToCatalog` (`cdc_flow.go:70-92`) persists structs. Entry normalization is
  pure ⇒ deterministic even if a run were not drained; Phase 2.4 covers stray activity
  retries as belt-and-suspenders.
- **Raw tables**: pre-deploy rows carry legacy-dotted `_peerdb_destination_table_name`;
  post-deploy writes the same `LegacyDotted()` ⇒ pending batches normalize. No migration.
- **table_schema_mapping**: V54 splits; `SetupTableSchema` re-upserts on next setup;
  `TableSchema.table_identifier` inside bytea stays readable via `NormalizeTableSchema`.
- **Resync**: suffix on `.Table` only; old-format `DropFlowInput` normalized at
  `DropFlowWorkflow` entry.
- **QRep**: old runs get entry normalization; partitions recorded pre-deploy handled by
  `NormalizeQRepPartition` at pull activity entry.
- **Nexus**: sends only legacy strings; handler normalizes ⇒ zero nexus changes. Responses
  keep legacy strings populated.
- **Rollback caveat**: once `config_proto` is rewritten with cleared strings, old code
  can't read it ⇒ recommended softer variant: `UpdateCDCConfigInCatalogActivity` writes
  both forms in release N, clears strings in N+1.

## Risk list

1. Raw-table string round-trip (`clickhouse/normalize.go:644`, SF/BQ/PG merge generators):
   centralize one `LegacyDotted` + one `BuildRawNameLookup(mappings) map[string]QualifiedTable`.
2. `LegacyDotted` collisions (`{"a","b.c"}` vs `{"a.b","c"}`): enforce per-flow uniqueness
   at validation.
3. Postgres partition/inheritance CDC (`cdc.go:159 getChildToParentRelIDMap`,
   qrep child ranges): children map to parent QualifiedTable; publication ALTERs use parent.
4. Snowflake uppercase (`client.go:19`): per-component normalize, byte-identical SQL for
   existing mirrors — golden MERGE SQL test.
5. BigQuery 3-part: `{p,"d.t"}` vs `{p.d,t}` must resolve identically — matrix test.
6. Synthesized identifiers: `_peerdb_raw_<flow>` (flow-derived, safe); `_resync` suffix on
   `.Table` only, incl. SF/PG `RenameTables` TrimSuffix sites.
7. Proto map → repeated conversions: deterministic ordering inside workflows
   (`slices.SortedFunc` by namespace,table) + duplicate detection.
8. Lua scripts (`pua/peerdb.go:505`): must keep legacy strings, never `String()`.
9. Child workflow IDs (`snapshot_flow.go:120 clone_%s_%s`): keep
   `LegacyDotted` + `ReplaceIllegalCharactersWithUnderscores` so re-attached snapshot
   clones reuse the same IDs.
10. Catalog PK migration vs in-flight old pods (Phase 5 ordering).
11. Codegen equality check (`flow/cmd/codegen/flow_config_converter.go:175`):
    `FlowConnectionConfigs` and `Core` must gain identical fields if any (plan adds none).

(Testing strategy is fully specified in Phase 7 above.)
