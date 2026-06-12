# QualifiedTable refactor — progress log

Plan: `.claude/plans/qualified-table-identifiers.md` (commit 6c9407e7)
Branch: `qualified-table-identifiers`

## Status legend
- [ ] not started  - [~] in progress  - [x] done  - [!] blocked/deviation (explained)

## Phase 1 — Proto + codegen + common helpers — DONE
- [x] 1.1 flow.proto field additions (all messages from plan + SetupFlowOutput) +
      route.proto CDCTableRowCounts.table=3
- [x] 1.2 codegen: buf generate + go generate OK (FlowConnectionConfigs equality check
      passed); cargo check OK (nexus unaffected); ui/grpc_generated/flow.ts regenerated
- [x] 1.3 common helpers: LegacyDotted + NormalizeTableIdentifier in pkg/common;
      Proto conversions in flow/internal/qualified.go (see deviation below) + tests
- [x] 1.4 flow/internal/identifier_normalization.go (Normalize*/Denormalize* for all
      identifier-bearing messages) + tests incl. old-wire-format fixture test

## Phase 2 — Normalization boundary — DONE (commit 3ad972fa)
- [x] 2.1 gRPC handlers: CreateCDCFlow/ValidateCDCMirror/CreateQRepFlow normalize at entry;
      FlowStateChange normalizes CDCFlowConfigUpdate + validates additions;
      validateTableMappingIdentifiers (dups, LegacyDotted collisions, empties);
      mirror_status getFlowConfigFromCatalog normalize+denormalize; persisted configs
      keep both forms (rollback safety: createCdcJobEntry, createQRepJobEntry,
      UpdateCDCConfigInCatalog, UpdateCdcJobEntry, MarshalTableSchemaForCatalog)
- [x] 2.2 catalog loads: FetchConfigFromDB, getTableNameSchemaMapping (split columns),
      internal/postgres.go Load/ReadModifyWrite/UpdateOIDs (struct keys + split columns)
- [x] 2.3 workflows: entry normalization in cdc/setup/snapshot/qrep/xmin/drop/cancel;
      resync `_resync` suffix on .Table; rename options structs; ensurePullability
      TableRelIds + uniqueness check; SetupFlowOutput.src_table_id_mapping;
      GetDefaultPartitionKeyForTablesOutput.table_partition_keys (proto gap, added)
- [x] 2.4 activity entry normalization: SetupTableSchema, CreateNormalizedTable,
      SyncFlow, EnsurePullability, CreateRawTable, RenameTables, RemoveTables*,
      AddTables/RemoveTablesFromPublication, CreateTablesFromExisting,
      MigratePostgresTableOIDs, QRep activities, SetupReplication, snapshot activities,
      cancel_table_addition activities; NormalizeTableSchemaDelta(s) added

## Phase 3 — Model & internal plumbing — DONE (commit 3ad972fa)
- [x] model.go map rekeying; record.go GetDestinationTable()/GetSourceTable();
      numeric_truncator struct keys; stream.go raw rows LegacyDotted;
      schema_helpers struct keys + CompareQualifiedTables/SortedQualifiedTables;
      flowable_core/flowable plumbing; telemetry activity_logging (split columns +
      LegacyDotted display); pua/peerdb.go LegacyDotted

## Phase 4 — Connectors — DONE (commit 3ad972fa, via parallel subagents + review)
- [x] postgres (agent; TableRelIds output, publication tuple checks, child ranges)
- [x] mysql + mongo (agent; binlog struct construction, dotted mongo collections)
- [x] clickhouse (agent; table-only namespace enforced in validate, LegacyDotted raw lookups)
- [x] snowflake + bigquery (agent; per-component casing, convertToDatasetTable matrix + test)
- [x] postgres destination + pkg/postgres; kafka/pubsub/s3/elasticsearch/eventhub (orchestrator)
- [x] connectors/core.go interfaces

## Phase 5 — Catalog migrations + monitoring — DONE (commit 3ad972fa)
- [x] V54/V55/V56 split-column migrations + backfills; V57 drops dead flows columns
      (verified nexus/ui don't reference)
- [x] monitoring.go (split columns), mirror_status.go CDCTableTotalCounts (+ struct in
      response), flowable.go RecordMetricsAggregates, reset_sequences quoting

## Phase 6 — UI — DONE (subagent; tsc + lint clean)
- [x] lib/utils/tableIdentifier.ts (displayQualifiedTable, parseDestinationInput with
      peer-type-aware splitting: table-only CH/Kafka/PubSub/ES/S3, last-dot BQ,
      first-dot otherwise incl. EventHub packing)
- [x] MirrorsDTO TableMapRow carries structured sourceTable
- [x] schemabox / handlers / qrep / helpers emit ONLY struct fields on create/edit
- [x] EditMirror / columnDisplayModal / tablePairs / tableStats struct-first with
      legacy fallback for old mirrors; CDCTableRowCounts renders .table
- [x] eventhubsCallout text updated
- [x] bonus: fixed never-firing duplicate-destination check in schema.ts

## Phase 7 — Tests
- [~] A: existing e2e suites — conversion in progress (subagent); unit tests across
      modules fixed & green (commit 4280a4c7; pua needs TZ=UTC env — preexisting;
      connectors/utils SSH test needs dev stack; pkg/mysql vet failure preexisting)
- [ ] B: dotted-name e2e (generic matrix, schema changes, add/remove, resync, qrep watermark, mongo collection, queue dests, api validation, snapshot-only)
- [~] C: backward-compat — normalization units + old-wire-format fixture done (Phase 1);
      raw-table continuity + migration tests TODO
- [~] D: per-component units — identifiers ✓, BQ convertToDatasetTable matrix ✓ (agent),
      EventHub scoped test ✓, CH dotted-destination BuildQuery test ✓ (headline case),
      pua legacy-dotted pinned via existing test; SF golden dotted cases TODO

## Verification (after implementation)
- [ ] build: go build ./... ; cargo check (nexus); ui build
- [ ] unit tests green locally
- [ ] tilt e2e/integration runs
- [ ] manual upgrade testing per plan section E (paused-deploy prerequisite)

## Deviations from plan
- Legacy proto fields annotated via comments instead of [deprecated = true] to avoid
  staticcheck SA1019 on the normalization layer which must read them forever.
- Added SetupFlowOutput.src_table_id_mapping (plan missed this output message).
- flow/pkg is a SEPARATE Go module (plan assumed same module): pkg/common cannot import
  generated/protos, so Proto()/FromProto became free functions
  internal.QualifiedTableProto / internal.QualifiedTableFromProto in
  flow/internal/qualified.go (the plan's fallback option, now mandatory).
- QRepConfig struct field named qualified_watermark_table (plan's watermark_table_struct
  was awkward).

## Manual work log
- 2026-06-12: Tilt env (port 10352) was already rebuilt from the branch; catalog
  migrations V54-V57 auto-applied. Verified on live catalog: table_schema_mapping has
  table_namespace + new PK (flow_name, table_namespace, table_name); 30 pre-existing
  flows backfilled (CH table-only rows → namespace=''); flows legacy identifier columns
  dropped.
- 2026-06-12: Upgrade scenario validated organically: `test_mirror_flow` (PG→CH CDC
  mirror created on OLD bits) resumed on the new worker, healthy sync loop; inserted a
  fresh row into source `public.test_mirror` → landed in ClickHouse `default.test_mirror`
  (raw-table write + normalize on new code against legacy-era table state). Covers plan
  section E scenarios 1 (paused/running mirror across upgrade) and 2 partially
  (raw-table LegacyDotted round-trip on a live mirror).
- 2026-06-12: `e2e_postgres` (TestGenericCH_PG) regression suite: **PASSED** on the
  refactored code. `connector_clickhouse`: **PASSED** (incl. live-CH raw table tests).
  `connector_postgres`: 4 failures, all pre-existing/environmental — 3 SSH keepalive
  tests (toxiproxy state) + TestSupportedDataTypes (fails identically on main; tz-
  sensitive time assertion). pua tests need TZ=UTC (also pre-existing).
- 2026-06-12: Triggered `e2e_mysql-gtid` (TestGenericCH_MySQL) and `e2e_api-postgres`
  (TestApiPg) suites; monitoring.
