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
- [x] A: existing e2e suites converted (subagent) + verified on dev stack:
      TestGenericCH_PG full suite PASSED; TestApiPg all subtests pass after fixes
      (two real issues found & fixed: error-code expectation + destination-keyed
      catalog lookup); connector_clickhouse PASSED; remaining failures pre-existing
      environmental (mysql e2e fails identically on pure main bits; SSH/tz tests)
- [x] B: dotted-name e2e — Test_Dotted_Names_CDC (PG→CH) PASSED;
      TestDottedTableAddition PASSED; Test_Dotted_Watermark_QRep PASSED;
      Test_Dotted_Collection_Name (Mongo) PASSED;
      TestMirrorValidation_DottedIdentifierCollisions PASSED (4/4 rejections).
      SF/BQ branches compile-only (SF suite-skipped upstream; BQ can't have dots)
- [x] C: backward-compat — normalization units + old-wire-format fixture;
      V54 backfill verified against 30 real catalog rows; live old-bits mirror
      continued syncing through upgrade (raw-table LegacyDotted round-trip live)
- [x] D: per-component units — identifiers, BQ convertToDatasetTable matrix, EventHub
      scoped test, CH dotted-destination BuildQuery (headline), SF per-component
      normalize, pua legacy-dotted

## Verification (after implementation)
- [x] build: go build ./... clean (flow + pkg modules); cargo check (nexus) clean;
      ui tsc + lint clean; golangci-lint ./... 0 issues
- [x] unit tests green locally (TZ=UTC; catalog env needed for avro-size tests;
      remaining failures pre-existing/environmental, verified against main)
- [x] tilt e2e runs: TestGenericCH_PG full suite PASSED TWICE (incl. final rerun on
      the finished code), TestApiPg subtests all PASS after fixes, connector_clickhouse
      PASSED, dotted-name tests all PASSED (incl. schema-change replay on dotted table)
- [x] manual upgrade testing per plan section E: live old-bits mirror resumed + synced
      on new bits (E1, E2 partial); V54-57 backfill verified on 30 real rows (E3);
      MirrorStatus dual-form verified via HTTP gateway after fixing a real bug (E5);
      old-mirror drop/resync paths covered by passing api e2e (E4 — no live old-bits
      mirror was expendable for a destructive drop)

## STATUS: COMPLETE (2026-06-12)
All plan phases implemented and verified. 17 commits on qualified-table-identifiers.

## Known acceptable gaps
- Dotted-name resync e2e not written: resync `_resync`/`_peerdb_resync` suffix operates
  on `.Table` only (compile-checked struct logic), resync flows covered dotless by
  TestResyncFailed/TestEditTablesBeforeResync; suffix logic has no string-splitting left.
- Snowflake/BigQuery dotted e2e branches compile-only (SF suite skipped upstream; BQ
  forbids dots) — SF behavior pinned by unit tests instead.
- EventHub names containing dots remain unsupported (documented; packing ambiguity,
  parity with pre-refactor).
- Legacy strings in persisted configs to be dropped in release N+1 (follow-up).

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
- 2026-06-12: `e2e_mysql-gtid`: fails with "source tables do not exist" — reproduced
  IDENTICALLY on pure main bits (main flow-api + main test code): pre-existing
  environmental issue in this dev env (mysql general log shows the validation SELECT
  never reaches the server the test creates tables on). NOT caused by the refactor.
- 2026-06-12: **Dotted-name feature verified live**: `TestGenericCH_PG/Test_Dotted_Names_CDC`
  PASSED (dotted PG schema "e2e_dot.X" + table "ta.ble" → dotted CH table "ta.ble_dst",
  snapshot + CDC insert/update/delete). `TestMirrorValidation_DottedIdentifierCollisions`
  PASSED (duplicate/collision/empty rejections).
- 2026-06-12: TestApiPg triage: (a) TestMirrorValidation_InvalidTableMappings — error
  code changed FailedPrecondition→InvalidArgument by design (boundary validation);
  updated test + tightened validator (source namespace required). (b)
  TestPostgresTableOIDsMigration — e2e conversion bug: helper queried catalog by SOURCE
  key but table_schema_mapping is DESTINATION-keyed; renamed helper to
  getCatalogTableSchemaForDestinationTable and fixed callers. (c) Resync/Drop subtest
  failures overlapped checkout-triggered flow-api rebuilds mid-suite; re-running on a
  stable stack.
- 2026-06-12: NOTE deviation: Tilt proto-gen regenerates flow/generated (gitignored!)
  from the working tree — switching branches clobbers it; rerun `buf generate protos`
  + `go generate` after any checkout.
