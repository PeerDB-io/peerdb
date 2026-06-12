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

## Phase 2 — Normalization boundary
- [ ] 2.1 gRPC handlers (CreateCDCFlow, CreateQRepFlow, FlowStateChange, validate_mirror, cancel_table_addition, mirror_status denormalize)
- [ ] 2.2 catalog config loads (FetchConfigFromDB, getTableNameSchemaMapping, internal/postgres.go)
- [ ] 2.3 workflows entry normalization + resync/rename/ensurePullability struct logic
- [ ] 2.4 activity entry normalization (belt-and-suspenders)

## Phase 3 — Model & internal plumbing
- [ ] model.go map rekeying (NameAndExclude, PullRecordsRequest, TableWithPkey, etc.)
- [ ] record.go GetDestinationTable()/GetSourceTable()
- [ ] conversion_avro + raw-row writers use LegacyDotted
- [ ] schema_helpers.go struct keys
- [ ] flowable_core.go / flowable.go plumbing
- [ ] telemetry activity_logging queries
- [ ] pua/peerdb.go LegacyDotted

## Phase 4 — Connectors
- [ ] postgres source
- [ ] mysql
- [ ] mongo
- [ ] clickhouse
- [ ] snowflake
- [ ] bigquery
- [ ] postgres destination + pkg/postgres
- [ ] kafka / pubsub / s3 / elasticsearch / eventhub
- [ ] connectors/core.go interfaces

## Phase 5 — Catalog migrations + monitoring
- [ ] V54 table_schema_mapping namespace split
- [ ] V55 cdc_table_aggregate_counts namespace split
- [ ] V56 qrep_runs namespaces
- [ ] V57 drop flows legacy identifier columns (verify nexus first)
- [ ] monitoring.go / mirror_status.go / flowable.go query updates

## Phase 6 — UI
- [ ] tableIdentifier.ts utils
- [ ] MirrorsDTO TableMapRow
- [ ] schemabox.tsx / handlers.ts / qrep.tsx / helpers
- [ ] EditMirror / columnDisplayModal / tablePairs
- [ ] eventhubsCallout text

## Phase 7 — Tests
- [ ] A: existing e2e suites updated & passing
- [ ] B: dotted-name e2e (generic matrix, schema changes, add/remove, resync, qrep watermark, mongo collection, queue dests, api validation, snapshot-only)
- [ ] C: backward-compat (normalization units, old-bytes fixtures, raw-table continuity, migration tests)
- [ ] D: per-component units (identifiers, SF golden, CH, PG, BQ matrix, eventhub, pua, schema_helpers, validation)

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
- (empty)
