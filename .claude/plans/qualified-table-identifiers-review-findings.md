# QualifiedTable refactor — adversarial review findings & tracking

Source: ultracode adversarial review (79 agents, 15 finder slices, 2 adversarial
verifiers per medium+ finding) of branch `qualified-table-identifiers` at 83e08a19
vs merge-base 0709374b, against `.claude/plans/qualified-table-identifiers.md`.

Findings deduplicated across finder slices (28 confirmed raw → 14 distinct bugs,
3 disputed, 15 low). Status legend: [ ] open · [~] in progress · [x] fixed+verified ·
[D] disproved/accepted with rationale.

## Confirmed bugs

- [x] **F1 (high) InitialLoadSummary drops table namespaces** — `flow/cmd/mirror_status.go`.
  V56 split `qrep_runs.source_table/destination_table` into namespace+table columns and
  `InitializeQRepRun` writes only the table component, but `InitialLoadSummary` selected
  the bare columns verbatim into `CloneTableSummary.TableName/SourceTable` (plan 0.4
  locks these as LegacyDotted display strings; UI snapshot page would show `users`
  instead of `public.users`). **Fix**: query selects the namespace columns and composes
  LegacyDotted in Go, matching CDCTableTotalCounts. Verified via TestApiPg e2e + manual
  HTTP check (see test log).

- [x] **F2 (medium) RemoveTablesFromRawTable missing entry normalization** —
  `flow/activities/flowable.go`. Siblings normalize; this one built `DestinationTables`
  from nil structs for legacy payloads (non-nil slice of nils that
  `NormalizeRemoveTablesFromRawTableInput` can't repair) ⇒ raw-table cleanup silently
  deleted nothing. **Fix**: `internal.NormalizeTableMappings(tablesToRemove)` at entry.

- [x] **F3 (medium) QRepHasNewRows missing NormalizeQRepConfig** — only QRep activity
  without it; legacy-only config reached `getMinMaxValues` with nil watermark ⇒
  incremental QRep stalls. **Fix**: normalize config + last partition at entry.

- [x] **F4 (medium) SetupQRepMetadataTables missing NormalizeQRepConfig** — SF/BQ read
  only struct `DestinationTable`; overwrite-mode TRUNCATE would target `"".""`.
  **Fix**: normalize at entry.

- [x] **F5 (medium) QRepWaitForNewRowsWorkflow missing entry normalization** — only
  workflow in qrep_flow.go without it, and it self-CANs the same legacy config forever.
  **Fix**: `internal.NormalizeQRepConfig(config)` at entry (pure, replay-safe).

- [x] **F6 (critical) Elasticsearch index uses `.Table` — legacy dotted index rerouted** —
  pre-refactor the index was the FULL destination string; first-dot-split of legacy
  `logs.v1` ⇒ writes to index `v1`: silent data redirection. **Fix**: index name (and
  bulk-indexer cache key) now `LegacyDotted()` in both CDC SyncRecords and QRep —
  identical to `.Table` when namespace empty, byte-reproduces pre-refactor name for
  legacy configs; matches kafka/pubsub.

- [x] **F7 (high) ClickHouse physical table name used `.Table` — legacy dotted CH
  destination retargeted** — a pre-refactor CH destination `dst.table` (single-part,
  dots legal — the plan's own headline regression case) arrives first-dot-split and the
  connector targeted bare `table`. The dotted e2e passed only because tests send struct
  `{"", dotted}`. **Fix**: all physical-name sites now use `LegacyDotted()` (no-op for
  valid new configs where validation enforces namespace==""): SetupNormalizedTable
  exists-check + CREATE, normalize INSERT INTO, schema-delta ALTERs, distributed shard
  lookup, resync EXCHANGE/RENAME/DROP, qrep avro INSERT/staging, object_sync. Unit test
  `TestBuildQuery_LegacyNormalizedDottedDestination` pins `{dst,table}` ⇒ `` `dst.table` ``.
  Note: this also resolves the related findings "QRep CH namespaced destination silently
  drops namespace" and "add-tables bypasses CH namespace check" — with LegacyDotted a
  namespaced destination reproduces exactly the pre-refactor dotted behavior instead of
  silently dropping the namespace; CDC create-path namespace rejection stays as the
  plan-mandated strictness for new configs.

- [x] **F8 (high) Add-tables signal path never validated against EXISTING tables** —
  plan 0.4 requires LegacyDotted uniqueness across the FLOW's destinations; additions
  were only checked within the batch ⇒ existing `{a.b,c}` + added `{a,"b.c"}` merge rows
  in the raw table. **Fix**: FlowStateChange loads the catalog config and validates the
  union (existing + additions). E2e cases added to TestDottedTableAddition (duplicate of
  existing destination, dotted collision with existing destination).

- [x] **F9 (medium) SkipValidation bypassed structural identifier validation** —
  structural invariants (dups/collisions/empties) are not environmental checks. **Fix**:
  CreateCDCFlow runs `validateTableMappingIdentifiers` even when SkipValidation is set.
  E2e: TestMirrorValidation_StructuralChecksDespiteSkipValidation.

- [x] **F10 (medium) Source LegacyDotted collisions not validated** — two distinct
  sources `{a,"b.c"}`/`{"a.b",c}` produce identical snapshot clone child-workflow IDs.
  **Fix**: sourcesDotted collision check mirroring destinations. E2e case "source dotted
  collision pair" added.

- [x] **F11 (medium) CancelTableAddition response lacked denormalization** — plan 0.7
  requires outbound responses to carry both forms. **Fix**:
  `internal.DenormalizeTableMappings(output.GetTablesAfterCancellation())` in handler.

- [x] **F12 (medium) No legacy fallback when consuming activity/child-workflow OUTPUTS
  recorded by old bits** — worker restart mid-setup across a deploy replays old-format
  `EnsurePullabilityBatchOutput`/`SetupFlowOutput`/`GetDefaultPartitionKeyForTablesOutput`
  ⇒ empty SrcTableIdMapping ⇒ silent CDC record skipping. **Fix**: fallbacks reading the
  legacy map fields (via `common.NormalizeTableIdentifier`) in setup_flow.go
  ensurePullability, cdc_flow.go SetupFlowOutput consumption, snapshot_flow.go partition
  keys.

- [x] **F13 (medium) Continue-As-New payloads carried struct-only identifiers, defeating
  the rollback dual-write** — catalog config_proto keeps both forms but post-CAN Temporal
  inputs had cleared legacy strings + nil SrcTableIdNameMapping; rollback to release N
  would break running mirrors. **Fix**: new `flow/workflows/continue_as_new.go` wrappers
  denormalize cfg/state (incl. refilling SrcTableIdNameMapping via new
  `DenormalizeSyncFlowOptions`/`DenormalizeDropFlowInput`) before every CAN in
  cdc/qrep/xmin/drop workflows; entry normalization (struct wins) makes it a no-op for
  current code. Unit round-trip tests added.

- [x] **F14 (medium) UI: QRep destination parsed with stale/unknown peer type** — free
  text parsed per keystroke with the then-selected destination type; never re-parsed.
  **Fix**: `handleCreateQRep` re-parses `displayQualifiedTable(destinationTable)` with
  the final peer type at submit (display∘parse is identity on the typed text).

## Low severity

- [x] **L1** `validateQRepIdentifiers` now implements the watermark-table check its doc
  comment promised (required when a watermark column is configured).
- [x] **L2** ResetMirrorSequences: empty-namespace destinations now rendered as
  `QuoteIdentifier(table)` (search_path resolution) instead of invalid `""."t"` regclass.
- [x] **L3** `LogActivityUpdateFlowConfig` reads structs (`.String()`) instead of cleared
  legacy `SourceTableIdentifier` (telemetry logged empty names).
- [x] **L4** UI EditMirror table removal: removals now reuse the CANONICAL TableMapping
  from the mirror config instead of re-parsing display text (legacy dotted destinations
  re-parsed differently ⇒ RemoveTablesFromCatalog deleted nothing).
- [x] **L5** NewScopedEventhub rejects packed tables with extra dots
  (`ns.hub.pk.extra`) — main rejected ≠3 parts; the Cut-based parse silently accepted
  and misrouted. Test case added.
- [D] **L6** Leading-dot legacy identifiers (`".t"`) lose the dot through
  Normalize∘LegacyDotted. Accepted: matches the catalog backfill (`split_part` yields
  namespace='' table='t'), and a leading-dot identifier was never producible by
  supported sources/UI.
- [D] **L7** Backfill idempotency: V54-V56 guards (`position('.' in x) > 0 AND
  namespace = ''`) make a second run a no-op for every row the backfill itself produced
  (split rows get namespace≠''; remainder-with-dots rows are skipped via the namespace
  guard). The only re-split hazard is a namespace='' dotted row written by NEW code
  (table-only CH/ES destination with a dot) — impossible before the migration has
  already run, and Flyway never re-runs. Multi-dot split (`a.b.c` ⇒ `a` + `b.c`)
  verified equal to Go `NormalizeTableIdentifier` semantics.

## Test gaps

- [x] **T1** pua (Lua) legacy-dotted test written
  (`TestRecordTargetSourceLegacyDotted`): dotted source/destination ⇒ `record.target`/
  `record.source` return legacy-dotted, never quoted `String()`. (Progress doc falsely
  claimed this existed — corrected.)
- [x] **T2** Snowflake golden MERGE SQL test (`TestGenerateMergeStmtGolden`): the
  dotless fixture was captured by RUNNING THE PRE-REFACTOR GENERATOR at merge-base
  0709374b in a worktree with identical inputs and verified byte-identical to the
  current generator before embedding; dotted case asserts per-component quoting.
  Covers plan D/Risk 4 given SF e2e suites are skipped upstream.
- [x] **T3** CH unit test for the legacy-normalized `{dst,table}` physical-name form
  (`TestBuildQuery_LegacyNormalizedDottedDestination`).
- [D] **T4** Queue-destination dotted e2e (Kafka topic/PubSub topic/ES index — plan B.7)
  not written: no local Kafka/PubSub/ES infra in the Tilt env. ES routing is fixed (F6)
  and kafka/pubsub already used LegacyDotted; gap documented in the progress doc for CI
  follow-up.
- [D] **T5** Plan C.4 catalog migration backfill Go tests not written: a test
  re-executing the migration SQL against fixture tables would duplicate the SQL rather
  than exercise the shipped file, and the dev catalog already ran V54-V57 organically
  over 30 real rows (verified in the progress work log). SQL semantics re-audited
  against Go normalizer this review (see L7). Deviation documented.
- [x] **T6** Validation tests for F8/F9/F10 added (e2e: add-tables union rejections,
  skip-validation structural check, source collision pair).
- [x] **T7** Schema-helpers dotted-overlap unit tests added
  (`flow/internal/schema_helpers_test.go`): struct-exact overlap vs dotted-string
  collision, destination overlap gating, BuildProcessedSchemaMapping dotted keys +
  exclusion.

## Disputed → resolution

- [D] **D1** V54/V55 PK swap vs old pods during rollout: maintenance pause IS the deploy
  model (old pods drained before migrations); a compat unique index on the old
  single-column key is impossible after the split. Prerequisite now documented in a V54
  comment. (Old-key `ON CONFLICT (flow_name, table_name)` from a live old pod would fail
  loudly with 42P10, not corrupt.)
- D2 = T2 (done). D3 = T5 (accepted, documented).

## Residual gaps (post-review doc-vs-reality audit, 2026-06-12)

Re-audit of every claim in the plan/progress docs and the finder coverage notes after
the fixes above. All actionable items now closed (second pass, same day):

- [x] **Plan 1.3 "ParseTableIdentifier deleted at end of refactor"** — was NOT done
  (one caller left in `flow/e2e/pg.go`). Caller migrated to
  `common.NormalizeTableIdentifier`, function deleted.
- [x] **Plan 7.D PG QRep dotted unit case** — added
  `TestCTIDPartitioningOnDottedPartitionedTable` (dotted schema + dotted parent +
  dotted children; ChildTableRange structs + full child coverage), passes against
  live PG.
- [x] **Plan 7.B.5 dotted partitioned-parent ctid QRep e2e** — added
  `Test_Dotted_Partitioned_Parent_QRep` (PG suite): dotted partitioned parent, ctid
  partitioning with NumRowsPerPartition=10 over 60 rows forcing multi-partition
  child-range pulls, SetupWatermarkTableOnDestination creating the dotted destination.
  PASSED on Tilt. (First version timed out — child-range pulls resolve the destination
  schema from table_schema_mapping keyed by ParentMirrorName, which the handler
  normally sets; test now sets it + the watermark-setup flag like real flows do.)
- [x] **Plan 7.B.9 InitialSnapshotOnly dotted e2e** — added
  `TestInitialSnapshotOnlyWithDottedNames` (API suite, PG→CH, dotted source + dotted CH
  destination, waits COMPLETED, compares rows). PASSED on Tilt. **Risk 9** now also
  pinned by `TestSnapshotCloneWorkflowIDMatchesLegacyFormat`: clone child workflow ID
  extraction (`snapshotCloneWorkflowID`) asserted byte-identical to the pre-refactor
  `clone_<flow>_<dotted source>_<runID>` format for dotted/dotless/table-only sources.
- [x] **Plan 7.C.3 raw-name round-trip units** —
  BQ: `TestGenerateFlattenedCTERawNameFilterIsLegacyDotted` pins the raw-table filter
  literal to LegacyDotted (dotless + dotted). SF: golden dotted subtest now also
  asserts the LegacyDotted-keyed unchangedToastColumnsMap lookup hits (a String()/.Table
  key would drop the toast UPDATE branch). PG/SF raw reads are runtime bind parameters
  (not generator-visible); they are covered by the passing dotted e2e (PG destination)
  and were refuter-verified consistent (see Refuted).
- [x] **Normalize\* helper unit tests** — added for NormalizeDropFlowInput,
  NormalizeCreateRawTableInput (incl. sorted-determinism + idempotence),
  NormalizeTableSchemaDeltas, and a NormalizeFlowConfigAPI ⇄ DenormalizeFlowConfigForAPI
  lossless round-trip.
- [x] **SF compile-only dotted branch** — namespace now suffixed
  (`"E2E.DOT_" + suffix`), no cross-run collision when the SF suite is re-enabled.
- [D] **Plan decision 0.5 enforcement**: Namespace=="" validation exists only for
  ClickHouse, not Kafka/PubSub/ES/S3. Deliberate: those connectors derive physical
  names via LegacyDotted, so a namespaced destination reproduces pre-refactor behavior
  exactly; rejecting would break legacy-client parity (this missing guard is what made
  the ES bug F6 silent — fixed at the connector instead).
- [D] **Phase 7.E.2 (pending un-normalized batches across upgrade)** remains "partial"
  per the progress log: the live test covered a running mirror's raw-table round-trip,
  not a held-back normalize. Static trace + refuter verification of the
  writer/reader string pairing is the compensating evidence.

## Refuted by verifiers (no action)

- Raw-table continuity: writer (`flow/connectors/utils/stream.go:147`) and all four
  destination readers verified to agree on LegacyDotted end-to-end by two independent
  verifiers; missing helper centralization is style, not a bug.

## Tilt test plan

1. [x] `go build ./...`, golangci-lint (0 issues), unit tests for changed packages
   (internal, pua, snowflake, clickhouse, eventhub, workflows, pkg/common), UI tsc+lint.
2. [x] Tilt rebuild of flow-api/flow-worker from working tree; wait Ready.
3. [x] `e2e_postgres` (TestGenericCH_PG) — PASSED incl. Test_Dotted_Names_CDC.
4. [x] `e2e_api-postgres` (TestApiPg) — PASSED incl. the new
   TestMirrorValidation_StructuralChecksDespiteSkipValidation, the
   source_dotted_collision_pair case, and TestDottedTableAddition's two new
   add-tables rejection subtests (duplicate of existing destination, dotted collision
   with existing destination).
5. [x] `connector_clickhouse` — PASSED incl. TestBuildQuery_LegacyNormalizedDottedDestination.
6. [x] Manual F7/F1 verification — see Manual test log below.

## Manual test log

- 2026-06-12: Rebuilt flow-api / flow-worker / flow-snapshot-worker on Tilt from the
  fixed working tree (all `ok`, containers restarted).
- 2026-06-12: `e2e_postgres` (TestGenericCH_PG): **PASSED** (all subtests incl.
  Test_Dotted_Names_CDC). `connector_clickhouse`: **PASSED** (incl. new
  TestBuildQuery_LegacyNormalizedDottedDestination).
- 2026-06-12: **F7 live verification (legacy dotted CH destination)**: created
  `public.legacy_dot_src` on the ancillary PG (port 5436); created CDC mirror
  `legacy_dot_manual` via the HTTP gateway (`POST /v1/flows/cdc/create`) with ONLY
  legacy string identifiers — `sourceTableIdentifier: "public.legacy_dot_src"`,
  `destinationTableIdentifier: "dot.legacy_dst"`, `skipValidation: true` (simulating a
  pre-refactor persisted config: boundary normalization splits it to {dot, legacy_dst}).
  Result: physical CH table created as a SINGLE table named `dot.legacy_dst` (not
  `legacy_dst`); snapshot row landed; CDC insert landed (2 rows total); raw table
  `_peerdb_destination_table_name` = `dot.legacy_dst` — write side and normalize side
  agree on LegacyDotted. Mirror terminated and source table dropped after the test.
- 2026-06-12: **F1 live verification**: `GET /v1/mirrors/cdc/initial_load/legacy_dot_manual`
  returned `sourceTable: "public.legacy_dot_src"` and `tableName: "dot.legacy_dst"` —
  namespaces recomposed from the V56 split columns (catalog row:
  `public|legacy_dot_src|dot|legacy_dst`). Pre-fix this returned bare `legacy_dot_src`/
  `legacy_dst`.
- 2026-06-12: T2 golden capture: ran the PRE-refactor SF merge generator at merge-base
  0709374b in a temporary git worktree (`/tmp/peerdb-mb`, since removed) with inputs
  identical to TestGenerateMergeStmtGolden; output compared byte-for-byte against the
  refactored generator (IDENTICAL) before embedding as the test fixture.
- 2026-06-12: Merged main (#4414 shared-destination/N:1 support) — removal logic and
  raw-table cleanup ported to QualifiedTable structs; validation now permits exact
  duplicate destinations (N:1) while still rejecting dotted collisions between
  different destinations; duplicate-destination e2e rejection cases dropped.
  Test_Removal_Shared_Destination passes on the branch locally.
- 2026-06-12: Draft PR #4416 opened. First full CI matrix run caught ONE real failure:
  Test_Dynamic_Mirror_Config_Via_Signals asserted the legacy SrcTableIdNameMapping in
  workflow state (cleared by entry normalization; missed in the suite conversion —
  the PG→PG suite doesn't run on the local dev stack). Fixed to assert
  SrcTableIdMapping; verified locally; CI fully green on 542578dd (all matrix legs,
  lint, CodeQL, clippy, UI).
