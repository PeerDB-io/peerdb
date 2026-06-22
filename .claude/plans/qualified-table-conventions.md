# QualifiedTable refactor — conversion conventions (for all sub-tasks)

Goal: table identifiers are `common.QualifiedTable{Namespace, Table}` structs everywhere
internally; the legacy dotted `schema.table` strings only exist at the API boundary and
in raw-table data. The compiler drives the conversion: fix every error by moving TO
structs, never by re-deriving dotted strings (except rules 5/6 below).

1. NEVER split a string on "." to derive namespace+table, and NEVER concatenate
   namespace+"."+table to build an identifier. Identifiers come from struct fields.
   If a native source provides parts (e.g. binlog event schema+table, Mongo db+coll),
   build `common.QualifiedTable{Namespace: ..., Table: ...}` directly.
2. Proto TableMapping: use `.SourceTable` / `.DestinationTable` (type *protos.QualifiedTable),
   convert with `internal.QualifiedTableFromProto(...)` / `internal.QualifiedTableProto(...)`.
   The legacy fields `SourceTableIdentifier`/`DestinationTableIdentifier` are EMPTY at
   runtime (cleared by boundary normalization) — never read them.
3. QRepConfig: use `.QualifiedWatermarkTable` and `.DestinationTable`
   (legacy `.WatermarkTable`/`.DestinationTableIdentifier` are empty). TableSchema: use
   `.Table` (legacy `.TableIdentifier` empty). ChildTableRange: `.ChildTable`.
   SetupReplicationInput/CreateRawTableInput: `.QualifiedTableMappings` (legacy maps nil).
   EnsurePullabilityBatchInput: `.SourceTables`. SyncFlowOptions: `.SrcTableIdMapping`.
   RenameTableOption: `.CurrentTable`/`.NewTable`.
4. Go maps: keyed by `common.QualifiedTable` (comparable). model.Record interface:
   `GetDestinationTable()` / `GetSourceTable()` return common.QualifiedTable; record
   structs have `SourceTable`/`DestinationTable` fields.
5. `_peerdb_destination_table_name` raw-table values: ALWAYS `qt.LegacyDotted()` —
   both when writing (already done in connectors/utils/stream.go) and when building
   lookup maps in normalize code (`map[string]...` keyed by `qt.LegacyDotted()` is
   correct THERE ONLY, because the column holds rows from before this refactor too).
   Same for queue topic names defaulting from destination (kafka/pubsub: LegacyDotted).
6. Logs, error messages, metric/telemetry labels: `qt.String()` (quoted form) unless the
   message interpolates into a context where the old dotted form is required.
7. SQL generation: quote per component — Postgres/Snowflake-style: `qt.String()` uses
   QuoteIdentifier on both parts; MySQL: `qt.MySQL()`. For destinations with table-only
   names (ClickHouse, ES, queues): use `qt.Table` and quote appropriately; namespace is
   "" there. BigQuery: pass the QualifiedTable to `convertToDatasetTable` (handles
   project.dataset packed into Namespace and a legacy mid-flight `{p, "d.t"}` form by
   re-splitting Table at the first dot).
8. Snowflake: apply `SnowflakeIdentifierNormalize` per component, semantics for dotless
   identifiers must remain byte-identical to before.
9. Activity inputs that may arrive with only legacy strings (recorded by an old release)
   are normalized via the matching `internal.Normalize*(...)` call at the top of the
   activity (NOT inside connectors; connectors assume normalized inputs). Exception:
   connector methods receiving raw proto inputs directly as activity payloads
   (CreateRawTable, SetupReplication, EnsurePullability, RenameTables,
   RemoveTableEntriesFromRawTable) call the matching Normalize* defensively first.
10. Don't remove existing comments; match surrounding style; no comments that narrate
    the refactor (exception: a brief note where LegacyDotted is load-bearing, see
    existing examples in stream.go / pua/peerdb.go).
11. `common.NormalizeTableIdentifier(s)` exists for genuinely-legacy string inputs
    (catalog rows, raw-table values, Lua script inputs) — never for identifiers that
    are already structured.
12. After your changes: `cd /Users/ilia/Code/peerdb/flow && go build ./...` must show no
    errors in YOUR packages (other packages may still fail — only fix files assigned to
    you). Run `gofmt -w` on edited files. Keep `go vet` clean for your packages.
