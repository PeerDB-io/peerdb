# Tricky areas

A map of the parts of the codebase that need extra care, so that people reviewing or changing them know where
to pay attention. Each area has a short explanation of why it is
tricky and what to watch for in a PR. If an area becomes simpler after a refactor, trim or remove its section in
the same PR.

Architecture lives in [`peerdb-architecture.md`](peerdb-architecture.md) and
[`deep-dive-design-document.md`](deep-dive-design-document.md); this document points into them instead of
repeating them.

| Area | Paths |
|---|---|
| [Mirror config vs workflow state](#mirror-config-vs-workflow-state) | `flow/workflows/`, `flow/activities/flowable.go`, `flow/activities/flowable_core.go`, `protos/flow.proto` |
| [Per-mirror behaviour: versions, flags, settings](#per-mirror-behaviour-versions-flags-settings) | `flow/shared/constants.go`, `flow/internal/dynamicconf.go`, `flow/cmd/handler.go`, `flow/cmd/validate_mirror.go`, `flow/connectors/clickhouse/validate.go` |
| [Temporal signal handling](#temporal-signal-handling) | `flow/workflows/cdc_flow.go`, `flow/workflows/drop_flow.go`, `flow/model/signals.go` |
| [Schema changes](#schema-changes) | `flow/activities/flowable_core.go`, `flow/model/cdc_stream.go`, `flow/internal/catalog_schema.go`, `flow/connectors/postgres/cdc.go`, `flow/connectors/mysql/cdc.go`, `flow/connectors/clickhouse/` |

---

## Mirror config vs workflow state

**Why it is tricky.** `CDCFlowWorkflow` carries two copies of a mirror's configuration: `cfg`, the config it was
created with, and `state.SyncFlowOptions`, which holds the live table mappings, batch size and idle timeout. A
config-update signal writes those three into `state`, and only env overrides and the BigQuery query-CDC config
into `cfg`; nothing else in `cfg` changes until a resync. The catalog row `flows.config_proto` is a merged snapshot
for API readers, not an input to the running workflow. Activities such as `SyncFlow` receive both copies, so code
can read the stale one and still pass every test that never changes a mirror after creation.

**What to watch for in a PR.**

- An activity reading table mappings, batch size or idle timeout from `cfg` instead of `SyncFlowOptions`.
- A new field on `CDCFlowConfigUpdate` with no home in `state` and in `updateFlowConfigWithLatestSettings`.
- A per-table or per-column setting tested only through mirror creation. A table added to a running mirror is
  loaded with its own mappings but replicated with the parent's, so only CDC sees the stale copy.
- A new path that operates on config but doesn't sync it to catalog, or updates catalog but not the workflow state

---

## Per-mirror behaviour: versions, flags, settings

**Why it is tricky.** Three mechanisms pin what a mirror writes, each with a different lifetime. `Version` is set
from `PEERDB_FORCE_INTERNAL_VERSION` at creation and again only when a completed or failed mirror is resynced by
recreation. Flags from `GetFlags` record destination capabilities at creation and are never re-detected. A dynamic
setting resolves from the mirror's env, then the catalog, then the process environment, and a non-immediate value
is written back into the env map it was resolved with, so one pinned into `cfg.Env` at creation lasts the mirror's
lifetime while an activity's copy lasts one call. A decision taken from the mechanism with the wrong lifetime
changes the output of a running mirror.

**What to watch for in a PR.**

- A change to what is written for a column type that existing mirrors use, without a new `InternalVersion_*` gate
  and a test for a mirror at the previous version.
- Carefully thread all transformations the column value goes through, as it can change at different layers.
  Changes that are bugfixes (replacing nonsensical data with something more reasonable) are okay without a bump.
- Flags re-detected outside creation, or a flag that stores a decision rather than a capability.

---

## Temporal signal handling

**Why it is tricky.** Pause, resume, terminate and resync requests can arrive while a workflow waits for an
activity or child workflow. The workflow must continue to handle the requests that it supports in that phase.
An interrupting request needs the correct state, even if the current operation is only partly complete.
For this guidance, small catalog interactions such as status updates or logging do not count as long waits.

**What to watch for in a PR.**

- When you add or move a long-running activity or child workflow, check which requests need handling during the wait.
  For example, snapshot and table-edit operations handle terminate/resync requests while they wait for completion.
  A direct `.Get()` on new work can delay those requests until the work finishes.
- Before continuing as new, wait for cancellation to finish. Then process pending signals for the last time.
  For example, `startCDC` cancels `SyncFlow`, waits for it, then drains pending signals.
  Draining means processing the signals that are already waiting.
  Put new long waits before that drain. This order lets the workflow handle requests that arrive during the wait.
- If an operation changes state that may be used by termination or resync, check that state before the operation completes.
  For example, table edits prepare the intended mappings before they handle signals.
  Thus, an interrupting resync includes added tables and excludes removed tables.
- If you add work between a decision and its action, check whether signals can change that decision.
  For example, during resync cleanup, a terminate request means the workflow must fully remove the mirror instead of restarting it.
  `DropFlowWorkflow` checks for this request again and completes the necessary catalog cleanup.
- When you add state changes through signals, prevent pause/resume requests from overriding termination or resync.
  For example, `FlowSignalHandler` applies pause only to a running signal state. It applies resume only to a paused signal state.

---

## Schema changes

**Why it is tricky.** PeerDB passes schema changes from the source connector through Pull, Sync, catalog updates,
and Normalize. These stages overlap and keep different views of schema and progress. Receiving a change,
applying destination DDL, updating the catalog, and saving a checkpoint are separate steps. Review the complete
path when changing one of them.

**What to watch for in a PR.**

- When changing how schema changes are detected or passed between stages, follow them through to destination DDL
  and catalog updates. Check batches with no rows, excluded columns, and repeated changes. For example, MySQL
  can emit DDL without row changes to carry it through the usual batch path.
- When reading schema information, check which stage produced it and which records will use it. The latest
  source schema can be ahead of Pull. Sync can update the catalog while Normalize uses an earlier copy of the
  schema. Writing that copy back can erase Sync's changes. A refresh must not make an unprocessed change appear
  complete or overwrite another stage's changes.
- When moving a schema update or checkpoint, trace what happens if the next step fails. Check what the retry
  reads, repeats, and skips. For example, one table can finish Normalize and save its progress while another
  table in the batch fails. Any associated schema update must remain recoverable.
- When changing catalog schemas, apply the specific change to the current catalog state.
  `ReadModifyWriteTableSchemasToCatalog` provides this pattern. Pass the columns to add or remove, rather than
  a replacement schema captured earlier.
- When recovering from a schema mismatch, check whether recovery changes the replicated data. For example:
  When recovering from a missing destination column, do not silently stop replicating that column. Check whether
  it still exists in the source and whether the mirror configuration includes it. Apply the same care to types,
  nullability, and defaults, including their effect on existing rows.
