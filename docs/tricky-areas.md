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
- Flags re-detected outside creation, or a flag that stores a decision rather than a capability.
