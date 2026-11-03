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

**Why it is tricky.** `CDCFlowWorkflow` in `flow/workflows/cdc_flow.go` runs one CDC mirror and carries two
copies of its configuration across every `ContinueAsNew`: `cfg *protos.FlowConnectionConfigsCore`, the config
the mirror was created with, and `state *cdc_state.CDCFlowWorkflowState`, whose `SyncFlowOptions` holds the live
table mappings, batch size and idle timeout. A `CDCFlowConfigUpdate` signal, sent by the `FlowStateChange` API
when a user adds tables or changes settings, writes table mappings, batch size, idle timeout and snapshot
settings into `state`, and only env overrides and the BigQuery query-CDC config into `cfg`. The workflow then writes a merged snapshot
to `flows.config_proto` in the catalog. The running workflow does not read it back; it becomes the starting
config only when a new workflow is created from the mirror, on resync by recreation or after a cancelled table
addition. Every other `cfg` field keeps its creation-time value until a resync. Activities such as `SyncFlow` receive both, code reads either copy, and tests that never change a
mirror after creation pass either way.

**What to watch for in a PR.**

- An activity reading table mappings, batch size or idle timeout from the config argument instead of `SyncFlowOptions`.
- A new field on `CDCFlowConfigUpdate` with no place in state and in the catalog merge.
- A per-table or per-column setting tested through mirror creation only. Adding a table to a running mirror
  takes different paths for initial load and CDC, and only CDC sees the stale config.

---

## Per-mirror behaviour: versions, flags, settings

**Why it is tricky.** `InternalVersion_*` constants pin behaviour per mirror. The version is set at creation from
`PEERDB_FORCE_INTERNAL_VERSION`, which defaults to the latest, and set again only when a completed or failed
mirror is resynced by recreation; a resync of a running mirror keeps it. Flags record destination capabilities at creation and resync keeps them. Dynamic settings are read
whenever code asks. A decision about what a mirror writes has to come from the mechanism with the right
lifetime; taking it from a run-time setting instead of a stored version or flag can change the output of a
mirror that is already running.

**What to watch for in a PR.**

- A change to what gets written for a column type that existing mirrors already use, for example a different
  JSON shape or encoding. It needs a new `InternalVersion_*` constant so older mirrors keep the old output, and a
  test that creates a mirror at the previous version and checks the old output is unchanged.
- Flags re-detected outside creation, or a flag that stores a decision rather than a capability. Resync must fix a
  mirror without changing compatibility.

