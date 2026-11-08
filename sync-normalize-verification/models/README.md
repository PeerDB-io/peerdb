# Baseline sync/normalize model scope

These specs describe baseline main `2511921646b7200f442f83feafae44dce66f3f64`, with fault-injection hooks disabled. CDC destination-specific claims cover PostgreSQL and ClickHouse only. Normalize-less destinations are excluded.

- `core/CDC.tla` checks generic coordination and worker lifetime. Readiness and PostgreSQL transaction claims belong to `destinations/CDCDestinations.tla`.
- `CDCDestinations` models PostgreSQL merges and the normalize checkpoint committing atomically. Pending merges roll back on failure/restart. ClickHouse applies tables separately before the global checkpoint. Its group-size-1 and group-size-2 configurations include completion liveness with three batches.
- Worker models allow a producer failure before all tables are dispatched, worker failure, and a joined error return. A retry can follow either an early return with children or a joined return without children.
- `QueryCDC` models per-table progress. Its separate `TableSpec` checks table isolation, releasing the slot before the catalog commit, and a fatal catalog error canceling all modeled tables. The error configuration enables this path; the ordinary table configuration checks uninterrupted completion.
- `QuerySlots` models both retryable errors and fatal errors that cancel all tables, while in-flight work releases its slot. It abstracts away progress values and does not compose the sync and normalize semaphores or every top-level loop with the per-table protocol. A capacity-one check is not a proof for every capacity. Eventual admission is an explicit fairness assumption, not a claim about guarantees in the Go language specification.
- `raw` in the destination protocol is a record of successful durable staging, not a list of currently retained raw rows. Required unnormalized input must remain available through recovery. Cleanup of already normalized rows is abstracted. Schema-only polls without a data batch are outside this data-batch protocol.
- Startup seeding abstracts the production conditional republish by seeding the saved counter; comparisons against completed progress determine whether it requests work.

The default ClickHouse parallel-normalize setting is 0, clamped to one worker. The pinned clickhouse-go v2.50.0 pool `Close` drains idle connections; an active borrowed connection may finish and then write its catalog checkpoint. The late-checkpoint witness does not require parallelism greater than one.

## Expected baseline failures

These are intended detections of baseline behavior, not failing model tests to suppress. All other enabled assertions are expected to pass. TLC normally stops on the first violation; isolate each listed assertion and check the remainder to establish every outcome.

| Configuration family | Expected failing assertions |
|---|---|
| `core/CDC-liveness` | `NoStrandedNormalize`, `NoStrandedSync`, `AllPublishedComplete` |
| `core/CDC-workers` | `WorkersJoinedBeforeReturn`, `RetryHasNoPriorWorkers` |
| Destination `*-liveness` | `NoStrandedNormalize`, `NoStrandedSync`, `AllPublishedComplete` |
| Destination `*-contracts` | `AcknowledgmentRequiresReadySync` |
| Destination `postgres-readiness` | `OnlyCatalogReadyWork`, `OnlyDestinationReadyWork`, `DestinationReadyBeforeSavedCheckpoint` |
| Destination `clickhouse-readiness` | `OnlyCatalogReadyWork` |
| Destination `clickhouse-group-*` | `OnlyCatalogReadyWork`, `AcknowledgmentRequiresReadySync`, `NoStrandedNormalize`, `NoStrandedSync`, `AllPublishedComplete` |
| `core/QueryCDC-liveness` | `NoRequestRegression`, `NoStrandedNormalize`, `AllCommittedComplete` |
| `core/QueryCDC-eventual` | `AllCommittedComplete` |
| `core/QueryCDC-visibility` | `CommittedProgressEventuallyVisible` |
| `core/Coordinator` | `NoStrandedWaiter`, `EventuallyResumes` |
| `core/Coordinator-stale` | `NoRegression` |
| `core/Coordinator-close` | `CloseIsPermanent` |
| `focused/ClickHouseMetadata*` | `DurableProgressMonotone`, `DurableWriteArgumentsOrdered` |

`Coordinator-subscribe-first` checks the caller ordering that captures a channel before checking its condition. Converter-child lifetime is separate from these INSERT-worker and top-level models; the bounded checks here do not cover all connector children.

## Query-CDC sync wakeup coverage

Requirement 9 is explicitly checked by `NoStrandedSync` and `CompletionReleasesSync` in `QueryCDC.tla`. The invariant excludes a non-canceled sync loop sleeping on an unclosed channel with room below the pending-batch limit while normalize is asleep. The temporal property requires sync to leave its wait (or observe cancellation) once durable normalization frees capacity; it allows the intervening completion-publication steps.

Both checks are enabled in the normal, larger, and liveness configurations. All pass. The existing request-regression, normalize-stranding, and completion-liveness failures remain. A scratch negative control that moves sync's channel capture after its pressure check fails both new checks. Eight TLC runs used 14 workers and a 2 GiB heap, with raw evidence in `/private/tmp/peerdb-query-sync-liveness/` and the repeatable runner at `/private/tmp/check-query-sync-liveness.py`.
