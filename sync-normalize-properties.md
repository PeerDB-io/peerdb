# Sync and normalize: properties to check

This checklist applies to both the baseline and the revised coordination code. These are requirements to check, not claims that either version already satisfies them.

## Assumptions

- Only one active sync/normalize worker pair owns a table, including across restarts. CDC shares one pair across the mirror.
- Nothing outside that pair changes its progress while it runs.
- Catalog writes are atomic individually. A catalog write and a notification are separate steps.
- For promises about eventual completion, workers keep getting scheduled, external operations eventually return and succeed, and there are only finitely many crashes. After a crash, the worker eventually restarts.
- A permanent shutdown need not finish pending work. It must preserve the information needed to resume later.
- Batch order is not row order. Tables may do work in parallel, and several batches may be processed together.

## Completion and order

1. **Every published batch eventually completes.** This must hold even when the source produces no more data.
2. **Acknowledgment never skips a batch.** Acknowledging batch 20 means every batch through 20 is fully normalized.
3. **Every affected table must finish.** Success for one table cannot acknowledge unfinished work for another table in the same batch.
4. **Targets are not completions.** Starting work on a target, or storing that target for heartbeats, must not count as finishing it.
5. **Normalization uses only ready work.** Staging, pull completion, and required schema updates must be finished before the work is made available.
6. **Progress never goes backward.** Duplicate or older updates cannot lower progress or erase a newer request. Persisted and in-memory values may differ temporarily.
7. **Coalescing loses no work.** Picking target 20 after target 10 still includes all unfinished batches in between. It does not require one notification per batch.

## Waiting and backpressure

8. **No request is lost while normalize begins waiting.** Publishing during the check/subscribe gap cannot leave it asleep with pending work.
9. **No completion is lost while sync begins waiting.** The final completion must release sync even when normalize has no more work to do.
10. **Use the exact buffer boundary.** Sync may proceed when `synced - normalized < maxPendingBatches`; at equality it must wait at the existing check point.
11. **Keep the existing check placement.** CDC checks after publishing a batch. Query-CDC checks before beginning another batch. This is not a blanket limit on every intermediate state, especially at startup.
12. **Partial completion can release backpressure.** When a connector returns a successful partial range, its reported completion can release sync before the whole target finishes. ClickHouse can return batch groups; PostgreSQL and Query-CDC report only after their captured range finishes. Internal durable commits alone are not completion reports.
13. **No-op updates cannot strand waiters.** Repeated progress reports must not make a waiting worker miss real progress.

## Failures and retries

14. **Failure cannot invent completion.** A failed operation does not acknowledge unfinished work. A later bookkeeping error does not undo work already completed.
15. **Partial success survives failure.** A call may apply or persist some batches and then fail. Retry must preserve that work and finish the remainder.
16. **Retry can include newer work.** After backoff, normalize can choose the latest target without skipping the earlier unfinished range.
17. **Failed pull or staging does not publish a partial batch.** Query-CDC runs these operations concurrently; both must succeed before publication.
18. **Empty polls create no batch obligation.** Query-CDC may advance its source cursor without advancing the synced batch ID.
19. **Missing staged data is not proof of success by itself.** Skipping a missing stage is safe only if that batch was already applied. Published work must retain a usable path to completion.
20. **Retries do not imply exactly-once effects.** Destination work can succeed before its completion write fails. The coordinator must allow recovery; connector replay safety is a separate obligation, not something two counters prove.
21. **Successful calls eventually advance completion.** If work remains, repeated successful calls must eventually finish it rather than spin forever reporting no progress.

## Persistence and restart

22. **Startup recovers all committed pending work.** No new source event is needed to restart normalization.
23. **Initialization cannot overwrite live progress.** Loading older startup state must not erase a newer sync publication.
24. **A commit/publication gap is recoverable.** If execution continues, committed progress is eventually published. If it crashes, startup reloads that progress.
25. **Restart preserves readiness requirements.** Persisted sync progress alone must not let normalization bypass unfinished schema or staging work.
26. **Use the right persistent store.** CDC PostgreSQL progress lives at the destination; other CDC paths use catalog metadata; query-CDC uses per-table catalog state.
27. **Completion may be ahead of its notification.** A destination or catalog commit can survive even when the caller returns an error or never reports it in memory. Recovery must tolerate that difference.

## Shutdown and ownership

28. **Cancellation releases both kinds of waiter.** This includes a waiter whose condition was already satisfied when cancellation arrived.
29. **Workers eventually stop and are joined.** Ready work and retry loops cannot keep shutdown running forever, assuming external calls return and honor cancellation. This includes converter children created by staging, not only top-level loops and INSERT workers.
30. **Closing a notification mechanism is permanent.** For the existing `LastChan`, a late update should not silently reopen a closed channel. Context cancellation and joining must be considered separately from channel closure.
31. **There is only one normalize operation at a time per worker pair.** Retries and notifications cannot launch overlapping normalization for the same pair.
32. **Tables keep separate progress in query-CDC.** One table's completion cannot acknowledge another table's work. Their shared semaphores are resource limits, not shared batch progress. A claim that every table eventually proceeds also requires fair access to those resources.

## Explicit acknowledgment, admission, and durable-write checks

33. **Normalize only acknowledges sync-ready batches.** Every batch covered by a completion report must already have finished sync, staging, and required schema preparation. This applies to both published and recovered progress.
34. **A completion cannot exceed its request.** A normalize call asked to finish through batch 10 cannot report batch 11. A retry may accept a newer target; its response is bounded by that attempt's target.
35. **Backpressure stops further pulls at the limit.** Once the pending count reaches the limit at the backpressure check, sync cannot begin another batch until normalization makes room. CDC checks after publication, so the first batch of a startup or reconstructed attempt can precede that check. Query-CDC checks before pulling. This is not an unconditional bound on every intermediate pending count.
36. **Waiting and retrying tables release shared slots.** Waiting for work, backpressure, the next poll, or retry backoff must not hold a sync or normalize slot. A table waiting to acquire a slot does not already own it. Active database I/O, connector cleanup, and joining work may legitimately retain a slot until the operation exits.
37. **The singleton worker never writes older durable progress.** For each durable sync, normalize, raw-copy, and per-table normalize checkpoint, the next submitted value must be at least the previously committed value, including after partial success, retries, and restarts. There are no competing writers in this property. It checks caller ordering, not whether SQL independently rejects stale writes with `GREATEST`. The documented empty-poll zero sentinel is not a checkpoint advancement: it must leave the saved checkpoint unchanged.
