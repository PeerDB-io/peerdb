# Plan 26 — exec: batch-level hang watchdog

## Problem

`internal/exec/exec.go` `runOne` pays, per fuzz case: one goroutine spawn, one
`make(chan Result, 1)`, one `time.NewTimer` (runtime timer-heap insert/remove), and a
channel handoff whose send must unpark a parked OS thread. A 20s CPU profile of a hot
campaign (~5.4 cores) shows ~60% of samples in the scheduler wakeup path
(`send.goready → ready → wakep → pthread_cond_signal`) and only ~30% in actual parsing.
The per-case allocs also feed GC pressure. Parallelism already lives at the
oracle-proc level (`fuzzloop.go` spawns one goroutine per proc, each calling
`RunBatch` sequentially), so the per-case goroutine exists *only* for hang isolation —
panics are already contained in-goroutine by `callParser`'s `recover`.

Hangs are rare-path. Amortize isolation to the batch; pay per-case cost only when a
hang actually happens.

## Design

`Worker` owns one long-lived parser goroutine, handed work per **batch**:

- Spawn state (a "generation"): `reqCh chan []run.Case` (unbuffered),
  `resCh chan []Result` (cap 1, so an abandoned goroutine can deliver late and exit
  instead of leaking blocked), and a shared `progress *atomic.Int64`.
- Parser goroutine loop: receive a batch, for each case do `progress.Store(i)` then
  `callParser` (existing per-case `recover` stays), send the `[]Result` back, wait for
  the next batch. Exit when `reqCh` closes (add `Worker.Close`; callers are long-lived
  so wiring Close is only needed for tests).
- `RunBatch`: send batch on `reqCh`, then `select` on `resCh` and a watchdog
  `time.Ticker` (one per worker, created lazily, period `deadline/2`, drained of stale
  ticks before each batch). On each tick compare `progress` to the last observed
  value: if the same case index has been current for ≥ `deadline`, declare it hung —
  mark `out[i] = Result{Panic: &PanicInfo{Value: "ddlfuzz parser timeout", Timeout: true}}`,
  abandon the generation (drop the channel refs; the stuck goroutine delivers into its
  own cap-1 `resCh` whenever it unsticks, then exits), spawn a fresh generation, and
  re-send `batch[i+1:]` to finish the batch. Multiple hangs in one batch just repeat
  this.
- Timing note: hang detection is now tick-granular — a case is flagged somewhere in
  `[deadline, deadline + tick]` rather than at exactly `deadline`. That's fine for a
  100ms hang deadline; do NOT add per-case `time.Now` calls to sharpen it.

Per-case hot-path cost after this: one atomic store. Per-batch: two channel handoffs
plus a few ticker wakeups. Zero per-case allocs.

## Non-goals

- The abandoned-goroutine leak on a true infinite loop is unchanged from today.
- The per-case sancov drain on the oracle side is a separate ceiling; not touched here.

## Files

- `tools/ddlfuzz/internal/exec/exec.go` — the rework. Public surface (`NewWorker`,
  `RunBatch`, `Result`, `PanicInfo`, `ParserFunc`) stays identical apart from the new
  `Close`.
- `tools/ddlfuzz/internal/exec/exec_test.go` — existing two tests must pass unchanged.
  Add: (1) batch resumes after a hung case — cases after the timeout still produce real
  results; (2) two hangs in one batch; (3) goroutine-count stability across many
  batches (`runtime.NumGoroutine` before/after, with slack); (4)
  `BenchmarkRunBatch` with a trivial parser reporting allocs/op — expect ~0 per case.

## Acceptance

- `go test ./internal/exec/...` green from `tools/ddlfuzz`.
- `go vet ./internal/exec/...` clean.
- Benchmark shows per-case allocations eliminated (report the before/after numbers).
- `go build ./...` from `tools/ddlfuzz` still compiles (callers unchanged).
