package activities

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

type unnormalizedResult struct {
	err     error
	batchID int64
}

func startUnnormalizedWait(ctx context.Context, p *syncNormalizeProgress) <-chan unnormalizedResult {
	done := make(chan unnormalizedResult, 1)
	go func() {
		batchID, err := p.WaitForUnnormalizedBatch(ctx)
		done <- unnormalizedResult{err: err, batchID: batchID}
	}()
	return done
}

func startCatchupWait(ctx context.Context, p *syncNormalizeProgress, threshold int64) <-chan error {
	done := make(chan error, 1)
	go func() { done <- p.WaitForNormalizeCatchup(ctx, threshold) }()
	return done
}

// Inside a synctest bubble, a receive or call that never returns fails the test as a
// deadlock instead of hanging, so tests receive results directly.

// gatedContext blocks the first Done call until released. The wait methods call Done
// only in their select, after releasing the mutex, so a test can update progress
// between a waiter's check and its select.
type gatedContext struct {
	context.Context //nolint:containedctx // wraps the waiter's context to gate Done
	entered         chan struct{}
	release         chan struct{}
	once            sync.Once
}

func newGatedContext(ctx context.Context) *gatedContext {
	return &gatedContext{Context: ctx, entered: make(chan struct{}), release: make(chan struct{})}
}

func (c *gatedContext) Done() <-chan struct{} {
	c.once.Do(func() {
		close(c.entered)
		<-c.release
	})
	return c.Context.Done()
}

func TestSyncNormalizeProgressRecordsOnlyMoveForward(t *testing.T) {
	for _, initial := range [][2]int64{{0, 0}, {5, 5}, {10, 5}} {
		t.Run(fmt.Sprintf("synced=%d/normalized=%d", initial[0], initial[1]), func(t *testing.T) {
			p := newSyncNormalizeProgress(initial[0], initial[1])
			require.Equal(t, initial[0], p.SyncedBatchID())
			require.Equal(t, initial[1], p.NormalizedBatchID())
		})
	}

	p := newSyncNormalizeProgress(10, 5)
	for _, step := range []struct {
		record                     func(int64)
		batchID                    int64
		wantSynced, wantNormalized int64
	}{
		{p.RecordSyncedBatch, 20, 20, 5},
		{p.RecordSyncedBatch, 15, 20, 5},
		{p.RecordSyncedBatch, 20, 20, 5},
		{p.RecordSyncedBatch, 21, 21, 5},
		{p.RecordNormalizedBatch, 8, 21, 8},
		{p.RecordNormalizedBatch, 6, 21, 8},
		{p.RecordNormalizedBatch, 8, 21, 8},
		{p.RecordNormalizedBatch, 9, 21, 9},
	} {
		step.record(step.batchID)
		require.Equal(t, step.wantSynced, p.SyncedBatchID(), "after recording %d", step.batchID)
		require.Equal(t, step.wantNormalized, p.NormalizedBatchID(), "after recording %d", step.batchID)
	}
}

func TestSyncNormalizeProgressWaitConditions(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		synced, normalized, threshold int64
		unnormalizedReturns           bool
		catchupReturns                bool
	}{
		{name: "nothing synced", synced: 0, normalized: 0, threshold: 3, unnormalizedReturns: false, catchupReturns: true},
		{name: "caught up", synced: 5, normalized: 5, threshold: 3, unnormalizedReturns: false, catchupReturns: true},
		{name: "gap below threshold", synced: 10, normalized: 8, threshold: 3, unnormalizedReturns: true, catchupReturns: true},
		{name: "gap at threshold", synced: 10, normalized: 7, threshold: 3, unnormalizedReturns: true, catchupReturns: false},
		{name: "gap above threshold", synced: 10, normalized: 6, threshold: 3, unnormalizedReturns: true, catchupReturns: false},
		{name: "threshold one caught up", synced: 5, normalized: 5, threshold: 1, unnormalizedReturns: false, catchupReturns: true},
		{name: "threshold one with one pending", synced: 6, normalized: 5, threshold: 1, unnormalizedReturns: true, catchupReturns: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				p := newSyncNormalizeProgress(tc.synced, tc.normalized)

				unnormalized := startUnnormalizedWait(ctx, p)
				catchup := startCatchupWait(ctx, p, tc.threshold)
				synctest.Wait()

				if tc.unnormalizedReturns {
					res := <-unnormalized
					require.NoError(t, res.err)
					require.Equal(t, tc.synced, res.batchID)
				} else {
					require.Empty(t, unnormalized, "WaitForUnnormalizedBatch returned without pending work")
				}

				if tc.catchupReturns {
					require.NoError(t, <-catchup)
				} else {
					require.Empty(t, catchup, "WaitForNormalizeCatchup returned at or above the threshold")
				}
			})
		})
	}
}

func TestSyncNormalizeProgressNeverBlocksBothSides(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for threshold := int64(1); threshold <= 4; threshold++ {
			for synced := range int64(7) {
				for normalized := range synced + 1 {
					ctx, cancel := context.WithCancel(t.Context())
					p := newSyncNormalizeProgress(synced, normalized)
					unnormalized := startUnnormalizedWait(ctx, p)
					catchup := startCatchupWait(ctx, p, threshold)
					synctest.Wait()
					require.NotZero(t, len(unnormalized)+len(catchup),
						"sync and normalize are both blocked at threshold=%d synced=%d normalized=%d", threshold, synced, normalized)
					cancel()
					synctest.Wait()
				}
			}
		}
	})
}

func TestSyncNormalizeProgressUnnormalizedTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newSyncNormalizeProgress(10, 5)
		for range 2 {
			target, err := p.WaitForUnnormalizedBatch(t.Context())
			require.NoError(t, err)
			require.Equal(t, int64(10), target)
		}
		require.Equal(t, int64(10), p.SyncedBatchID(), "waiting must not change progress")
		require.Equal(t, int64(5), p.NormalizedBatchID(), "waiting must not change progress")

		p.RecordNormalizedBatch(8)
		target, err := p.WaitForUnnormalizedBatch(t.Context())
		require.NoError(t, err)
		require.Equal(t, int64(10), target, "partial completion must keep the target")

		p.RecordSyncedBatch(11)
		p.RecordSyncedBatch(12)
		p.RecordNormalizedBatch(10)
		target, err = p.WaitForUnnormalizedBatch(t.Context())
		require.NoError(t, err)
		require.Equal(t, int64(12), target, "newer synced work must stay visible")

		p.RecordNormalizedBatch(12)
		done := startUnnormalizedWait(t.Context(), p)
		synctest.Wait()
		require.Empty(t, done, "returned after every batch was normalized")
		p.RecordSyncedBatch(13)
		res := <-done
		require.NoError(t, res.err)
		require.Equal(t, int64(13), res.batchID)
	})
}

func TestSyncNormalizeProgressCatchupRechecksLatestProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newSyncNormalizeProgress(10, 5)
		done := startCatchupWait(t.Context(), p, 3)
		synctest.Wait()
		require.Empty(t, done)

		p.RecordSyncedBatch(12)
		synctest.Wait()
		require.Empty(t, done, "a sync update released backpressure")
		p.RecordNormalizedBatch(8)
		synctest.Wait()
		require.Empty(t, done, "released before the newer synced batch was considered")
		p.RecordNormalizedBatch(9)
		synctest.Wait()
		require.Empty(t, done, "released at the buffer boundary")
		p.RecordNormalizedBatch(10)
		require.NoError(t, <-done)
	})
}

func TestSyncNormalizeProgressStaleUpdatesKeepWaitersBlocked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newSyncNormalizeProgress(5, 5)
		unnormalized := startUnnormalizedWait(t.Context(), p)
		synctest.Wait()
		p.RecordSyncedBatch(5)
		p.RecordSyncedBatch(4)
		p.RecordNormalizedBatch(5)
		p.RecordNormalizedBatch(4)
		synctest.Wait()
		require.Empty(t, unnormalized, "a stale update released normalize")
		p.RecordSyncedBatch(6)
		res := <-unnormalized
		require.NoError(t, res.err)
		require.Equal(t, int64(6), res.batchID)

		p = newSyncNormalizeProgress(10, 5)
		catchup := startCatchupWait(t.Context(), p, 3)
		synctest.Wait()
		p.RecordNormalizedBatch(5)
		p.RecordNormalizedBatch(4)
		p.RecordSyncedBatch(10)
		p.RecordSyncedBatch(9)
		synctest.Wait()
		require.Empty(t, catchup, "a stale update released sync")
		p.RecordNormalizedBatch(8)
		require.NoError(t, <-catchup)
	})
}

func TestSyncNormalizeProgressUpdateBeforeSelectIsNotLost(t *testing.T) {
	t.Run("one synced batch wakes normalize", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p := newSyncNormalizeProgress(5, 5)
			ctx := newGatedContext(t.Context())
			done := startUnnormalizedWait(ctx, p)
			<-ctx.entered
			p.RecordSyncedBatch(6)
			close(ctx.release)
			res := <-done
			require.NoError(t, res.err)
			require.Equal(t, int64(6), res.batchID)
		})
	})

	t.Run("several synced batches wake normalize with the latest", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p := newSyncNormalizeProgress(5, 5)
			ctx := newGatedContext(t.Context())
			done := startUnnormalizedWait(ctx, p)
			<-ctx.entered
			p.RecordSyncedBatch(6)
			p.RecordSyncedBatch(7)
			p.RecordSyncedBatch(8)
			close(ctx.release)
			res := <-done
			require.NoError(t, res.err)
			require.Equal(t, int64(8), res.batchID)
		})
	})

	t.Run("normalize progress wakes sync", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p := newSyncNormalizeProgress(10, 5)
			ctx := newGatedContext(t.Context())
			done := startCatchupWait(ctx, p, 3)
			<-ctx.entered
			p.RecordNormalizedBatch(7)
			p.RecordNormalizedBatch(8)
			close(ctx.release)
			require.NoError(t, <-done)
		})
	})

	t.Run("normalize rechecks after pending work is already done", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p := newSyncNormalizeProgress(5, 5)
			ctx := newGatedContext(t.Context())
			done := startUnnormalizedWait(ctx, p)
			<-ctx.entered
			p.RecordSyncedBatch(6)
			p.RecordNormalizedBatch(6)
			close(ctx.release)
			synctest.Wait()
			require.Empty(t, done, "returned for a batch that was already normalized")
			p.RecordSyncedBatch(7)
			res := <-done
			require.NoError(t, res.err)
			require.Equal(t, int64(7), res.batchID)
		})
	})

	t.Run("sync rechecks after the buffer fills again", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p := newSyncNormalizeProgress(10, 5)
			ctx := newGatedContext(t.Context())
			done := startCatchupWait(ctx, p, 3)
			<-ctx.entered
			p.RecordNormalizedBatch(8)
			p.RecordSyncedBatch(11)
			close(ctx.release)
			synctest.Wait()
			require.Empty(t, done, "returned while the buffer was full")
			p.RecordNormalizedBatch(9)
			require.NoError(t, <-done)
		})
	})
}

func TestSyncNormalizeProgressInstancesAreIndependent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first := newSyncNormalizeProgress(5, 5)
		second := newSyncNormalizeProgress(5, 5)
		firstDone := startUnnormalizedWait(t.Context(), first)
		secondDone := startUnnormalizedWait(t.Context(), second)
		synctest.Wait()

		first.RecordSyncedBatch(6)
		res := <-firstDone
		require.NoError(t, res.err)
		require.Equal(t, int64(6), res.batchID)
		synctest.Wait()
		require.Empty(t, secondDone, "an update to one instance woke another")

		second.RecordSyncedBatch(7)
		res = <-secondDone
		require.NoError(t, res.err)
		require.Equal(t, int64(7), res.batchID)
	})
}

func TestSyncNormalizeProgressCancellation(t *testing.T) {
	for _, pending := range []bool{false, true} {
		for _, cancelBeforeWait := range []bool{false, true} {
			t.Run(fmt.Sprintf("pending=%t/cancelBeforeWait=%t", pending, cancelBeforeWait), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					p := newSyncNormalizeProgress(5, 5)
					if pending {
						p.RecordSyncedBatch(10)
					}
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					if cancelBeforeWait {
						cancel()
					}
					var unnormalized <-chan unnormalizedResult
					var catchup <-chan error
					if pending {
						catchup = startCatchupWait(ctx, p, 3)
					} else {
						unnormalized = startUnnormalizedWait(ctx, p)
					}
					synctest.Wait()
					cancel()
					synctest.Wait()
					if pending {
						require.ErrorIs(t, <-catchup, context.Canceled)
					} else {
						res := <-unnormalized
						require.ErrorIs(t, res.err, context.Canceled)
						require.Zero(t, res.batchID)
					}
				})
			})
		}
	}
}

func TestSyncNormalizeProgressCancellationWinsOverSatisfiedCondition(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	p := newSyncNormalizeProgress(10, 5)

	batchID, err := p.WaitForUnnormalizedBatch(ctx)
	require.ErrorIs(t, err, context.Canceled, "pending batch ignored cancellation")
	require.Zero(t, batchID)
	require.ErrorIs(t, p.WaitForNormalizeCatchup(ctx, 6), context.Canceled, "available capacity ignored cancellation")
}

func TestSyncNormalizeProgressCancellationAndUpdateBeforeSelect(t *testing.T) {
	// select picks a ready case at random, so repeat to exercise both orders.
	const rounds = 20

	t.Run("normalize", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			for round := range rounds {
				p := newSyncNormalizeProgress(5, 5)
				cancelCtx, cancel := context.WithCancel(t.Context())
				ctx := newGatedContext(cancelCtx)
				done := startUnnormalizedWait(ctx, p)
				<-ctx.entered
				p.RecordSyncedBatch(6)
				cancel()
				close(ctx.release)
				synctest.Wait()
				res := <-done
				require.ErrorIs(t, res.err, context.Canceled, "round %d", round)
				require.Zero(t, res.batchID, "round %d", round)
			}
		})
	})

	t.Run("sync", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			for round := range rounds {
				p := newSyncNormalizeProgress(10, 5)
				cancelCtx, cancel := context.WithCancel(t.Context())
				ctx := newGatedContext(cancelCtx)
				done := startCatchupWait(ctx, p, 3)
				<-ctx.entered
				p.RecordNormalizedBatch(8)
				cancel()
				close(ctx.release)
				synctest.Wait()
				require.ErrorIs(t, <-done, context.Canceled, "round %d", round)
			}
		})
	})
}

func TestSyncNormalizeProgressDeadline(t *testing.T) {
	t.Run("expired before waiting", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
		defer cancel()
		p := newSyncNormalizeProgress(10, 5)
		_, err := p.WaitForUnnormalizedBatch(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.ErrorIs(t, p.WaitForNormalizeCatchup(ctx, 6), context.DeadlineExceeded)
	})

	t.Run("expires while waiting", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			unnormalized := startUnnormalizedWait(ctx, newSyncNormalizeProgress(5, 5))
			catchup := startCatchupWait(ctx, newSyncNormalizeProgress(10, 5), 3)
			synctest.Wait()
			require.Empty(t, unnormalized)
			require.Empty(t, catchup)

			time.Sleep(time.Minute)
			synctest.Wait()
			res := <-unnormalized
			require.ErrorIs(t, res.err, context.DeadlineExceeded)
			require.Zero(t, res.batchID)
			require.ErrorIs(t, <-catchup, context.DeadlineExceeded)
		})
	})
}

func TestSyncNormalizeProgressReturnsContextErrorNotCause(t *testing.T) {
	cause := errors.New("worker stopped")
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(cause)
	p := newSyncNormalizeProgress(5, 5)

	_, err := p.WaitForUnnormalizedBatch(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, cause)
	err = p.WaitForNormalizeCatchup(ctx, 3)
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, cause)
}

func TestSyncNormalizeProgressWorksAfterCancelledWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newSyncNormalizeProgress(5, 5)
		ctx, cancel := context.WithCancel(t.Context())
		cancelled := startUnnormalizedWait(ctx, p)
		synctest.Wait()
		cancel()
		res := <-cancelled
		require.ErrorIs(t, res.err, context.Canceled)

		done := startUnnormalizedWait(t.Context(), p)
		synctest.Wait()
		require.Empty(t, done)
		p.RecordSyncedBatch(6)
		res = <-done
		require.NoError(t, res.err)
		require.Equal(t, int64(6), res.batchID)
	})
}
