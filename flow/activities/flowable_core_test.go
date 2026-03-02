package activities

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

var goLeakOpts = []goleak.Option{
	goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
}

type pipeStreamCloser struct{ *io.PipeWriter }

func (p *pipeStreamCloser) Close(err error) { p.PipeWriter.CloseWithError(err) }

type testPipe struct {
	reader  *io.PipeReader
	stream  *pipeStreamCloser
	cleanup func(error)
}

func newTestPipe() testPipe {
	r, w := io.Pipe()
	return testPipe{
		reader:  r,
		stream:  &pipeStreamCloser{w},
		cleanup: func(err error) { r.CloseWithError(err) },
	}
}

// continuousPull writes to the pipe until error or context cancellation.
func continuousPull(ctx context.Context, s *pipeStreamCloser) error {
	buf := make([]byte, 4096)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if _, err := s.PipeWriter.Write(buf); err != nil {
				return err
			}
		}
	}
}

// readThenFail reads once from the pipe then returns the given error.
func readThenFail(err error) func(context.Context, *io.PipeReader) (int64, error) {
	return func(_ context.Context, r *io.PipeReader) (int64, error) {
		if _, readErr := r.Read(make([]byte, 1024)); readErr != nil {
			return 0, readErr
		}
		return 0, err
	}
}

func readAllSync(_ context.Context, r *io.PipeReader) (int64, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	return int64(len(data)), nil
}

// runOrchestrate runs orchestratePullAndSync in a synctest goroutine
// and returns (rowsSynced, error, completed).
func runOrchestrate(
	p testPipe,
	pull func(context.Context, *pipeStreamCloser) error,
	sync func(context.Context, *io.PipeReader) (int64, error),
) (int64, error, bool) {
	var rows int64
	var retErr error
	var done atomic.Bool
	go func() {
		rows, retErr = orchestratePullAndSync(context.Background(), p.stream, p.reader, pull, sync, p.cleanup)
		done.Store(true)
	}()
	synctest.Wait()
	return rows, retErr, done.Load()
}

func runOrchestrateQRecord(
	stream *model.QRecordStream,
	pull func(context.Context, *model.QRecordStream) error,
	sync func(context.Context, *model.QRecordStream) (int64, error),
	onSyncFailure func(error),
) (int64, error, bool) {
	var rows int64
	var retErr error
	var done atomic.Bool
	go func() {
		rows, retErr = orchestratePullAndSync(context.Background(), stream, stream, pull, sync, onSyncFailure)
		done.Store(true)
	}()
	synctest.Wait()
	return rows, retErr, done.Load()
}

// Sync errors while pull is writing to the pipe. Without closing the read end,
// pull blocks forever in pipe.Write().
func TestOrchestratePullAndSync_SyncErrorUnblocksPull(t *testing.T) {
	defer goleak.VerifyNone(t, goLeakOpts...)

	synctest.Test(t, func(t *testing.T) {
		p := newTestPipe()
		errDst := errors.New("column \"new_col\" of relation \"target_table\" does not exist")

		_, retErr, completed := runOrchestrate(p, continuousPull, readThenFail(errDst))

		require.True(t, completed, "deadlocked: pull stuck in pipe.Write()")
		require.ErrorIs(t, retErr, errDst)
	})
}

// Repeated sync-side errors must not leak goroutines.
func TestOrchestratePullAndSync_NoGoroutineLeaks(t *testing.T) {
	defer goleak.VerifyNone(t, goLeakOpts...)

	synctest.Test(t, func(t *testing.T) {
		for i := range 3 {
			p := newTestPipe()
			_, _, completed := runOrchestrate(p, continuousPull, readThenFail(errors.New("destination error")))
			require.True(t, completed, "iteration %d deadlocked", i)
		}
	})
}

func TestOrchestratePullAndSync_Success(t *testing.T) {
	defer goleak.VerifyNone(t, goLeakOpts...)

	p := newTestPipe()
	data := []byte("hello from source")

	pull := func(_ context.Context, s *pipeStreamCloser) error {
		_, err := s.PipeWriter.Write(data)
		return err
	}

	rows, err := orchestratePullAndSync(context.Background(), p.stream, p.reader, pull, readAllSync, p.cleanup)

	require.NoError(t, err)
	require.Equal(t, int64(len(data)), rows)
}

// Pull errors and closes the write end; sync unblocks and returns.
func TestOrchestratePullAndSync_PullErrorUnblocksSync(t *testing.T) {
	defer goleak.VerifyNone(t, goLeakOpts...)

	p := newTestPipe()
	errSrc := errors.New("source connection lost")

	pull := func(_ context.Context, s *pipeStreamCloser) error {
		s.PipeWriter.Write([]byte("partial"))
		return errSrc
	}

	_, err := orchestratePullAndSync(context.Background(), p.stream, p.reader, pull, readAllSync, p.cleanup)

	require.ErrorIs(t, err, errSrc)
}

func TestOrchestratePullAndSync_QRecordSuccess_DoesNotCallOnStreamError(t *testing.T) {
	defer goleak.VerifyNone(t, goLeakOpts...)

	synctest.Test(t, func(t *testing.T) {
		stream := model.NewQRecordStream(0)
		var onSyncFailureCalls atomic.Int32

		pull := func(_ context.Context, s *model.QRecordStream) error {
			s.SetSchema(types.QRecordSchema{})
			s.Records <- nil
			return nil
		}
		sync := func(_ context.Context, out *model.QRecordStream) (int64, error) {
			if _, err := out.Schema(); err != nil {
				return 0, err
			}
			if _, ok := <-out.Records; !ok {
				return 0, errors.New("stream closed before sync consumed first record")
			}
			return 1, nil
		}

		rows, retErr, completed := runOrchestrateQRecord(
			stream,
			pull,
			sync,
			func(error) { onSyncFailureCalls.Add(1) },
		)

		require.True(t, completed, "orchestration deadlocked")
		require.NoError(t, retErr)
		require.Equal(t, int64(1), rows)
		require.Zero(t, onSyncFailureCalls.Load(), "onSyncFailure should not run when pull/sync both succeed")
	})
}

func TestOrchestratePullAndSync_QRecordSyncError_WithNoSyncFailureHook_DoesNotPanicPull(t *testing.T) {
	defer goleak.VerifyNone(t, goLeakOpts...)

	synctest.Test(t, func(t *testing.T) {
		stream := model.NewQRecordStream(0)
		syncErr := errors.New("destination error")
		var pullPanicked atomic.Bool

		pull := func(ctx context.Context, s *model.QRecordStream) (retErr error) {
			defer func() {
				if recovered := recover(); recovered != nil {
					pullPanicked.Store(true)
					retErr = fmt.Errorf("panic in pull: %v", recovered)
				}
			}()

			s.SetSchema(types.QRecordSchema{})
			s.Records <- nil
			// After sync consumes the first record and errors, this second
			// send blocks on the unbuffered channel. Pull must select on
			// ctx.Done() to avoid deadlocking here.
			select {
			case s.Records <- nil:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}
		sync := func(_ context.Context, out *model.QRecordStream) (int64, error) {
			if _, err := out.Schema(); err != nil {
				return 0, err
			}
			if _, ok := <-out.Records; !ok {
				return 0, errors.New("stream closed before sync consumed first record")
			}
			return 0, syncErr
		}

		_, retErr, completed := runOrchestrateQRecord(
			stream,
			pull,
			sync,
			nil,
		)

		require.True(t, completed, "orchestration deadlocked")
		require.ErrorIs(t, retErr, syncErr)
		require.False(t, pullPanicked.Load(), "pull should not panic when sync fails")
	})
}

func TestOrchestratePullAndSync_QRecordSyncError_OnSyncFailureHookUnblocksBlockedProducer(t *testing.T) {
	defer goleak.VerifyNone(t, goLeakOpts...)

	synctest.Test(t, func(t *testing.T) {
		stream := model.NewQRecordStream(0)
		syncErr := errors.New("destination error")

		pull := func(_ context.Context, s *model.QRecordStream) error {
			s.SetSchema(types.QRecordSchema{})
			s.Records <- nil
			// This blocks after sync returns unless onSyncFailure drains the stream.
			s.Records <- nil
			return nil
		}
		sync := func(_ context.Context, out *model.QRecordStream) (int64, error) {
			if _, err := out.Schema(); err != nil {
				return 0, err
			}
			if _, ok := <-out.Records; !ok {
				return 0, errors.New("stream closed before sync consumed first record")
			}
			return 0, syncErr
		}

		_, retErr, completed := runOrchestrateQRecord(
			stream,
			pull,
			sync,
			drainChannelOnSyncFailure(internal.LoggerFromCtx(context.Background()), "draining record stream to unblock pull", stream.Records),
		)

		require.True(t, completed, "deadlocked: blocked producer was not unblocked on sync failure")
		require.ErrorIs(t, retErr, syncErr)
	})
}

func TestOrchestratePullAndSync_QRecordSyncError_WithoutHookDeadlocks(t *testing.T) {
	defer goleak.VerifyNone(t, goLeakOpts...)

	synctest.Test(t, func(t *testing.T) {
		stream := model.NewQRecordStream(0)
		syncErr := errors.New("destination error")
		var done atomic.Bool
		var retErr error

		pull := func(_ context.Context, s *model.QRecordStream) error {
			s.SetSchema(types.QRecordSchema{})
			s.Records <- nil
			// This send blocks forever without a sync-failure hook that drains/aborts.
			s.Records <- nil
			return nil
		}
		sync := func(_ context.Context, out *model.QRecordStream) (int64, error) {
			if _, err := out.Schema(); err != nil {
				return 0, err
			}
			if _, ok := <-out.Records; !ok {
				return 0, errors.New("stream closed before sync consumed first record")
			}
			return 0, syncErr
		}

		go func() {
			_, retErr = orchestratePullAndSync(context.Background(), stream, stream, pull, sync, nil)
			done.Store(true)
		}()

		// Reproduces the original bug: pull is stuck on send after sync returned an error.
		synctest.Wait()
		require.False(t, done.Load(), "expected deadlock without sync-failure hook")

		// Cleanup the blocked sender so goleak stays clean.
		go func() {
			for range stream.Records {
			}
		}()
		synctest.Wait()

		require.True(t, done.Load(), "expected cleanup drain to unblock deadlocked pull")
		require.ErrorIs(t, retErr, syncErr)
	})
}
