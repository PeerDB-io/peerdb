package common

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestPullRecordsWorkerPoolSendsEveryItemInOrder(t *testing.T) {
	var gotChunks [][]int
	var gotTokens []string

	pool := PullRecordsWorkerPool[int, []int, string]{
		Concurrency: 4,
		ChunkSize:   3,
		WorkerFunc:  func(events []int) ([]int, error) { return events, nil },
		Send: func(_ context.Context, items []int, resumeToken string) error {
			// Send only ever runs on the pool's single send loop, so this needs no locking.
			gotChunks = append(gotChunks, items)
			gotTokens = append(gotTokens, resumeToken)
			return nil
		},
	}
	pool.Init(t.Context())

	for i := range 10 {
		require.NoError(t, pool.AddItem(t.Context(), i, fmt.Sprintf("token-%d", i)))
	}
	require.NoError(t, pool.Flush(t.Context()))
	require.NoError(t, pool.Wait(t.Context()))

	// 10 items at ChunkSize 3 chunk up as [0 1 2] [3 4 5] [6 7 8] [9], and even with four
	// workers decoding in parallel they must reach Send in AddItem order.
	require.Equal(t, [][]int{{0, 1, 2}, {3, 4, 5}, {6, 7, 8}, {9}}, gotChunks)
	// Each chunk carries the resume token of the last item that went into it.
	require.Equal(t, []string{"token-2", "token-5", "token-8", "token-9"}, gotTokens)
}

func TestPullRecordsWorkerPoolErrorFromWorker(t *testing.T) {
	alreadyRunning := goleak.IgnoreCurrent()
	defer goleak.VerifyNone(t, alreadyRunning)

	errDecode := errors.New("decode failed")
	var lastResumeToken string
	barrier := make(chan struct{}, 1)
	pool := PullRecordsWorkerPool[int, []int, string]{
		Concurrency: 4,
		ChunkSize:   2,
		WorkerFunc: func(events []int) ([]int, error) {
			if slices.Contains(events, 5) {
				// Before returning an error, ensure the previous batch has been sent
				// to avoid a flaky test.
				<-barrier
				return nil, errDecode
			}
			return events, nil
		},
		Send: func(_ context.Context, vals []int, token string) error {
			if slices.Contains(vals, 3) {
				barrier <- struct{}{}
			}
			lastResumeToken = token
			return nil
		},
	}
	pool.Init(t.Context())

	// Keep feeding past the chunk that fails: the pool should wind itself down instead of
	// wedging, and AddItem stays quiet because Wait is what reports the worker's error.
	for i := range 20 {
		require.NoError(t, pool.AddItem(t.Context(), i, fmt.Sprintf("token-%d", i)))
	}
	require.NoError(t, pool.Flush(t.Context()))
	require.ErrorIs(t, pool.Wait(t.Context()), errDecode)
	require.Equal(t, "token-3", lastResumeToken)
}
