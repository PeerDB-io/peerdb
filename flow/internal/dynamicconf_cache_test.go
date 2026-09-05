package internal

import (
	"context"
	"errors"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCachedDynconfSettingCachesWithinTTL(t *testing.T) {
	var calls int
	setting := NewCachedDynconfSetting(func(context.Context, map[string]string) (int64, error) {
		calls++
		return 10, nil
	}, time.Minute)

	for range 2 {
		value, err := setting.Get(t.Context(), nil)
		require.NoError(t, err)
		require.Equal(t, int64(10), value)
	}
	require.Equal(t, 1, calls)
}

func TestCachedDynconfSettingRefreshesAfterTTL(t *testing.T) {
	var calls int64
	setting := NewCachedDynconfSetting(func(context.Context, map[string]string) (int64, error) {
		calls++
		return calls * 10, nil
	}, time.Millisecond)

	value, err := setting.Get(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, int64(10), value)

	time.Sleep(10 * time.Millisecond)

	value, err = setting.Get(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, int64(20), value)
	require.Equal(t, int64(2), calls)
}

func TestCachedDynconfSettingDoesNotCacheErrors(t *testing.T) {
	wantErr := errors.New("lookup failed")
	var calls int
	setting := NewCachedDynconfSetting(func(context.Context, map[string]string) (int64, error) {
		calls++
		if calls == 1 {
			return 0, wantErr
		}
		return 10, nil
	}, time.Minute)

	_, err := setting.Get(t.Context(), nil)
	require.ErrorIs(t, err, wantErr)
	value, err := setting.Get(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, int64(10), value)
	require.Equal(t, 2, calls)
}

func TestCachedDynconfSettingCoalescesConcurrentRefreshes(t *testing.T) {
	const goroutines = 8

	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	setting := NewCachedDynconfSetting(func(context.Context, map[string]string) (int64, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return 10, nil
	}, time.Minute)

	results := make(chan int64, goroutines)
	errs := make(chan error, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			value, err := setting.Get(t.Context(), nil)
			results <- value
			errs <- err
		}()
	}

	<-started
	close(release)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	for value := range results {
		require.Equal(t, int64(10), value)
	}
	require.Equal(t, int32(1), calls.Load())
}

func TestCachedDynconfSettingWithTypedDynconfGetter(t *testing.T) {
	//nolint:gosec // Test data does not need cryptographically secure randomness.
	expectedPartSize := rand.Int64()
	setting := NewCachedDynconfSetting(PeerDBS3PartSize, time.Minute)

	value, err := setting.Get(t.Context(), map[string]string{
		"PEERDB_S3_PART_SIZE": strconv.FormatInt(expectedPartSize, 10),
	})
	require.NoError(t, err)
	require.Equal(t, expectedPartSize, value)
}
