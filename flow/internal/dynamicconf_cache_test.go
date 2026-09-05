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
	getter := func(context.Context, map[string]string, string) (int64, error) {
		calls++
		return 10, nil
	}
	var setting CachedDynconfSetting[int64]
	setting.InitOnce("TEST_SETTING", time.Minute, getter)

	for range 2 {
		value, err := setting.Get(t.Context(), nil)
		require.NoError(t, err)
		require.Equal(t, int64(10), value)
	}
	require.Equal(t, 1, calls)
}

func TestCachedDynconfSettingRequiresInitialization(t *testing.T) {
	var setting CachedDynconfSetting[int64]

	_, err := setting.Get(t.Context(), nil)
	require.EqualError(t, err, "cached dynamic setting is not initialized")
}

func TestCachedDynconfSettingRefreshesAfterTTL(t *testing.T) {
	var calls int64
	getter := func(context.Context, map[string]string, string) (int64, error) {
		calls++
		return calls * 10, nil
	}
	var setting CachedDynconfSetting[int64]
	setting.InitOnce("TEST_SETTING", time.Millisecond, getter)

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
	getter := func(context.Context, map[string]string, string) (int64, error) {
		calls++
		if calls == 1 {
			return 0, wantErr
		}
		return 10, nil
	}
	var setting CachedDynconfSetting[int64]
	setting.InitOnce("TEST_SETTING", time.Minute, getter)

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
	getter := func(context.Context, map[string]string, string) (int64, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return 10, nil
	}
	var setting CachedDynconfSetting[int64]

	results := make(chan int64, goroutines)
	errs := make(chan error, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			setting.InitOnce("TEST_SETTING", time.Minute, getter)
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
	var setting CachedDynconfSetting[int64]
	setting.InitOnce("PEERDB_S3_PART_SIZE", time.Minute, dynamicConfSigned[int64])

	value, err := setting.Get(t.Context(), map[string]string{
		"PEERDB_S3_PART_SIZE": strconv.FormatInt(expectedPartSize, 10),
	})
	require.NoError(t, err)
	require.Equal(t, expectedPartSize, value)
}

func TestCachedDynconfSettingDoesNotCacheEnvOverrides(t *testing.T) {
	const name = "TEST_SETTING"
	var calls int
	getter := func(_ context.Context, env map[string]string, name string) (int64, error) {
		calls++
		if value, overridden := env[name]; overridden {
			return strconv.ParseInt(value, 10, 64)
		}
		return 10, nil
	}
	var setting CachedDynconfSetting[int64]
	setting.InitOnce(name, time.Minute, getter)

	value, err := setting.Get(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, int64(10), value)

	value, err = setting.Get(t.Context(), map[string]string{name: "20"})
	require.NoError(t, err)
	require.Equal(t, int64(20), value)

	value, err = setting.Get(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, int64(10), value)
	require.Equal(t, 2, calls)
}
