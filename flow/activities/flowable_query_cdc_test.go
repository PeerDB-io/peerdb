package activities

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQueryCDCPollWait(t *testing.T) {
	now := time.Date(2026, time.September, 16, 12, 0, 0, 0, time.UTC)
	syncInterval := 24 * time.Hour

	t.Run("first poll is due immediately", func(t *testing.T) {
		require.Zero(t, queryCDCPollWait(time.Time{}, time.Time{}, now, syncInterval))
	})

	t.Run("cadence starts when successful poll started", func(t *testing.T) {
		lastAttemptAt := now.Add(-2 * time.Hour)
		lastSyncedAt := now
		require.Equal(t, 22*time.Hour, queryCDCPollWait(lastAttemptAt, lastSyncedAt, now, syncInterval))
	})

	t.Run("failed or interrupted poll is due immediately", func(t *testing.T) {
		lastSyncedAt := now.Add(-20 * time.Hour)
		lastAttemptAt := now.Add(-time.Hour)
		require.Zero(t, queryCDCPollWait(lastAttemptAt, lastSyncedAt, now, syncInterval))
	})

	t.Run("overdue successful poll is due immediately", func(t *testing.T) {
		lastAttemptAt := now.Add(-25 * time.Hour)
		lastSyncedAt := now.Add(-23 * time.Hour)
		require.Zero(t, queryCDCPollWait(lastAttemptAt, lastSyncedAt, now, syncInterval))
	})

	t.Run("legacy state without attempt uses successful completion", func(t *testing.T) {
		require.Equal(t, syncInterval, queryCDCPollWait(time.Time{}, now, now, syncInterval))
	})
}

func TestQueryCDCRetryWait(t *testing.T) {
	t.Run("exponential backoff capped at one minute", func(t *testing.T) {
		wait := time.Duration(0)
		want := []time.Duration{
			5 * time.Second,
			10 * time.Second,
			20 * time.Second,
			40 * time.Second,
			time.Minute,
			time.Minute,
		}
		for _, expected := range want {
			wait = queryCDCRetryWait(wait, time.Hour)
			require.Equal(t, expected, wait)
		}
	})

	t.Run("never exceeds normal poll cadence", func(t *testing.T) {
		wait := time.Duration(0)
		for range 5 {
			wait = queryCDCRetryWait(wait, 3*time.Second)
		}
		require.Equal(t, 3*time.Second, wait)
	})

	t.Run("non-positive cadence does not wait", func(t *testing.T) {
		require.Zero(t, queryCDCRetryWait(0, 0))
	})
}
