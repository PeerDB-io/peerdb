package activities

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
