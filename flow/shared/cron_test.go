package shared

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseSyncCron(t *testing.T) {
	from := time.Date(2026, time.September, 16, 12, 34, 56, 0, time.UTC)

	t.Run("hourly fires at the top of the hour in UTC", func(t *testing.T) {
		schedule, err := ParseSyncCron("0 * * * *")
		require.NoError(t, err)
		require.Equal(t, time.Date(2026, time.September, 16, 13, 0, 0, 0, time.UTC), schedule.Next(from))
	})

	t.Run("descriptor", func(t *testing.T) {
		schedule, err := ParseSyncCron("@hourly")
		require.NoError(t, err)
		require.Equal(t, time.Date(2026, time.September, 16, 13, 0, 0, 0, time.UTC), schedule.Next(from))
	})

	t.Run("explicit timezone wins over the UTC default", func(t *testing.T) {
		schedule, err := ParseSyncCron("CRON_TZ=Europe/Berlin 0 6 * * *")
		require.NoError(t, err)
		// 06:00 CEST (UTC+2) on 17 Sep is 04:00 UTC
		require.Equal(t, time.Date(2026, time.September, 17, 4, 0, 0, 0, time.UTC), schedule.Next(from).UTC())
	})

	t.Run("rejects invalid expressions", func(t *testing.T) {
		for _, expr := range []string{"", "   ", "not a cron", "61 * * * *", "* * * *", "0 0 * * * *"} {
			_, err := ParseSyncCron(expr)
			require.Error(t, err, expr)
		}
	})

	t.Run("rejects schedules firing more often than once a minute", func(t *testing.T) {
		_, err := ParseSyncCron("@every 10s")
		require.Error(t, err)
		_, err = ParseSyncCron("@every 1m")
		require.NoError(t, err)
	})
}
