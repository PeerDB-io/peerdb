package types

import (
	"fmt"
	"time"
)

// FormatExtendedTimeDuration formats a time.Duration as a time string.
// The extended format [-]HHH:MM:SS.xxxxxx supports:
//   - Negative values (e.g., "-1:30:00.000000")
//   - Hours exceeding 24 (e.g., "838:59:59.999999")
//
// The extended format is compatible with ClickHouse's Time64 type and toTime64OrNull().
func FormatExtendedTimeDuration(d time.Duration) string {
	negative := d < 0
	if negative {
		d = -d
	}

	totalMicros := d.Microseconds()
	totalSeconds := totalMicros / 1_000_000
	micros := totalMicros % 1_000_000

	hours := totalSeconds / 3600
	minutes := (totalSeconds % 3600) / 60
	seconds := totalSeconds % 60

	if negative {
		return fmt.Sprintf("-%d:%02d:%02d.%06d", hours, minutes, seconds, micros)
	}
	return fmt.Sprintf("%d:%02d:%02d.%06d", hours, minutes, seconds, micros)
}
