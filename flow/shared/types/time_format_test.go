package types

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFormatExtendedTimeDuration(t *testing.T) {
	tests := map[string]time.Duration{
		"0:00:00.000000":    0,
		"14:21:00.000000":   14*time.Hour + 21*time.Minute,
		"8:30:00.123456":    8*time.Hour + 30*time.Minute + 123456*time.Microsecond,
		"24:00:00.000000":   24 * time.Hour,
		"-1:30:00.000000":   -(1*time.Hour + 30*time.Minute),
		"-2:15:30.500000":   -(2*time.Hour + 15*time.Minute + 30*time.Second + 500000*time.Microsecond),
		"838:59:59.999999":  838*time.Hour + 59*time.Minute + 59*time.Second + 999999*time.Microsecond,
		"-838:59:59.000000": -(838*time.Hour + 59*time.Minute + 59*time.Second),
		"999:59:59.999999":  999*time.Hour + 59*time.Minute + 59*time.Second + 999999*time.Microsecond,
		"-999:59:59.999999": -(999*time.Hour + 59*time.Minute + 59*time.Second + 999999*time.Microsecond),
	}

	for expected, duration := range tests {
		t.Run(expected, func(t *testing.T) {
			require.Equal(t, expected, FormatExtendedTimeDuration(duration))
		})
	}
}
