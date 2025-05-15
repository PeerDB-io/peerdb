package connmysql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProcessTime(t *testing.T) {
	for _, ts := range []struct {
		out time.Time
		in  string
	}{
		{time.Date(1970, 1, 1, 23, 30, 0, 500000000, time.UTC), "23:30.5"},
		{time.Date(1970, 2, 3, 8, 0, 1, 0, time.UTC), "800:0:1"},
		{time.Date(1969, 11, 28, 15, 59, 59, 0, time.UTC), "-800:0:1"},
		{time.Date(1969, 11, 28, 15, 59, 58, 900000000, time.UTC), "-800:0:1.1"},
		{time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC), "1."},
		{time.Date(1970, 1, 1, 0, 12, 34, 0, time.UTC), "1234"},
		{time.Date(1970, 1, 1, 3, 12, 34, 0, time.UTC), "31234"},
		{time.Date(1970, 1, 1, 0, 0, 1, 120000000, time.UTC), "1.12"},
		{time.Date(1970, 1, 1, 0, 0, 1, 12000000, time.UTC), "1.012"},
		{time.Date(1970, 1, 1, 0, 0, 1, 12300000, time.UTC), "1.0123"},
		{time.Date(1970, 1, 1, 0, 0, 1, 1230000, time.UTC), "1.00123"},
		{time.Date(1970, 1, 1, 0, 0, 1, 1000, time.UTC), "1.000001"},
		{time.Date(1970, 1, 1, 0, 0, 1, 200, time.UTC), "1.0000002"},
		{time.Date(1970, 1, 1, 0, 0, 1, 30, time.UTC), "1.00000003"},
		{time.Date(1970, 1, 1, 0, 0, 1, 4, time.UTC), "1.000000004"},
		{time.Time{}, "123.aa"},
		{time.Time{}, "hh:00:00"},
		{time.Time{}, "00:mm:00"},
		{time.Time{}, "00:00:ss"},
		{time.Time{}, "hh:00"},
		{time.Time{}, "00:mm"},
		{time.Time{}, "ss"},
		{time.Time{}, "mm00"},
		{time.Time{}, "00ss"},
		{time.Time{}, "hh0000"},
		{time.Time{}, "00mm00"},
		{time.Time{}, "0000ss"},
	} {
		tm, err := processTime(ts.in)
		if tm.IsZero() {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, ts.out, tm)
	}
}
