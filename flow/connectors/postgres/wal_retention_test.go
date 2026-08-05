package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWalRetentionSettingsLimitBytes(t *testing.T) {
	t.Parallel()
	oneGB := int64(1024 * 1024 * 1024)
	tenGB := 10 * oneGB
	for _, tc := range []struct {
		name     string
		settings walRetentionSettings
		expected *int64
	}{
		{
			name:     "no settings, as on PG<13",
			settings: walRetentionSettings{},
			expected: nil,
		},
		{
			name: "max_slot_wal_keep_size unlimited, the default, imposes no cap even with wal_keep_size set",
			// wal_keep_size is a retention floor, not a cap, so on its own it bounds nothing.
			settings: walRetentionSettings{WalKeepSizeBytes: &oneGB},
			expected: nil,
		},
		{
			name:     "max_slot_wal_keep_size alone",
			settings: walRetentionSettings{MaxSlotWalKeepSizeBytes: &tenGB},
			expected: &tenGB,
		},
		{
			name:     "wal_keep_size below max_slot_wal_keep_size",
			settings: walRetentionSettings{MaxSlotWalKeepSizeBytes: &tenGB, WalKeepSizeBytes: &oneGB},
			expected: &tenGB,
		},
		{
			name:     "wal_keep_size raises the cap above max_slot_wal_keep_size",
			settings: walRetentionSettings{MaxSlotWalKeepSizeBytes: &oneGB, WalKeepSizeBytes: &tenGB},
			expected: &tenGB,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			actual := tc.settings.LimitBytes()
			if tc.expected == nil {
				require.Nil(t, actual)
				return
			}
			require.NotNil(t, actual)
			require.Equal(t, *tc.expected, *actual)
		})
	}
}
