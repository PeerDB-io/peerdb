package conncockroachdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestParseGCTTLSeconds(t *testing.T) {
	testCases := []struct {
		name         string
		rawConfigSQL string
		expected     int64
		ok           bool
	}{
		{
			// captured from SHOW ZONE CONFIGURATION FOR TABLE on CockroachDB v25.4
			// for a table inheriting the default range zone config
			name: "range default",
			rawConfigSQL: "ALTER RANGE default CONFIGURE ZONE USING\n" +
				"\trange_min_bytes = 134217728,\n" +
				"\trange_max_bytes = 536870912,\n" +
				"\tgc.ttlseconds = 14400,\n" +
				"\tnum_replicas = 1,\n" +
				"\tconstraints = '[]',\n" +
				"\tlease_preferences = '[]'",
			expected: 14400,
			ok:       true,
		},
		{
			name: "table level override",
			rawConfigSQL: "ALTER TABLE testdb.public.zt CONFIGURE ZONE USING\n" +
				"\trange_min_bytes = 134217728,\n" +
				"\trange_max_bytes = 536870912,\n" +
				"\tgc.ttlseconds = 300,\n" +
				"\tnum_replicas = 1,\n" +
				"\tconstraints = '[]',\n" +
				"\tlease_preferences = '[]'",
			expected: 300,
			ok:       true,
		},
		{
			name: "database level override",
			rawConfigSQL: "ALTER DATABASE testdb CONFIGURE ZONE USING\n" +
				"\trange_min_bytes = 134217728,\n" +
				"\trange_max_bytes = 536870912,\n" +
				"\tgc.ttlseconds = 900,\n" +
				"\tnum_replicas = 1,\n" +
				"\tconstraints = '[]',\n" +
				"\tlease_preferences = '[]'",
			expected: 900,
			ok:       true,
		},
		{
			name:         "single line",
			rawConfigSQL: "ALTER TABLE t CONFIGURE ZONE USING gc.ttlseconds = 90000",
			expected:     90000,
			ok:           true,
		},
		{
			name:         "no spaces around equals",
			rawConfigSQL: "gc.ttlseconds=600",
			expected:     600,
			ok:           true,
		},
		{
			name:         "zero",
			rawConfigSQL: "gc.ttlseconds = 0",
			expected:     0,
			ok:           true,
		},
		{
			name: "missing gc.ttlseconds",
			rawConfigSQL: "ALTER TABLE t CONFIGURE ZONE USING\n" +
				"\trange_min_bytes = 134217728,\n" +
				"\tnum_replicas = 3",
			ok: false,
		},
		{
			name:         "empty",
			rawConfigSQL: "",
			ok:           false,
		},
		{
			name:         "overflow",
			rawConfigSQL: "gc.ttlseconds = 99999999999999999999999999",
			ok:           false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ttl, ok := parseGCTTLSeconds(tc.rawConfigSQL)
			require.Equal(t, tc.ok, ok)
			if tc.ok {
				require.Equal(t, tc.expected, ttl)
			}
		})
	}
}

func TestGCTTLFloorError(t *testing.T) {
	require.NoError(t, gcTTLFloorError(`"public"."users"`, "RANGE default", snapshotGCTTLFloorSeconds))
	require.NoError(t, gcTTLFloorError(`"public"."users"`, "RANGE default", 14400))

	err := gcTTLFloorError(`"public"."users"`, `TABLE defaultdb.public.users`, snapshotGCTTLFloorSeconds-1)
	require.Error(t, err)
	require.ErrorContains(t, err, "gc.ttlseconds")
	require.ErrorContains(t, err, `"public"."users"`)
	require.ErrorContains(t, err, "TABLE defaultdb.public.users")
	require.ErrorContains(t, err, "below the 600 second minimum")
	require.ErrorContains(t, err, "CONFIGURE ZONE USING gc.ttlseconds")
}

func TestShouldProtectSnapshotHistory(t *testing.T) {
	changefeeds := &protos.CockroachDBConfig{UseChangefeeds: true}
	noChangefeeds := &protos.CockroachDBConfig{}

	require.True(t, shouldProtectSnapshotHistory(changefeeds, true))
	require.False(t, shouldProtectSnapshotHistory(changefeeds, false))
	require.False(t, shouldProtectSnapshotHistory(noChangefeeds, true))
	require.False(t, shouldProtectSnapshotHistory(noChangefeeds, false))
}

func TestProtectMVCCHistoryQuery(t *testing.T) {
	require.Equal(t,
		"SELECT crdb_internal.protect_mvcc_history($1::decimal, '86400 seconds'::interval, $2)",
		protectMVCCHistoryQuery(24*time.Hour))
	require.Equal(t,
		"SELECT crdb_internal.protect_mvcc_history($1::decimal, '3600 seconds'::interval, $2)",
		protectMVCCHistoryQuery(time.Hour))
}

func TestHistoryRetentionDescription(t *testing.T) {
	require.Equal(t, "peerdb initial load my_mirror", historyRetentionDescription("my_mirror"))
}
