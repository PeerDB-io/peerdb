package conncockroachdb

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestIsUnknownSettingError(t *testing.T) {
	// captured live from CockroachDB v25.4.13: SHOW CLUSTER SETTING for a
	// setting the cluster does not have fails with the uncategorized SQLSTATE
	// XXUUU and an "unknown setting" message
	unknownSetting := &pgconn.PgError{
		Code:    "XXUUU",
		Message: `unknown setting: "server.serverless.enabled"`,
	}
	require.True(t, isUnknownSettingError(unknownSetting))
	require.True(t, isUnknownSettingError(fmt.Errorf("probe failed: %w", unknownSetting)))
	require.True(t, isUnknownSettingError(toCrdbError(unknownSetting)))

	// privilege and connectivity failures are not the expected negative probe
	require.False(t, isUnknownSettingError(&pgconn.PgError{
		Code:    pgerrcode.InsufficientPrivilege,
		Message: "user testuser does not have VIEWCLUSTERSETTING privilege",
	}))
	require.False(t, isUnknownSettingError(errors.New("dial tcp: connection refused")))
}

func TestIsCockroachCloudHost(t *testing.T) {
	require.True(t, isCockroachCloudHost("banko-ai-assistant-standard-cc-8272.jxf.cockroachlabs.cloud"))
	require.True(t, isCockroachCloudHost("MY-CLUSTER.AWS-US-EAST-1.COCKROACHLABS.CLOUD"))
	require.False(t, isCockroachCloudHost("localhost"))
	require.False(t, isCockroachCloudHost("cockroachlabs.cloud"))
	require.False(t, isCockroachCloudHost("db.internal.example.com"))
	require.False(t, isCockroachCloudHost("evil.cockroachlabs.cloud.example.com"))
	require.False(t, isUnknownSettingError(nil))
}

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
	require.ErrorContains(t, err, "below the 3600 second minimum")
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

func TestHistoryRetentionLookupQuery(t *testing.T) {
	// the job is matched by an exact suffix comparison: a LIKE pattern would
	// let `_`/`%` in flow names match another mirror's job, whose id then goes
	// straight to CANCEL JOB
	require.Equal(t,
		"SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'HISTORY RETENTION'"+
			" AND status = 'running' AND right(description, length($1::text)) = $1::text LIMIT 1",
		historyRetentionLookupQuery)
	require.NotContains(t, historyRetentionLookupQuery, "LIKE")
}

func TestHistoryRetentionExtendQuery(t *testing.T) {
	// verified against CockroachDB v25.4.13:
	// crdb_internal.extend_mvcc_history_protection(job_id) returns void and
	// resets the job's heartbeat, moving its expiry to now + the expiration
	// window the job was created with
	require.Equal(t,
		"SELECT crdb_internal.extend_mvcc_history_protection($1)",
		historyRetentionExtendQuery)

	// the throttle must leave several extension chances inside one window,
	// otherwise a single failed extension could let the protection lapse
	require.Equal(t, historyRetentionWindow/4, historyRetentionExtendInterval)
	require.Positive(t, historyRetentionExtendInterval)
}

func TestShouldExtendSnapshotHistory(t *testing.T) {
	changefeeds := &protos.CockroachDBConfig{UseChangefeeds: true}
	noChangefeeds := &protos.CockroachDBConfig{}

	testCases := []struct {
		name     string
		config   *protos.CockroachDBConfig
		qrep     *protos.QRepConfig
		expected bool
	}{
		{
			name:   "initial snapshot pull of a changefeed mirror",
			config: changefeeds,
			qrep: &protos.QRepConfig{
				ParentMirrorName: "my_mirror",
				SnapshotName:     "1712345678901234567.0000000001",
			},
			expected: true,
		},
		{
			name:   "integer HLC timestamp without logical part",
			config: changefeeds,
			qrep: &protos.QRepConfig{
				ParentMirrorName: "my_mirror",
				SnapshotName:     "1712345678901234567",
			},
			expected: true,
		},
		{
			name:   "changefeeds disabled never creates protection",
			config: noChangefeeds,
			qrep: &protos.QRepConfig{
				ParentMirrorName: "my_mirror",
				SnapshotName:     "1712345678901234567.0000000001",
			},
			expected: false,
		},
		{
			name:   "standalone qrep flow has no parent mirror",
			config: changefeeds,
			qrep: &protos.QRepConfig{
				SnapshotName: "1712345678901234567.0000000001",
			},
			expected: false,
		},
		{
			name:   "no snapshot name means no pinned timestamp",
			config: changefeeds,
			qrep: &protos.QRepConfig{
				ParentMirrorName: "my_mirror",
			},
			expected: false,
		},
		{
			name:   "non HLC snapshot name",
			config: changefeeds,
			qrep: &protos.QRepConfig{
				ParentMirrorName: "my_mirror",
				SnapshotName:     "00000003-0000001B-1",
			},
			expected: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, shouldExtendSnapshotHistory(tc.config, tc.qrep))
		})
	}
}

func TestAcquireHistoryExtendSlot(t *testing.T) {
	const interval = int64(historyRetentionExtendInterval)
	base := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC).UnixNano()

	var last atomic.Int64
	require.True(t, acquireHistoryExtendSlot(&last, base),
		"zero initial stamp should let the first pull claim a slot")
	require.False(t, acquireHistoryExtendSlot(&last, base),
		"a second pull at the same instant must be throttled")
	require.False(t, acquireHistoryExtendSlot(&last, base+interval-1),
		"a pull just inside the interval must be throttled")
	require.True(t, acquireHistoryExtendSlot(&last, base+interval),
		"a pull one full interval later should claim a slot")
	require.Equal(t, base+interval, last.Load())

	// concurrent partition pulls at the same instant: exactly one claims the slot
	var fresh atomic.Int64
	var wg sync.WaitGroup
	var claimed atomic.Int32
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if acquireHistoryExtendSlot(&fresh, base) {
				claimed.Add(1)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int32(1), claimed.Load())
}
