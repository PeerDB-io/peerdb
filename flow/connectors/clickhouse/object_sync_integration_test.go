//go:build integration

package connclickhouse

import (
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/testutil"
)

func TestFetchURLBatchLimits(t *testing.T) {
	ctx := t.Context()

	conn, err := NewClickHouseConnector(ctx, nil, &protos.ClickhouseConfig{
		Host:       testutil.ClickHouseTestHost(),
		Port:       testutil.ClickHouseTestPort(),
		Database:   "default",
		DisableTls: true,
	})
	require.NoError(t, err)
	defer conn.Close()

	// Baseline: whatever the server's effective defaults are.
	baseline, err := conn.fetchURLBatchLimits(ctx)
	require.NoError(t, err)
	require.Positive(t, baseline.maxURLBytes)
	require.Positive(t, baseline.maxURLCount)

	// Override the two settings for this query only. Values are deliberately distinct
	// from any plausible server default so the assertions can't pass by coincidence.
	const overrideMaxQuerySize int64 = 128 * 1024
	const overrideRemoteMaxAddresses int64 = 42
	overrideCtx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"max_query_size":                      overrideMaxQuerySize,
		"table_function_remote_max_addresses": overrideRemoteMaxAddresses,
	}))

	overridden, err := conn.fetchURLBatchLimits(overrideCtx)
	require.NoError(t, err)
	require.Equal(t, overrideMaxQuerySize*3/4, overridden.maxURLBytes)
	require.Equal(t, int(overrideRemoteMaxAddresses*9/10), overridden.maxURLCount)
	require.NotEqual(t, baseline, overridden, "override should change the computed limits")

	// The override was scoped to overrideCtx; a plain query still sees the baseline,
	// proving the setting did not leak into the shared connection (and other tests).
	after, err := conn.fetchURLBatchLimits(ctx)
	require.NoError(t, err)
	require.Equal(t, baseline, after)
}
