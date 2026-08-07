package conncockroachdb

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
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
	require.False(t, isUnknownSettingError(nil))
}

func TestIsCockroachCloudHost(t *testing.T) {
	require.True(t, isCockroachCloudHost("banko-ai-assistant-standard-cc-8272.jxf.cockroachlabs.cloud"))
	require.True(t, isCockroachCloudHost("MY-CLUSTER.AWS-US-EAST-1.COCKROACHLABS.CLOUD"))
	require.False(t, isCockroachCloudHost("localhost"))
	require.False(t, isCockroachCloudHost("cockroachlabs.cloud"))
	require.False(t, isCockroachCloudHost("db.internal.example.com"))
	require.False(t, isCockroachCloudHost("evil.cockroachlabs.cloud.example.com"))
}
