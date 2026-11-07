//go:build tilt

package mysql_clickhouse

import (
	"strings"
	"testing"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/internal/testhooks"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// The Test_FaultKnown_* scenarios come from the embedded e2e.ClickHouseSuite
// and run under TestPeerFlowE2ETestSuiteMySQL_CH.
func TestFaultShard(t *testing.T) {
	setup := e2e.SetupClickHouseSuite(t, false, func(t *testing.T) (*e2e.MySqlSource, string, error) {
		t.Helper()
		suffix := "mych_" + strings.ToLower(common.RandomString(8))
		source, err := e2e.SetupMySQL(t, suffix)
		return source, suffix, err
	})
	e2e.RunFaultShards(t, testhooks.WorkloadMySQLToCH, setup, e2e.ClickHouseSuite.FaultWorkload)
}
