package postgres_clickhouse

import (
	"strings"
	"testing"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func TestPeerFlowE2ETestSuitePG_CH(t *testing.T) {
	e2eshared.RunSuite(t, e2e.SetupClickHouseSuite(t, false, func(t *testing.T) (*e2e.PostgresSource, string, error) {
		t.Helper()
		suffix := "pgch_" + strings.ToLower(common.RandomString(8))
		source, err := e2e.SetupPostgres(t, suffix)
		return source, suffix, err
	}))
}

func TestPeerFlowE2ETestSuitePG_CH_Cluster(t *testing.T) {
	e2eshared.RunSuite(t, e2e.SetupClickHouseSuite(t, true, func(t *testing.T) (*e2e.PostgresSource, string, error) {
		t.Helper()
		suffix := "pgchcl_" + strings.ToLower(common.RandomString(8))
		source, err := e2e.SetupPostgres(t, suffix)
		return source, suffix, err
	}))
}
