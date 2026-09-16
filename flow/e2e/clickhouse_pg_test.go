//go:build postgres

package e2e

import (
	"strings"
	"testing"

	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func TestPeerFlowE2ETestSuitePG_CH(t *testing.T) {
	e2eshared.RunSuite(t, SetupClickHouseSuite(t, false, func(t *testing.T) (*PostgresSource, string, error) {
		t.Helper()
		suffix := "pgch_" + strings.ToLower(common.RandomString(8))
		source, err := SetupPostgres(t, suffix)
		return source, suffix, err
	}))
}

func TestPeerFlowE2ETestSuitePG_CH_Cluster(t *testing.T) {
	e2eshared.RunSuite(t, SetupClickHouseSuite(t, true, func(t *testing.T) (*PostgresSource, string, error) {
		t.Helper()
		suffix := "pgchcl_" + strings.ToLower(common.RandomString(8))
		source, err := SetupPostgres(t, suffix)
		return source, suffix, err
	}))
}
