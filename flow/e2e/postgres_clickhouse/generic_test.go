//go:build tilt

package postgres_clickhouse

import (
	"strings"
	"testing"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func TestGenericCH_PG(t *testing.T) {
	e2eshared.RunSuite(t, e2e.SetupGenericSuite(e2e.SetupClickHouseSuite(t, false, func(t *testing.T) (*e2e.PostgresSource, string, error) {
		t.Helper()
		suffix := "pgchg_" + strings.ToLower(common.RandomString(8))
		source, err := e2e.SetupPostgres(t, suffix)
		return source, suffix, err
	})))
}

func TestGenericChCluster_PG(t *testing.T) {
	e2eshared.RunSuite(t, e2e.SetupGenericSuite(e2e.SetupClickHouseSuite(t, true, func(t *testing.T) (*e2e.PostgresSource, string, error) {
		t.Helper()
		suffix := "pgchclg_" + strings.ToLower(common.RandomString(8))
		source, err := e2e.SetupPostgres(t, suffix)
		return source, suffix, err
	})))
}
