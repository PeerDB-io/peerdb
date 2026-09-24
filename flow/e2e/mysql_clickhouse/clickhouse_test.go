package mysql_clickhouse

import (
	"strings"
	"testing"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func runMySQLClickHouseSuite(
	t *testing.T, cluster bool, setupSource func(*testing.T) (*e2e.MySqlSource, string, error),
) {
	t.Helper()
	setup := e2e.SetupClickHouseSuite(t, cluster, setupSource)
	e2eshared.RunSuite(t, func(t *testing.T) MySQLClickHouseSuite {
		t.Helper()
		return MySQLClickHouseSuite{setup(t)}
	})
}

func TestPeerFlowE2ETestSuiteMySQL_CH(t *testing.T) {
	runMySQLClickHouseSuite(t, false, func(t *testing.T) (*e2e.MySqlSource, string, error) {
		t.Helper()
		suffix := "mych_" + strings.ToLower(common.RandomString(8))
		source, err := e2e.SetupMySQL(t, suffix)
		return source, suffix, err
	})
}

func TestPeerFlowE2ETestSuiteMySQL_CH_Cluster(t *testing.T) {
	runMySQLClickHouseSuite(t, true, func(t *testing.T) (*e2e.MySqlSource, string, error) {
		t.Helper()
		suffix := "mychcl_" + strings.ToLower(common.RandomString(8))
		source, err := e2e.SetupMySQL(t, suffix)
		return source, suffix, err
	})
}

func TestPeerFlowE2ETestSuiteMariaDB_CH(t *testing.T) {
	runMySQLClickHouseSuite(t, false, func(t *testing.T) (*e2e.MySqlSource, string, error) {
		t.Helper()
		suffix := "mach_" + strings.ToLower(common.RandomString(8))
		source, err := e2e.SetupMariaDB(t, suffix)
		return source, suffix, err
	})
}

func TestPeerFlowE2ETestSuiteMariaDB_CH_Cluster(t *testing.T) {
	runMySQLClickHouseSuite(t, true, func(t *testing.T) (*e2e.MySqlSource, string, error) {
		t.Helper()
		suffix := "machcl_" + strings.ToLower(common.RandomString(8))
		source, err := e2e.SetupMariaDB(t, suffix)
		return source, suffix, err
	})
}
