//go:build postgres

package e2e

import (
	"strings"
	"testing"

	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func TestGenericPG(t *testing.T) {
	e2eshared.RunSuite(t, SetupGenericSuite(SetupPostgresSuite))
}

func TestGenericSF(t *testing.T) {
	t.Skip("snowflake payment expired")
	e2eshared.RunSuite(t, SetupGenericSuite(SetupSnowflakeSuite))
}

func TestGenericBQ(t *testing.T) {
	e2eshared.RunSuite(t, SetupGenericSuite(SetupBigquerySuite))
}

func TestGenericCH_PG(t *testing.T) {
	e2eshared.RunSuite(t, SetupGenericSuite(SetupClickHouseSuite(t, false, func(t *testing.T) (*PostgresSource, string, error) {
		t.Helper()
		suffix := "pgchg_" + strings.ToLower(common.RandomString(8))
		source, err := SetupPostgres(t, suffix)
		return source, suffix, err
	})))
}

func TestGenericChCluster_PG(t *testing.T) {
	e2eshared.RunSuite(t, SetupGenericSuite(SetupClickHouseSuite(t, true, func(t *testing.T) (*PostgresSource, string, error) {
		t.Helper()
		suffix := "pgchclg_" + strings.ToLower(common.RandomString(8))
		source, err := SetupPostgres(t, suffix)
		return source, suffix, err
	})))
}
