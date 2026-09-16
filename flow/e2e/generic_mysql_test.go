package e2e

import (
	"strings"
	"testing"

	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func TestGenericCH_MySQL(t *testing.T) {
	e2eshared.RunSuite(t, SetupGenericSuite(SetupClickHouseSuite(t, false, func(t *testing.T) (*MySqlSource, string, error) {
		t.Helper()
		suffix := "mychg_" + strings.ToLower(common.RandomString(8))
		source, err := SetupMySQL(t, suffix)
		return source, suffix, err
	})))
}

func TestGenericCH_MariaDB(t *testing.T) {
	e2eshared.RunSuite(t, SetupGenericSuite(SetupClickHouseSuite(t, false, func(t *testing.T) (*MySqlSource, string, error) {
		t.Helper()
		suffix := "machg_" + strings.ToLower(common.RandomString(8))
		source, err := SetupMariaDB(t, suffix)
		return source, suffix, err
	})))
}

func TestGenericChCluster_MySQL(t *testing.T) {
	e2eshared.RunSuite(t, SetupGenericSuite(SetupClickHouseSuite(t, true, func(t *testing.T) (*MySqlSource, string, error) {
		t.Helper()
		suffix := "mychclg_" + strings.ToLower(common.RandomString(8))
		source, err := SetupMySQL(t, suffix)
		return source, suffix, err
	})))
}

func TestGenericChCluster_MariaDB(t *testing.T) {
	e2eshared.RunSuite(t, SetupGenericSuite(SetupClickHouseSuite(t, true, func(t *testing.T) (*MySqlSource, string, error) {
		t.Helper()
		suffix := "machclg_" + strings.ToLower(common.RandomString(8))
		source, err := SetupMariaDB(t, suffix)
		return source, suffix, err
	})))
}
