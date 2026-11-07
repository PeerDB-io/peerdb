//go:build tilt

package postgres_clickhouse

import (
	"strings"
	"testing"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/internal/testhooks"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func TestFaultShard(t *testing.T) {
	setup := e2e.SetupClickHouseSuite(t, false, func(t *testing.T) (*e2e.PostgresSource, string, error) {
		t.Helper()
		suffix := "pgch_" + strings.ToLower(common.RandomString(8))
		source, err := e2e.SetupPostgres(t, suffix)
		return source, suffix, err
	})
	e2e.RunFaultShards(t, testhooks.WorkloadPGToCH, setup, e2e.ClickHouseSuite.FaultWorkload)
}
