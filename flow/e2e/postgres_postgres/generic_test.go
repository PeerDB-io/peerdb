package postgres_postgres

import (
	"testing"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
)

func TestGenericPG(t *testing.T) {
	e2eshared.RunSuite(t, e2e.SetupGenericSuite(SetupPostgresSuite))
}
