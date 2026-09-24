package postgres_other

import (
	"testing"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
)

func TestGenericSF(t *testing.T) {
	t.Skip("snowflake payment expired")
	e2eshared.RunSuite(t, e2e.SetupGenericSuite(SetupSnowflakeSuite))
}

func TestGenericBQ(t *testing.T) {
	e2eshared.RunSuite(t, e2e.SetupGenericSuite(SetupBigquerySuite))
}
