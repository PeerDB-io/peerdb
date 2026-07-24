package e2e

import (
	"testing"

	"github.com/PeerDB-io/peerdb/flow/e2eshared"
)

func Test_OpenSearch(t *testing.T) {
	e2eshared.RunSuite(t, SetupOpenSearchSuite)
}
