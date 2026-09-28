//go:build tilt

package mongo_clickhouse

import (
	"testing"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
)

func TestApiMongo(t *testing.T) {
	e2e.RunApiSuite(t, e2e.SetupMongo)
}
