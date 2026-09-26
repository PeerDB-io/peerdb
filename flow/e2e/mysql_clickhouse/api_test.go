//go:build tilt

package mysql_clickhouse

import (
	"testing"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
)

func TestApiMy(t *testing.T) {
	e2e.RunApiSuite(t, e2e.SetupMySQL)
}
