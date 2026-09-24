package db

import (
	"github.com/PeerDB-io/peerdb/flow/pkg/testutil"
)

// The migration tests connect to the catalog to create scratch databases, so
// load connection settings from the project-root .env like the other test suites do.
func init() {
	testutil.LoadEnv()
}
