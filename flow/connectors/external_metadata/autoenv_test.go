package connmetadata

import (
	"github.com/PeerDB-io/peerdb/flow/internal/testutil"
)

// The store tests connect to the catalog, so load connection settings from
// the project-root .env like the other test suites do.
func init() {
	testutil.LoadEnv()
}
