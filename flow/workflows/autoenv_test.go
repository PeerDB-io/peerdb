package peerflow

import (
	"github.com/PeerDB-io/peerdb/flow/pkg/testutil"
)

// The replay fixture generator reaches the catalog through local activities,
// so load connection settings from the project-root .env like the other
// test suites do.
func init() {
	testutil.LoadEnv()
}
