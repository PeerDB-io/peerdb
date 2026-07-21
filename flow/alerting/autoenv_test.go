package alerting

import (
	"github.com/PeerDB-io/peerdb/flow/internal/testutil"
)

// The error-classification tests connect to the catalog to produce real
// postgres errors, so load connection settings from the project-root .env
// like the other test suites do.
func init() {
	testutil.LoadEnv()
}
