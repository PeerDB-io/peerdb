package model

import (
	"github.com/PeerDB-io/peerdb/flow/internal/testutil"
)

// Tests in this package reach the catalog through dynamic-setting lookups,
// so load connection settings from the project-root .env like the other
// test suites do.
func init() {
	testutil.LoadEnv()
}
