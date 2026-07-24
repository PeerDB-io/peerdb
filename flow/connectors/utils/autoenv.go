package utils

import (
	"testing"

	"github.com/PeerDB-io/peerdb/flow/internal/testutil"
)

func init() {
	if testing.Testing() {
		testutil.LoadEnv()
	}
}
