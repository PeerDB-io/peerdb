package e2e

import (
	"os"
	"testing"

	"github.com/PeerDB-io/peer-flow/cmd"
)

func TestMain(m *testing.M) {
	//nolint:errcheck
	go cmd.WorkerMain(&cmd.WorkerOptions{
		TemporalHostPort:  "temporal:7233",
		EnableProfiling:   false,
		PyroscopeServer:   "",
		TemporalNamespace: "default",
		TemporalCert:      "",
		TemporalKey:       "",
	})
	//nolint:errcheck
	go cmd.SnapshotWorkerMain(&cmd.SnapshotWorkerOptions{
		TemporalHostPort:  "temporal:7233",
		TemporalNamespace: "default",
		TemporalCert:      "",
		TemporalKey:       "",
	})
	os.Exit(m.Run())
}
