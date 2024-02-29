package e2e

import (
	"os"
	"testing"

	"github.com/PeerDB-io/peer-flow/cmd"
)

func TestMain(m *testing.M) {
	peerWorker, peerErr := cmd.WorkerMain(&cmd.WorkerOptions{
		TemporalHostPort:  "localhost:7233",
		EnableProfiling:   false,
		PyroscopeServer:   "",
		TemporalNamespace: "default",
		TemporalCert:      "",
		TemporalKey:       "",
	})
	if peerErr != nil {
		panic(peerErr)
	} else if err := peerWorker.Start(); err != nil {
		panic(err)
	}

	snapWorker, snapErr := cmd.SnapshotWorkerMain(&cmd.SnapshotWorkerOptions{
		TemporalHostPort:  "localhost:7233",
		TemporalNamespace: "default",
		TemporalCert:      "",
		TemporalKey:       "",
	})
	if snapErr != nil {
		panic(snapErr)
	} else if err := snapWorker.Start(); err != nil {
		panic(err)
	}

	os.Exit(m.Run())
}
