package e2e

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/PeerDB-io/peer-flow/cmd"
	"github.com/PeerDB-io/peer-flow/logger"
)

// TestMain would require merging all tests into e2e package
func init() {
	fmt.Println("INIT BEGIN")
	slog.SetDefault(slog.New(logger.NewHandler(slog.NewJSONHandler(os.Stdout, nil))))

	_, peerWorker, peerErr := cmd.WorkerMain(&cmd.WorkerOptions{
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
	fmt.Println("INIT PEERFLOW DONE")

	_, snapWorker, snapErr := cmd.SnapshotWorkerMain(&cmd.SnapshotWorkerOptions{
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
	fmt.Println("INIT SNAPSHOT DONE")
}
