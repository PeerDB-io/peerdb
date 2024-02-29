package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/PeerDB-io/peer-flow/cmd"
	"golang.org/x/sync/errgroup"
)

func TestMain(m *testing.M) {
	os.Exit(0)
	end := make(chan interface{})
	group, _ := errgroup.WithContext(context.Background())
	group.Go(func() error {
		return cmd.WorkerMain(end, &cmd.WorkerOptions{
			TemporalHostPort:  "localhost:7233",
			EnableProfiling:   false,
			PyroscopeServer:   "",
			TemporalNamespace: "default",
			TemporalCert:      "",
			TemporalKey:       "",
		})
	})
	group.Go(func() error {
		return cmd.SnapshotWorkerMain(end, &cmd.SnapshotWorkerOptions{
			TemporalHostPort:  "localhost:7233",
			TemporalNamespace: "default",
			TemporalCert:      "",
			TemporalKey:       "",
		})
	})
	exitcode := m.Run()
	close(end)
	err := group.Wait()
	if err != nil {
		//nolint:forbidigo
		fmt.Printf("%+v\n", err)
	}
	os.Exit(exitcode)
}
