package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/PeerDB-io/peer-flow/cmd"
)

func TestMain(m *testing.M) {
	end := make(chan interface{})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	group, _ := errgroup.WithContext(ctx)
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
	end <- os.Interrupt
	close(end)
	err := group.Wait()
	cancel()
	if err != nil {
		//nolint:forbidigo
		fmt.Printf("%+v\n", err)
	}
	os.Exit(exitcode)
}
