package main

import (
	"fmt"

	"github.com/PeerDB-io/peer-flow/activities"
	"github.com/PeerDB-io/peer-flow/shared"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

type SnapshotWorkerOptions struct {
	TemporalHostPort string
}

func SnapshotWorkerMain(opts *SnapshotWorkerOptions) error {
	clientOptions := client.Options{
		HostPort: opts.TemporalHostPort,
	}

	c, err := client.Dial(clientOptions)
	if err != nil {
		return fmt.Errorf("unable to create Temporal client: %w", err)
	}
	defer c.Close()

	w := worker.New(c, shared.PeerFlowTaskQueue, worker.Options{
		EnableSessionWorker: true,
	})
	w.RegisterActivity(&activities.SnapshotActivity{})

	err = w.Run(worker.InterruptCh())
	if err != nil {
		return fmt.Errorf("worker run error: %w", err)
	}

	return nil
}
