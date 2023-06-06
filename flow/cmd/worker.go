package main

import (
	"fmt"

	"github.com/PeerDB-io/peer-flow/activities"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

type WorkerOptions struct {
	TemporalHostPort string
}

func WorkerMain(opts *WorkerOptions) error {
	c, err := client.Dial(client.Options{
		HostPort: opts.TemporalHostPort,
	})
	if err != nil {
		return fmt.Errorf("unable to create Temporal client: %w", err)
	}
	defer c.Close()

	w := worker.New(c, shared.PeerFlowTaskQueue, worker.Options{})
	w.RegisterWorkflow(peerflow.PeerFlowWorkflow)
	w.RegisterWorkflow(peerflow.SyncFlowWorkflow)
	w.RegisterWorkflow(peerflow.SetupFlowWorkflow)
	w.RegisterWorkflow(peerflow.NormalizeFlowWorkflow)
	w.RegisterWorkflow(peerflow.QRepFlowWorkflow)
	w.RegisterWorkflow(peerflow.DropFlowWorkflow)
	w.RegisterActivity(&activities.FetchConfigActivity{})
	w.RegisterActivity(&activities.FlowableActivity{})

	err = w.Run(worker.InterruptCh())
	if err != nil {
		return fmt.Errorf("worker run error: %w", err)
	}

	return nil
}
