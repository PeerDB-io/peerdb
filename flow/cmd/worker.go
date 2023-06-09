package main

import (
	"fmt"
	"net/http"
	"time"

	//nolint:gosec
	_ "net/http/pprof"

	"github.com/PeerDB-io/peer-flow/activities"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"

	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

type WorkerOptions struct {
	TemporalHostPort string
	EnableProfiling  bool
	ProfilingServer  string
}

func WorkerMain(opts *WorkerOptions) error {
	if opts.EnableProfiling {
		// Start HTTP profiling server with timeouts
		go func() {
			server := http.Server{
				Addr:         opts.ProfilingServer,
				ReadTimeout:  5 * time.Minute,
				WriteTimeout: 15 * time.Minute,
			}

			log.Infof("starting profiling server on %s", opts.ProfilingServer)

			if err := server.ListenAndServe(); err != nil {
				log.Errorf("unable to start profiling server: %v", err)
			}
		}()
	}

	c, err := client.Dial(client.Options{
		HostPort: opts.TemporalHostPort,
	})
	if err != nil {
		return fmt.Errorf("unable to create Temporal client: %w", err)
	}
	defer c.Close()

	w := worker.New(c, shared.PeerFlowTaskQueue, worker.Options{})
	w.RegisterWorkflow(peerflow.PeerFlowWorkflow)
	w.RegisterWorkflow(peerflow.PeerFlowWorkflowWithConfig)
	w.RegisterWorkflow(peerflow.SyncFlowWorkflow)
	w.RegisterWorkflow(peerflow.SetupFlowWorkflow)
	w.RegisterWorkflow(peerflow.NormalizeFlowWorkflow)
	w.RegisterWorkflow(peerflow.QRepFlowWorkflow)
	w.RegisterWorkflow(peerflow.QRepPartitionWorkflow)
	w.RegisterWorkflow(peerflow.DropFlowWorkflow)
	w.RegisterActivity(&activities.FetchConfigActivity{})
	w.RegisterActivity(&activities.FlowableActivity{})

	err = w.Run(worker.InterruptCh())
	if err != nil {
		return fmt.Errorf("worker run error: %w", err)
	}

	return nil
}
