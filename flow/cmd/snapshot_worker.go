package main

import (
	"crypto/tls"
	"fmt"

	"github.com/PeerDB-io/peer-flow/activities"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

type SnapshotWorkerOptions struct {
	TemporalHostPort  string
	TemporalNamespace string
	TemporalCert      string
	TemporalKey       string
}

func SnapshotWorkerMain(opts *SnapshotWorkerOptions) error {
	clientOptions := client.Options{
		HostPort:  opts.TemporalHostPort,
		Namespace: opts.TemporalNamespace,
	}

	if opts.TemporalCert != "" && opts.TemporalKey != "" {
		certs, err := ProcessCertAndKey(opts.TemporalCert, opts.TemporalKey)
		if err != nil {
			return fmt.Errorf("unable to process certificate and key: %w", err)
		}

		connOptions := client.ConnectionOptions{
			TLS: &tls.Config{Certificates: certs},
		}
		clientOptions.ConnectionOptions = connOptions
	}

	c, err := client.Dial(clientOptions)
	if err != nil {
		return fmt.Errorf("unable to create Temporal client: %w", err)
	}
	defer c.Close()

	w := worker.New(c, shared.SnapshotFlowTaskQueue, worker.Options{
		EnableSessionWorker: true,
	})
	w.RegisterWorkflow(peerflow.SnapshotFlowWorkflow)
	w.RegisterActivity(&activities.SnapshotActivity{})

	err = w.Run(worker.InterruptCh())
	if err != nil {
		return fmt.Errorf("worker run error: %w", err)
	}

	return nil
}
