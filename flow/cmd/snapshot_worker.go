package main

import (
	"crypto/tls"
	"fmt"

	"github.com/PeerDB-io/peer-flow/activities"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
	log "github.com/sirupsen/logrus"

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
		cert, err := tls.X509KeyPair([]byte(opts.TemporalCert), []byte(opts.TemporalKey))
		if err != nil {
			log.Fatalln("Unable to load cert and key pair.", err)
		}

		connOptions := client.ConnectionOptions{
			TLS: &tls.Config{Certificates: []tls.Certificate{cert}},
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
