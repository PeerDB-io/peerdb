package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/PeerDB-io/peer-flow/activities"
	"github.com/PeerDB-io/peer-flow/alerting"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

type SnapshotWorkerOptions struct {
	TemporalHostPort  string
	TemporalNamespace string
}

func SnapshotWorkerMain(opts *SnapshotWorkerOptions) (client.Client, worker.Worker, error) {
	clientOptions := client.Options{
		HostPort:  opts.TemporalHostPort,
		Namespace: opts.TemporalNamespace,
		Logger:    slog.New(shared.NewSlogHandler(slog.NewJSONHandler(os.Stdout, nil))),
	}

	if peerdbenv.PeerDBTemporalEnableCertAuth() {
		slog.Info("Using temporal certificate/key for authentication")
		certs, err := parseTemporalCertAndKey(context.Background())
		if err != nil {
			return nil, nil, fmt.Errorf("unable to process certificate and key: %w", err)
		}

		connOptions := client.ConnectionOptions{
			TLS: &tls.Config{
				Certificates: certs,
				MinVersion:   tls.VersionTLS13,
			},
		}
		clientOptions.ConnectionOptions = connOptions
	}

	conn, err := peerdbenv.GetCatalogConnectionPoolFromEnv(context.Background())
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create catalog connection pool: %w", err)
	}

	c, err := client.Dial(clientOptions)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create Temporal client: %w", err)
	}

	taskQueue := peerdbenv.PeerFlowTaskQueueName(shared.SnapshotFlowTaskQueue)
	w := worker.New(c, taskQueue, worker.Options{
		EnableSessionWorker: true,
		OnFatalError: func(err error) {
			slog.Error("Snapshot Worker failed", slog.Any("error", err))
		},
	})

	w.RegisterWorkflow(peerflow.SnapshotFlowWorkflow)
	// explicitly not initializing mutex, in line with design
	w.RegisterActivity(&activities.SnapshotActivity{
		SlotSnapshotStates: make(map[string]activities.SlotSnapshotState),
		TxSnapshotStates:   make(map[string]activities.TxSnapshotState),
		Alerter:            alerting.NewAlerter(context.Background(), conn),
		CatalogPool:        conn,
	})

	return c, w, nil
}
