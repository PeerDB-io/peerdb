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
	utils "github.com/PeerDB-io/peer-flow/connectors/utils/catalog"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/PeerDB-io/peer-flow/shared/alerting"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
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
		Logger:    slog.New(logger.NewHandler(slog.NewJSONHandler(os.Stdout, nil))),
	}

	if opts.TemporalCert != "" && opts.TemporalKey != "" {
		certs, err := Base64DecodeCertAndKey(opts.TemporalCert, opts.TemporalKey)
		if err != nil {
			return fmt.Errorf("unable to process certificate and key: %w", err)
		}

		connOptions := client.ConnectionOptions{
			TLS: &tls.Config{
				Certificates: certs,
				MinVersion:   tls.VersionTLS13,
			},
		}
		clientOptions.ConnectionOptions = connOptions
	}

	c, err := client.Dial(clientOptions)
	if err != nil {
		return fmt.Errorf("unable to create Temporal client: %w", err)
	}
	defer c.Close()

	taskQueue, queueErr := shared.GetPeerFlowTaskQueueName(shared.SnapshotFlowTaskQueueID)
	if queueErr != nil {
		return queueErr
	}

	w := worker.New(c, taskQueue, worker.Options{
		EnableSessionWorker: true,
	})

	conn, err := utils.GetCatalogConnectionPoolFromEnv(context.Background())
	if err != nil {
		return fmt.Errorf("unable to create catalog connection pool: %w", err)
	}

	alerter, err := alerting.NewAlerter(conn)
	if err != nil {
		return fmt.Errorf("unable to create alerter: %w", err)
	}

	w.RegisterWorkflow(peerflow.SnapshotFlowWorkflow)
	w.RegisterActivity(&activities.SnapshotActivity{
		SnapshotConnections: make(map[string]activities.SlotSnapshotSignal),
		Alerter:             alerter,
	})

	err = w.Run(worker.InterruptCh())
	if err != nil {
		return fmt.Errorf("worker run error: %w", err)
	}

	return nil
}
