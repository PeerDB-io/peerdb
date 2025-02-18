package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"

	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peerdb/flow/activities"
	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/peerdbenv"
	"github.com/PeerDB-io/peerdb/flow/shared"
	peerflow "github.com/PeerDB-io/peerdb/flow/workflows"
)

type SnapshotWorkerOptions struct {
	TemporalHostPort  string
	TemporalNamespace string
	EnableOtelMetrics bool
}

func SnapshotWorkerMain(opts *SnapshotWorkerOptions) (*WorkerSetupResponse, error) {
	clientOptions := client.Options{
		HostPort:  opts.TemporalHostPort,
		Namespace: opts.TemporalNamespace,
		Logger:    slog.New(shared.NewSlogHandler(slog.NewJSONHandler(os.Stdout, nil))),
		ContextPropagators: []workflow.ContextPropagator{
			internal.NewContextPropagator[*protos.FlowContextMetadata](internal.FlowMetadataKey),
		},
	}

	if opts.EnableOtelMetrics {
		metricsProvider, metricsErr := otel_metrics.SetupTemporalMetricsProvider("flow-snapshot-worker")
		if metricsErr != nil {
			return nil, metricsErr
		}
		clientOptions.MetricsHandler = temporalotel.NewMetricsHandler(temporalotel.MetricsHandlerOptions{
			Meter: metricsProvider.Meter("temporal-sdk-go"),
		})
	}

	if peerdbenv.PeerDBTemporalEnableCertAuth() {
		slog.Info("Using temporal certificate/key for authentication")
		certs, err := parseTemporalCertAndKey(context.Background())
		if err != nil {
			return nil, fmt.Errorf("unable to process certificate and key: %w", err)
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
		return nil, fmt.Errorf("unable to create catalog connection pool: %w", err)
	}

	c, err := client.Dial(clientOptions)
	if err != nil {
		return nil, fmt.Errorf("unable to create Temporal client: %w", err)
	}

	taskQueue := peerdbenv.PeerFlowTaskQueueName(shared.SnapshotFlowTaskQueue)
	w := worker.New(c, taskQueue, worker.Options{
		EnableSessionWorker: true,
		OnFatalError: func(err error) {
			slog.Error("Snapshot Worker failed", slog.Any("error", err))
		},
	})

	var otelManager *otel_metrics.OtelManager
	if opts.EnableOtelMetrics {
		otelManager, err = otel_metrics.NewOtelManager()
		if err != nil {
			return nil, fmt.Errorf("unable to create otel manager: %w", err)
		}
	}

	w.RegisterWorkflow(peerflow.SnapshotFlowWorkflow)
	// explicitly not initializing mutex, in line with design
	w.RegisterActivity(&activities.SnapshotActivity{
		SlotSnapshotStates: make(map[string]activities.SlotSnapshotState),
		TxSnapshotStates:   make(map[string]activities.TxSnapshotState),
		Alerter:            alerting.NewAlerter(context.Background(), conn, otelManager),
		CatalogPool:        conn,
	})

	return &WorkerSetupResponse{
		Client:      c,
		Worker:      w,
		OtelManager: otelManager,
	}, nil
}
