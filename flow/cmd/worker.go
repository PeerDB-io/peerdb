package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"log/slog"
	"os"
	"runtime"

	"github.com/grafana/pyroscope-go"
	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peerdb/flow/activities"
	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
	peerflow "github.com/PeerDB-io/peerdb/flow/workflows"
)

type WorkerSetupOptions struct {
	TemporalHostPort                   string
	PyroscopeServer                    string
	TemporalNamespace                  string
	TemporalMaxConcurrentActivities    int
	TemporalMaxConcurrentWorkflowTasks int
	EnableProfiling                    bool
	EnableOtelMetrics                  bool
	UseMaintenanceTaskQueue            bool
}

type WorkerSetupResponse struct {
	Client      client.Client
	Worker      worker.Worker
	OtelManager *otel_metrics.OtelManager
}

func (w *WorkerSetupResponse) Close(ctx context.Context) {
	slog.Info("Shutting down worker")
	w.Client.Close()
	if err := w.OtelManager.Close(ctx); err != nil {
		slog.Error("Failed to shutdown metrics provider", slog.Any("error", err))
	}
}

func setupPyroscope(opts *WorkerSetupOptions) {
	if opts.PyroscopeServer == "" {
		log.Fatal("pyroscope server address is not set but profiling is enabled")
	}

	// measure contention
	runtime.SetMutexProfileFraction(5)
	runtime.SetBlockProfileRate(5)

	_, err := pyroscope.Start(pyroscope.Config{
		ApplicationName: "io.peerdb.flow_worker",

		ServerAddress: opts.PyroscopeServer,

		// you can disable logging by setting this to nil
		Logger: nil,

		// you can provide static tags via a map:
		Tags: map[string]string{"hostname": os.Getenv("HOSTNAME")},

		ProfileTypes: []pyroscope.ProfileType{
			// these profile types are enabled by default:
			pyroscope.ProfileCPU,
			pyroscope.ProfileAllocObjects,
			pyroscope.ProfileAllocSpace,
			pyroscope.ProfileInuseObjects,
			pyroscope.ProfileInuseSpace,

			// these profile types are optional:
			pyroscope.ProfileGoroutines,
			pyroscope.ProfileMutexCount,
			pyroscope.ProfileMutexDuration,
			pyroscope.ProfileBlockCount,
			pyroscope.ProfileBlockDuration,
		},
	})
	if err != nil {
		log.Fatal(err)
	}
}

func WorkerSetup(ctx context.Context, opts *WorkerSetupOptions) (*WorkerSetupResponse, error) {
	if opts.EnableProfiling {
		setupPyroscope(opts)
	}

	clientOptions := client.Options{
		HostPort:  opts.TemporalHostPort,
		Namespace: opts.TemporalNamespace,
		Logger:    slog.New(shared.NewSlogHandler(slog.NewJSONHandler(os.Stdout, nil))),
		ContextPropagators: []workflow.ContextPropagator{
			internal.NewContextPropagator[*protos.FlowContextMetadata](internal.FlowMetadataKey),
		},
	}

	metricsProvider, metricsErr := otel_metrics.SetupTemporalMetricsProvider(
		ctx, otel_metrics.FlowWorkerServiceName, opts.EnableOtelMetrics,
	)
	if metricsErr != nil {
		return nil, metricsErr
	}
	clientOptions.MetricsHandler = temporalotel.NewMetricsHandler(temporalotel.MetricsHandlerOptions{
		Meter: metricsProvider.Meter("temporal-sdk-go"),
	})

	if internal.PeerDBTemporalEnableCertAuth() {
		slog.Info("Using temporal certificate/key for authentication")
		certs, err := parseTemporalCertAndKey(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to process certificate and key: %w", err)
		}
		clientOptions.ConnectionOptions = client.ConnectionOptions{
			TLS: &tls.Config{
				Certificates: certs,
				MinVersion:   tls.VersionTLS13,
			},
		}
	}

	conn, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to create catalog connection pool: %w", err)
	}

	c, err := client.Dial(clientOptions)
	if err != nil {
		return nil, fmt.Errorf("unable to create Temporal client: %w", err)
	}
	slog.Info("Created temporal client")
	queueId := shared.PeerFlowTaskQueue
	if opts.UseMaintenanceTaskQueue {
		queueId = shared.MaintenanceFlowTaskQueue
	}
	taskQueue := internal.PeerFlowTaskQueueName(queueId)
	slog.Info(
		fmt.Sprintf("Creating temporal worker for queue %v: %v workflow workers %v activity workers",
			taskQueue,
			opts.TemporalMaxConcurrentWorkflowTasks,
			opts.TemporalMaxConcurrentActivities,
		),
	)
	w := worker.New(c, taskQueue, worker.Options{
		EnableSessionWorker:                    true,
		MaxConcurrentActivityExecutionSize:     opts.TemporalMaxConcurrentActivities,
		MaxConcurrentWorkflowTaskExecutionSize: opts.TemporalMaxConcurrentWorkflowTasks,
		OnFatalError: func(err error) {
			slog.Error("Peerflow Worker failed", slog.Any("error", err))
		},
	})
	peerflow.RegisterFlowWorkerWorkflows(w)

	otelManager, err := otel_metrics.NewOtelManager(ctx, otel_metrics.FlowWorkerServiceName, opts.EnableOtelMetrics)
	if err != nil {
		return nil, fmt.Errorf("unable to create otel manager: %w", err)
	}

	w.RegisterActivity(&activities.FlowableActivity{
		CatalogPool:    conn,
		Alerter:        alerting.NewAlerter(ctx, conn, otelManager),
		OtelManager:    otelManager,
		TemporalClient: c,
	})

	w.RegisterActivity(&activities.MaintenanceActivity{
		CatalogPool:    conn,
		Alerter:        alerting.NewAlerter(ctx, conn, otelManager),
		OtelManager:    otelManager,
		TemporalClient: c,
	})

	return &WorkerSetupResponse{
		Client:      c,
		Worker:      w,
		OtelManager: otelManager,
	}, nil
}
