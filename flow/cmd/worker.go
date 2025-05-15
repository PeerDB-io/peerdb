package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // Import pprof
	"os"
	"runtime"

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
	TemporalNamespace                  string
	TemporalMaxConcurrentActivities    int
	TemporalMaxConcurrentWorkflowTasks int
	EnableProfiling                    bool
	EnableOtelMetrics                  bool
	UseMaintenanceTaskQueue            bool
	PprofPort                          string // Port for pprof HTTP server
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

func setupPprof(opts *WorkerSetupOptions) {
	// Set default pprof port if not specified
	pprofPort := opts.PprofPort
	if pprofPort == "" {
		pprofPort = "6060"
	}

	// Enable mutex and block profiling
	runtime.SetMutexProfileFraction(5)
	runtime.SetBlockProfileRate(5)

	// Start HTTP server with pprof endpoints
	go func() {
		pprofAddr := fmt.Sprintf("localhost:%s", pprofPort)
		slog.Info(fmt.Sprintf("Starting pprof HTTP server on %s", pprofAddr))
		if err := http.ListenAndServe(pprofAddr, nil); err != nil {
			log.Fatal(fmt.Sprintf("Failed to start pprof HTTP server: %v", err))
		}
	}()
}

func WorkerSetup(ctx context.Context, opts *WorkerSetupOptions) (*WorkerSetupResponse, error) {
	if opts.EnableProfiling {
		setupPprof(opts)
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
