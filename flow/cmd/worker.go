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

	"github.com/PeerDB-io/peer-flow/activities"
	"github.com/PeerDB-io/peer-flow/alerting"
	"github.com/PeerDB-io/peer-flow/otel_metrics"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
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

type workerSetupResponse struct {
	Client      client.Client
	Worker      worker.Worker
	OtelManager *otel_metrics.OtelManager
}

func (w *workerSetupResponse) Close() {
	w.Client.Close()
	if w.OtelManager != nil {
		if err := w.OtelManager.Close(context.Background()); err != nil {
			slog.Error("Failed to shutdown metrics provider", slog.Any("error", err))
		}
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

func WorkerSetup(opts *WorkerSetupOptions) (*workerSetupResponse, error) {
	if opts.EnableProfiling {
		setupPyroscope(opts)
	}

	clientOptions := client.Options{
		HostPort:  opts.TemporalHostPort,
		Namespace: opts.TemporalNamespace,
		Logger:    slog.New(shared.NewSlogHandler(slog.NewJSONHandler(os.Stdout, nil))),
	}
	if opts.EnableOtelMetrics {
		metricsProvider, metricsErr := otel_metrics.SetupTemporalMetricsProvider("flow-worker")
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
	slog.Info("Created temporal client")
	queueId := shared.PeerFlowTaskQueue
	if opts.UseMaintenanceTaskQueue {
		queueId = shared.MaintenanceFlowTaskQueue
	}
	taskQueue := peerdbenv.PeerFlowTaskQueueName(queueId)
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

	var otelManager *otel_metrics.OtelManager
	if opts.EnableOtelMetrics {
		otelManager, err = otel_metrics.NewOtelManager()
		if err != nil {
			return nil, fmt.Errorf("unable to create otel manager: %w", err)
		}
	}

	w.RegisterActivity(&activities.FlowableActivity{
		CatalogPool: conn,
		Alerter:     alerting.NewAlerter(context.Background(), conn),
		CdcCache:    make(map[string]activities.CdcCacheEntry),
		OtelManager: otelManager,
	})

	w.RegisterActivity(&activities.MaintenanceActivity{
		CatalogPool:    conn,
		Alerter:        alerting.NewAlerter(context.Background(), conn),
		TemporalClient: c,
	})

	return &workerSetupResponse{
		Client:      c,
		Worker:      w,
		OtelManager: otelManager,
	}, nil
}
