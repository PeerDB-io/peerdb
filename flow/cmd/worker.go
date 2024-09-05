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
	"go.temporal.io/sdk/worker"

	"github.com/PeerDB-io/peer-flow/activities"
	"github.com/PeerDB-io/peer-flow/alerting"
	"github.com/PeerDB-io/peer-flow/logger"
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
}

type workerSetupResponse struct {
	Client  client.Client
	Worker  worker.Worker
	Cleanup func()
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
		Logger:    slog.New(logger.NewHandler(slog.NewJSONHandler(os.Stdout, nil))),
	}

	if peerdbenv.PeerDBTemporalEnableCertAuth() {
		slog.Info("Using temporal certificate/key for authentication")
		certs, err := parseTemporalCertAndKey()
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

	taskQueue := peerdbenv.PeerFlowTaskQueueName(shared.PeerFlowTaskQueue)
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

	cleanupOtelManagerFunc := func() {}
	var otelManager *otel_metrics.OtelManager
	if opts.EnableOtelMetrics {
		metricsProvider, metricErr := otel_metrics.SetupOtelMetricsExporter("flow-worker")
		if metricErr != nil {
			return nil, metricErr
		}
		otelManager = &otel_metrics.OtelManager{
			MetricsProvider:    metricsProvider,
			Meter:              metricsProvider.Meter("io.peerdb.flow-worker"),
			Float64GaugesCache: make(map[string]*otel_metrics.Float64SyncGauge),
			Int64GaugesCache:   make(map[string]*otel_metrics.Int64SyncGauge),
		}
		cleanupOtelManagerFunc = func() {
			shutDownErr := otelManager.MetricsProvider.Shutdown(context.Background())
			if shutDownErr != nil {
				slog.Error("Failed to shutdown metrics provider", slog.Any("error", shutDownErr))
			}
		}
	}
	w.RegisterActivity(&activities.FlowableActivity{
		CatalogPool: conn,
		Alerter:     alerting.NewAlerter(context.Background(), conn),
		CdcCache:    make(map[string]activities.CdcCacheEntry),
		OtelManager: otelManager,
	})

	return &workerSetupResponse{
		Client: c,
		Worker: w,
		Cleanup: func() {
			cleanupOtelManagerFunc()
			c.Close()
		},
	}, nil
}
