package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"log/slog"
	"os"
	"runtime"
	"time"

	"github.com/grafana/pyroscope-go"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/PeerDB-io/peer-flow/activities"
	"github.com/PeerDB-io/peer-flow/alerting"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

type WorkerSetupOptions struct {
	TemporalHostPort                   string
	PyroscopeServer                    string
	TemporalNamespace                  string
	TemporalCert                       string
	TemporalKey                        string
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

// newResource returns a resource describing this application.
func newOtelResource(otelServiceName string) (*resource.Resource, error) {
	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(otelServiceName),
		),
	)

	return r, err
}

func setupOtelMetricsExporter() (*sdkmetric.MeterProvider, error) {
	metricExporter, err := otlpmetrichttp.New(context.Background(),
		otlpmetrichttp.WithCompression(otlpmetrichttp.GzipCompression),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenTelemetry metrics exporter: %w", err)
	}

	resource, err := newOtelResource("flow-worker")
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenTelemetry resource: %w", err)
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter,
			sdkmetric.WithInterval(1*time.Minute))),
		sdkmetric.WithResource(resource),
	)
	return meterProvider, nil
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

	if opts.TemporalCert != "" && opts.TemporalKey != "" {
		slog.Info("Using temporal certificate/key for authentication")
		certs, err := base64DecodeCertAndKey(opts.TemporalCert, opts.TemporalKey)
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

	var metricsProvider *sdkmetric.MeterProvider
	var otelManager *activities.OtelManager
	if opts.EnableOtelMetrics {
		metricsProvider, err = setupOtelMetricsExporter()
		if err != nil {
			return nil, err
		}
		otelManager = &activities.OtelManager{
			MetricsProvider:            metricsProvider,
			SlotLagMeter:               metricsProvider.Meter("flow-worker/cdc/slot-lag"),
			OpenConnectionsMeter:       metricsProvider.Meter("flow-worker/open-connections"),
			SlotLagGaugesCache:         make(map[string]*shared.Float64Gauge),
			OpenConnectionsGaugesCache: make(map[string]*shared.Int64Gauge),
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
			if otelManager != nil {
				err := otelManager.MetricsProvider.Shutdown(context.Background())
				if err != nil {
					slog.Error("Failed to shutdown metrics provider", slog.Any("error", err))
				}
			}
			c.Close()
		},
	}, nil
}
