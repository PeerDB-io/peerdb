package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/grafana/pyroscope-go"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/PeerDB-io/peer-flow/activities"
	"github.com/PeerDB-io/peer-flow/connectors"
	utils "github.com/PeerDB-io/peer-flow/connectors/utils/catalog"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/PeerDB-io/peer-flow/shared/alerting"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

type WorkerOptions struct {
	TemporalHostPort  string
	EnableProfiling   bool
	PyroscopeServer   string
	TemporalNamespace string
	TemporalCert      string
	TemporalKey       string
}

func setupPyroscope(opts *WorkerOptions) {
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

func WorkerMain(opts *WorkerOptions) error {
	if opts.EnableProfiling {
		setupPyroscope(opts)
	}

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGQUIT)
		buf := make([]byte, 1<<20)
		for {
			<-sigs
			stacklen := runtime.Stack(buf, true)
			log.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
		}
	}()

	clientOptions := client.Options{
		HostPort:  opts.TemporalHostPort,
		Namespace: opts.TemporalNamespace,
		Logger:    slog.New(logger.NewHandler(slog.NewJSONHandler(os.Stdout, nil))),
	}

	if opts.TemporalCert != "" && opts.TemporalKey != "" {
		slog.Info("Using temporal certificate/key for authentication")
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

	conn, err := utils.GetCatalogConnectionPoolFromEnv(context.Background())
	if err != nil {
		return fmt.Errorf("unable to create catalog connection pool: %w", err)
	}

	c, err := client.Dial(clientOptions)
	if err != nil {
		return fmt.Errorf("unable to create Temporal client: %w", err)
	}
	slog.Info("Created temporal client")
	defer c.Close()

	taskQueue, queueErr := shared.GetPeerFlowTaskQueueName(shared.PeerFlowTaskQueueID)
	if queueErr != nil {
		return queueErr
	}

	w := worker.New(c, taskQueue, worker.Options{
		EnableSessionWorker: true,
	})
	peerflow.RegisterFlowWorkerWorkflows(w)

	alerter, err := alerting.NewAlerter(conn)
	if err != nil {
		return fmt.Errorf("unable to create alerter: %w", err)
	}

	w.RegisterActivity(&activities.FlowableActivity{
		CatalogPool: conn,
		Alerter:     alerter,
		CdcCache:    make(map[string]connectors.CDCPullConnector),
	})

	err = w.Run(worker.InterruptCh())
	if err != nil {
		return fmt.Errorf("worker run error: %w", err)
	}

	return nil
}
