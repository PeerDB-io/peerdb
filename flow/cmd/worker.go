package main

import (
	"crypto/tls"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/PeerDB-io/peer-flow/activities"
	utils "github.com/PeerDB-io/peer-flow/connectors/utils/catalog"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"

	"github.com/grafana/pyroscope-go"
	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
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
		Logger: log.StandardLogger(),

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
	}

	if opts.TemporalCert != "" && opts.TemporalKey != "" {
		log.Info("Using temporal certificate/key for authentication")
		certs, err := Base64DecodeCertAndKey(opts.TemporalCert, opts.TemporalKey)
		if err != nil {
			return fmt.Errorf("unable to process certificate and key: %w", err)
		}
		connOptions := client.ConnectionOptions{
			TLS: &tls.Config{Certificates: certs},
		}
		clientOptions.ConnectionOptions = connOptions
	}

	conn, err := utils.GetCatalogConnectionPoolFromEnv()
	if err != nil {
		return fmt.Errorf("unable to create catalog connection pool: %w", err)
	}

	c, err := client.Dial(clientOptions)
	if err != nil {
		return fmt.Errorf("unable to create Temporal client: %w", err)
	}
	log.Info("Created temporal client")
	defer c.Close()

	taskQueue, queueErr := shared.GetPeerFlowTaskQueueName(shared.PeerFlowTaskQueueID)
	if queueErr != nil {
		return queueErr
	}

	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflow(peerflow.CDCFlowWorkflowWithConfig)
	w.RegisterWorkflow(peerflow.SyncFlowWorkflow)
	w.RegisterWorkflow(peerflow.SetupFlowWorkflow)
	w.RegisterWorkflow(peerflow.NormalizeFlowWorkflow)
	w.RegisterWorkflow(peerflow.QRepFlowWorkflow)
	w.RegisterWorkflow(peerflow.QRepPartitionWorkflow)
	w.RegisterWorkflow(peerflow.XminFlowWorkflow)
	w.RegisterWorkflow(peerflow.DropFlowWorkflow)
	w.RegisterWorkflow(peerflow.HeartbeatFlowWorkflow)
	w.RegisterActivity(&activities.FlowableActivity{
		CatalogPool: conn,
	})

	err = w.Run(worker.InterruptCh())
	if err != nil {
		return fmt.Errorf("worker run error: %w", err)
	}

	return nil
}
