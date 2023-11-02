package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/PeerDB-io/peer-flow/activities"
	utils "github.com/PeerDB-io/peer-flow/connectors/utils/catalog"
	"github.com/PeerDB-io/peer-flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
	"github.com/uber-go/tally/v4"
	"github.com/uber-go/tally/v4/prometheus"

	"github.com/grafana/pyroscope-go"
	prom "github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/client"
	sdktally "go.temporal.io/sdk/contrib/tally"
	"go.temporal.io/sdk/worker"
)

type WorkerOptions struct {
	TemporalHostPort  string
	EnableProfiling   bool
	EnableMetrics     bool
	PyroscopeServer   string
	MetricsServer     string
	TemporalNamespace string
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
	if opts.EnableMetrics {
		clientOptions.MetricsHandler = sdktally.NewMetricsHandler(newPrometheusScope(
			prometheus.Configuration{
				ListenAddress: opts.MetricsServer,
				TimerType:     "histogram",
			},
		))
	}

	conn, err := utils.GetCatalogConnectionPoolFromEnv()
	if err != nil {
		return fmt.Errorf("unable to create catalog connection pool: %w", err)
	}
	catalogMirrorMonitor := monitoring.NewCatalogMirrorMonitor(conn)
	defer catalogMirrorMonitor.Close()

	c, err := client.Dial(clientOptions)
	if err != nil {
		return fmt.Errorf("unable to create Temporal client: %w", err)
	}
	defer c.Close()

	w := worker.New(c, shared.PeerFlowTaskQueue, worker.Options{})
	w.RegisterWorkflow(peerflow.CDCFlowWorkflowWithConfig)
	w.RegisterWorkflow(peerflow.SyncFlowWorkflow)
	w.RegisterWorkflow(peerflow.SetupFlowWorkflow)
	w.RegisterWorkflow(peerflow.NormalizeFlowWorkflow)
	w.RegisterWorkflow(peerflow.QRepFlowWorkflow)
	w.RegisterWorkflow(peerflow.QRepPartitionWorkflow)
	w.RegisterWorkflow(peerflow.DropFlowWorkflow)
	w.RegisterActivity(&activities.FlowableActivity{
		EnableMetrics:        opts.EnableMetrics,
		CatalogMirrorMonitor: catalogMirrorMonitor,
	})

	err = w.Run(worker.InterruptCh())
	if err != nil {
		return fmt.Errorf("worker run error: %w", err)
	}

	return nil
}

func newPrometheusScope(c prometheus.Configuration) tally.Scope {
	reporter, err := c.NewReporter(
		prometheus.ConfigurationOptions{
			Registry: prom.NewRegistry(),
			OnError: func(err error) {
				log.Println("error in prometheus reporter", err)
			},
		},
	)
	if err != nil {
		log.Fatalln("error creating prometheus reporter", err)
	}
	scopeOpts := tally.ScopeOptions{
		CachedReporter:  reporter,
		Separator:       prometheus.DefaultSeparator,
		SanitizeOptions: &sdktally.PrometheusSanitizeOptions,
		Prefix:          "flow_worker",
	}
	scope, _ := tally.NewRootScope(scopeOpts, time.Second)
	scope = sdktally.NewPrometheusNamingScope(scope)

	log.Println("prometheus metrics scope created")
	return scope
}
