package main

import (
	"fmt"
	"net/http"
	"time"

	//nolint:gosec
	_ "net/http/pprof"

	"github.com/PeerDB-io/peer-flow/activities"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
	"github.com/uber-go/tally/v4"
	"github.com/uber-go/tally/v4/prometheus"

	prom "github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/client"
	sdktally "go.temporal.io/sdk/contrib/tally"
	"go.temporal.io/sdk/worker"
)

type WorkerOptions struct {
	TemporalHostPort string
	EnableProfiling  bool
	EnableMetrics    bool
	ProfilingServer  string
	MetricsServer    string
}

func WorkerMain(opts *WorkerOptions) error {
	if opts.EnableProfiling {
		// Start HTTP profiling server with timeouts
		go func() {
			server := http.Server{
				Addr:         opts.ProfilingServer,
				ReadTimeout:  5 * time.Minute,
				WriteTimeout: 15 * time.Minute,
			}

			log.Infof("starting profiling server on %s", opts.ProfilingServer)

			if err := server.ListenAndServe(); err != nil {
				log.Errorf("unable to start profiling server: %v", err)
			}
		}()
	}

	var clientOptions client.Options
	if opts.EnableMetrics {
		clientOptions = client.Options{
			HostPort: opts.TemporalHostPort,
			MetricsHandler: sdktally.NewMetricsHandler(newPrometheusScope(
				prometheus.Configuration{
					ListenAddress: opts.MetricsServer,
					TimerType:     "histogram",
				},
			)),
		}
	} else {
		clientOptions = client.Options{
			HostPort: opts.TemporalHostPort,
		}
	}

	c, err := client.Dial(clientOptions)
	if err != nil {
		return fmt.Errorf("unable to create Temporal client: %w", err)
	}
	defer c.Close()

	w := worker.New(c, shared.PeerFlowTaskQueue, worker.Options{})
	w.RegisterWorkflow(peerflow.PeerFlowWorkflow)
	w.RegisterWorkflow(peerflow.PeerFlowWorkflowWithConfig)
	w.RegisterWorkflow(peerflow.SyncFlowWorkflow)
	w.RegisterWorkflow(peerflow.SetupFlowWorkflow)
	w.RegisterWorkflow(peerflow.NormalizeFlowWorkflow)
	w.RegisterWorkflow(peerflow.QRepFlowWorkflow)
	w.RegisterWorkflow(peerflow.QRepPartitionWorkflow)
	w.RegisterWorkflow(peerflow.DropFlowWorkflow)
	w.RegisterActivity(&activities.FetchConfigActivity{})
	w.RegisterActivity(&activities.FlowableActivity{
		EnableMetrics: opts.EnableMetrics,
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
