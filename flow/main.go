package main

//go:generate go run ./cmd/codegen

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	_ "net/http/pprof" //nolint:gosec
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/urfave/cli/v3"
	"go.temporal.io/sdk/worker"
	_ "go.uber.org/automaxprocs"

	"github.com/PeerDB-io/peerdb/flow/cmd"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func main() {
	appCtx, appClose := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer appClose()

	slog.SetDefault(slog.New(shared.NewSlogHandler(slog.NewJSONHandler(os.Stdout, shared.NewSlogHandlerOptions()))))

	temporalHostPortFlag := &cli.StringFlag{
		Name:    "temporal-host-port",
		Value:   "localhost:7233",
		Sources: cli.EnvVars("TEMPORAL_HOST_PORT"),
	}

	profilingFlag := &cli.BoolFlag{
		Name:    "enable-profiling",
		Value:   false, // Default is off
		Usage:   "Enable profiling for the application",
		Sources: cli.EnvVars("ENABLE_PROFILING"),
	}
	otelMetricsFlag := &cli.BoolFlag{
		Name:    "enable-otel-metrics",
		Value:   false, // Default is off
		Usage:   "Enable OpenTelemetry metrics for the application",
		Sources: cli.EnvVars("ENABLE_OTEL_METRICS"),
	}

	pprofPortFlag := &cli.Uint16Flag{
		Name:    "pprof-port",
		Value:   6060,
		Usage:   "Port for pprof HTTP server",
		Sources: cli.EnvVars("PPROF_PORT"),
	}

	temporalNamespaceFlag := &cli.StringFlag{
		Name:    "temporal-namespace",
		Value:   "default",
		Usage:   "Temporal namespace to use for workflow orchestration",
		Sources: cli.EnvVars("PEERDB_TEMPORAL_NAMESPACE"),
	}

	temporalMaxConcurrentActivitiesFlag := &cli.IntFlag{
		Name:    "temporal-max-concurrent-activities",
		Value:   1000,
		Usage:   "Temporal: maximum number of concurrent activities",
		Sources: cli.EnvVars("TEMPORAL_MAX_CONCURRENT_ACTIVITIES"),
	}

	temporalMaxConcurrentWorkflowTasksFlag := &cli.IntFlag{
		Name:    "temporal-max-concurrent-workflow-tasks",
		Value:   1000,
		Usage:   "Temporal: maximum number of concurrent workflows",
		Sources: cli.EnvVars("TEMPORAL_MAX_CONCURRENT_WORKFLOW_TASKS"),
	}

	maintenanceModeWorkflowFlag := &cli.StringFlag{
		Name:    "run-maintenance-flow",
		Value:   "",
		Usage:   "Run a maintenance flow. Options are 'start' or 'end'",
		Sources: cli.EnvVars("RUN_MAINTENANCE_FLOW"),
	}

	maintenanceSkipOnApiVersionMatchFlag := &cli.BoolFlag{
		Name:    "skip-on-api-version-match",
		Value:   false,
		Usage:   "Skip maintenance flow if the API version matches",
		Sources: cli.EnvVars("MAINTENANCE_SKIP_ON_API_VERSION_MATCH"),
	}

	maintenanceSkipOnDeploymentVersionMatch := &cli.BoolFlag{
		Name:    "skip-on-deployment-version-match",
		Value:   false,
		Usage:   "Skip maintenance flow if the deployment version matches",
		Sources: cli.EnvVars("MAINTENANCE_SKIP_ON_DEPLOYMENT_VERSION_MATCH"),
	}

	maintenanceSkipOnNoMirrorsFlag := &cli.BoolFlag{
		Name:    "skip-on-no-mirrors",
		Value:   false,
		Usage:   "Skip maintenance flow if there are no mirrors",
		Sources: cli.EnvVars("MAINTENANCE_SKIP_ON_NO_MIRRORS"),
	}

	flowGrpcAddressFlag := &cli.StringFlag{
		Name:    "flow-grpc-address",
		Value:   "",
		Usage:   "Address of the flow gRPC server",
		Sources: cli.EnvVars("FLOW_GRPC_ADDRESS"),
	}

	flowTlsEnabledFlag := &cli.BoolFlag{
		Name:    "flow-tls-enabled",
		Value:   false,
		Usage:   "Enable TLS for the flow gRPC server",
		Sources: cli.EnvVars("FLOW_TLS_ENABLED"),
	}

	useMaintenanceTaskQueueFlag := &cli.BoolFlag{
		Name:    "use-maintenance-task-queue",
		Value:   false,
		Usage:   "Use the maintenance task queue for the worker",
		Sources: cli.EnvVars("USE_MAINTENANCE_TASK_QUEUE"),
	}

	assumedSkippedMaintenanceWorkflowsFlag := &cli.BoolFlag{
		Name:  "assume-skipped-workflow",
		Value: false,
		Usage: "Skip running maintenance workflows and simply output to catalog",
	}

	skipIfK8sServiceMissingFlag := &cli.StringFlag{
		Name:  "skip-if-k8s-service-missing",
		Value: "",
		Usage: "Skip maintenance if the k8s service is missing, generally used during pre-upgrade hook",
	}

	apiPortFlag := &cli.Uint16Flag{
		Name:    "port",
		Aliases: []string{"p"},
		Value:   8110,
	}

	apiGatewayPortFlag := &cli.Uint16Flag{
		Name:  "gateway-port",
		Value: 8111,
		Usage: "Port grpc-gateway listens on",
	}

	app := &cli.Command{
		Name: "PeerDB Flows CLI",
		Before: func(ctx context.Context, clicmd *cli.Command) (context.Context, error) {
			if clicmd.Bool(profilingFlag.Name) {
				// Enable mutex and block profiling
				runtime.SetMutexProfileFraction(5)
				runtime.SetBlockProfileRate(5)
				pprofPort := clicmd.Uint16(pprofPortFlag.Name)
				pprofAddr := fmt.Sprintf(":%d", pprofPort)

				// Start HTTP server with pprof endpoints
				go func() {
					slog.InfoContext(ctx, "Starting pprof HTTP server", slog.String("address", pprofAddr))

					server := &http.Server{
						Addr:         pprofAddr,
						ReadTimeout:  1 * time.Minute,
						WriteTimeout: 11 * time.Minute,
					}
					if err := server.ListenAndServe(); err != nil {
						log.Fatalf("Failed to start pprof HTTP server: %v", err)
					}
				}()
			}
			return nil, nil
		},
		Flags: []cli.Flag{
			profilingFlag,
			pprofPortFlag,
		},
		Commands: []*cli.Command{
			{
				Name: "worker",
				Action: func(ctx context.Context, clicmd *cli.Command) error {
					res, err := cmd.WorkerSetup(ctx, &cmd.WorkerSetupOptions{
						TemporalHostPort:                   clicmd.String(temporalHostPortFlag.Name),
						TemporalNamespace:                  clicmd.String(temporalNamespaceFlag.Name),
						TemporalMaxConcurrentActivities:    clicmd.Int(temporalMaxConcurrentActivitiesFlag.Name),
						TemporalMaxConcurrentWorkflowTasks: clicmd.Int(temporalMaxConcurrentWorkflowTasksFlag.Name),
						UseMaintenanceTaskQueue:            clicmd.Bool(useMaintenanceTaskQueueFlag.Name),
						EnableOtelMetrics:                  clicmd.Bool(otelMetricsFlag.Name),
					})
					if err != nil {
						return err
					}
					defer res.Close(context.Background())
					return res.Worker.Run(worker.InterruptCh())
				},
				Flags: []cli.Flag{
					temporalHostPortFlag,
					temporalNamespaceFlag,
					temporalMaxConcurrentActivitiesFlag,
					temporalMaxConcurrentWorkflowTasksFlag,
					useMaintenanceTaskQueueFlag,
					otelMetricsFlag,
				},
			},
			{
				Name: "snapshot-worker",
				Action: func(ctx context.Context, clicmd *cli.Command) error {
					res, err := cmd.SnapshotWorkerMain(ctx, &cmd.SnapshotWorkerOptions{
						TemporalHostPort:  clicmd.String(temporalHostPortFlag.Name),
						TemporalNamespace: clicmd.String(temporalNamespaceFlag.Name),
						EnableOtelMetrics: clicmd.Bool(otelMetricsFlag.Name),
					})
					if err != nil {
						return err
					}
					defer res.Close(context.Background())
					return res.Worker.Run(worker.InterruptCh())
				},
				Flags: []cli.Flag{
					temporalHostPortFlag,
					temporalNamespaceFlag,
					otelMetricsFlag,
				},
			},
			{
				Name: "api",
				Flags: []cli.Flag{
					apiPortFlag,
					apiGatewayPortFlag,
					temporalHostPortFlag,
					temporalNamespaceFlag,
					otelMetricsFlag,
				},
				Action: func(ctx context.Context, clicmd *cli.Command) error {
					return cmd.APIMain(ctx, &cmd.APIServerParams{
						Port:              clicmd.Uint16(apiPortFlag.Name),
						GatewayPort:       clicmd.Uint16(apiGatewayPortFlag.Name),
						TemporalHostPort:  clicmd.String(temporalHostPortFlag.Name),
						TemporalNamespace: clicmd.String(temporalNamespaceFlag.Name),
						EnableOtelMetrics: clicmd.Bool(otelMetricsFlag.Name),
					})
				},
			},
			{
				Name: "maintenance",
				Flags: []cli.Flag{
					temporalHostPortFlag,
					temporalNamespaceFlag,
					maintenanceModeWorkflowFlag,
					maintenanceSkipOnApiVersionMatchFlag,
					maintenanceSkipOnDeploymentVersionMatch,
					maintenanceSkipOnNoMirrorsFlag,
					flowGrpcAddressFlag,
					flowTlsEnabledFlag,
					useMaintenanceTaskQueueFlag,
					assumedSkippedMaintenanceWorkflowsFlag,
					skipIfK8sServiceMissingFlag,
				},
				Action: func(ctx context.Context, clicmd *cli.Command) error {
					temporalHostPort := clicmd.String("temporal-host-port")

					return cmd.MaintenanceMain(ctx, &cmd.MaintenanceCLIParams{
						TemporalHostPort:                  temporalHostPort,
						TemporalNamespace:                 clicmd.String(temporalNamespaceFlag.Name),
						Mode:                              clicmd.String(maintenanceModeWorkflowFlag.Name),
						SkipOnApiVersionMatch:             clicmd.Bool(maintenanceSkipOnApiVersionMatchFlag.Name),
						SkipOnDeploymentVersionMatch:      clicmd.Bool(maintenanceSkipOnDeploymentVersionMatch.Name),
						SkipOnNoMirrors:                   clicmd.Bool(maintenanceSkipOnNoMirrorsFlag.Name),
						FlowGrpcAddress:                   clicmd.String(flowGrpcAddressFlag.Name),
						FlowTlsEnabled:                    clicmd.Bool(flowTlsEnabledFlag.Name),
						UseMaintenanceTaskQueue:           clicmd.Bool(useMaintenanceTaskQueueFlag.Name),
						AssumeSkippedMaintenanceWorkflows: clicmd.Bool(assumedSkippedMaintenanceWorkflowsFlag.Name),
						SkipIfK8sServiceMissing:           clicmd.String(skipIfK8sServiceMissingFlag.Name),
					})
				},
			},
		},
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

	if err := app.Run(appCtx, os.Args); err != nil {
		log.Printf("error running app: %+v", err)
		panic(err)
	}
}
