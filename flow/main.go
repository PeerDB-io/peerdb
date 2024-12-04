package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/urfave/cli/v3"
	"go.temporal.io/sdk/worker"
	_ "go.uber.org/automaxprocs"

	"github.com/PeerDB-io/peer-flow/cmd"
	"github.com/PeerDB-io/peer-flow/shared"
)

func main() {
	appCtx, appClose := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer appClose()

	slog.SetDefault(slog.New(shared.NewSlogHandler(slog.NewJSONHandler(os.Stdout, nil))))

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

	pyroscopeServerFlag := &cli.StringFlag{
		Name:    "pyroscope-server-address",
		Value:   "http://pyroscope:4040",
		Usage:   "HTTP server address for pyroscope",
		Sources: cli.EnvVars("PYROSCOPE_SERVER_ADDRESS"),
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

	app := &cli.Command{
		Name: "PeerDB Flows CLI",
		Commands: []*cli.Command{
			{
				Name: "worker",
				Action: func(ctx context.Context, clicmd *cli.Command) error {
					temporalHostPort := clicmd.String("temporal-host-port")
					res, err := cmd.WorkerSetup(&cmd.WorkerSetupOptions{
						TemporalHostPort:                   temporalHostPort,
						EnableProfiling:                    clicmd.Bool("enable-profiling"),
						EnableOtelMetrics:                  clicmd.Bool("enable-otel-metrics"),
						PyroscopeServer:                    clicmd.String("pyroscope-server-address"),
						TemporalNamespace:                  clicmd.String("temporal-namespace"),
						TemporalMaxConcurrentActivities:    int(clicmd.Int("temporal-max-concurrent-activities")),
						TemporalMaxConcurrentWorkflowTasks: int(clicmd.Int("temporal-max-concurrent-workflow-tasks")),
						UseMaintenanceTaskQueue:            clicmd.Bool(useMaintenanceTaskQueueFlag.Name),
					})
					if err != nil {
						return err
					}
					defer res.Close()
					return res.Worker.Run(worker.InterruptCh())
				},
				Flags: []cli.Flag{
					temporalHostPortFlag,
					profilingFlag,
					otelMetricsFlag,
					pyroscopeServerFlag,
					temporalNamespaceFlag,
					temporalMaxConcurrentActivitiesFlag,
					temporalMaxConcurrentWorkflowTasksFlag,
					useMaintenanceTaskQueueFlag,
				},
			},
			{
				Name: "snapshot-worker",
				Action: func(ctx context.Context, clicmd *cli.Command) error {
					temporalHostPort := clicmd.String("temporal-host-port")
					c, w, err := cmd.SnapshotWorkerMain(&cmd.SnapshotWorkerOptions{
						TemporalHostPort:  temporalHostPort,
						TemporalNamespace: clicmd.String("temporal-namespace"),
					})
					if err != nil {
						return err
					}
					defer c.Close()
					return w.Run(worker.InterruptCh())
				},
				Flags: []cli.Flag{
					temporalHostPortFlag,
					temporalNamespaceFlag,
				},
			},
			{
				Name: "api",
				Flags: []cli.Flag{
					&cli.UintFlag{
						Name:    "port",
						Aliases: []string{"p"},
						Value:   8110,
					},
					// gateway port is the port that the grpc-gateway listens on
					&cli.UintFlag{
						Name:  "gateway-port",
						Value: 8111,
					},
					temporalHostPortFlag,
					temporalNamespaceFlag,
				},
				Action: func(ctx context.Context, clicmd *cli.Command) error {
					temporalHostPort := clicmd.String("temporal-host-port")

					return cmd.APIMain(ctx, &cmd.APIServerParams{
						Port:              uint16(clicmd.Uint("port")),
						TemporalHostPort:  temporalHostPort,
						GatewayPort:       uint16(clicmd.Uint("gateway-port")),
						TemporalNamespace: clicmd.String("temporal-namespace"),
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
