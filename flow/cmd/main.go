package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/urfave/cli/v3"
	_ "go.uber.org/automaxprocs"
)

func main() {
	appCtx, appCancel := context.WithCancel(context.Background())
	slog.SetDefault(slog.New(logger.NewHandler(slog.NewJSONHandler(os.Stdout, nil))))
	// setup shutdown handling
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// cancel the context when we receive a shutdown signal
	go func() {
		<-quit
		appCancel()
	}()

	temporalHostPortFlag := &cli.StringFlag{
		Name:    "temporal-host-port",
		Value:   "localhost:7233",
		Sources: cli.EnvVars("TEMPORAL_HOST_PORT"),
	}

	temporalCertFlag := cli.StringFlag{
		Name:    "temporal-cert",
		Value:   "", // default: no cert needed
		Sources: cli.EnvVars("TEMPORAL_CLIENT_CERT"),
	}

	temporalKeyFlag := cli.StringFlag{
		Name:    "temporal-key",
		Value:   "", // default: no key needed
		Sources: cli.EnvVars("TEMPORAL_CLIENT_KEY"),
	}

	profilingFlag := &cli.BoolFlag{
		Name:    "enable-profiling",
		Value:   false, // Default is off
		Usage:   "Enable profiling for the application",
		Sources: cli.EnvVars("ENABLE_PROFILING"),
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

	app := &cli.Command{
		Name: "PeerDB Flows CLI",
		Commands: []*cli.Command{
			{
				Name: "worker",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					temporalHostPort := cmd.String("temporal-host-port")
					return WorkerMain(&WorkerOptions{
						TemporalHostPort:  temporalHostPort,
						EnableProfiling:   cmd.Bool("enable-profiling"),
						PyroscopeServer:   cmd.String("pyroscope-server-address"),
						TemporalNamespace: cmd.String("temporal-namespace"),
						TemporalCert:      cmd.String("temporal-cert"),
						TemporalKey:       cmd.String("temporal-key"),
					})
				},
				Flags: []cli.Flag{
					temporalHostPortFlag,
					profilingFlag,
					pyroscopeServerFlag,
					temporalNamespaceFlag,
					&temporalCertFlag,
					&temporalKeyFlag,
				},
			},
			{
				Name: "snapshot-worker",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					temporalHostPort := cmd.String("temporal-host-port")
					return SnapshotWorkerMain(&SnapshotWorkerOptions{
						TemporalHostPort:  temporalHostPort,
						TemporalNamespace: cmd.String("temporal-namespace"),
						TemporalCert:      cmd.String("temporal-cert"),
						TemporalKey:       cmd.String("temporal-key"),
					})
				},
				Flags: []cli.Flag{
					temporalHostPortFlag,
					temporalNamespaceFlag,
					&temporalCertFlag,
					&temporalKeyFlag,
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
					&temporalCertFlag,
					&temporalKeyFlag,
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					temporalHostPort := cmd.String("temporal-host-port")

					return APIMain(&APIServerParams{
						ctx:               appCtx,
						Port:              uint16(cmd.Uint("port")),
						TemporalHostPort:  temporalHostPort,
						GatewayPort:       uint16(cmd.Uint("gateway-port")),
						TemporalNamespace: cmd.String("temporal-namespace"),
						TemporalCert:      cmd.String("temporal-cert"),
						TemporalKey:       cmd.String("temporal-key"),
					})
				},
			},
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		log.Fatalf("error running app: %v", err)
	}
}
