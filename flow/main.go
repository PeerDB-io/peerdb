package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/urfave/cli/v3"
	"go.temporal.io/sdk/worker"
	_ "go.uber.org/automaxprocs"

	"github.com/PeerDB-io/peer-flow/cmd"
	"github.com/PeerDB-io/peer-flow/logger"
)

func main() {
	appCtx, appClose := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer appClose()

	slog.SetDefault(slog.New(logger.NewHandler(slog.NewJSONHandler(os.Stdout, nil))))

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
				Action: func(ctx context.Context, clicmd *cli.Command) error {
					temporalHostPort := clicmd.String("temporal-host-port")
					return cmd.WorkerMain(worker.InterruptCh(), &cmd.WorkerOptions{
						TemporalHostPort:  temporalHostPort,
						EnableProfiling:   clicmd.Bool("enable-profiling"),
						PyroscopeServer:   clicmd.String("pyroscope-server-address"),
						TemporalNamespace: clicmd.String("temporal-namespace"),
						TemporalCert:      clicmd.String("temporal-cert"),
						TemporalKey:       clicmd.String("temporal-key"),
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
				Action: func(ctx context.Context, clicmd *cli.Command) error {
					temporalHostPort := clicmd.String("temporal-host-port")
					return cmd.SnapshotWorkerMain(worker.InterruptCh(), &cmd.SnapshotWorkerOptions{
						TemporalHostPort:  temporalHostPort,
						TemporalNamespace: clicmd.String("temporal-namespace"),
						TemporalCert:      clicmd.String("temporal-cert"),
						TemporalKey:       clicmd.String("temporal-key"),
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
				Action: func(ctx context.Context, clicmd *cli.Command) error {
					temporalHostPort := clicmd.String("temporal-host-port")

					return cmd.APIMain(ctx, &cmd.APIServerParams{
						Port:              uint16(clicmd.Uint("port")),
						TemporalHostPort:  temporalHostPort,
						GatewayPort:       uint16(clicmd.Uint("gateway-port")),
						TemporalNamespace: clicmd.String("temporal-namespace"),
						TemporalCert:      clicmd.String("temporal-cert"),
						TemporalKey:       clicmd.String("temporal-key"),
					})
				},
			},
		},
	}

	if err := app.Run(appCtx, os.Args); err != nil {
		log.Printf("error running app: %+v", err)
	}
}
