package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/urfave/cli/v2"
	_ "go.uber.org/automaxprocs"
)

func main() {
	appCtx, appCancel := context.WithCancel(context.Background())

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
		EnvVars: []string{"TEMPORAL_HOST_PORT"},
	}

	profilingFlag := &cli.BoolFlag{
		Name:    "enable-profiling",
		Value:   false, // Default is off
		Usage:   "Enable profiling for the application",
		EnvVars: []string{"ENABLE_PROFILING"},
	}

	metricsFlag := &cli.BoolFlag{
		Name:    "enable-metrics",
		Value:   false, // Default is off
		Usage:   "Enable metrics collection for the application",
		EnvVars: []string{"ENABLE_METRICS"},
	}

	monitoringFlag := &cli.BoolFlag{
		Name:    "enable-monitoring",
		Value:   false, // Default is off
		Usage:   "Enable mirror monitoring for the application",
		EnvVars: []string{"ENABLE_STATS"},
	}

	pyroscopeServerFlag := &cli.StringFlag{
		Name:    "pyroscope-server-address",
		Value:   "http://pyroscope:4040",
		Usage:   "HTTP server address for pyroscope",
		EnvVars: []string{"PYROSCOPE_SERVER_ADDRESS"},
	}

	metricsServerFlag := &cli.StringFlag{
		Name:    "metrics-server",
		Value:   "localhost:6061", // Default is localhost:6061
		Usage:   "HTTP server address for metrics collection",
		EnvVars: []string{"METRICS_SERVER"},
	}

	app := &cli.App{
		Name: "PeerDB Flows CLI",
		Commands: []*cli.Command{
			{
				Name: "worker",
				Action: func(ctx *cli.Context) error {
					temporalHostPort := ctx.String("temporal-host-port")
					return WorkerMain(&WorkerOptions{
						TemporalHostPort: temporalHostPort,
						EnableProfiling:  ctx.Bool("enable-profiling"),
						EnableMetrics:    ctx.Bool("enable-metrics"),
						EnableMonitoring: ctx.Bool("enable-monitoring"),
						PyroscopeServer:  ctx.String("pyroscope-server-address"),
						MetricsServer:    ctx.String("metrics-server"),
					})
				},
				Flags: []cli.Flag{
					temporalHostPortFlag,
					profilingFlag,
					metricsFlag,
					monitoringFlag,
					pyroscopeServerFlag,
					metricsServerFlag,
				},
			},
			{
				Name: "snapshot-worker",
				Action: func(ctx *cli.Context) error {
					temporalHostPort := ctx.String("temporal-host-port")
					return SnapshotWorkerMain(&SnapshotWorkerOptions{
						TemporalHostPort: temporalHostPort,
					})
				},
				Flags: []cli.Flag{
					temporalHostPortFlag,
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
				},
				Action: func(ctx *cli.Context) error {
					temporalHostPort := ctx.String("temporal-host-port")

					return APIMain(&APIServerParams{
						ctx:              appCtx,
						Port:             ctx.Uint("port"),
						TemporalHostPort: temporalHostPort,
						GatewayPort:      ctx.Uint("gateway-port"),
					})
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
