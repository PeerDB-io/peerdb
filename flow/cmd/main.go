package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/urfave/cli/v2"
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

	profilingServerFlag := &cli.StringFlag{
		Name:    "profiling-server",
		Value:   "localhost:6060", // Default is localhost:6060
		Usage:   "HTTP server address for profiling",
		EnvVars: []string{"PROFILING_SERVER"},
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
						ProfilingServer:  ctx.String("profiling-server"),
					})
				},
				Flags: []cli.Flag{
					temporalHostPortFlag,
					profilingFlag,
					profilingServerFlag,
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
					&cli.StringFlag{
						Name:    "catalog-host",
						Value:   "localhost",
						EnvVars: []string{"PEERDB_CATALOG_HOST"},
					},
					&cli.UintFlag{
						Name:    "catalog-port",
						Value:   5432,
						EnvVars: []string{"PEERDB_CATALOG_PORT"},
					},
					&cli.StringFlag{
						Name:    "catalog-user",
						Value:   "postgres",
						EnvVars: []string{"PEERDB_CATALOG_USER"},
					},
					&cli.StringFlag{
						Name:    "catalog-password",
						Value:   "postgres",
						EnvVars: []string{"PEERDB_CATALOG_PASSWORD"},
					},
					&cli.StringFlag{
						Name:    "catalog-db",
						Value:   "postgres",
						EnvVars: []string{"PEERDB_CATALOG_DATABASE"},
					},
					temporalHostPortFlag,
				},
				Action: func(ctx *cli.Context) error {
					temporalHostPort := ctx.String("temporal-host-port")

					return APIMain(&APIServerParams{
						ctx:              appCtx,
						Port:             ctx.Uint("port"),
						TemporalHostPort: temporalHostPort,
					})
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
