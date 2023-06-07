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

	app := &cli.App{
		Name: "PeerDB Flows CLI",
		Commands: []*cli.Command{
			{
				Name: "worker",
				Action: func(ctx *cli.Context) error {
					temporalHostPort := ctx.String("temporal-host-port")
					return WorkerMain(&WorkerOptions{
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
					// JDBC connection string for catalog database
					&cli.StringFlag{
						Name:    "catalog-dsn",
						Aliases: []string{"c"},
						Value:   "postgresql://postgres:postgres@localhost:5432/postgres?sslmode=disable",
						EnvVars: []string{"CATALOG_DSN"},
					},
					temporalHostPortFlag,
				},
				Action: func(ctx *cli.Context) error {
					catalogURL := ctx.String("catalog-dsn")
					temporalHostPort := ctx.String("temporal-host-port")

					return APIMain(&APIServerParams{
						ctx:              appCtx,
						Port:             ctx.Uint("port"),
						CatalogJdbcURL:   catalogURL,
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
