package instrumentation

import (
	"context"
	"errors"

	"github.com/PeerDB-io/peer-flow/instrumentation/tracing"
)

type Config struct {
	EnableTracing bool
}

func SetupInstrumentation(ctx context.Context, serviceName string, config Config) (func(ctx context.Context) error, error) {
	var shutdownFuncs []func(context.Context) error
	shutdown := func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}

	var err error
	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}

	if config.EnableTracing {
		traceShutdown, err := tracing.SetupOtelTraceProviderExporter(serviceName)
		if err != nil {
			handleErr(err)
			return shutdown, err
		}
		shutdownFuncs = append(shutdownFuncs, traceShutdown)
	}
	// Setup other stuff here in the future like metrics, logs etc
	return shutdown, err
}
