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

	if config.EnableTracing {
		traceShutdown, err := tracing.SetupOtelTraceProviderExporter(serviceName)
		if err != nil {
			return shutdown, errors.Join(err, shutdown(ctx))
		}
		shutdownFuncs = append(shutdownFuncs, traceShutdown)
	}
	// Setup other stuff here in the future like metrics, logs etc
	return shutdown, nil
}
