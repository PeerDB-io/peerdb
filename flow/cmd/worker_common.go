package cmd

import (
	"log"

	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
)

func GetTemporalClientInterceptors() []interceptor.ClientInterceptor {
	// Maybe do this only when otel is actually enabled?
	tracingInterceptor, err := opentelemetry.NewTracingInterceptor(opentelemetry.TracerOptions{})
	if err != nil {
		log.Printf("failed to create tracing interceptor: %v\n", err)
		return nil
	}
	return []interceptor.ClientInterceptor{tracingInterceptor}
}
