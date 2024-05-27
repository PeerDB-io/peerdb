package tracing

import (
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

var tracerName = "peerdb"

var globalTracerOnce sync.Once

var tracer trace.Tracer

func SetupTracer(name string) trace.Tracer {
	globalTracerOnce.Do(func() {
		tracerName = name
		tracer = otel.GetTracerProvider().Tracer(tracerName)
	})
	return tracer
}

func Tracer() trace.Tracer {
	return SetupTracer("peerdb")
}
