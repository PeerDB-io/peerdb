package tracing

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/PeerDB-io/peer-flow/instrumentation/otel_common"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

// PeerDBSpanProcessor adds PeerDB specific attributes to spans like deploymentUID
type PeerDBSpanProcessor struct{}

func (p *PeerDBSpanProcessor) OnStart(parent context.Context, s sdktrace.ReadWriteSpan) {
	s.SetAttributes(attribute.String("deploymentUID", peerdbenv.PeerDBDeploymentUID()))
}

func (p *PeerDBSpanProcessor) OnEnd(s sdktrace.ReadOnlySpan) {
}

func (p *PeerDBSpanProcessor) Shutdown(ctx context.Context) error {
	return nil
}

func (p *PeerDBSpanProcessor) ForceFlush(ctx context.Context) error {
	return nil
}

func NewPeerDBSpanProcessor() sdktrace.SpanProcessor {
	return &PeerDBSpanProcessor{}
}

func setupHttpOtelTraceExporter() (*otlptrace.Exporter, error) {
	return otlptracehttp.New(context.Background())
}

func setupGrpcOtelTraceExporter() (*otlptrace.Exporter, error) {
	return otlptracegrpc.New(context.Background())
}

func SetupOtelTraceProviderExporter(otelServiceName string) (func(ctx context.Context) error, error) {
	otlpTraceProtocol := peerdbenv.GetEnvString("OTEL_EXPORTER_OTLP_PROTOCOL",
		peerdbenv.GetEnvString("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/protobuf"))
	var traceExporter *otlptrace.Exporter
	var err error
	switch otlpTraceProtocol {
	case "http/protobuf":
		traceExporter, err = setupHttpOtelTraceExporter()
	case "grpc":
		traceExporter, err = setupGrpcOtelTraceExporter()
	default:
		return nil, fmt.Errorf("unsupported otel trace protocol: %s", otlpTraceProtocol)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenTelemetry trace exporter: %w", err)
	}
	otelResource, err := otel_common.NewOtelResource(otelServiceName)
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenTelemetry resource: %w", err)
	}
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(otelResource),
	)
	// This sets up the trace provider globally, now a tracer can be retrieved via tracer.Tracer()
	otel.SetTracerProvider(tracerProvider)
	tracerProvider.RegisterSpanProcessor(NewPeerDBSpanProcessor())
	return traceExporter.Shutdown, nil
}
