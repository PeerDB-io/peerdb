package otel_common

import (
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/semconv/v1.24.0"
)

// NewOtelResource returns a resource describing this application.
func NewOtelResource(otelServiceName string) (*resource.Resource, error) {
	return resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(otelServiceName),
		),
	)
}
