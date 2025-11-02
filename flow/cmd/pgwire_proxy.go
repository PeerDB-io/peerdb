package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pgwire"
)

// PgWireProxyParams holds parameters for the PgWire proxy server
type PgWireProxyParams struct {
	Port              uint16
	UpstreamDSN       string
	EnableTLS         bool
	TLSCert           string
	TLSKey            string
	MaxConnections    int
	QueryTimeout      time.Duration
	MaxRows           int64
	MaxBytes          int64
	DenyStatements    []string
	AllowStatements   []string
	EnableOtelMetrics bool
}

// PgWireProxyMain is the main entry point for the PgWire proxy server
func PgWireProxyMain(ctx context.Context, params *PgWireProxyParams) error {
	slog.InfoContext(ctx, "Starting PgWire proxy server",
		slog.Uint64("port", uint64(params.Port)),
		slog.Bool("tls", params.EnableTLS),
		slog.Int("max_connections", params.MaxConnections),
		slog.String("query_timeout", params.QueryTimeout.String()),
	)

	// Setup metrics if enabled
	// For now, we'll skip metrics integration and add it later
	// TODO: integrate with OtelManager properly
	var otelManager *otel_metrics.OtelManager
	if params.EnableOtelMetrics {
		slog.InfoContext(ctx, "OpenTelemetry metrics requested but not yet implemented for pgwire proxy")
	}

	// Create server configuration
	config := &pgwire.ServerConfig{
		ListenAddress:   fmt.Sprintf(":%d", params.Port),
		UpstreamDSN:     params.UpstreamDSN,
		EnableTLS:       params.EnableTLS,
		TLSCertPath:     params.TLSCert,
		TLSKeyPath:      params.TLSKey,
		MaxConnections:  params.MaxConnections,
		QueryTimeout:    params.QueryTimeout,
		MaxRows:         params.MaxRows,
		MaxBytes:        params.MaxBytes,
		DenyStatements:  params.DenyStatements,
		AllowStatements: params.AllowStatements,
		OtelManager:     otelManager,
	}

	// Create and start server
	server, err := pgwire.NewServer(ctx, config)
	if err != nil {
		return fmt.Errorf("failed to create pgwire server: %w", err)
	}

	// Run server until context is cancelled
	if err := server.ListenAndServe(ctx); err != nil {
		return fmt.Errorf("server error: %w", err)
	}

	slog.InfoContext(ctx, "PgWire proxy server shut down gracefully")
	return nil
}
