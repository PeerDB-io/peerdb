package pgwire

import (
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
)

// ServerConfig holds configuration for the PgWire proxy server
type ServerConfig struct {
	// ListenAddress is the address to listen on (e.g., ":5432")
	ListenAddress string

	// UpstreamDSN is the PostgreSQL connection string for the upstream database
	UpstreamDSN string

	// EnableTLS enables server-side TLS for client connections
	EnableTLS bool

	// TLSCertPath is the path to the TLS certificate file
	TLSCertPath string

	// TLSKeyPath is the path to the TLS key file
	TLSKeyPath string

	// TLSConfig is the parsed TLS configuration (set during validation)
	TLSConfig *tls.Config

	// MaxConnections is the maximum number of concurrent client connections
	MaxConnections int

	// QueryTimeout is the maximum time a query can run
	QueryTimeout time.Duration

	// IdleTimeout is the maximum time a connection can be idle before being closed
	// Default is 30 minutes if not set
	IdleTimeout time.Duration

	// MaxRows is the maximum number of rows to return per query (0 = unlimited)
	MaxRows int64

	// MaxBytes is the maximum bytes to return per query (0 = unlimited)
	MaxBytes int64

	// DenyStatements is a list of SQL statement prefixes to deny (e.g., ["DROP", "TRUNCATE"])
	DenyStatements []string

	// AllowStatements is a list of SQL statement prefixes to allow (if set, only these are allowed)
	AllowStatements []string

	// OtelManager for metrics collection (can be nil)
	OtelManager *otel_metrics.OtelManager
}

// Validate checks the configuration and returns an error if invalid
func (c *ServerConfig) Validate() error {
	if c.ListenAddress == "" {
		return errors.New("listen address cannot be empty")
	}

	if c.UpstreamDSN == "" {
		return errors.New("upstream DSN cannot be empty")
	}

	if c.MaxConnections <= 0 {
		return fmt.Errorf("max connections must be positive, got %d", c.MaxConnections)
	}

	if c.QueryTimeout <= 0 {
		return fmt.Errorf("query timeout must be positive, got %v", c.QueryTimeout)
	}

	// Default idle timeout to 30 minutes if not set
	if c.IdleTimeout == 0 {
		c.IdleTimeout = 30 * time.Minute
	} else if c.IdleTimeout < 0 {
		return fmt.Errorf("idle timeout cannot be negative, got %v", c.IdleTimeout)
	}

	if c.MaxRows < 0 {
		return fmt.Errorf("max rows cannot be negative, got %d", c.MaxRows)
	}

	if c.MaxBytes < 0 {
		return fmt.Errorf("max bytes cannot be negative, got %d", c.MaxBytes)
	}

	// Load TLS configuration if enabled
	if c.EnableTLS {
		if c.TLSCertPath == "" || c.TLSKeyPath == "" {
			return errors.New("TLS cert and key paths required when TLS is enabled")
		}

		cert, err := tls.LoadX509KeyPair(c.TLSCertPath, c.TLSKeyPath)
		if err != nil {
			return fmt.Errorf("failed to load TLS certificate: %w", err)
		}

		c.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
	}

	return nil
}

// SessionMetrics tracks metrics for a single session
type SessionMetrics struct {
	ConnectionTime time.Time
	QueriesTotal   int64
	BytesSent      int64
	RowsSent       int64
	ErrorsTotal    int64
}
