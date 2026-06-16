package internal

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestPGMustUseTlsConnection(t *testing.T) {
	boolPtr := func(b bool) *bool { return &b }

	tests := []struct {
		disableTls *bool
		name       string
		requireTls bool
		expected   bool
	}{
		{
			// Legacy API: disable_tls missing, no TLS required.
			name:       "require=false disable=missing",
			requireTls: false,
			disableTls: nil,
			expected:   false,
		},
		{
			// New API using DisableTLS: disable_tls explicitly false => use TLS.
			name:       "require=false disable=false",
			requireTls: false,
			disableTls: boolPtr(false),
			expected:   true,
		},
		{
			// New API using DisableTLS: disable_tls explicitly true => no TLS.
			name:       "require=false disable=true",
			requireTls: false,
			disableTls: boolPtr(true),
			expected:   false,
		},
		{
			// RequireTls dominates regardless of disable_tls being missing.
			name:       "require=true disable=missing",
			requireTls: true,
			disableTls: nil,
			expected:   true,
		},
		{
			// Should not be seen in practice, RequireTls dominates.
			name:       "require=true disable=false",
			requireTls: true,
			disableTls: boolPtr(false),
			expected:   true,
		},
		{
			// Should not be seen in practice, RequireTls dominates.
			name:       "require=true disable=true",
			requireTls: true,
			disableTls: boolPtr(true),
			expected:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &protos.PostgresConfig{
				RequireTls: tt.requireTls,
				DisableTls: tt.disableTls,
			}
			assert.Equal(t, tt.expected, PGMustUseTlsConnection(config))
		})
	}
}

func TestGetPGConnectionString(t *testing.T) {
	tests := []struct {
		name         string
		config       *protos.PostgresConfig
		expectedHost string
	}{
		{
			name: "password with space",
			config: &protos.PostgresConfig{
				Host:       "localhost",
				Port:       5432,
				Database:   "testdb",
				User:       "testuser",
				Password:   "pass word",
				RequireTls: false,
			},
		},
		{
			name: "password with special chars",
			config: &protos.PostgresConfig{ //nolint:gosec // test credentials
				Host:       "localhost",
				Port:       5432,
				Database:   "testdb",
				User:       "testuser",
				Password:   "p@ss:w/?ord#test",
				RequireTls: false,
			},
		},
		{
			name: "require tls",
			config: &protos.PostgresConfig{
				Host:       "localhost",
				Port:       5432,
				Database:   "testdb",
				User:       "testuser",
				Password:   "password",
				RequireTls: true,
			},
		},
		{
			name: "empty password",
			config: &protos.PostgresConfig{
				Host:       "localhost",
				Port:       5432,
				Database:   "testdb",
				User:       "testuser",
				Password:   "",
				RequireTls: false,
			},
		},
		{
			name: "host with path and query params",
			config: &protos.PostgresConfig{
				Host:       "some-host.azure.neon.tech/results?sslmode=require&channel_binding=require",
				Port:       5432,
				Database:   "testdb",
				User:       "testuser",
				Password:   "password",
				RequireTls: false,
			},
			expectedHost: "some-host.azure.neon.tech",
		},
		{
			name: "host with query params only",
			config: &protos.PostgresConfig{
				Host:       "some-host.azure.neon.tech?sslmode=require&channel_binding=require",
				Port:       5432,
				Database:   "testdb",
				User:       "testuser",
				Password:   "password",
				RequireTls: false,
			},
			expectedHost: "some-host.azure.neon.tech",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connStr := GetPGConnectionString(tt.config, "test")
			cfg, err := pgx.ParseConfig(connStr)
			require.NoError(t, err, "ParseConfig error: %v", err)
			assert.Equal(t, tt.config.Password, cfg.Password, "Password mismatch")
			assert.Equal(t, tt.config.Database, cfg.Database, "Database mismatch")
			expectedHost := tt.config.Host
			if tt.expectedHost != "" {
				expectedHost = tt.expectedHost
			}
			assert.Equal(t, expectedHost, cfg.Host, "Host mismatch")
			assert.Equal(t, uint16(tt.config.Port), cfg.Port, "Port mismatch")
			assert.Equal(t, "peerdb_test", cfg.Config.RuntimeParams["application_name"], "Application name mismatch")
			assert.Equal(t, "UTF8", cfg.Config.RuntimeParams["client_encoding"], "Client encoding mismatch")

			// without explicit sslmode pgx assumes sslmode=prefer
			// in this case it also adds a Fallback to connect without TLS if the server doesn't support it
			// when sslmode is set to "require" the Fallback is not added
			// we can't check cfg.TLSConfig because it is not nil in both cases
			if tt.config.RequireTls {
				assert.NotNil(t, cfg.TLSConfig, "TLSConfig should be set if RequireTls is true")
				assert.Contains(t, connStr, "sslmode=require", "Connection string should contain sslmode=require")
				assert.Empty(t, cfg.Config.Fallbacks)
			} else {
				assert.NotContains(t, connStr, "sslmode=require", "Connection string shouldn't contain sslmode=require")
				assert.NotEmpty(t, cfg.Config.Fallbacks)
			}
		})
	}
}
