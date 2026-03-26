package internal

import (
	"testing"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetPGConnectionString(t *testing.T) {
	tests := []struct {
		name   string
		config *protos.PostgresConfig
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
			config: &protos.PostgresConfig{
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connStr := GetPGConnectionString(tt.config, "test")
			cfg, err := pgx.ParseConfig(connStr)
			require.NoError(t, err, "ParseConfig error: %v", err)
			assert.Equal(t, tt.config.Password, cfg.Password, "Password mismatch")
			assert.Equal(t, tt.config.Database, cfg.Database, "Database mismatch")
			assert.Equal(t, tt.config.Host, cfg.Host, "Host mismatch")
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
				assert.NotContains(t, connStr, "sslmode=require", "Connection string should contain sslmode=require")
				assert.NotEmpty(t, cfg.Config.Fallbacks)
			}
		})
	}
}
