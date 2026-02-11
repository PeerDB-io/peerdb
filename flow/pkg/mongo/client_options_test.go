package mongo

import (
	"net"
	"testing"
	"time"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildClientOptions_TLS(t *testing.T) {
	tests := []struct {
		name                     string
		uri                      string
		disableTls               bool
		rootCa                   string
		tlsHost                  string
		expectTLSConfig          bool
		expectInsecureSkipVerify bool
		expectServerName         string
	}{
		{
			name:            "toggle disables TLS is respected",
			uri:             "mongodb://localhost:27017/",
			disableTls:      true,
			expectTLSConfig: false,
		},
		{
			name:            "tls=false in URI is respected",
			uri:             "mongodb://localhost:27017/?tls=false",
			disableTls:      false,
			expectTLSConfig: false,
		},
		{
			name:                     "tlsInsecure=true in uri param is respected",
			uri:                      "mongodb://localhost:27017/?tlsInsecure=true",
			disableTls:               false,
			expectTLSConfig:          true,
			expectInsecureSkipVerify: true,
		},
		{
			name:                     "default TLS config is used",
			uri:                      "mongodb://localhost:27017/",
			disableTls:               false,
			expectTLSConfig:          true,
			expectInsecureSkipVerify: false,
		},
		{
			name:            "default TLS config with ServerName set",
			uri:             "mongodb://localhost:27017/",
			disableTls:      false,
			expectTLSConfig: true,
			tlsHost:         "custom.host.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := ClientConfig{
				Uri:                 tt.uri,
				Username:            "testuser",
				Password:            "testpass",
				ReadPreference:      ReadPreferenceSecondaryPreferred,
				RootCa:              tt.rootCa,
				TlsHost:             tt.tlsHost,
				DisableTls:          tt.disableTls,
				CreateTlsConfigFunc: common.CreateTlsConfigFromRootCAString,
				Dialer:              &net.Dialer{Timeout: time.Second},
			}

			opts, err := BuildClientOptions(config)
			require.NoError(t, err)

			if !tt.expectTLSConfig {
				assert.Nil(t, opts.TLSConfig)
			} else {
				require.NotNil(t, opts.TLSConfig)
				assert.Equal(t, tt.expectInsecureSkipVerify, opts.TLSConfig.InsecureSkipVerify)
				if tt.tlsHost != "" {
					assert.Equal(t, tt.tlsHost, opts.TLSConfig.ServerName)
				}
			}
		})
	}
}
