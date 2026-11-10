package mongo

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

type fakeAWSCredentialsProvider struct{}

func (fakeAWSCredentialsProvider) Retrieve(context.Context) (options.AWSCredentials, error) {
	return options.AWSCredentials{}, nil
}

func TestBuildClientOptions_Auth(t *testing.T) {
	provider := fakeAWSCredentialsProvider{}
	tests := []struct {
		name       string
		username   string
		password   string
		provider   options.AWSCredentialsProvider
		expectAuth options.Credential
	}{
		{
			name:       "password login is unchanged",
			username:   "testuser",
			password:   "testpass",
			expectAuth: options.Credential{Username: "testuser", Password: "testpass"},
		},
		{
			name:     "IAM login uses MONGODB-AWS against $external",
			provider: provider,
			expectAuth: options.Credential{
				AuthMechanism:          "MONGODB-AWS",
				AuthSource:             "$external",
				AWSCredentialsProvider: provider,
			},
		},
		{
			name:     "IAM login ignores a username and password, which the driver would use as AWS keys",
			username: "testuser",
			password: "testpass",
			provider: provider,
			expectAuth: options.Credential{
				AuthMechanism:          "MONGODB-AWS",
				AuthSource:             "$external",
				AWSCredentialsProvider: provider,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := BuildClientOptions(ClientConfig{
				Uri:                    "mongodb://localhost:27017/",
				Username:               tt.username,
				Password:               tt.password,
				AWSCredentialsProvider: tt.provider,
				ReadPreference:         ReadPreferenceSecondaryPreferred,
				CreateTlsConfigFunc:    common.CreateTlsConfigFromRootCAString,
				Dialer:                 &net.Dialer{Timeout: time.Second},
			})
			require.NoError(t, err)
			require.NotNil(t, opts.Auth)
			assert.Equal(t, tt.expectAuth, *opts.Auth)
		})
	}
}

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
			tlsHost:         "custom.host.com",
			expectTLSConfig: true,
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
