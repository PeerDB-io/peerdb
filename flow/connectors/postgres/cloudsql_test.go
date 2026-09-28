package connpostgres

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestPostgresCloudSQLAuthRejectsUnsafeTLS(t *testing.T) {
	disableTLS := true
	for name, config := range map[string]*protos.PostgresConfig{
		"TLS disabled":           {DisableTls: &disableTLS},
		"skip cert verification": {SkipCertVerification: true},
		"no TLS host":            {},
	} {
		t.Run(name, func(t *testing.T) {
			config.AuthType = protos.PostgresAuthType_POSTGRES_GCP_CLOUD_SQL_IAM_AUTH
			_, err := NewPostgresConnector(t.Context(), nil, config)
			_, ok := errors.AsType[*exceptions.CloudSQLIAMAuthError](err)
			require.True(t, ok, "expected CloudSQLIAMAuthError, got %v", err)
		})
	}
}
