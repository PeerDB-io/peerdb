package connpostgres

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestPostgresCloudSQLAuthRejectsBadInstance(t *testing.T) {
	for name, host := range map[string]string{
		"no host":            "",
		"malformed instance": "project:instance",
	} {
		t.Run(name, func(t *testing.T) {
			config := &protos.PostgresConfig{
				Host:     host,
				AuthType: protos.PostgresAuthType_POSTGRES_GCP_CLOUD_SQL_IAM_AUTH,
			}
			_, err := NewPostgresConnector(t.Context(), nil, config)
			_, ok := errors.AsType[*exceptions.CloudSQLIAMAuthError](err)
			require.True(t, ok, "expected CloudSQLIAMAuthError, got %v", err)
		})
	}
}
