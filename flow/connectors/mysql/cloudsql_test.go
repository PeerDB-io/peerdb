package connmysql

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestMySQLCloudSQLAuthRejectsBadInstance(t *testing.T) {
	for name, host := range map[string]string{
		"no host":            "",
		"malformed instance": "project:instance",
		"IP address":         "35.238.144.132",
	} {
		t.Run(name, func(t *testing.T) {
			config := &protos.MySqlConfig{
				Host:     host,
				AuthType: protos.MySqlAuthType_MYSQL_GCP_CLOUD_SQL_IAM_AUTH,
			}
			_, err := NewMySqlConnector(t.Context(), config)
			_, ok := errors.AsType[*exceptions.CloudSQLIAMAuthError](err)
			require.True(t, ok, "expected CloudSQLIAMAuthError, got %v", err)
		})
	}
}
