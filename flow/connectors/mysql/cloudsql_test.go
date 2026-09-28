package connmysql

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestMySQLCloudSQLAuthRejectsUnsafeTLS(t *testing.T) {
	for name, config := range map[string]*protos.MySqlConfig{
		"TLS disabled":           {DisableTls: true},
		"skip cert verification": {SkipCertVerification: true},
		"no TLS host":            {},
	} {
		t.Run(name, func(t *testing.T) {
			config.AuthType = protos.MySqlAuthType_MYSQL_GCP_CLOUD_SQL_IAM_AUTH
			_, err := NewMySqlConnector(t.Context(), config)
			_, ok := errors.AsType[*exceptions.CloudSQLIAMAuthError](err)
			require.True(t, ok, "expected CloudSQLIAMAuthError, got %v", err)
		})
	}
}
