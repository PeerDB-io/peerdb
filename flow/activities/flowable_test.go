package activities

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestPgDumpSchemaAuthSupported(t *testing.T) {
	for _, test := range []struct {
		name      string
		authType  protos.PostgresAuthType
		supported bool
	}{
		{
			name:      "password reaches schema dump",
			authType:  protos.PostgresAuthType_POSTGRES_PASSWORD,
			supported: true,
		},
		{
			name:      "Cloud SQL IAM reaches schema dump",
			authType:  protos.PostgresAuthType_POSTGRES_GCP_CLOUD_SQL_IAM_AUTH,
			supported: true,
		},
		{
			name:     "AWS IAM remains rejected",
			authType: protos.PostgresAuthType_POSTGRES_IAM_AUTH,
		},
		{
			name:     "unknown auth remains rejected",
			authType: protos.PostgresAuthType(99),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.supported, pgDumpSchemaAuthSupported(test.authType))
		})
	}
}
