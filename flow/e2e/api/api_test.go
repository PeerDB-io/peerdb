package e2e_api

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func NewClient(t *testing.T) protos.FlowServiceClient {
	t.Helper()

	client, err := grpc.NewClient("0.0.0.0:8112", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	return protos.NewFlowServiceClient(client)
}

func TestPostgresValidation(t *testing.T) {
	client := NewClient(t)
	config := internal.GetCatalogPostgresConfigFromEnv(t.Context())
	config.Password = "wrong"
	response, err := client.ValidatePeer(t.Context(), &protos.ValidatePeerRequest{
		Peer: &protos.Peer{
			Name:   "testfail",
			Type:   protos.DBType_POSTGRES,
			Config: &protos.Peer_PostgresConfig{PostgresConfig: config},
		},
	})
	t.Log(response, err)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Equal(t, protos.ValidatePeerStatus_INVALID, response.Status)
}
