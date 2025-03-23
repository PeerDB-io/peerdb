package e2e_api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func NewClient(t *testing.T) protos.FlowServiceClient {
	t.Helper()

	client, err := grpc.NewClient("0.0.0.0:8112", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	return protos.NewFlowServiceClient(client)
}

type Suite struct {
	protos.FlowServiceClient
	*testing.T
}

func (s Suite) Teardown(ctx context.Context) {
}

func TestApi(t *testing.T) {
	e2eshared.RunSuite(t, func(t *testing.T) Suite {
		t.Helper()
		return Suite{FlowServiceClient: NewClient(t), T: t}
	})
}

func (s Suite) TestPostgresValidation_WrongPassword() {
	config := internal.GetCatalogPostgresConfigFromEnv(s.Context())
	config.Password = "wrong"
	response, err := s.ValidatePeer(s.Context(), &protos.ValidatePeerRequest{
		Peer: &protos.Peer{
			Name:   "testfail",
			Type:   protos.DBType_POSTGRES,
			Config: &protos.Peer_PostgresConfig{PostgresConfig: config},
		},
	})
	require.NoError(s.T, err)
	require.NotNil(s.T, response)
	require.Equal(s.T, protos.ValidatePeerStatus_INVALID, response.Status)
}

func (s Suite) TestPostgresValidation_Pass() {
	config := internal.GetCatalogPostgresConfigFromEnv(s.Context())
	response, err := s.ValidatePeer(s.Context(), &protos.ValidatePeerRequest{
		Peer: &protos.Peer{
			Name:   "testfail",
			Type:   protos.DBType_POSTGRES,
			Config: &protos.Peer_PostgresConfig{PostgresConfig: config},
		},
	})
	require.NoError(s.T, err)
	require.NotNil(s.T, response)
	require.Equal(s.T, protos.ValidatePeerStatus_VALID, response.Status)
}
