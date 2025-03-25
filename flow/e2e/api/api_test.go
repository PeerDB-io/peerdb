package e2e_api

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func NewClient(t *testing.T) protos.FlowServiceClient {
	t.Helper()

	client, err := grpc.NewClient("0.0.0.0:8112", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	return protos.NewFlowServiceClient(client)
}

type Suite struct {
	protos.FlowServiceClient
	t      *testing.T
	pg     *e2e.PostgresSource
	suffix string
}

func (s Suite) Teardown(ctx context.Context) {
	s.pg.Teardown(s.t, ctx, s.suffix)
}

func (s Suite) T() *testing.T {
	return s.t
}

func (s Suite) Suffix() string {
	return s.suffix
}

func (s Suite) Source() e2e.SuiteSource {
	return s.pg
}

func (s Suite) Connector() *connpostgres.PostgresConnector {
	return s.pg.PostgresConnector
}

func (s Suite) DestinationTable(table string) string {
	return table
}

func TestApi(t *testing.T) {
	e2eshared.RunSuite(t, func(t *testing.T) Suite {
		t.Helper()

		suffix := "api_" + strings.ToLower(shared.RandomString(8))
		pg, err := e2e.SetupPostgres(t, suffix)
		require.NoError(t, err)
		return Suite{
			FlowServiceClient: NewClient(t),
			t:                 t,
			pg:                pg,
			suffix:            suffix,
		}
	})
}

func (s Suite) TestGetVersion() {
	response, err := s.GetVersion(s.t.Context(), &protos.PeerDBVersionRequest{})
	require.NoError(s.t, err)
	require.Equal(s.t, internal.PeerDBVersionShaShort(), response.Version)
}

func (s Suite) TestPostgresValidation_WrongPassword() {
	config := internal.GetCatalogPostgresConfigFromEnv(s.t.Context())
	config.Password = "wrong"
	response, err := s.ValidatePeer(s.t.Context(), &protos.ValidatePeerRequest{
		Peer: &protos.Peer{
			Name:   "testfail",
			Type:   protos.DBType_POSTGRES,
			Config: &protos.Peer_PostgresConfig{PostgresConfig: config},
		},
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)
	require.Equal(s.t, protos.ValidatePeerStatus_INVALID, response.Status)
}

func (s Suite) TestPostgresValidation_Pass() {
	config := internal.GetCatalogPostgresConfigFromEnv(s.t.Context())
	response, err := s.ValidatePeer(s.t.Context(), &protos.ValidatePeerRequest{
		Peer: &protos.Peer{
			Name:   "testfail",
			Type:   protos.DBType_POSTGRES,
			Config: &protos.Peer_PostgresConfig{PostgresConfig: config},
		},
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)
	require.Equal(s.t, protos.ValidatePeerStatus_VALID, response.Status)
}

func (s Suite) TestPostgresClickHouseMirrorValidation_Pass() {
	require.NoError(s.t, s.pg.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", e2e.AttachSchema(s, "valid"))))
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      "ch_validation_%s" + s.suffix,
		TableNameMapping: map[string]string{e2e.AttachSchema(s, "valid"): "valid"},
		Destination:      e2e.GeneratePostgresPeer(s.t).Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	response, err := s.ValidateCDCMirror(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)
}
