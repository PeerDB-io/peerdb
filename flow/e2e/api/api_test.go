package e2e_api

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/e2e"
	e2e_clickhouse "github.com/PeerDB-io/peerdb/flow/e2e/clickhouse"
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
	ch     e2e_clickhouse.ClickHouseSuite
	source e2e.SuiteSource
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
	return s.source
}

func (s Suite) Connector() *connpostgres.PostgresConnector {
	return s.pg.PostgresConnector
}

func (s Suite) DestinationTable(table string) string {
	return table
}

func testApi[TSource e2e.SuiteSource](
	t *testing.T,
	setup func(*testing.T, string) (TSource, error),
) {
	t.Helper()
	e2eshared.RunSuite(t, func(t *testing.T) Suite {
		t.Helper()

		suffix := "api_" + strings.ToLower(shared.RandomString(8))
		pg, err := e2e.SetupPostgres(t, suffix)
		require.NoError(t, err)
		source, err := setup(t, suffix)
		require.NoError(t, err)
		return Suite{
			FlowServiceClient: NewClient(t),
			t:                 t,
			pg:                pg,
			source:            source,
			ch: e2e_clickhouse.SetupSuite(t, func(*testing.T, string) (TSource, error) {
				return source, nil
			})(t),
			suffix: suffix,
		}
	})
}

func TestApiPg(t *testing.T) {
	testApi(t, e2e.SetupPostgres)
}

func TestApiMy(t *testing.T) {
	testApi(t, e2e.SetupMySQL)
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

func (s Suite) TestClickHouseMirrorValidation_Pass() {
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", e2e.AttachSchema(s, "valid"))))
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      "ch_validation_" + s.suffix,
		TableNameMapping: map[string]string{e2e.AttachSchema(s, "valid"): "valid"},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	response, err := s.ValidateCDCMirror(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)
}

func (s Suite) TestSchemaEndpoints() {
	peerInfo, err := s.GetPeerInfo(s.t.Context(), &protos.PeerInfoRequest{
		PeerName: s.source.GeneratePeer(s.t).Name,
	})
	require.NoError(s.t, err)
	switch config := peerInfo.Peer.Config.(type) {
	case *protos.Peer_PostgresConfig:
		require.Equal(s.t, "********", config.PostgresConfig.Password)
	case *protos.Peer_MysqlConfig:
		require.Equal(s.t, "********", config.MysqlConfig.Password)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown peer type %T", config))
	}

	peerType, err := s.GetPeerType(s.t.Context(), &protos.PeerInfoRequest{
		PeerName: s.source.GeneratePeer(s.t).Name,
	})
	require.NoError(s.t, err)
	switch s.source.(type) {
	case *e2e.MySqlSource:
		require.Equal(s.t, "MYSQL", peerType.PeerType)
	case *e2e.PostgresSource:
		require.Equal(s.t, "POSTGRES", peerType.PeerType)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", e2e.AttachSchema(s, "listing"))))
	tablesInSchema, err := s.GetTablesInSchema(s.t.Context(), &protos.SchemaTablesRequest{
		PeerName:   s.source.GeneratePeer(s.t).Name,
		SchemaName: "e2e_test_" + s.suffix,
	})
	require.NoError(s.t, err)
	require.Len(s.t, tablesInSchema.Tables, 1)
	require.Equal(s.t, "listing", tablesInSchema.Tables[0].TableName)

	columns, err := s.GetColumns(s.t.Context(), &protos.TableColumnsRequest{
		PeerName:   s.source.GeneratePeer(s.t).Name,
		SchemaName: "e2e_test_" + s.suffix,
		TableName:  "listing",
	})
	require.NoError(s.t, err)
	require.Len(s.t, columns.Columns, 2)
	require.Equal(s.t, "id", columns.Columns[0].Name)
	require.True(s.t, columns.Columns[0].IsKey)
	switch s.source.(type) {
	case *e2e.MySqlSource:
		require.Equal(s.t, "int", columns.Columns[0].Type)
	case *e2e.PostgresSource:
		require.Equal(s.t, "integer", columns.Columns[0].Type)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}
	require.Equal(s.t, "val", columns.Columns[1].Name)
	require.False(s.t, columns.Columns[1].IsKey)
	require.Equal(s.t, "text", columns.Columns[1].Type)
}

func (s Suite) TestScripts() {
	if _, ok := s.source.(*e2e.PostgresSource); !ok {
		s.t.Skip("only run with pg so only one test against scripts runs")
	}

	script := &protos.Script{
		Id:     -1,
		Name:   "apitest",
		Lang:   "lua",
		Source: "test",
	}
	postResponse, err := s.PostScript(s.t.Context(), &protos.PostScriptRequest{
		Script: script,
	})
	require.NoError(s.t, err)

	getResponse, err := s.GetScripts(s.t.Context(), &protos.GetScriptsRequest{
		Id: postResponse.Id,
	})
	require.NoError(s.t, err)
	require.Len(s.t, getResponse.Scripts, 1)
	script.Id = getResponse.Scripts[0].Id
	require.Equal(s.t, "apitest", getResponse.Scripts[0].Name)
	require.Equal(s.t, "lua", getResponse.Scripts[0].Lang)
	require.Equal(s.t, "test", getResponse.Scripts[0].Source)

	script.Name = "apitest2"
	postResponse, err = s.PostScript(s.t.Context(), &protos.PostScriptRequest{
		Script: script,
	})
	require.NoError(s.t, err)
	require.Equal(s.t, script.Id, postResponse.Id)

	getResponse, err = s.GetScripts(s.t.Context(), &protos.GetScriptsRequest{
		Id: postResponse.Id,
	})
	require.NoError(s.t, err)
	require.Len(s.t, getResponse.Scripts, 1)
	require.Equal(s.t, "apitest2", getResponse.Scripts[0].Name)

	getResponse, err = s.GetScripts(s.t.Context(), &protos.GetScriptsRequest{
		Id: -1,
	})
	require.NoError(s.t, err)
	var contains bool
	for _, responseScript := range getResponse.Scripts {
		if responseScript.Name == script.Name {
			contains = true
			break
		}
	}
	if !contains {
		require.Fail(s.t, "script missing from listing")
	}

	_, err = s.DeleteScript(s.t.Context(), &protos.DeleteScriptRequest{
		Id: script.Id,
	})
	require.NoError(s.t, err)

	getResponse, err = s.GetScripts(s.t.Context(), &protos.GetScriptsRequest{
		Id: -1,
	})
	require.NoError(s.t, err)
	for _, responseScript := range getResponse.Scripts {
		if responseScript.Name == script.Name {
			require.Fail(s.t, "script not deleted")
		}
	}
}

func (s Suite) TestMySQLBinlogValidation() {
	if _, ok := s.source.(*e2e.MySqlSource); !ok {
		s.t.Skip("only for MySQL")
	}

	response, err := s.ValidatePeer(s.t.Context(), &protos.ValidatePeerRequest{
		Peer: s.source.GeneratePeer(s.t),
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)
	require.Equal(s.t, protos.ValidatePeerStatus_VALID, response.Status)

	err = s.source.Exec(s.t.Context(), "CREATE TABLE mysql.rds_configuration(name TEXT, value TEXT);")
	require.NoError(s.t, err)
	err = s.source.Exec(s.t.Context(), "INSERT INTO mysql.rds_configuration(name, value) VALUES ('binlog retention hours', NULL);")
	require.NoError(s.t, err)

	response, err = s.ValidatePeer(s.t.Context(), &protos.ValidatePeerRequest{
		Peer: s.source.GeneratePeer(s.t),
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)
	require.Equal(s.t, protos.ValidatePeerStatus_INVALID, response.Status)
	require.Equal(s.t,
		"failed to validate peer mysql: binlog configuration error: "+
			"RDS/Aurora setting 'binlog retention hours' should be at least 24, "+
			"currently unset",
		response.Message)

	err = s.source.Exec(s.t.Context(), "UPDATE mysql.rds_configuration SET value = '1' WHERE name = 'binlog retention hours';")
	require.NoError(s.t, err)
	response, err = s.ValidatePeer(s.t.Context(), &protos.ValidatePeerRequest{
		Peer: s.source.GeneratePeer(s.t),
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)
	require.Equal(s.t, protos.ValidatePeerStatus_INVALID, response.Status)
	require.Equal(s.t,
		"failed to validate peer mysql: binlog configuration error: "+
			"RDS/Aurora setting 'binlog retention hours' should be at least 24, "+
			"currently 1",
		response.Message)

	err = s.source.Exec(s.t.Context(), "UPDATE mysql.rds_configuration SET value = '24' WHERE name = 'binlog retention hours';")
	require.NoError(s.t, err)
	response, err = s.ValidatePeer(s.t.Context(), &protos.ValidatePeerRequest{
		Peer: s.source.GeneratePeer(s.t),
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)
	require.Equal(s.t, protos.ValidatePeerStatus_VALID, response.Status)

	err = s.source.Exec(s.t.Context(), "DROP TABLE mysql.rds_configuration;")
	require.NoError(s.t, err)
}

func (s Suite) TestMySQLFlavorSwap() {
	if _, ok := s.source.(*e2e.MySqlSource); !ok {
		s.t.Skip("only for MySQL")
	}

	peer := s.source.GeneratePeer(s.t)
	peer.GetMysqlConfig().Flavor = protos.MySqlFlavor_MYSQL_MARIA
	response, err := s.ValidatePeer(s.t.Context(), &protos.ValidatePeerRequest{
		Peer: peer,
	})

	require.NoError(s.t, err)
	require.NotNil(s.t, response)
	require.Equal(s.t, protos.ValidatePeerStatus_INVALID, response.Status)
	require.Equal(s.t,
		"failed to validate peer mysql: server appears to be MySQL but flavor is set to MariaDB", response.Message)
}
