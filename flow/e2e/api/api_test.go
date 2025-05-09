package e2e_api

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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
	e2eshared.RunSuiteNoParallel(t, func(t *testing.T) Suite {
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
			ch: e2e_clickhouse.SetupSuite(t, func(*testing.T) (TSource, string, error) {
				return source, suffix, nil
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

	schemasResponse, err := s.GetSchemas(s.t.Context(), &protos.PostgresPeerActivityInfoRequest{
		PeerName: s.source.GeneratePeer(s.t).Name,
	})
	require.NoError(s.t, err)
	require.Contains(s.t, schemasResponse.Schemas, "e2e_test_"+s.suffix)

	tablesResponse, err := s.GetAllTables(s.t.Context(), &protos.PostgresPeerActivityInfoRequest{
		PeerName: s.source.GeneratePeer(s.t).Name,
	})
	require.NoError(s.t, err)
	require.Contains(s.t, tablesResponse.Tables, e2e.AttachSchema(s, "listing"))

	columns, err := s.GetColumns(s.t.Context(), &protos.TableColumnsRequest{
		PeerName:   s.source.GeneratePeer(s.t).Name,
		SchemaName: "e2e_test_" + s.suffix,
		TableName:  "listing",
	})
	require.NoError(s.t, err)
	require.Len(s.t, columns.Columns, 2)
	require.Equal(s.t, "id", columns.Columns[0].Name)
	require.True(s.t, columns.Columns[0].IsKey)
	switch source := s.source.(type) {
	case *e2e.MySqlSource:
		if source.Config.Flavor == protos.MySqlFlavor_MYSQL_MARIA {
			require.Equal(s.t, "int(11)", columns.Columns[0].Type)
		} else {
			require.Equal(s.t, "int", columns.Columns[0].Type)
		}
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
	_, ok := s.source.(*e2e.MySqlSource)
	if !ok {
		s.t.Skip("only for MySQL")
	}

	response, err := s.ValidatePeer(s.t.Context(), &protos.ValidatePeerRequest{
		Peer: s.source.GeneratePeer(s.t),
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)
	require.Equal(s.t, protos.ValidatePeerStatus_VALID, response.Status)

	require.NoError(s.t, s.source.Exec(s.t.Context(), "CREATE TABLE IF NOT EXISTS mysql.rds_configuration(name TEXT, value TEXT)"))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		"INSERT INTO mysql.rds_configuration(name, value) VALUES ('binlog retention hours', NULL)"))

	response, err = s.ValidatePeer(s.t.Context(), &protos.ValidatePeerRequest{
		Peer: s.source.GeneratePeer(s.t),
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)
	require.Equal(s.t, protos.ValidatePeerStatus_INVALID, response.Status)
	require.Equal(s.t,
		"failed to validate peer mysql: binlog configuration error: "+
			"RDS/Aurora setting 'binlog retention hours' should be at least 24, currently unset",
		response.Message)

	require.NoError(s.t, s.source.Exec(s.t.Context(), "UPDATE mysql.rds_configuration SET value = '1' WHERE name = 'binlog retention hours'"))
	response, err = s.ValidatePeer(s.t.Context(), &protos.ValidatePeerRequest{
		Peer: s.source.GeneratePeer(s.t),
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)
	require.Equal(s.t, protos.ValidatePeerStatus_INVALID, response.Status)
	require.Equal(s.t,
		"failed to validate peer mysql: binlog configuration error: "+
			"RDS/Aurora setting 'binlog retention hours' should be at least 24, currently 1",
		response.Message)

	err = s.source.Exec(s.t.Context(), "UPDATE mysql.rds_configuration SET value = '24' WHERE name = 'binlog retention hours';")
	require.NoError(s.t, err)
	response, err = s.ValidatePeer(s.t.Context(), &protos.ValidatePeerRequest{
		Peer: s.source.GeneratePeer(s.t),
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)
	require.Equal(s.t, protos.ValidatePeerStatus_VALID, response.Status)

	require.NoError(s.t, s.source.Exec(s.t.Context(), "DROP TABLE IF EXISTS mysql.rds_configuration;"))
}

func (s Suite) TestMySQLFlavorSwap() {
	my, ok := s.source.(*e2e.MySqlSource)
	if !ok {
		s.t.Skip("only for MySQL")
	}

	peer := s.source.GeneratePeer(s.t)
	peer.GetMysqlConfig().Flavor = protos.MySqlFlavor_MYSQL_MARIA
	response, err := s.ValidatePeer(s.t.Context(), &protos.ValidatePeerRequest{
		Peer: peer,
	})

	require.NoError(s.t, err)
	require.NotNil(s.t, response)
	if my.Config.Flavor != protos.MySqlFlavor_MYSQL_MARIA {
		require.Equal(s.t, protos.ValidatePeerStatus_INVALID, response.Status)
		require.Equal(s.t,
			"failed to validate peer mysql: server appears to be MySQL but flavor is set to MariaDB", response.Message)
	}
}

func (s Suite) TestResyncCompleted() {
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", e2e.AttachSchema(s, "valid"))))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", e2e.AttachSchema(s, "valid"))))
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      "resync_completed_" + s.suffix,
		TableNameMapping: map[string]string{e2e.AttachSchema(s, "valid"): "valid"},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.InitialSnapshotOnly = true
	response, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)

	tc := e2e.NewTemporalClient(s.t)
	env, err := e2e.GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)
	e2e.RequireEqualTables(s.ch, "valid", "id,val")

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (2,'resync')", e2e.AttachSchema(s, "valid"))))

	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RESYNC,
	})
	require.NoError(s.t, err)
	e2e.EnvWaitForEqualTables(env, s.ch, "resync", "valid", "id,val")
	env, err = e2e.GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	e2e.EnvWaitForFinished(s.t, env, time.Minute)
}

func (s Suite) TestDropCompleted() {
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", e2e.AttachSchema(s, "valid"))))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", e2e.AttachSchema(s, "valid"))))
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      "drop_completed_" + s.suffix,
		TableNameMapping: map[string]string{e2e.AttachSchema(s, "valid"): "valid"},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.InitialSnapshotOnly = true
	response, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)

	tc := e2e.NewTemporalClient(s.t)
	env, err := e2e.GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)
	e2e.RequireEqualTables(s.ch, "valid", "id,val")

	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_TERMINATING,
	})
	require.NoError(s.t, err)
	e2e.EnvWaitFor(s.t, env, time.Minute, "drop", func() bool {
		var workflowID string
		return s.pg.PostgresConnector.Conn().QueryRow(
			s.t.Context(), "select workflow_id from flows where name = $1", flowConnConfig.FlowJobName,
		).Scan(&workflowID) == pgx.ErrNoRows
	})
}

func (s Suite) TestAlertConfig() {
	create, err := s.PostAlertConfig(s.t.Context(), &protos.PostAlertConfigRequest{
		Config: &protos.AlertConfig{
			Id:              -1,
			ServiceType:     "slack",
			ServiceConfig:   "config",
			AlertForMirrors: []string{"mirror"},
		},
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, create)
	getCreate, err := s.GetAlertConfigs(s.t.Context(), &protos.GetAlertConfigsRequest{})
	require.NoError(s.t, err)
	require.NotNil(s.t, getCreate)
	require.True(s.t, slices.ContainsFunc(getCreate.Configs, func(config *protos.AlertConfig) bool {
		return config.Id == create.Id && config.ServiceType == "slack" && config.ServiceConfig == "config"
	}))

	update, err := s.PostAlertConfig(s.t.Context(), &protos.PostAlertConfigRequest{
		Config: &protos.AlertConfig{
			Id:              create.Id,
			ServiceType:     "email",
			ServiceConfig:   "newconfig",
			AlertForMirrors: []string{"mirror"},
		},
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, update)
	getUpdate, err := s.GetAlertConfigs(s.t.Context(), &protos.GetAlertConfigsRequest{})
	require.NoError(s.t, err)
	require.NotNil(s.t, getUpdate)
	require.True(s.t, slices.ContainsFunc(getUpdate.Configs, func(config *protos.AlertConfig) bool {
		return config.Id == create.Id && config.ServiceType == "email" && config.ServiceConfig == "newconfig"
	}))

	del, err := s.DeleteAlertConfig(s.t.Context(), &protos.DeleteAlertConfigRequest{Id: create.Id})
	require.NoError(s.t, err)
	require.NotNil(s.t, del)
	getDelete, err := s.GetAlertConfigs(s.t.Context(), &protos.GetAlertConfigsRequest{})
	require.NoError(s.t, err)
	require.NotNil(s.t, getDelete)
	require.False(s.t, slices.ContainsFunc(getDelete.Configs, func(config *protos.AlertConfig) bool {
		return config.Id == create.Id
	}))
}

func (s Suite) TestSettings() {
	newValue := "90"
	firstResponse, err := s.GetDynamicSettings(s.t.Context(), &protos.GetDynamicSettingsRequest{})
	require.NoError(s.t, err)
	require.NotNil(s.t, firstResponse)
	require.True(s.t, slices.EqualFunc(firstResponse.Settings, internal.DynamicSettings[:],
		func(x *protos.DynamicSetting, y *protos.DynamicSetting) bool {
			return x.Name == y.Name &&
				x.DefaultValue == y.DefaultValue &&
				x.Description == y.Description &&
				x.ValueType == y.ValueType &&
				x.ApplyMode == y.ApplyMode &&
				x.TargetForSetting == y.TargetForSetting
		}))

	postResponse, err := s.PostDynamicSetting(s.t.Context(), &protos.PostDynamicSettingRequest{
		Name:  "PEERDB_PKM_EMPTY_BATCH_THROTTLE_THRESHOLD_SECONDS",
		Value: &newValue,
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, postResponse)

	secondResponse, err := s.GetDynamicSettings(s.t.Context(), &protos.GetDynamicSettingsRequest{})
	require.NoError(s.t, err)
	require.NotNil(s.t, secondResponse)
	require.True(s.t, slices.ContainsFunc(secondResponse.Settings, func(x *protos.DynamicSetting) bool {
		return x.Name == "PEERDB_PKM_EMPTY_BATCH_THROTTLE_THRESHOLD_SECONDS" && x.Value != nil && *x.Value == newValue
	}))
}

func (s Suite) TestCustomSync() {
	_, err := s.CustomSyncFlow(s.t.Context(), &protos.CreateCustomSyncRequest{FlowJobName: "", NumberOfSyncs: 1})
	require.ErrorContains(s.t, err, "mirror name cannot be empty")
	_, err = s.CustomSyncFlow(s.t.Context(), &protos.CreateCustomSyncRequest{FlowJobName: "flow", NumberOfSyncs: 0})
	require.ErrorContains(s.t, err, "sync number request must be greater than 0")
	_, err = s.CustomSyncFlow(s.t.Context(), &protos.CreateCustomSyncRequest{FlowJobName: "unknown-flow", NumberOfSyncs: 1})
	require.ErrorContains(s.t, err, "mirror unknown-flow does not exist")

	tblName := "apitable"
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", e2e.AttachSchema(s, tblName))))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", e2e.AttachSchema(s, tblName))))
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      "mirrorapi" + s.suffix,
		TableNameMapping: map[string]string{e2e.AttachSchema(s, tblName): tblName},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_ENABLE_PARALLEL_SYNC_NORMALIZE": "false"}
	response, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)

	tc := e2e.NewTemporalClient(s.t)
	env, err := e2e.GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	e2e.EnvWaitForEqualTables(env, s.ch, "initial load", tblName, "id,val")

	// test tags api
	_, err = s.GetFlowTags(s.t.Context(), &protos.GetFlowTagsRequest{FlowName: "unknown-flow"})
	require.ErrorContains(s.t, err, "does not exist")
	_, err = s.CreateOrReplaceFlowTags(s.t.Context(), &protos.CreateOrReplaceFlowTagsRequest{FlowName: "unknown-flow"})
	require.ErrorContains(s.t, err, "does not exist")
	_, err = s.CreateOrReplaceFlowTags(s.t.Context(), &protos.CreateOrReplaceFlowTagsRequest{
		FlowName: flowConnConfig.FlowJobName,
		Tags:     []*protos.FlowTag{{Key: "k", Value: "v"}},
	})
	require.NoError(s.t, err)
	tagsResponse, err := s.GetFlowTags(s.t.Context(), &protos.GetFlowTagsRequest{FlowName: flowConnConfig.FlowJobName})
	require.NoError(s.t, err)
	require.True(s.t, slices.ContainsFunc(tagsResponse.Tags, func(tag *protos.FlowTag) bool {
		return tag.Key == "k" && tag.Value == "v"
	}))

	// test ListMirrors
	listResponse, err := s.ListMirrors(s.t.Context(), &protos.ListMirrorsRequest{})
	require.NoError(s.t, err)
	sourcePeer := s.Source().GeneratePeer(s.t)
	require.True(s.t, slices.ContainsFunc(listResponse.Mirrors, func(mirror *protos.ListMirrorsItem) bool {
		return mirror.WorkflowId == env.GetID() && mirror.SourceName == sourcePeer.Name &&
			mirror.SourceType == sourcePeer.Type && mirror.IsCdc
	}))

	_, err = s.CustomSyncFlow(s.t.Context(), &protos.CreateCustomSyncRequest{FlowJobName: flowConnConfig.FlowJobName, NumberOfSyncs: 1})
	require.ErrorContains(s.t, err, "is not paused")

	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_PAUSED,
	})
	require.NoError(s.t, err)
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "pausing for add table", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_PAUSED
	})

	customResponse, err := s.CustomSyncFlow(s.t.Context(),
		&protos.CreateCustomSyncRequest{FlowJobName: flowConnConfig.FlowJobName, NumberOfSyncs: 1})
	require.NoError(s.t, err)
	require.Equal(s.t, flowConnConfig.FlowJobName, customResponse.FlowJobName)
	require.Equal(s.t, int32(1), customResponse.NumberOfSyncs)
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (2,'pause')", e2e.AttachSchema(s, tblName))))
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "pausing for add table", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_PAUSED
	})
	e2e.RequireEqualTables(s.ch, tblName, "id,val")

	// test MirrorStatus
	statusResponse, err := s.MirrorStatus(s.t.Context(), &protos.MirrorStatusRequest{
		FlowJobName:     flowConnConfig.FlowJobName,
		IncludeFlowInfo: true,
		ExcludeBatches:  false,
	})
	require.NoError(s.t, err)
	require.Equal(s.t, protos.FlowStatus_STATUS_PAUSED, statusResponse.CurrentFlowState)
	cdcStatus := statusResponse.GetCdcStatus()
	require.NotNil(s.t, cdcStatus)
	require.NotNil(s.t, cdcStatus.SnapshotStatus)
	require.Equal(s.t, sourcePeer.Type, cdcStatus.SourceType)
	require.Len(s.t, cdcStatus.SnapshotStatus.Clones, 1)
	require.Equal(s.t, int64(1), cdcStatus.SnapshotStatus.Clones[0].NumRowsSynced)

	for _, aggregateType := range []string{"1min", "1hour", "1day", "1month"} {
		graphResponse, err := s.CDCGraph(s.t.Context(), &protos.GraphRequest{
			FlowJobName:   flowConnConfig.FlowJobName,
			AggregateType: aggregateType,
		})
		require.NoError(s.t, err)
		require.NotEmpty(s.t, graphResponse.Data)
	}

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s Suite) TestQRep() {
	if _, ok := s.source.(*e2e.PostgresSource); !ok {
		s.t.Skip("only run with pg as mysql qrep isn't really supported")
	}

	tblName := "qrepapi"
	schemaQualified := e2e.AttachSchema(s, tblName)
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", schemaQualified)))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", schemaQualified)))

	sourcePeer := s.Source().GeneratePeer(s.t)
	qrepConfig := e2e.CreateQRepWorkflowConfig(
		s.t,
		"qrepapiflow",
		schemaQualified,
		schemaQualified+"dst",
		fmt.Sprintf("SELECT * FROM %s WHERE id BETWEEN {{.start}} AND {{.end}}", schemaQualified),
		sourcePeer.Name,
		"",
		true,
		"",
		"",
	)
	qrepConfig.WatermarkColumn = "id"

	_, err := s.CreateQRepFlow(s.t.Context(), &protos.CreateQRepFlowRequest{
		QrepConfig:         qrepConfig,
		CreateCatalogEntry: true,
	})
	require.NoError(s.t, err)

	tc := e2e.NewTemporalClient(s.t)
	env, err := e2e.GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, qrepConfig.FlowJobName)
	require.NoError(s.t, err)
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)
	require.NoError(s.t, env.Error(s.t.Context()))

	statusResponse, err := s.MirrorStatus(s.t.Context(), &protos.MirrorStatusRequest{
		FlowJobName:     qrepConfig.FlowJobName,
		IncludeFlowInfo: true,
		ExcludeBatches:  false,
	})
	require.NoError(s.t, err)
	require.Equal(s.t, protos.FlowStatus_STATUS_COMPLETED, statusResponse.CurrentFlowState)
	qStatus := statusResponse.GetQrepStatus()
	require.NotNil(s.t, qStatus)
	require.Len(s.t, qStatus.Partitions, 1)
	require.Equal(s.t, int64(1), qStatus.Partitions[0].RowsInPartition)
	require.Equal(s.t, int64(1), qStatus.Partitions[0].RowsSynced)
}
