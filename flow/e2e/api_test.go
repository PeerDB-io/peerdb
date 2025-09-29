package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	connmongo "github.com/PeerDB-io/peerdb/flow/connectors/mongo"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/mongo"
)

type APITestSuite struct {
	protos.FlowServiceClient
	t      *testing.T
	pg     *PostgresSource
	source SuiteSource
	suffix string
	ch     ClickHouseSuite
}

func (s APITestSuite) Teardown(ctx context.Context) {
	s.pg.Teardown(s.t, ctx, s.suffix)
}

func (s APITestSuite) T() *testing.T {
	return s.t
}

func (s APITestSuite) Suffix() string {
	return s.suffix
}

func (s APITestSuite) Source() SuiteSource {
	return s.source
}

func (s APITestSuite) Connector() *connpostgres.PostgresConnector {
	return s.pg.PostgresConnector
}

func (s APITestSuite) DestinationTable(table string) string {
	return table
}

// checkMetadataLastSyncStateValues checks the values of sync_batch_id and normalize_batch_id
// in the metadata_last_sync_state table
func (s APITestSuite) checkMetadataLastSyncStateValues(
	env WorkflowRun,
	flowConnConfig *protos.FlowConnectionConfigs,
	reason string,
	expectedSyncBatchId int64,
	expectedNormalizeBatchId int64,
) {
	EnvWaitFor(s.t, env, 5*time.Minute, "sync flow check: "+reason, func() bool {
		var syncBatchID pgtype.Int8
		queryErr := s.pg.PostgresConnector.Conn().QueryRow(
			s.t.Context(),
			"select sync_batch_id from metadata_last_sync_state where job_name = $1",
			flowConnConfig.FlowJobName,
		).Scan(&syncBatchID)
		if queryErr != nil {
			return false
		}
		return syncBatchID.Valid && (syncBatchID.Int64 == expectedSyncBatchId)
	})

	EnvWaitFor(s.t, env, 5*time.Minute, "normalize flow check: "+reason, func() bool {
		var normalizeBatchID pgtype.Int8
		queryErr := s.pg.PostgresConnector.Conn().QueryRow(
			s.t.Context(),
			"select normalize_batch_id from metadata_last_sync_state where job_name = $1",
			flowConnConfig.FlowJobName,
		).Scan(&normalizeBatchID)
		if queryErr != nil {
			return false
		}
		return normalizeBatchID.Valid && (normalizeBatchID.Int64 == expectedNormalizeBatchId)
	})
}

func (s APITestSuite) waitForActiveSlotForPostgresMirror(env WorkflowRun, conn *pgx.Conn, mirrorName string) {
	slotName := "peerflow_slot_" + mirrorName
	EnvWaitFor(s.t, env, 3*time.Minute, "waiting for replication to become active", func() bool {
		var active pgtype.Bool
		err := conn.QueryRow(s.t.Context(),
			`SELECT active FROM pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&active)
		if err != nil {
			return false
		}
		return active.Valid && active.Bool
	})
}

func (s APITestSuite) loadConfigFromCatalog(
	ctx context.Context,
	conn *pgx.Conn,
	flowName string,
) (*protos.FlowConnectionConfigs, error) {
	var configBytes sql.RawBytes
	if err := conn.QueryRow(ctx,
		"SELECT config_proto FROM flows WHERE name = $1", flowName,
	).Scan(&configBytes); err != nil {
		return nil, err
	}

	var config protos.FlowConnectionConfigs
	return &config, proto.Unmarshal(configBytes, &config)
}

// checkCatalogTableMapping checks the table mappings in the catalog for a given flow
func (s APITestSuite) checkCatalogTableMapping(
	ctx context.Context,
	conn *pgx.Conn,
	flowName string,
	expectedSourceTableNames []string,
) (bool, error) {
	config, err := s.loadConfigFromCatalog(ctx, conn, flowName)
	if err != nil {
		return false, fmt.Errorf("failed to load config from catalog: %w", err)
	}

	if len(config.TableMappings) != len(expectedSourceTableNames) {
		return false, fmt.Errorf("expected %d table mappings, got %d", len(expectedSourceTableNames), len(config.TableMappings))
	}
	sourceTableNames := make([]string, len(config.TableMappings))
	for i, tableMapping := range config.TableMappings {
		sourceTableNames[i] = tableMapping.SourceTableIdentifier
	}

	if len(sourceTableNames) != len(expectedSourceTableNames) {
		return false, fmt.Errorf("expected %d source table names, got %d", len(expectedSourceTableNames), len(sourceTableNames))
	}

	for _, expectedName := range expectedSourceTableNames {
		if !slices.Contains(sourceTableNames, expectedName) {
			return false, fmt.Errorf("expected source table name %s not found in %v", expectedName, sourceTableNames)
		}
	}
	return true, nil
}

func testApi[TSource SuiteSource](
	t *testing.T,
	setup func(*testing.T, string) (TSource, error),
) {
	t.Helper()
	e2eshared.RunSuite(t, func(t *testing.T) APITestSuite {
		t.Helper()

		suffix := "api_" + strings.ToLower(shared.RandomString(8))
		pg, err := SetupPostgres(t, suffix)
		require.NoError(t, err)
		source, err := setup(t, suffix)
		require.NoError(t, err)
		client, err := NewApiClient()
		require.NoError(t, err)
		return APITestSuite{
			FlowServiceClient: client,
			t:                 t,
			pg:                pg,
			source:            source,
			ch: SetupClickHouseSuite(t, false, func(*testing.T) (TSource, string, error) {
				return source, suffix, nil
			})(t),
			suffix: suffix,
		}
	})
}

func TestApiPg(t *testing.T) {
	testApi(t, SetupPostgres)
}

func TestApiMy(t *testing.T) {
	testApi(t, SetupMySQL)
}

func TestApiMongo(t *testing.T) {
	testApi(t, SetupMongo)
}

func (s APITestSuite) TestGetVersion() {
	response, err := s.GetVersion(s.t.Context(), &protos.PeerDBVersionRequest{})
	require.NoError(s.t, err)
	require.Equal(s.t, internal.PeerDBVersionShaShort(), response.Version)
}

func (s APITestSuite) TestPostgresValidation_WrongPassword() {
	config := internal.GetCatalogPostgresConfigFromEnv(s.t.Context())
	config.Password = "wrong"
	_, err := s.ValidatePeer(s.t.Context(), &protos.ValidatePeerRequest{
		Peer: &protos.Peer{
			Name:   "testfail",
			Type:   protos.DBType_POSTGRES,
			Config: &protos.Peer_PostgresConfig{PostgresConfig: config},
		},
	})
	require.Error(s.t, err)
	grpcStatus, ok := status.FromError(err)
	require.True(s.t, ok, "expected error to be gRPC status")
	require.Equal(s.t, codes.FailedPrecondition, grpcStatus.Code())
}

func (s APITestSuite) TestPostgresValidation_Pass() {
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

func (s APITestSuite) TestClickHouseMirrorValidation_Pass() {
	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "valid"))))
	case *MongoSource:
		require.NoError(s.t, s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).CreateCollection(s.t.Context(), "valid"))
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "ch_validation_" + s.suffix,
		TableNameMapping: map[string]string{AttachSchema(s, "valid"): "valid"},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	response, err := s.ValidateCDCMirror(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)
}

func (s APITestSuite) TestSchemaEndpoints() {
	tableName := "listing"
	peer := s.source.GeneratePeer(s.t)
	peerInfo, err := s.GetPeerInfo(s.t.Context(), &protos.PeerInfoRequest{
		PeerName: peer.Name,
	})
	require.NoError(s.t, err)
	switch config := peerInfo.Peer.Config.(type) {
	case *protos.Peer_PostgresConfig:
		require.Equal(s.t, "********", config.PostgresConfig.Password)
	case *protos.Peer_MysqlConfig:
		require.Equal(s.t, "********", config.MysqlConfig.Password)
	case *protos.Peer_MongoConfig:
		require.Equal(s.t, "********", config.MongoConfig.Password)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown peer type %T", config))
	}

	peerType, err := s.GetPeerType(s.t.Context(), &protos.PeerInfoRequest{
		PeerName: peer.Name,
	})
	require.NoError(s.t, err)
	switch s.source.(type) {
	case *MySqlSource:
		require.Equal(s.t, "MYSQL", peerType.PeerType)
	case *PostgresSource:
		require.Equal(s.t, "POSTGRES", peerType.PeerType)
	case *MongoSource:
		require.Equal(s.t, "MONGO", peerType.PeerType)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}

	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, tableName))))
	case *MongoSource:
		require.NoError(s.t, s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).CreateCollection(s.t.Context(), tableName))
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}

	tablesInSchema, err := s.GetTablesInSchema(s.t.Context(), &protos.SchemaTablesRequest{
		PeerName:   peer.Name,
		SchemaName: Schema(s),
	})
	require.NoError(s.t, err)
	require.Len(s.t, tablesInSchema.Tables, 1)
	require.Equal(s.t, tableName, tablesInSchema.Tables[0].TableName)

	schemasResponse, err := s.GetSchemas(s.t.Context(), &protos.PostgresPeerActivityInfoRequest{
		PeerName: peer.Name,
	})
	require.NoError(s.t, err)
	require.Contains(s.t, schemasResponse.Schemas, "e2e_test_"+s.suffix)

	tablesResponse, err := s.GetAllTables(s.t.Context(), &protos.PostgresPeerActivityInfoRequest{
		PeerName: peer.Name,
	})
	require.NoError(s.t, err)
	require.Contains(s.t, tablesResponse.Tables, AttachSchema(s, tableName))

	switch source := s.source.(type) {
	case *PostgresSource:
		columns, err := s.GetColumns(s.t.Context(), &protos.TableColumnsRequest{
			PeerName:   peer.Name,
			SchemaName: Schema(s),
			TableName:  tableName,
		})
		require.NoError(s.t, err)
		require.Len(s.t, columns.Columns, 2)
		require.Equal(s.t, "id", columns.Columns[0].Name)
		require.True(s.t, columns.Columns[0].IsKey)
		require.Equal(s.t, "integer", columns.Columns[0].Type)
		require.Equal(s.t, "val", columns.Columns[1].Name)
		require.False(s.t, columns.Columns[1].IsKey)
		require.Equal(s.t, "text", columns.Columns[1].Type)
	case *MySqlSource:
		columns, err := s.GetColumns(s.t.Context(), &protos.TableColumnsRequest{
			PeerName:   peer.Name,
			SchemaName: Schema(s),
			TableName:  tableName,
		})
		require.NoError(s.t, err)
		require.Len(s.t, columns.Columns, 2)
		require.Equal(s.t, "id", columns.Columns[0].Name)
		require.True(s.t, columns.Columns[0].IsKey)
		if source.Config.Flavor == protos.MySqlFlavor_MYSQL_MARIA {
			require.Equal(s.t, "int(11)", columns.Columns[0].Type)
		} else {
			require.Equal(s.t, "int", columns.Columns[0].Type)
		}
		require.Equal(s.t, "val", columns.Columns[1].Name)
		require.False(s.t, columns.Columns[1].IsKey)
		require.Equal(s.t, "text", columns.Columns[1].Type)
	case *MongoSource:
		// GetColumns is not implemented for Mongo
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", source))
	}
}

func (s APITestSuite) TestScripts() {
	if _, ok := s.source.(*PostgresSource); !ok {
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

func (s APITestSuite) TestMongoDBOplogRetentionValidation() {
	if _, ok := s.source.(*MongoSource); !ok {
		s.t.Skip("only for MongoDB")
	}

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "mongo_validation_" + s.suffix,
		TableNameMapping: map[string]string{AttachSchema(s, "t1"): "t1"},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)

	// test retention hours (< 24 hours) validation failure
	adminClient := s.Source().(*MongoSource).AdminClient()
	err := adminClient.Database("admin").RunCommand(s.t.Context(), bson.D{
		bson.E{Key: "replSetResizeOplog", Value: 1},
		bson.E{Key: "minRetentionHours", Value: mongo.MinOplogRetentionHours - 1},
	}).Err()
	require.NoError(s.t, err)
	res2, err := s.ValidateCDCMirror(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.Nil(s.t, res2)
	require.Error(s.t, err)
	st, ok := status.FromError(err)
	require.True(s.t, ok)
	require.Equal(s.t, codes.FailedPrecondition, st.Code())
	require.Contains(s.t, st.Message(), "oplog retention must be set to >= 24 hours")

	// test retention hours (>= 24 hours) validation success
	err = adminClient.Database("admin").RunCommand(s.t.Context(), bson.D{
		bson.E{Key: "replSetResizeOplog", Value: 1},
		bson.E{Key: "minRetentionHours", Value: mongo.MinOplogRetentionHours},
	}).Err()
	require.NoError(s.t, err)
	res1, err := s.ValidateCDCMirror(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, res1)
}

func (s APITestSuite) TestMongoDBUserRolesValidation() {
	if _, ok := s.source.(*MongoSource); !ok {
		s.t.Skip("only for MongoDB")
	}

	adminClient := s.Source().(*MongoSource).AdminClient()
	user := "test_role_validation_user"
	pass := "test_role_validation_pass"
	mongoConfig := s.source.GeneratePeer(s.t).GetMongoConfig()
	mongoConfig.Username = user
	mongoConfig.Password = pass
	peer := &protos.Peer{
		Name:   AddSuffix(s, "mongo"),
		Type:   protos.DBType_MONGO,
		Config: &protos.Peer_MongoConfig{MongoConfig: mongoConfig},
	}

	defer func() {
		_ = adminClient.Database("admin").RunCommand(s.t.Context(), bson.D{
			bson.E{Key: "dropUser", Value: user},
		})
	}()

	// case 1: user without `readAnyDatabase` and `clusterMonitor` roles
	require.NoError(s.t, adminClient.Database("admin").RunCommand(s.t.Context(), bson.D{
		{Key: "createUser", Value: user},
		{Key: "pwd", Value: pass},
		{Key: "roles", Value: bson.A{}},
	}).Err())
	_, err := s.ValidatePeer(s.t.Context(), &protos.ValidatePeerRequest{Peer: peer})
	require.Error(s.t, err)
	grpcStatus, ok := status.FromError(err)
	require.True(s.t, ok, "expected error to be gRPC status")
	require.Equal(s.t, codes.FailedPrecondition, grpcStatus.Code())
	require.Contains(s.t, grpcStatus.Message(), "missing required role: readAnyDatabase")

	// case 2: user with only `readAnyDatabase` role (missing `clusterMonitor`)
	require.NoError(s.t, adminClient.Database("admin").RunCommand(s.t.Context(), bson.D{
		bson.E{Key: "grantRolesToUser", Value: user},
		bson.E{Key: "roles", Value: bson.A{"readAnyDatabase"}},
	}).Err())
	_, err = s.ValidatePeer(s.t.Context(), &protos.ValidatePeerRequest{Peer: peer})
	require.Error(s.t, err)
	grpcStatus, ok = status.FromError(err)
	require.True(s.t, ok, "expected error to be gRPC status")
	require.Equal(s.t, codes.FailedPrecondition, grpcStatus.Code())
	require.Contains(s.t, grpcStatus.Message(), "missing required role: clusterMonitor")

	// case 3: user with only `clusterMonitor` role (missing `readAnyDatabase`)
	require.NoError(s.t, adminClient.Database("admin").RunCommand(s.t.Context(), bson.D{
		bson.E{Key: "revokeRolesFromUser", Value: user},
		bson.E{Key: "roles", Value: bson.A{"readAnyDatabase"}},
	}).Err())
	require.NoError(s.t, adminClient.Database("admin").RunCommand(s.t.Context(), bson.D{
		bson.E{Key: "grantRolesToUser", Value: user},
		bson.E{Key: "roles", Value: bson.A{"clusterMonitor"}},
	}).Err())
	_, err = s.ValidatePeer(s.t.Context(), &protos.ValidatePeerRequest{Peer: peer})
	require.Error(s.t, err)
	grpcStatus, ok = status.FromError(err)
	require.True(s.t, ok, "expected error to be gRPC status")
	require.Equal(s.t, codes.FailedPrecondition, grpcStatus.Code())
	require.Contains(s.t, grpcStatus.Message(), "missing required role: readAnyDatabase")

	// case 4: user with both `readAnyDatabase` and `clusterMonitor` roles
	require.NoError(s.t, adminClient.Database("admin").RunCommand(s.t.Context(), bson.D{
		bson.E{Key: "grantRolesToUser", Value: user},
		bson.E{Key: "roles", Value: bson.A{"readAnyDatabase", "clusterMonitor"}},
	}).Err())
	response, err := s.ValidatePeer(s.t.Context(), &protos.ValidatePeerRequest{Peer: peer})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)
	require.Equal(s.t, protos.ValidatePeerStatus_VALID, response.Status)
}

func (s APITestSuite) TestMySQLFlavorSwap() {
	my, ok := s.source.(*MySqlSource)
	if !ok {
		s.t.Skip("only for MySQL")
	}

	peer := s.source.GeneratePeer(s.t)
	peer.GetMysqlConfig().Flavor = protos.MySqlFlavor_MYSQL_MARIA
	response, err := s.ValidatePeer(s.t.Context(), &protos.ValidatePeerRequest{
		Peer: peer,
	})

	if my.Config.Flavor != protos.MySqlFlavor_MYSQL_MARIA {
		require.Error(s.t, err)
		grpcStatus, ok := status.FromError(err)
		require.True(s.t, ok, "expected error to be gRPC status")
		require.Equal(s.t, codes.FailedPrecondition, grpcStatus.Code())
		require.Equal(s.t, protos.ValidatePeerStatus_INVALID, response.Status)
		require.Equal(s.t,
			"failed to validate peer mysql: server appears to be MySQL but flavor is set to MariaDB", grpcStatus.Message())
	}
}

func (s APITestSuite) TestResyncCompleted() {
	tableName := "valid"
	var cols string
	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, tableName))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, tableName))))
		cols = "id,val"
	case *MongoSource:
		res, err := s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection(tableName).
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 1}, bson.E{Key: "val", Value: "first"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		cols = fmt.Sprintf("%s,%s", connmongo.DefaultDocumentKeyColumnName, connmongo.DefaultFullDocumentColumnName)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "resync_completed_" + s.suffix,
		TableNameMapping: map[string]string{AttachSchema(s, tableName): tableName},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.InitialSnapshotOnly = true
	flowConnConfig.SnapshotNumRowsPerPartition = 1234
	flowConnConfig.SnapshotNumPartitionsOverride = 6789
	flowConnConfig.SnapshotMaxParallelWorkers = 7
	flowConnConfig.SnapshotNumTablesInParallel = 13
	flowConnConfig.Env = map[string]string{
		"PEERDB_FORCE_INTERNAL_VERSION": strconv.Itoa(int(shared.InternalVersion_First)),
	}
	// if true, then the flow will be resynced
	response, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)

	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitForFinished(s.t, env, 3*time.Minute)
	RequireEqualTables(s.ch, tableName, cols)

	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (2,'resync')", AttachSchema(s, tableName))))
	case *MongoSource:
		res, err := s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection(tableName).
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 2}, bson.E{Key: "val", Value: "resync"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}

	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RESYNC,
	})
	require.NoError(s.t, err)

	EnvWaitForEqualTables(env, s.ch, "resync", tableName, cols)
	env, err = GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	EnvWaitForFinished(s.t, env, time.Minute)

	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RESYNC,
	})
	require.NoError(s.t, err)

	EnvWaitForEqualTables(env, s.ch, "resync 2", tableName, cols)
	env, err = GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	EnvWaitForFinished(s.t, env, time.Minute)

	// check that custom config options persist across resync
	config, err := s.loadConfigFromCatalog(s.t.Context(), s.pg.PostgresConnector.Conn(), flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	// ignore fields whose comparison is by identity
	flowConnConfig.Env = nil
	flowConnConfig.TableMappings = nil
	config.Env = nil
	config.TableMappings = nil
	require.EqualExportedValues(s.t, flowConnConfig, config)
}

func (s APITestSuite) TestDropCompleted() {
	tableName := "valid"
	var cols string
	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, tableName))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, tableName))))
		cols = "id,val"
	case *MongoSource:
		res, err := s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection(tableName).
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 1}, bson.E{Key: "val", Value: "first"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		cols = fmt.Sprintf("%s,%s", connmongo.DefaultDocumentKeyColumnName, connmongo.DefaultFullDocumentColumnName)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "drop_completed_" + s.suffix,
		TableNameMapping: map[string]string{AttachSchema(s, tableName): tableName},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.InitialSnapshotOnly = true
	response, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)

	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitForFinished(s.t, env, 3*time.Minute)
	RequireEqualTables(s.ch, tableName, cols)

	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_TERMINATING,
	})
	require.NoError(s.t, err)
	EnvWaitFor(s.t, env, time.Minute, "wait for avro stage dropped", func() bool {
		var workflowID string
		return s.pg.PostgresConnector.Conn().QueryRow(
			s.t.Context(), "SELECT avro_file FROM ch_s3_stage WHERE flow_job_name = $1", flowConnConfig.FlowJobName,
		).Scan(&workflowID) == pgx.ErrNoRows
	})
	EnvWaitFor(s.t, env, time.Minute, "wait for flow dropped", func() bool {
		var workflowID string
		return s.pg.PostgresConnector.Conn().QueryRow(
			s.t.Context(), "select workflow_id from flows where name = $1", flowConnConfig.FlowJobName,
		).Scan(&workflowID) == pgx.ErrNoRows
	})
}

func (s APITestSuite) TestEditTablesBeforeResync() {
	var cols string
	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "original"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "added"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "original"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "added"))))
		cols = "id,val"
	case *MongoSource:
		res, err := s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("original").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 1}, bson.E{Key: "val", Value: "first"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		res, err = s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("added").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 1}, bson.E{Key: "val", Value: "first"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		cols = fmt.Sprintf("%s,%s", connmongo.DefaultDocumentKeyColumnName, connmongo.DefaultFullDocumentColumnName)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "edit_tables_before_resync_" + s.suffix,
		TableNameMapping: map[string]string{AttachSchema(s, "original"): "original"},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	response, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)

	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for initial load to finish", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	RequireEqualTables(s.ch, "original", cols)

	// pause the mirror
	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_PAUSED,
	})
	require.NoError(s.t, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for pause for add table", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_PAUSED
	})
	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
		FlowConfigUpdate: &protos.FlowConfigUpdate{
			Update: &protos.FlowConfigUpdate_CdcFlowConfigUpdate{
				CdcFlowConfigUpdate: &protos.CDCFlowConfigUpdate{
					AdditionalTables: []*protos.TableMapping{
						{
							SourceTableIdentifier:      AttachSchema(s, "added"),
							DestinationTableIdentifier: "added",
						},
					},
				},
			},
		},
	})
	require.NoError(s.t, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for table addition to finish", func() bool {
		valid, err := s.checkCatalogTableMapping(s.t.Context(), s.pg.PostgresConnector.Conn(), flowConnConfig.FlowJobName, []string{
			AttachSchema(s, "added"),
			AttachSchema(s, "original"),
		})
		if err != nil {
			return false
		}

		return valid && env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	RequireEqualTables(s.ch, "added", cols)

	// Check initial load stats
	initialLoadClientFacingDataAfterAddTable, err := s.InitialLoadSummary(s.t.Context(), &protos.InitialLoadSummaryRequest{
		ParentMirrorName: flowConnConfig.FlowJobName,
	})
	require.NoError(s.t, err)
	require.Len(s.t, initialLoadClientFacingDataAfterAddTable.TableSummaries, 2)

	// Test CDC
	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (2,'added_table_cdc')", AttachSchema(s, "added"))))
	case *MongoSource:
		res, err := s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("added").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 2}, bson.E{Key: "val", Value: "added_table_cdc"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}
	s.checkMetadataLastSyncStateValues(env, flowConnConfig, "cdc after add table", 1, 1)
	RequireEqualTables(s.ch, "added", cols)

	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_PAUSED,
	})
	require.NoError(s.t, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for pause for remove table", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_PAUSED
	})
	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
		FlowConfigUpdate: &protos.FlowConfigUpdate{
			Update: &protos.FlowConfigUpdate_CdcFlowConfigUpdate{
				CdcFlowConfigUpdate: &protos.CDCFlowConfigUpdate{
					RemovedTables: []*protos.TableMapping{
						{
							SourceTableIdentifier:      AttachSchema(s, "original"),
							DestinationTableIdentifier: "original",
						},
					},
				},
			},
		},
	})
	require.NoError(s.t, err)

	EnvWaitFor(s.t, env, 3*time.Minute, "wait for table removal to finish", func() bool {
		valid, err := s.checkCatalogTableMapping(s.t.Context(), s.pg.PostgresConnector.Conn(), flowConnConfig.FlowJobName, []string{
			AttachSchema(s, "added"),
		})
		if err != nil {
			return false
		}

		return valid && env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	if pgconn, ok := s.source.Connector().(*connpostgres.PostgresConnector); ok {
		s.waitForActiveSlotForPostgresMirror(env, pgconn.Conn(), flowConnConfig.FlowJobName)
	}

	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_PAUSED,
	})
	require.NoError(s.t, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for pause after table removal", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_PAUSED
	})
	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (3,'resync')", AttachSchema(s, "added"))))
	case *MongoSource:
		res, err := s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("added").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 3}, bson.E{Key: "val", Value: "resync"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}
	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RESYNC,
		DropMirrorStats:    true,
	})
	require.NoError(s.t, err)
	newEnv, newErr := GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, newErr)
	// test resync initial load
	EnvWaitForEqualTables(newEnv, s.ch, "resync", "added", cols)
	EnvWaitFor(s.t, newEnv, 3*time.Minute, "wait for resynced mirror to be in cdc", func() bool {
		return newEnv.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})

	initialLoadClientFacingDataAfterResync, err := s.InitialLoadSummary(s.t.Context(), &protos.InitialLoadSummaryRequest{
		ParentMirrorName: flowConnConfig.FlowJobName,
	})
	require.NoError(s.t, err)
	require.Len(s.t, initialLoadClientFacingDataAfterResync.TableSummaries, 1)
	require.Equal(s.t, "added_resync", initialLoadClientFacingDataAfterResync.TableSummaries[0].TableName)

	// Test resync CDC
	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (4,'cdc_after_resync')", AttachSchema(s, "added"))))
	case *MongoSource:
		res, err := s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("added").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 4}, bson.E{Key: "val", Value: "cdc_after_resync"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}
	s.checkMetadataLastSyncStateValues(newEnv, flowConnConfig, "cdc after resync", 1, 1)
	EnvWaitForEqualTables(newEnv, s.ch, "resync after normalize", "added", cols)
	newEnv.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, newEnv)
}

func (s APITestSuite) TestAlertConfig() {
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

func (s APITestSuite) TestTotalRowsSyncedByMirror() {
	var cols string
	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "table1"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "table2"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "table1"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "table2"))))
		cols = "id,val"
	case *MongoSource:
		res, err := s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("table1").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 1}, bson.E{Key: "val", Value: "first"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		res, err = s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("table2").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 1}, bson.E{Key: "val", Value: "first"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		cols = fmt.Sprintf("%s,%s", connmongo.DefaultDocumentKeyColumnName, connmongo.DefaultFullDocumentColumnName)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "test_total_rows_synced_mirror" + s.suffix,
		TableNameMapping: map[string]string{AttachSchema(s, "table1"): "table1", AttachSchema(s, "table2"): "table2"},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	response, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)

	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for initial load to finish", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	// test initial load
	RequireEqualTables(s.ch, "table1", cols)
	RequireEqualTables(s.ch, "table2", cols)

	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (2,'second')", AttachSchema(s, "table1"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (2,'second')", AttachSchema(s, "table2"))))
	case *MongoSource:
		res, err := s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("table1").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 2}, bson.E{Key: "val", Value: "second"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		res, err = s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("table2").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 2}, bson.E{Key: "val", Value: "second"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}

	// test cdc
	EnvWaitForEqualTables(env, s.ch, "cdc equal", "table1", cols)
	EnvWaitForEqualTables(env, s.ch, "cdc equal", "table2", cols)

	// check total rows synced
	mirrorTotalRowsSynced, err := s.TotalRowsSyncedByMirror(s.t.Context(), &protos.TotalRowsSyncedByMirrorRequest{
		FlowJobName: flowConnConfig.FlowJobName,
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, mirrorTotalRowsSynced)
	require.Equal(s.t, int64(2), mirrorTotalRowsSynced.TotalCountCDC)
	require.Equal(s.t, int64(2), mirrorTotalRowsSynced.TotalCountInitialLoad)
	require.Equal(s.t, int64(4), mirrorTotalRowsSynced.TotalCount)

	// check table stats cdc
	tableStats, err := s.CDCTableTotalCounts(s.t.Context(), &protos.CDCTableTotalCountsRequest{
		FlowJobName: flowConnConfig.FlowJobName,
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, tableStats)
	require.Len(s.t, tableStats.TablesData, 2)
	require.Equal(s.t, int64(1), tableStats.TablesData[0].Counts.InsertsCount)
	require.Equal(s.t, int64(1), tableStats.TablesData[1].Counts.InsertsCount)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s APITestSuite) TestSettings() {
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

func (s APITestSuite) TestQRep() {
	if _, ok := s.source.(*MongoSource); ok {
		s.t.Skip("QRepFlowWorkFlow is not implemented for MongoDB")
	}

	peerType, err := s.GetPeerType(s.t.Context(), &protos.PeerInfoRequest{
		PeerName: s.source.GeneratePeer(s.t).Name,
	})
	require.NoError(s.t, err)
	tblName := "qrepapi" + s.suffix
	schemaQualified := AttachSchema(s, tblName)
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", schemaQualified)))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", schemaQualified)))

	qrepConfig := CreateQRepWorkflowConfig(
		s.t,
		"qrepapiflow"+"_"+peerType.PeerType,
		schemaQualified,
		tblName,
		fmt.Sprintf("SELECT * FROM %s WHERE id BETWEEN {{.start}} AND {{.end}}", schemaQualified),
		s.ch.Peer().Name,
		"",
		true,
		"",
		"",
	)
	qrepConfig.SourceName = s.source.GeneratePeer(s.t).Name
	qrepConfig.WatermarkColumn = "id"
	qrepConfig.InitialCopyOnly = false
	qrepConfig.WaitBetweenBatchesSeconds = 5
	qrepConfig.NumRowsPerPartition = 1
	_, err = s.CreateQRepFlow(s.t.Context(), &protos.CreateQRepFlowRequest{
		QrepConfig: qrepConfig,
	})
	require.NoError(s.t, err)

	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, qrepConfig.FlowJobName)
	require.NoError(s.t, err)

	EnvWaitForEqualTables(env, s.ch, "qrep initial load", tblName, "id,val")

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (2,'second')", schemaQualified)))

	EnvWaitForEqualTables(env, s.ch, "insert post qrep initial load", tblName, "id,val")
	statusResponse, err := s.MirrorStatus(s.t.Context(), &protos.MirrorStatusRequest{
		FlowJobName:     qrepConfig.FlowJobName,
		IncludeFlowInfo: true,
		ExcludeBatches:  false,
	})
	require.NoError(s.t, err)
	qStatus := statusResponse.GetQrepStatus()
	require.NotNil(s.t, qStatus)
	require.Len(s.t, qStatus.Partitions, 2)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s APITestSuite) TestTableAdditionWithoutInitialLoad() {
	var cols string
	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "original"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "added"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "original"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "added"))))
		cols = "id,val"
	case *MongoSource:
		res, err := s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("original").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 1}, bson.E{Key: "val", Value: "first"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		res, err = s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("added").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 1}, bson.E{Key: "val", Value: "first"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		cols = fmt.Sprintf("%s,%s", connmongo.DefaultDocumentKeyColumnName, connmongo.DefaultFullDocumentColumnName)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "added_tables_no_initial_load_" + s.suffix,
		TableNameMapping: map[string]string{AttachSchema(s, "original"): "original"},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	response, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)
	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for initial load to finish", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	RequireEqualTables(s.ch, "original", cols)
	// add table
	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_PAUSED,
	})
	require.NoError(s.t, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for pause for add table", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_PAUSED
	})
	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
		FlowConfigUpdate: &protos.FlowConfigUpdate{
			Update: &protos.FlowConfigUpdate_CdcFlowConfigUpdate{
				CdcFlowConfigUpdate: &protos.CDCFlowConfigUpdate{
					AdditionalTables: []*protos.TableMapping{
						{
							SourceTableIdentifier:      AttachSchema(s, "added"),
							DestinationTableIdentifier: "added",
						},
					},
					SkipInitialSnapshotForTableAdditions: true,
				},
			},
		},
	})
	require.NoError(s.t, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for table addition to finish", func() bool {
		valid, err := s.checkCatalogTableMapping(s.t.Context(), s.pg.PostgresConnector.Conn(), flowConnConfig.FlowJobName, []string{
			AttachSchema(s, "added"),
			AttachSchema(s, "original"),
		})
		if err != nil {
			return false
		}

		return valid && env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})

	// No initial load should have happened for added table, so it should be empty on ClickHouse
	RequireEmptyDestinationTable(s.ch, "added", cols)

	// cdc should occur for added table still, so insert row into added table to test cdc
	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (2,'second')", AttachSchema(s, "added"))))
	case *MongoSource:
		res, err := s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("added").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 2}, bson.E{Key: "val", Value: "second"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}
	EnvWaitForCount(env, s.ch, "test cdc of cdc_only table addition", "added", cols, 1)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s APITestSuite) TestDropMissing() {
	conn := s.pg.PostgresConnector.Conn()
	peer := s.source.GeneratePeer(s.t)
	var peerId int32
	require.NoError(s.t, conn.QueryRow(s.t.Context(), "select id from peers where name = $1", peer.Name).Scan(&peerId))

	_, err := conn.Exec(s.t.Context(),
		"insert into flows (name,source_peer,destination_peer,workflow_id,status) values ('test-drop-missing',$1,$1,'drop-missing-wf-id',$2)",
		peerId, protos.FlowStatus_STATUS_COMPLETED,
	)
	require.NoError(s.t, err)

	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        "test-drop-missing",
		RequestedFlowState: protos.FlowStatus_STATUS_TERMINATING,
	})
	require.NoError(s.t, err)
}
