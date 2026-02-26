package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	tp "github.com/Shopify/toxiproxy/v2/client"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	connmongo "github.com/PeerDB-io/peerdb/flow/connectors/mongo"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/mongo"
	"github.com/PeerDB-io/peerdb/flow/shared"
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

// checkMigrationCompleted checks if a migration has been completed for a given flow
func (s APITestSuite) checkMigrationCompleted(
	ctx context.Context,
	conn *pgx.Conn,
	flowName string,
	migrationName string,
) (bool, error) {
	var completed bool
	err := conn.QueryRow(
		ctx,
		"SELECT completed FROM flow_migrations WHERE flow_name = $1 AND migration_name = $2",
		flowName,
		migrationName,
	).Scan(&completed)
	if err != nil {
		if err == pgx.ErrNoRows {
			// Migration record doesn't exist, so it hasn't been completed
			return false, nil
		}
		return false, fmt.Errorf("failed to check migration status: %w", err)
	}

	return completed, nil
}

// checkMetadataLastSyncStateValues checks the values of sync_batch_id and normalize_batch_id
// in the metadata_last_sync_state table
func (s APITestSuite) checkMetadataLastSyncStateValues(
	env WorkflowRun,
	flowConnConfig *protos.FlowConnectionConfigs,
	reason string,
	expectedSyncBatchId int64, //nolint:unparam
	expectedNormalizeBatchId int64, //nolint:unparam
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

func (s APITestSuite) getCatalogTableSchemaForSourceTable(
	ctx context.Context,
	conn *pgx.Conn,
	flowName string,
	sourceTableIdentifier string,
) (*protos.TableSchema, error) {
	var configBytes sql.RawBytes
	if err := conn.QueryRow(ctx,
		`SELECT table_schema FROM table_schema_mapping WHERE flow_name = $1 AND table_name = $2`,
		flowName, sourceTableIdentifier,
	).Scan(&configBytes); err != nil {
		return nil, err
	}

	var config protos.TableSchema
	return &config, proto.Unmarshal(configBytes, &config)
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

func (s APITestSuite) TestClickHouseMirrorValidation_NoPrimaryKey() {
	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int, val text)", AttachSchema(s, "no_pkey"))))
	case *MongoSource:
		s.t.Skip("MongoDB always has _id as primary key")
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}

	chVersion, err := s.ch.connector.GetVersion(s.t.Context())
	require.NoError(s.t, err)
	isAtLeast25_12 := chproto.CheckMinVersion(
		chproto.Version{Major: 25, Minor: 12, Patch: 0}, chproto.ParseVersion(chVersion))

	srcTable := AttachSchema(s, "no_pkey")

	tests := []struct {
		name      string
		dstTable  string
		engine    protos.TableEngine
		expectErr bool
	}{
		{
			name:      "ReplacingMergeTree rejects empty ordering on 25.12+",
			dstTable:  "no_pkey_rmt",
			engine:    protos.TableEngine_CH_ENGINE_REPLACING_MERGE_TREE,
			expectErr: isAtLeast25_12,
		},
		{
			name:     "MergeTree engine allows empty ordering",
			dstTable: "no_pkey_mt",
			engine:   protos.TableEngine_CH_ENGINE_MERGE_TREE,
		},
		{
			name:     "Null engine allows empty ordering",
			dstTable: "no_pkey_null",
			engine:   protos.TableEngine_CH_ENGINE_NULL,
		},
	}

	for _, tc := range tests {
		s.t.Run(tc.name, func(t *testing.T) {
			connectionGen := FlowConnectionGenerationConfig{
				FlowJobName: "ch_no_pkey_" + tc.dstTable + "_" + s.suffix,
				TableMappings: []*protos.TableMapping{{
					SourceTableIdentifier:      srcTable,
					DestinationTableIdentifier: tc.dstTable,
					Engine:                     tc.engine,
				}},
				Destination: s.ch.Peer().Name,
			}
			flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
			flowConnConfig.DoInitialSnapshot = true

			response, err := s.ValidateCDCMirror(t.Context(),
				&protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
			if tc.expectErr {
				require.Error(t, err)
				require.Nil(t, response)
				st, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, codes.FailedPrecondition, st.Code())
				require.Contains(t, st.Message(), "empty sort key is not supported")
			} else {
				require.NoError(t, err)
				require.NotNil(t, response)
			}
		})
	}
}

func (s APITestSuite) TestClickHouseMirrorValidation_NoPrimaryKey_ReplicaIdentityFull() {
	switch s.source.(type) {
	case *PostgresSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf(`CREATE TABLE %s(id int, val text);
				ALTER TABLE %[1]s REPLICA IDENTITY FULL`, AttachSchema(s, "no_pkey_rif"))))
	default:
		s.t.Skip("replica identity full only applies to Postgres")
	}

	srcTable := AttachSchema(s, "no_pkey_rif")

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: "ch_no_pkey_rif_" + s.suffix,
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      srcTable,
			DestinationTableIdentifier: "no_pkey_rif",
			Engine:                     protos.TableEngine_CH_ENGINE_REPLACING_MERGE_TREE,
		}},
		Destination: s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	response, err := s.ValidateCDCMirror(s.t.Context(),
		&protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)
}

func (s APITestSuite) TestMirrorValidation_InvalidTableMappings() {
	tests := []struct {
		name                       string
		sourceTableIdentifier      string
		destinationTableIdentifier string
	}{
		{
			name:                  "empty source (dot only)",
			sourceTableIdentifier: ".",
		},
		{
			name:                       "empty source schema",
			sourceTableIdentifier:      ".table",
			destinationTableIdentifier: "valid_dest",
		},
		{
			name:                       "empty source table",
			sourceTableIdentifier:      "schema.",
			destinationTableIdentifier: "valid_dest",
		},
		{
			name:                       "empty destination",
			sourceTableIdentifier:      "public.valid_src",
			destinationTableIdentifier: "",
		},
	}

	for _, tc := range tests {
		s.t.Run(tc.name, func(t *testing.T) {
			flowConnConfig := &protos.FlowConnectionConfigs{
				FlowJobName:     "invalid_mapping_test_" + s.suffix,
				SourceName:      s.source.GeneratePeer(t).Name,
				DestinationName: s.ch.Peer().Name,
				TableMappings: []*protos.TableMapping{
					{
						SourceTableIdentifier:      tc.sourceTableIdentifier,
						DestinationTableIdentifier: tc.destinationTableIdentifier,
					},
				},
				DoInitialSnapshot: true,
			}

			response, err := s.ValidateCDCMirror(t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
			require.Error(t, err, "expected validation to fail for %s", tc.name)
			require.Nil(t, response)

			st, ok := status.FromError(err)
			require.True(t, ok, "expected gRPC status error")
			require.Equal(t, codes.FailedPrecondition, st.Code(), "expected FailedPrecondition error code")
		})
	}
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
		cmp, err := source.CompareServerVersion(s.t.Context(), "8")
		require.NoError(s.t, err)
		if source.Config.Flavor == protos.MySqlFlavor_MYSQL_MARIA {
			require.Equal(s.t, "int(11)", columns.Columns[0].Type)
		} else if cmp < 0 {
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

func (s APITestSuite) TestGetTablesExcludeViews() {
	tableName := "test_table"
	viewName := "test_view"

	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, tableName))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) VALUES (1, 'foo')", AttachSchema(s, tableName))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM %s", AttachSchema(s, viewName), AttachSchema(s, tableName))))
	case *MongoSource:
		adminClient := s.Source().(*MongoSource).AdminClient()
		res, err := adminClient.Database(Schema(s)).Collection(tableName).
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 1}, bson.E{Key: "val", Value: "foo"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		require.NoError(s.t, adminClient.Database(Schema(s)).CreateView(s.t.Context(), viewName, tableName, bson.A{}))
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}

	peer := s.source.GeneratePeer(s.t)
	tablesInSchema, err := s.GetTablesInSchema(s.t.Context(), &protos.SchemaTablesRequest{
		PeerName:   peer.Name,
		SchemaName: Schema(s),
	})
	require.NoError(s.t, err)

	tableNames := make([]string, len(tablesInSchema.Tables))
	for i, table := range tablesInSchema.Tables {
		tableNames[i] = table.TableName
	}
	require.Contains(s.t, tableNames, tableName, "table should be in the list")
	require.NotContains(s.t, tableNames, viewName, "view should not be in the list")
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

	adminClient := s.Source().(*MongoSource).AdminClient()
	err := adminClient.Database(Schema(s)).CreateCollection(s.t.Context(), "t1")
	require.NoError(s.t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "mongo_validation_" + s.suffix,
		TableNameMapping: map[string]string{AttachSchema(s, "t1"): "t1"},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)

	// test retention hours (< 24 hours) validation failure
	err = adminClient.Database("admin").RunCommand(s.t.Context(), bson.D{
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
	flowConnConfig.SnapshotNumRowsPerPartition = 3
	flowConnConfig.SnapshotMaxParallelWorkers = 7
	flowConnConfig.SnapshotNumTablesInParallel = 13
	flowConnConfig.IdleTimeoutSeconds = 9
	flowConnConfig.MaxBatchSize = 5040
	flowConnConfig.Flags = []string{"dummy_config"}
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

	configBeforeResync, err := s.loadConfigFromCatalog(s.t.Context(), s.pg.PostgresConnector.Conn(), flowConnConfig.FlowJobName)
	require.NoError(s.t, err)

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
	configAfterResync, err := s.loadConfigFromCatalog(s.t.Context(), s.pg.PostgresConnector.Conn(), flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	configBeforeResync.Resync = true
	require.EqualExportedValues(s.t, configBeforeResync, configAfterResync)
}

func (s APITestSuite) TestResyncFailed() {
	pgSource, ok := s.source.(*PostgresSource)
	if !ok {
		s.t.Skip("only for PostgreSQL")
	}

	srcTableName := "resync_failed"
	dstTableName := "resync_failed_" + s.suffix
	cols := "id,val"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, srcTableName))))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, srcTableName))))

	err := s.ch.CreateRMTTable(dstTableName, []TestClickHouseColumn{
		{Name: "id", Type: "Int64"},
		{Name: "val", Type: "String"},
		{Name: "_peerdb_is_deleted", Type: "Int8"},
		{Name: "_peerdb_version", Type: "Int64"},
		{Name: "_peerdb_synced_at", Type: "DateTime64(9)"},
	}, "id")
	require.NoError(s.t, err)
	mvManager := s.ch.NewMVManager(dstTableName, s.suffix)
	err = mvManager.CreateBadMV(s.t.Context())
	require.NoError(s.t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "resync_failed_" + s.suffix,
		TableNameMapping: map[string]string{AttachSchema(s, srcTableName): dstTableName},
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

	EnvWaitFor(s.t, env, 5*time.Minute, "wait for MV error messages", func() bool {
		count, err := s.pg.GetLogCount(
			s.t.Context(), flowConnConfig.FlowJobName, "error",
			fmt.Sprintf("while pushing to view %s.%s", s.ch.connector.Config.Database, mvManager.mvName),
		)
		return err == nil && count > 0
	})

	_, err = pgSource.PostgresConnector.Conn().Exec(s.t.Context(),
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query LIKE $1`,
		"%"+s.suffix+"%")
	require.NoError(s.t, err)

	EnvWaitFor(s.t, env, 3*time.Minute, "wait for failed", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_FAILED
	})

	err = mvManager.DropBadMV(s.t.Context())
	require.NoError(s.t, err)

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (2,'second')", AttachSchema(s, srcTableName))))

	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RESYNC,
	})
	require.NoError(s.t, err)

	env, err = GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	EnvWaitForCount(env, s.ch, "resync should have 2 rows", dstTableName, cols, 2)
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

// drop on completed mirror doesn't access peers, so should still drop immediately
func (s APITestSuite) TestDropCompletedAndUnavailable() {
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only testing with PostgreSQL")
	}

	suffix := "drop_unavailable_" + s.suffix
	proxyConfig := internal.GetCatalogPostgresConfigFromEnv(s.t.Context())
	pgWithProxy, proxy, err := SetupPostgresWithToxiproxy(s.t, suffix, 9903)
	require.NoError(s.t, err)
	defer func() {
		require.NoError(s.t, proxy.Enable())
		connectionString := internal.GetPGConnectionString(proxyConfig, "")
		connConfig, err := connpostgres.ParseConfig(connectionString, proxyConfig)
		require.NoError(s.t, err)
		conn, err := connpostgres.NewPostgresConnFromConfig(s.t.Context(), connConfig, "", nil, nil)
		if err != nil {
			s.t.Logf("failed to connect for teardown: %v", err)
		} else if err := cleanPostgres(s.t.Context(), conn, suffix); err != nil {
			s.t.Logf("failed to teardown: %v", err)
		}
	}()

	require.NoError(s.t, pgWithProxy.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "valid"))))
	require.NoError(s.t, pgWithProxy.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "valid"))))

	// Create peer for the proxy connection
	proxyConfig.Port = uint32(9903)
	proxyPeer := &protos.Peer{
		Name: "proxy_postgres_" + suffix,
		Type: protos.DBType_POSTGRES,
		Config: &protos.Peer_PostgresConfig{
			PostgresConfig: proxyConfig,
		},
	}
	CreatePeer(s.t, proxyPeer)
	defer func() {
		_, _ = s.DropPeer(s.t.Context(), &protos.DropPeerRequest{PeerName: proxyPeer.Name})
	}()

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: "create_concurrent_toxi_" + suffix,
		TableNameMapping: map[string]string{
			AttachSchema(s, "valid"): "valid",
		},
		Destination: s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.InitialSnapshotOnly = true
	flowConnConfig.SourceName = proxyPeer.Name

	response, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)

	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitForFinished(s.t, env, 3*time.Minute)
	RequireEqualTables(s.ch, "valid", "id,val")
	require.NoError(s.t, proxy.Disable())

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

	// check initial load NumRowsSynced via mirror status API
	statusResponse, err := s.MirrorStatus(s.t.Context(), &protos.MirrorStatusRequest{
		FlowJobName:     flowConnConfig.FlowJobName,
		IncludeFlowInfo: true,
	})
	require.NoError(s.t, err)
	cdcStatus := statusResponse.GetCdcStatus()
	require.NotNil(s.t, cdcStatus)
	require.NotNil(s.t, cdcStatus.SnapshotStatus)
	var initialLoadRowsSynced int64
	for _, clone := range cdcStatus.SnapshotStatus.Clones {
		initialLoadRowsSynced += clone.NumRowsSynced
	}
	require.Equal(s.t, int64(2), initialLoadRowsSynced)

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

func (s APITestSuite) TestPostgresTableOIDsMigration() {
	pgconn, ok := s.source.Connector().(*connpostgres.PostgresConnector)
	if !ok {
		s.t.Skip("only for PostgreSQL source")
	}

	cols := "id,val"
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "table1"))))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "table2"))))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "table1"))))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "table2"))))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "test_postgres_table_oids_" + s.suffix,
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

	// Test initial load
	RequireEqualTables(s.ch, "table1", cols)
	RequireEqualTables(s.ch, "table2", cols)

	// Check Postgres table OIDs in catalog
	var table1OID, table2OID uint32
	err = pgconn.Conn().QueryRow(s.t.Context(),
		`SELECT c.oid FROM pg_class c JOIN pg_namespace n
         ON n.oid = c.relnamespace WHERE n.nspname=$1 AND c.relname=$2`,
		Schema(s), "table1").Scan(&table1OID)
	require.NoError(s.t, err)
	err = pgconn.Conn().QueryRow(s.t.Context(),
		`SELECT c.oid FROM pg_class c JOIN pg_namespace n
         ON n.oid = c.relnamespace WHERE n.nspname=$1 AND c.relname=$2`,
		Schema(s), "table2").Scan(&table2OID)
	require.NoError(s.t, err)
	require.NotEqual(s.t, uint32(0), table1OID)
	require.NotEqual(s.t, uint32(0), table2OID)

	schema1, err := s.getCatalogTableSchemaForSourceTable(
		s.t.Context(),
		pgconn.Conn(),
		flowConnConfig.FlowJobName,
		"table1",
	)
	require.NoError(s.t, err)
	require.Equal(s.t, AttachSchema(s, "table1"), schema1.TableIdentifier)
	require.Equal(s.t, table1OID, schema1.TableOid)

	schema2, err := s.getCatalogTableSchemaForSourceTable(
		s.t.Context(),
		pgconn.Conn(),
		flowConnConfig.FlowJobName,
		"table2",
	)
	require.NoError(s.t, err)
	require.Equal(s.t, AttachSchema(s, "table2"), schema2.TableIdentifier)
	require.Equal(s.t, table2OID, schema2.TableOid)

	ok, err = s.checkMigrationCompleted(
		s.t.Context(),
		pgconn.Conn(),
		flowConnConfig.FlowJobName,
		shared.POSTGRES_TABLE_OID_MIGRATION,
	)
	require.NoError(s.t, err)
	require.True(s.t, ok, "expected Postgres table OID migration to be completed")

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
	tableName := AddSuffix(s, "qrepapi")
	schemaQualified := AttachSchema(s, tableName)
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", schemaQualified)))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", schemaQualified)))

	flowName := fmt.Sprintf("qrepapiflow_%s_%s", peerType.PeerType, s.suffix)
	qrepConfig := CreateQRepWorkflowConfig(
		s.t,
		flowName,
		schemaQualified,
		tableName,
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

	EnvWaitForEqualTables(env, s.ch, "qrep initial load", tableName, "id,val")

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (2,'second')", schemaQualified)))

	EnvWaitForEqualTables(env, s.ch, "insert post qrep initial load", tableName, "id,val")
	statusResponse, err := s.MirrorStatus(s.t.Context(), &protos.MirrorStatusRequest{
		FlowJobName:     qrepConfig.FlowJobName,
		IncludeFlowInfo: true,
		ExcludeBatches:  false,
	})
	require.NoError(s.t, err)
	qStatus := statusResponse.GetQrepStatus()
	require.NotNil(s.t, qStatus)
	require.Len(s.t, qStatus.Partitions, 2)

	var totalRowsSynced int64
	for _, p := range qStatus.Partitions {
		require.Positive(s.t, p.RowsSynced, "each partition should have rows_synced > 0")
		totalRowsSynced += p.RowsSynced
	}
	require.Equal(s.t, int64(2), totalRowsSynced)

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

func (s APITestSuite) TestCreateCDCFlowAttachConcurrentRequests() {
	// Test: two concurrent requests succeed
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only testing with PostgreSQL")
	}

	tableName := "concurrent_test"
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, tableName))))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, tableName))))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "create_concurrent_" + s.suffix,
		TableNameMapping: map[string]string{AttachSchema(s, tableName): tableName},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	// Two concurrent requests should succeed and return the same workflow ID
	var wg sync.WaitGroup
	var response1, response2 *protos.CreateCDCFlowResponse
	var err1, err2 error

	wg.Add(2)
	go func() {
		defer wg.Done()
		response1, err1 = s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{
			ConnectionConfigs: flowConnConfig,
			AttachToExisting:  true,
		})
	}()
	go func() {
		defer wg.Done()
		response2, err2 = s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{
			ConnectionConfigs: flowConnConfig,
			AttachToExisting:  true,
		})
	}()
	wg.Wait()

	require.NoError(s.t, err1)
	require.NoError(s.t, err2)
	require.NotNil(s.t, response1)
	require.NotNil(s.t, response2)
	require.Equal(s.t, response1.WorkflowId, response2.WorkflowId)

	// Verify workflow is actually running
	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for flow to be running", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})

	// Clean up
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s APITestSuite) TestCreateCDCFlowAttachConcurrentRequestsToxi() {
	// Test: use Toxiproxy to ensure concurrent requests are truly concurrent

	// To run locally, requires toxiproxy running:
	// docker run -d \
	//   --name peerdb-toxiproxy \
	//   -p 18474:8474 \
	//   -p 9902:9902 \
	//   ghcr.io/shopify/toxiproxy:2.11.0

	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only testing with PostgreSQL")
	}

	// Setup PostgreSQL with Toxiproxy
	suffix := "race_" + s.suffix
	pgWithProxy, proxy, err := SetupPostgresWithToxiproxy(s.t, suffix, 9902)
	require.NoError(s.t, err)
	defer pgWithProxy.Teardown(s.t, s.t.Context(), suffix)

	// Create table
	tableName := "toxiproxy_race_test"
	require.NoError(s.t, pgWithProxy.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE e2e_test_%s.%s(id int primary key, val text)", suffix, tableName)))

	// Create peer for the proxy connection
	proxyConfig := internal.GetCatalogPostgresConfigFromEnv(s.t.Context())
	proxyConfig.Port = uint32(9902)
	proxyPeer := &protos.Peer{
		Name: "proxy_postgres_" + suffix,
		Type: protos.DBType_POSTGRES,
		Config: &protos.Peer_PostgresConfig{
			PostgresConfig: proxyConfig,
		},
	}
	CreatePeer(s.t, proxyPeer)
	defer func() {
		_, _ = s.DropPeer(s.t.Context(), &protos.DropPeerRequest{PeerName: proxyPeer.Name})
	}()

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: "create_concurrent_toxi_" + suffix,
		TableNameMapping: map[string]string{
			fmt.Sprintf("e2e_test_%s.%s", suffix, tableName): tableName,
		},
		Destination: s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.SourceName = proxyPeer.Name

	// Add latency toxic to ensure concurrent execution
	const toxicDelay = 2 * time.Second
	toxic, err := proxy.AddToxic("latency", "latency", "downstream", 1.0, tp.Attributes{
		"latency": int(toxicDelay.Milliseconds()),
	})
	require.NoError(s.t, err)

	// Start concurrent requests
	var wg sync.WaitGroup
	var response1, response2 *protos.CreateCDCFlowResponse
	var err1, err2 error
	var duration1, duration2 time.Duration

	wg.Add(2)
	go func() {
		defer wg.Done()
		start := time.Now()
		response1, err1 = s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{
			ConnectionConfigs: flowConnConfig,
			AttachToExisting:  true,
		})
		duration1 = time.Since(start)
	}()
	go func() {
		defer wg.Done()
		start := time.Now()
		response2, err2 = s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{
			ConnectionConfigs: flowConnConfig,
			AttachToExisting:  true,
		})
		duration2 = time.Since(start)
	}()

	// Let goroutines start with toxic active
	time.Sleep(toxicDelay)

	// Remove toxic so requests can complete
	err = proxy.RemoveToxic(toxic.Name)
	require.NoError(s.t, err)

	wg.Wait()

	// Verify both requests were delayed by toxic
	require.Greater(s.t, duration1, toxicDelay)
	require.Greater(s.t, duration2, toxicDelay)

	// Verify both succeeded with same workflow ID
	require.NoError(s.t, err1)
	require.NoError(s.t, err2)
	require.NotNil(s.t, response1)
	require.NotNil(s.t, response2)
	require.Equal(s.t, response1.WorkflowId, response2.WorkflowId)

	// Verify workflow is actually running
	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for flow to be running", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})

	// Clean up
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s APITestSuite) TestCreateCDCFlowAttachSequentialRequests() {
	// Test: two sequential requests succeed, same workflow is returned
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only testing with PostgreSQL")
	}

	tableName := "sequential_test"
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, tableName))))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, tableName))))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "create_sequential_" + s.suffix,
		TableNameMapping: map[string]string{AttachSchema(s, tableName): tableName},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	// First request
	response1, err1 := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{
		ConnectionConfigs: flowConnConfig,
		AttachToExisting:  true,
	})
	require.NoError(s.t, err1)
	require.NotNil(s.t, response1)

	// Verify workflow is actually running
	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for flow to be running", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})

	// Second sequential request should return the same workflow ID
	response2, err2 := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{
		ConnectionConfigs: flowConnConfig,
		AttachToExisting:  true,
	})
	require.NoError(s.t, err2)
	require.NotNil(s.t, response2)
	require.Equal(s.t, response1.WorkflowId, response2.WorkflowId)

	// Clean up
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s APITestSuite) TestCreateCDCFlowAttachExternalFlowEntry() {
	// Test: create a flows entry from the outside (simulate a crash before the workflow is created), do a request, should succeed
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only testing with PostgreSQL")
	}

	tableName := "external_entry_test"
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, tableName))))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "create_external_" + s.suffix,
		TableNameMapping: map[string]string{AttachSchema(s, tableName): tableName},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	// Simulate a crash: create flows entry without creating workflow
	conn := s.pg.PostgresConnector.Conn()
	sourcePeer := s.source.GeneratePeer(s.t)
	destPeer, err := s.GetPeerInfo(s.t.Context(), &protos.PeerInfoRequest{PeerName: s.ch.Peer().Name})
	require.NoError(s.t, err)

	var sourcePeerID, destPeerID int32
	require.NoError(s.t, conn.QueryRow(s.t.Context(),
		"SELECT id FROM peers WHERE name = $1", sourcePeer.Name).Scan(&sourcePeerID))
	require.NoError(s.t, conn.QueryRow(s.t.Context(),
		"SELECT id FROM peers WHERE name = $1", destPeer.Peer.Name).Scan(&destPeerID))

	cfgBytes, err := proto.Marshal(flowConnConfig)
	require.NoError(s.t, err)

	workflowID := flowConnConfig.FlowJobName + "-peerflow"
	_, err = conn.Exec(s.t.Context(),
		`INSERT INTO flows (workflow_id, name, source_peer, destination_peer, config_proto, status,	description)
		VALUES ($1,$2,$3,$4,$5,$6,'gRPC')`,
		workflowID, flowConnConfig.FlowJobName, sourcePeerID, destPeerID, cfgBytes, protos.FlowStatus_STATUS_SETUP,
	)
	require.NoError(s.t, err)

	// Now call CreateCDCFlow - should start the workflow successfully
	response, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{
		ConnectionConfigs: flowConnConfig,
		AttachToExisting:  true,
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)
	require.Equal(s.t, workflowID, response.WorkflowId)

	// Verify workflow is created and running
	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), conn, tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for flow to be running", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})

	// Clean up
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s APITestSuite) TestCreateCDCFlowAttachCanceledWorkflow() {
	// Test: when cdc flow workflow is failed/canceled, a new run can be created with the same workflow ID
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only testing with PostgreSQL")
	}

	tableName := "canceled_workflow_test"
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, tableName))))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "create_canceled_" + s.suffix,
		TableNameMapping: map[string]string{AttachSchema(s, tableName): tableName},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	// First create a normal flow
	response1, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{
		ConnectionConfigs: flowConnConfig,
		AttachToExisting:  true,
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, response1)

	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)

	// Cancel the workflow to simulate failure
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)

	// Wait for workflow to be canceled
	var firstRunID string
	EnvWaitFor(s.t, env, 30*time.Second, "wait for workflow to be canceled", func() bool {
		desc, err := tc.DescribeWorkflowExecution(s.t.Context(), response1.WorkflowId, "")
		if err != nil {
			return false
		}
		status := desc.GetWorkflowExecutionInfo().GetStatus()
		if status == enums.WORKFLOW_EXECUTION_STATUS_CANCELED {
			firstRunID = desc.GetWorkflowExecutionInfo().GetExecution().GetRunId()
			return true
		}
		return false
	})

	// Attempt to create again - should create a new workflow run with the same workflow ID
	response2, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{
		ConnectionConfigs: flowConnConfig,
		AttachToExisting:  true,
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, response2)
	require.Equal(s.t, response1.WorkflowId, response2.WorkflowId)

	// Verify a new workflow run was created (different run ID, status is RUNNING)
	desc, err := tc.DescribeWorkflowExecution(s.t.Context(), response2.WorkflowId, "")
	require.NoError(s.t, err)
	require.Equal(s.t, enums.WORKFLOW_EXECUTION_STATUS_RUNNING, desc.GetWorkflowExecutionInfo().GetStatus())
	require.NotEqual(s.t, firstRunID, desc.GetWorkflowExecutionInfo().GetExecution().GetRunId(),
		"should have created a new workflow run")

	// Clean up
	env, err = GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s APITestSuite) TestCreateCDCFlowAttachIdempotentAfterContinueAsNew() {
	// Test: cdc flow workflow can continue-as-new and use the same id
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only testing with PostgreSQL")
	}

	tableName := "continue_as_new_test"
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, tableName))))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "create_continue_" + s.suffix,
		TableNameMapping: map[string]string{AttachSchema(s, tableName): tableName},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	// First create a normal flow
	response1, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{
		ConnectionConfigs: flowConnConfig,
		AttachToExisting:  true,
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, response1)

	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	// Wait for flow to be running
	EnvWaitFor(s.t, env, 30*time.Second, "wait for flow to be running", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})

	// Check workflow executions - should have multiple due to continue-as-new during setup->running transition
	listReq := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: "default",
		Query:     fmt.Sprintf("WorkflowId = '%s'", response1.WorkflowId),
	}
	listResp, err := tc.ListWorkflow(s.t.Context(), listReq)
	require.NoError(s.t, err)
	require.Greater(s.t, len(listResp.Executions), 1, "Should have multiple executions (continue-as-new happened)")

	// Call CreateCDCFlow again after continue-as-new - should return the same workflow ID
	response2, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{
		ConnectionConfigs: flowConnConfig,
		AttachToExisting:  true,
	})
	require.NoError(s.t, err)
	require.NotNil(s.t, response2)
	require.Equal(s.t, response1.WorkflowId, response2.WorkflowId, "Should return same workflow ID after continue-as-new")

	// Verify workflow is still running
	desc, err := tc.DescribeWorkflowExecution(s.t.Context(), response2.WorkflowId, "")
	require.NoError(s.t, err)
	require.Equal(s.t, enums.WORKFLOW_EXECUTION_STATUS_RUNNING, desc.GetWorkflowExecutionInfo().GetStatus())

	// Clean up
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}
