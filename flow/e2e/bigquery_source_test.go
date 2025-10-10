package e2e

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connbigquery "github.com/PeerDB-io/peerdb/flow/connectors/bigquery"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type bigQuerySource struct {
	conn   *connbigquery.BigQueryConnector
	config *protos.BigqueryConfig
	helper *BigQueryTestHelper
	client *bigquery.Client
}

type BigQueryClickhouseSuite struct {
	GenericSuite
}

// trips1kExpectedColumns is an expected BigQuery schema for the trips_1k test table
var trips1kExpectedBQColumns = map[string]string{
	"trip_id":           "INTEGER",
	"vendor_id":         "INTEGER",
	"pickup_datetime":   "TIMESTAMP",
	"dropoff_datetime":  "TIMESTAMP",
	"passenger_count":   "INTEGER",
	"trip_distance":     "FLOAT",
	"pickup_longitude":  "FLOAT",
	"pickup_latitude":   "FLOAT",
	"dropoff_longitude": "FLOAT",
	"dropoff_latitude":  "FLOAT",
	"fare_amount":       "FLOAT",
	"tip_amount":        "FLOAT",
	"total_amount":      "FLOAT",
	"payment_type":      "STRING",
	"cab_type":          "STRING",
	"pickup_date":       "DATE",
	"dropoff_date":      "DATE",
}

// trips1kExpectedQValueColumns are expected QValue types for trips_1k test table
var trips1kExpectedQValueColumns = map[string]types.QValueKind{
	"trip_id":           types.QValueKindInt64,
	"vendor_id":         types.QValueKindInt64,
	"pickup_datetime":   types.QValueKindTimestamp,
	"dropoff_datetime":  types.QValueKindTimestamp,
	"passenger_count":   types.QValueKindInt64,
	"trip_distance":     types.QValueKindFloat64,
	"pickup_longitude":  types.QValueKindFloat64,
	"pickup_latitude":   types.QValueKindFloat64,
	"dropoff_longitude": types.QValueKindFloat64,
	"dropoff_latitude":  types.QValueKindFloat64,
	"fare_amount":       types.QValueKindFloat64,
	"tip_amount":        types.QValueKindFloat64,
	"total_amount":      types.QValueKindFloat64,
	"payment_type":      types.QValueKindString,
	"cab_type":          types.QValueKindString,
	"pickup_date":       types.QValueKindDate,
	"dropoff_date":      types.QValueKindDate,
}

func TestBigQueryClickhouseSuite(t *testing.T) {
	e2eshared.RunSuite(t, SetupBigQueryClickhouseSuite)
}

func SetupBigQueryClickhouseSuite(t *testing.T) BigQueryClickhouseSuite {
	t.Helper()
	return BigQueryClickhouseSuite{SetupClickHouseSuite(t, false, func(t *testing.T) (*bigQuerySource, string, error) {
		t.Helper()
		suffix := "bqch_" + strings.ToLower(shared.RandomString(8))
		source, err := setupBigQuerySource(t)
		return source, suffix, err
	})(t)}
}

// setupBigQuerySource initializes the BigQuery source for testing
// This assumes BigQuery config will be provided with access to:
// - `ch-integrations` project
// - `do_not_remove_peerdb_source_test_dataset` dataset
// - tables: `trips_1k` (more will be added as needed)
// - `gs://do_not_remove_peerdb_source_test_bucket` bucket is accessible
// with sufficient permissions
func setupBigQuerySource(t *testing.T) (*bigQuerySource, error) {
	t.Helper()

	helper, err := NewBigQueryTestHelper(t, "do_not_remove_peerdb_source_test_dataset")
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery helper: %w", err)
	}

	conn, err := connbigquery.NewBigQueryConnector(t.Context(), helper.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery connector: %w", err)
	}

	bqsa, err := connbigquery.NewBigQueryServiceAccount(helper.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQueryServiceAccount: %w", err)
	}

	client, err := bqsa.CreateBigQueryClient(t.Context())
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	return &bigQuerySource{
		conn:   conn,
		config: helper.Config,
		helper: helper,
		client: client,
	}, nil
}

func (s BigQueryClickhouseSuite) Test_BigQuery_Source_Connection() {
	t := s.T()
	ctx := t.Context()

	source := s.Source().(*bigQuerySource)
	bqConn := source.conn

	err := bqConn.ConnectionActive(ctx)
	require.NoError(t, err, "BigQuery connection should be active")

	schemasResp, err := bqConn.GetSchemas(ctx)
	require.NoError(t, err, "should successfully get schemas/datasets")
	require.NotEmpty(t, schemasResp.Schemas, "should have at least one dataset")

	tablesResp, err := bqConn.GetTablesInSchema(ctx, source.config.DatasetId, false)
	require.NoError(t, err, "should successfully get tables in dataset")
	require.NotNil(t, tablesResp, "tables response should not be nil")

	allTablesResp, err := bqConn.GetAllTables(ctx)
	require.NoError(t, err, "should successfully get all tables")
	require.NotNil(t, allTablesResp, "all tables response should not be nil")
}

func (s BigQueryClickhouseSuite) Test_BigQuery_Source_CDC_Not_Supported() {
	t := s.T()
	ctx := t.Context()

	source := s.Source().(*bigQuerySource)
	bqConn := source.conn

	flowConfig := &protos.FlowConnectionConfigs{
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      source.config.DatasetId + ".trips_1k",
				DestinationTableIdentifier: "trips_1k_dst",
				Engine:                     protos.TableEngine_CH_ENGINE_MERGE_TREE,
			},
		},
		SnapshotStagingPath: "gs://do_not_remove_peerdb_source_test_bucket/test",
	}

	t.Run("CDC Not Supported", func(t *testing.T) {
		flowConfig.InitialSnapshotOnly = false
		flowConfig.DoInitialSnapshot = true

		err := bqConn.ValidateMirrorSource(ctx, flowConfig)
		require.Error(t, err, "should reject CDC flow")
		require.Contains(t, err.Error(), "only supports initial snapshot flows", "error should mention snapshot-only support")
	})

	t.Run("No Initial Snapshot Not Supported", func(t *testing.T) {
		flowConfig.InitialSnapshotOnly = true
		flowConfig.DoInitialSnapshot = false

		err := bqConn.ValidateMirrorSource(ctx, flowConfig)
		require.Error(t, err, "should reject flow without initial snapshot")
		require.Contains(t, err.Error(), "only supports initial snapshot flows", "error should mention snapshot-only support")
	})
}

func (s BigQueryClickhouseSuite) Test_BigQuery_Source_Staging_Path_Required() {
	t := s.T()
	ctx := t.Context()

	source := s.Source().(*bigQuerySource)
	bqConn := source.conn

	flowConfig := &protos.FlowConnectionConfigs{
		InitialSnapshotOnly: true,
		DoInitialSnapshot:   true,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      source.config.DatasetId + ".trips_1k",
				DestinationTableIdentifier: "trips_1k_dst",
			},
		},
		SnapshotStagingPath: "", // empty
	}

	err := bqConn.ValidateMirrorSource(ctx, flowConfig)
	require.Error(t, err, "should fail when staging path is not provided")
	require.Contains(t, err.Error(), "snapshot bucket is required", "error should mention staging path requirement")
}

func (s BigQueryClickhouseSuite) Test_BigQuery_Source_Staging_Path_Invalid_Format() {
	t := s.T()
	ctx := t.Context()

	source := s.Source().(*bigQuerySource)
	bqConn := source.conn

	invalidPaths := []string{
		"not-a-gcs-path",
		"http://example.com/path",
		"s3://bucket/path", // S3, not GCS
		"gs://",            // Missing bucket and path
		"bucket/path",      // Missing gs:// prefix
	}

	for _, invalidPath := range invalidPaths {
		t.Run("Invalid Staging Path: "+invalidPath, func(t *testing.T) {
			flowConfig := &protos.FlowConnectionConfigs{
				InitialSnapshotOnly: true,
				DoInitialSnapshot:   true,
				TableMappings: []*protos.TableMapping{
					{
						SourceTableIdentifier:      source.config.DatasetId + ".trips_1k",
						DestinationTableIdentifier: "trips_1k_dst",
					},
				},
				SnapshotStagingPath: invalidPath,
			}

			err := bqConn.ValidateMirrorSource(ctx, flowConfig)
			require.Error(t, err, "should fail with invalid staging path: %s", invalidPath)
			require.Contains(t, err.Error(), "invalid snapshot bucket", "error should mention invalid staging path for: %s", invalidPath)
		})
	}
}

func (s BigQueryClickhouseSuite) Test_BigQuery_Source_Staging_Path_Inaccessible() {
	t := s.T()
	ctx := t.Context()

	source := s.Source().(*bigQuerySource)
	bqConn := source.conn

	flowConfig := &protos.FlowConnectionConfigs{
		InitialSnapshotOnly: true,
		DoInitialSnapshot:   true,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      source.config.DatasetId + ".trips_1k",
				DestinationTableIdentifier: "trips_1k_dst",
			},
		},
		SnapshotStagingPath: "gs://nonexistent-bucket-peerdb-test-12345/path",
	}

	err := bqConn.ValidateMirrorSource(ctx, flowConfig)
	require.Error(t, err, "should fail with inaccessible bucket")
	require.Contains(t, err.Error(), "failed to access staging bucket", "error should mention staging path access failure")
}

func (s BigQueryClickhouseSuite) Test_BigQuery_Source_Invalid_Table_Mappings() {
	t := s.T()
	ctx := t.Context()

	source := s.Source().(*bigQuerySource)
	bqConn := source.conn

	flowConfig := &protos.FlowConnectionConfigs{
		InitialSnapshotOnly: true,
		DoInitialSnapshot:   true,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      source.config.DatasetId + ".nonexistent_table",
				DestinationTableIdentifier: "nonexistent_dst",
			},
		},
		SnapshotStagingPath: "gs://do_not_remove_peerdb_source_test_bucket/test",
	}

	err := bqConn.ValidateMirrorSource(ctx, flowConfig)
	require.Error(t, err, "should fail with non-existent table")
	require.Contains(t, err.Error(), "failed to get metadata for table", "error should mention metadata failure")
}

func (s BigQueryClickhouseSuite) Test_BigQuery_Source_ValidateMirrorSource_Success() {
	t := s.T()
	ctx := t.Context()

	source := s.Source().(*bigQuerySource)
	bqConn := source.conn

	flowConfig := &protos.FlowConnectionConfigs{
		InitialSnapshotOnly: true,
		DoInitialSnapshot:   true,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      source.config.DatasetId + ".trips_1k",
				DestinationTableIdentifier: "trips_1k_dst",
			},
		},
		SnapshotStagingPath: "gs://do_not_remove_peerdb_source_test_bucket/test",
	}

	err := bqConn.ValidateMirrorSource(ctx, flowConfig)
	require.NoError(t, err, "should succeed with valid configuration")
}

func (s BigQueryClickhouseSuite) Test_BigQuery_Source_Get_Columns() {
	t := s.T()
	ctx := t.Context()

	source := s.Source().(*bigQuerySource)
	bqConn := source.conn

	columnsResp, err := bqConn.GetColumns(ctx, 0, source.config.DatasetId, "trips_1k")
	require.NoError(t, err, "should successfully get columns from trips_1k")
	require.NotNil(t, columnsResp, "columns response should not be nil")
	require.NotEmpty(t, columnsResp.Columns, "should have at least one column")

	typeMap := make(map[string]string, len(columnsResp.Columns))
	qKindMap := make(map[string]types.QValueKind, len(columnsResp.Columns))
	for _, col := range columnsResp.Columns {
		require.NotEmpty(t, col.Name, "column name should not be empty")
		require.NotEmpty(t, col.Type, "column type should not be empty")
		require.NotEmpty(t, col.Qkind, "column QKind should not be empty")
		typeMap[col.Name] = col.Type
		qKindMap[col.Name] = types.QValueKind(col.Qkind)
	}

	for colName, expectedType := range trips1kExpectedBQColumns {
		actualType, exists := typeMap[colName]
		require.True(t, exists, "column %s should exist", colName)
		require.Equal(t, expectedType, actualType, "column %s should have type %s", colName, expectedType)
	}

	for colName, expectedType := range trips1kExpectedQValueColumns {
		actualType, exists := qKindMap[colName]
		require.True(t, exists, "column %s should exist", colName)
		require.Equal(t, expectedType, actualType, "column %s should have type %s", colName, expectedType)
	}
}

func (s BigQueryClickhouseSuite) Test_BigQuery_Source_Get_Table_Schema() {
	t := s.T()
	ctx := t.Context()

	source := s.Source().(*bigQuerySource)
	bqConn := source.conn

	tableMappings := []*protos.TableMapping{
		{
			SourceTableIdentifier:      source.config.DatasetId + ".trips_1k",
			DestinationTableIdentifier: "trips_1k_dst",
		},
	}

	schemas, err := bqConn.GetTableSchema(ctx, map[string]string{}, 0, protos.TypeSystem_Q, tableMappings)
	require.NoError(t, err, "should successfully get table schema")
	require.NotNil(t, schemas, "schemas should not be nil")
	require.Len(t, schemas, 1, "should have one table schema")

	tableSchema := schemas[source.config.DatasetId+".trips_1k"]
	require.NotNil(t, tableSchema, "table schema should not be nil")
	require.NotEmpty(t, tableSchema.Columns, "should have columns")
	require.Equal(t, protos.TypeSystem_Q, tableSchema.System, "should use Q type system")

	pkColumns := tableSchema.PrimaryKeyColumns
	require.NotNil(t, pkColumns, "primary key columns should not be nil")
	require.Contains(t, pkColumns, "trip_id", "trip_id should be part of primary key columns")

	columnMap := make(map[string]types.QValueKind)
	for _, col := range tableSchema.Columns {
		require.NotEmpty(t, col.Name, "column name should not be empty")
		require.NotEmpty(t, col.Type, "column type should not be empty")
		columnMap[col.Name] = types.QValueKind(col.Type)
	}

	for colName, expectedType := range trips1kExpectedQValueColumns {
		actualType, exists := columnMap[colName]
		require.True(t, exists, "column %s should exist", colName)
		require.Equal(t, expectedType, actualType, "column %s should have QValue type %s", colName, expectedType)
	}
}

func (s BigQueryClickhouseSuite) Test_Trips_Flow() {
	t := s.T()

	source := s.Source().(*bigQuerySource)
	srcTable := "trips_1k"
	dstTable := "trips_1k_dst"

	t.Logf("ClickHouse database: %s", s.Peer().Config.(*protos.Peer_ClickhouseConfig).ClickhouseConfig.Database)

	count, err := source.helper.countRowsWithDataset(t.Context(), source.config.DatasetId, srcTable, "")
	require.NoError(t, err, "should be able to count rows in source table")
	require.Positive(t, count, "source table should have data")
	t.Logf("Source table %s has %d rows", srcTable, count)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: AddSuffix(s, srcTable),
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      fmt.Sprintf("%s.%s", source.config.DatasetId, srcTable),
				DestinationTableIdentifier: s.DestinationTable(dstTable),
			},
		},
		Destination: s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.InitialSnapshotOnly = true
	flowConnConfig.SnapshotStagingPath = "gs://do_not_remove_peerdb_source_test_bucket/test"

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)

	EnvWaitForEqualTablesWithNames(
		env,
		s,
		"initial load to match",
		srcTable,
		dstTable,
		"trip_id,vendor_id,passenger_count,trip_distance,fare_amount",
	)

	env.Cancel(t.Context())
}

func (s *bigQuerySource) GeneratePeer(t *testing.T) *protos.Peer {
	t.Helper()
	peer := &protos.Peer{
		Name: "bigquery_source",
		Type: protos.DBType_BIGQUERY,
		Config: &protos.Peer_BigqueryConfig{
			BigqueryConfig: s.config,
		},
	}
	CreatePeer(t, peer)
	return peer
}

func (s *bigQuerySource) Teardown(t *testing.T, ctx context.Context, suffix string) {
	t.Helper()
}

func (s *bigQuerySource) Connector() connectors.Connector {
	return s.conn
}

func (s *bigQuerySource) Exec(ctx context.Context, sql string) error {
	return errors.New("not implemented")
}

func (s *bigQuerySource) GetRows(ctx context.Context, suffix string, table string, cols string) (*model.QRecordBatch, error) {
	query := fmt.Sprintf("SELECT %s FROM `%s.%s.%s`", cols, s.config.ProjectId, s.config.DatasetId, table)

	orderByColumns := strings.TrimSpace(strings.Split(cols, ",")[0])
	if orderByColumns != "" {
		query += " ORDER BY " + orderByColumns
	}

	return s.helper.ExecuteAndProcessQuery(ctx, query)
}
