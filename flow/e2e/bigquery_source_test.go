package e2e

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
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
	"trip_distance":     "FLOAT64",
	"pickup_longitude":  "FLOAT64",
	"pickup_latitude":   "FLOAT64",
	"dropoff_longitude": "FLOAT64",
	"dropoff_latitude":  "FLOAT64",
	"fare_amount":       "FLOAT64",
	"tip_amount":        "FLOAT64",
	"total_amount":      "FLOAT64",
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

var stagingTestBucket = "gs://peerdb_bigquery_source_test_do_not_remove"

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
// - `do_not_remove_peerdb_source_test_dataset` dataset
// - tables: `trips_1k` (more will be added as needed)
// - `gs://peerdb_bigquery_source_test_do_not_remove` bucket is accessible
// with sufficient permissions
func setupBigQuerySource(t *testing.T) (*bigQuerySource, error) {
	t.Helper()

	if v, ok := os.LookupEnv("PEERDB_BIGQUERY_TEST_STAGING_BUCKET"); ok {
		stagingTestBucket = v
	}

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

	flowConfig := &protos.FlowConnectionConfigsCore{
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      source.config.DatasetId + ".trips_1k",
				DestinationTableIdentifier: "trips_1k_dst",
				Engine:                     protos.TableEngine_CH_ENGINE_MERGE_TREE,
			},
		},
		SnapshotStagingPath: stagingTestBucket + "/test",
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

	flowConfig := &protos.FlowConnectionConfigsCore{
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
			flowConfig := &protos.FlowConnectionConfigsCore{
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

	flowConfig := &protos.FlowConnectionConfigsCore{
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

	flowConfig := &protos.FlowConnectionConfigsCore{
		InitialSnapshotOnly: true,
		DoInitialSnapshot:   true,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      source.config.DatasetId + ".nonexistent_table",
				DestinationTableIdentifier: "nonexistent_dst",
			},
		},
		SnapshotStagingPath: stagingTestBucket + "/test",
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

	flowConfig := &protos.FlowConnectionConfigsCore{
		InitialSnapshotOnly: true,
		DoInitialSnapshot:   true,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      source.config.DatasetId + ".trips_1k",
				DestinationTableIdentifier: "trips_1k_dst",
			},
		},
		SnapshotStagingPath: stagingTestBucket + "/test",
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
	ctx := t.Context()

	source := s.Source().(*bigQuerySource)
	srcTable := "trips_1k"
	dstTable := "trips_1k_dst"

	t.Logf("ClickHouse database: %s", s.Peer().Config.(*protos.Peer_ClickhouseConfig).ClickhouseConfig.Database)

	count, err := source.helper.countRowsWithDataset(ctx, source.config.DatasetId, srcTable, "")
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
	flowConnConfig.SnapshotStagingPath = stagingTestBucket

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

	EnvWaitForFinished(t, env, 3*time.Minute)

	apiClient, err := NewApiClient()
	require.NoError(t, err)

	statusResp, err := apiClient.MirrorStatus(ctx, &protos.MirrorStatusRequest{
		FlowJobName:     flowConnConfig.FlowJobName,
		IncludeFlowInfo: true,
	})
	require.NoError(t, err)

	cdcStatus := statusResp.GetCdcStatus()
	require.NotNil(t, cdcStatus)
	require.NotNil(t, cdcStatus.SnapshotStatus)
	require.NotEmpty(t, cdcStatus.SnapshotStatus.Clones)

	var totalRowsSynced int64
	for _, clone := range cdcStatus.SnapshotStatus.Clones {
		totalRowsSynced += clone.NumRowsSynced
	}
	require.Equal(t, int64(count), totalRowsSynced,
		"total NumRowsSynced across clones should equal source row count")

	totalRowsResp, err := apiClient.TotalRowsSyncedByMirror(ctx, &protos.TotalRowsSyncedByMirrorRequest{
		FlowJobName: flowConnConfig.FlowJobName,
	})
	require.NoError(t, err)
	require.Equal(t, int64(count), totalRowsResp.TotalCountInitialLoad,
		"TotalCountInitialLoad should equal source row count")
}

func (s BigQueryClickhouseSuite) Test_Trips_Flow_Small_Partitions() {
	t := s.T()

	source := s.Source().(*bigQuerySource)
	srcTable := "trips_1k"
	dstTable := "trips_1k_dst_small_partitions"

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
	flowConnConfig.SnapshotStagingPath = stagingTestBucket
	flowConnConfig.SnapshotNumRowsPerPartition = 10 // 1000 rows / 10 = 100 partitions
	flowConnConfig.SnapshotMaxParallelWorkers = 10

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

func (s BigQueryClickhouseSuite) Test_Types() {
	t := s.T()
	ctx := t.Context()

	t.Logf("ClickHouse database: %s", s.Peer().Config.(*protos.Peer_ClickhouseConfig).ClickhouseConfig.Database)

	source := s.Source().(*bigQuerySource)
	srcTable := "test_types_" + strings.ToLower(shared.RandomString(8))
	dstTable := srcTable + "_dst"

	t.Logf("Creating test table %s with all supported types", srcTable)

	schema := bigquery.Schema{
		{Name: "id", Type: bigquery.IntegerFieldType, Required: true},
		{Name: "str_col", Type: bigquery.StringFieldType, Required: false},
		{Name: "bytes_col", Type: bigquery.BytesFieldType, Required: false},
		{Name: "int_col", Type: bigquery.IntegerFieldType, Required: false},
		{Name: "float_col", Type: bigquery.FloatFieldType, Required: false},
		{Name: "bool_col", Type: bigquery.BooleanFieldType, Required: false},
		{Name: "ts_col", Type: bigquery.TimestampFieldType, Required: false},
		{Name: "date_col", Type: bigquery.DateFieldType, Required: false},
		{Name: "time_col", Type: bigquery.TimeFieldType, Required: false},
		{Name: "numeric_col", Type: bigquery.NumericFieldType, Required: false, Precision: 38, Scale: 9},
		{Name: "bignumeric_col", Type: bigquery.BigNumericFieldType, Required: false, Precision: 76, Scale: 38},
		{Name: "geography_col", Type: bigquery.GeographyFieldType, Required: false},
		{Name: "array_str", Type: bigquery.StringFieldType, Repeated: true},
		{Name: "array_int", Type: bigquery.IntegerFieldType, Repeated: true},
		{Name: "array_float", Type: bigquery.FloatFieldType, Repeated: true},
		{Name: "array_bool", Type: bigquery.BooleanFieldType, Repeated: true},
		{Name: "array_ts", Type: bigquery.TimestampFieldType, Repeated: true},
		{Name: "array_date", Type: bigquery.DateFieldType, Repeated: true},
		{Name: "record_col", Type: bigquery.RecordFieldType, Required: false, Schema: bigquery.Schema{
			{Name: "nested_str", Type: bigquery.StringFieldType},
			{Name: "nested_int", Type: bigquery.IntegerFieldType},
			{Name: "nested_bool", Type: bigquery.BooleanFieldType},
		}},
		{Name: "array_record", Type: bigquery.RecordFieldType, Repeated: true, Schema: bigquery.Schema{
			{Name: "item_name", Type: bigquery.StringFieldType},
			{Name: "item_count", Type: bigquery.IntegerFieldType},
		}},
	}

	table := source.client.DatasetInProject(source.config.ProjectId, source.config.DatasetId).Table(srcTable)
	err := table.Create(ctx, &bigquery.TableMetadata{
		Schema: schema,
		TableConstraints: &bigquery.TableConstraints{
			PrimaryKey: &bigquery.PrimaryKey{
				Columns: []string{"id"},
			},
		},
	})
	require.NoError(t, err, "should create table successfully")

	defer func() {
		if err := table.Delete(ctx); err != nil {
			t.Logf("Warning: failed to delete test table %s: %v", srcTable, err)
		}
	}()

	type NestedRecord struct {
		NestedStr  string `bigquery:"nested_str"`
		NestedInt  int64  `bigquery:"nested_int"`
		NestedBool bool   `bigquery:"nested_bool"`
	}

	type ArrayRecordItem struct {
		ItemName  string `bigquery:"item_name"`
		ItemCount int64  `bigquery:"item_count"`
	}

	type TestRow struct {
		RecordCol     *NestedRecord          `bigquery:"record_col"`
		BignumericCol *big.Rat               `bigquery:"bignumeric_col"`
		NumericCol    *big.Rat               `bigquery:"numeric_col"`
		TsCol         bigquery.NullTimestamp `bigquery:"ts_col"`
		ArrayBool     []bool                 `bigquery:"array_bool"`
		ArrayFloat    []float64              `bigquery:"array_float"`
		ArrayRecord   []ArrayRecordItem      `bigquery:"array_record"`
		StrCol        bigquery.NullString    `bigquery:"str_col"`
		ArrayDate     []civil.Date           `bigquery:"array_date"`
		ArrayTs       []time.Time            `bigquery:"array_ts"`
		BytesCol      []byte                 `bigquery:"bytes_col"`
		GeographyCol  bigquery.NullGeography `bigquery:"geography_col"`
		ArrayStr      []string               `bigquery:"array_str"`
		ArrayInt      []int64                `bigquery:"array_int"`
		TimeCol       bigquery.NullTime      `bigquery:"time_col"`
		DateCol       bigquery.NullDate      `bigquery:"date_col"`
		IntCol        bigquery.NullInt64     `bigquery:"int_col"`
		FloatCol      bigquery.NullFloat64   `bigquery:"float_col"`
		ID            int64                  `bigquery:"id"`
		BoolCol       bigquery.NullBool      `bigquery:"bool_col"`
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	testData := []TestRow{
		{
			ID:            1,
			StrCol:        bigquery.NullString{StringVal: "test string", Valid: true},
			BytesCol:      []byte("test bytes"),
			IntCol:        bigquery.NullInt64{Int64: 42, Valid: true},
			FloatCol:      bigquery.NullFloat64{Float64: 3.14159, Valid: true},
			BoolCol:       bigquery.NullBool{Bool: true, Valid: true},
			TsCol:         bigquery.NullTimestamp{Timestamp: now, Valid: true},
			DateCol:       bigquery.NullDate{Date: civil.DateOf(time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)), Valid: true},
			TimeCol:       bigquery.NullTime{Time: civil.TimeOf(time.Date(0, 1, 1, 14, 30, 0, 0, time.UTC)), Valid: true},
			NumericCol:    big.NewRat(12345, 100),                                                      // 123.45
			BignumericCol: big.NewRat(9876543, 1000),                                                   // 9876.543
			GeographyCol:  bigquery.NullGeography{GeographyVal: "POINT(-122.084 37.422)", Valid: true}, // Google HQ
			ArrayStr:      []string{"foo", "bar", "baz"},
			ArrayInt:      []int64{1, 2, 3, 4, 5},
			ArrayFloat:    []float64{1.1, 2.2, 3.3},
			ArrayBool:     []bool{true, false, true},
			ArrayTs:       []time.Time{now, now.Add(-time.Hour), now.Add(-24 * time.Hour)},
			ArrayDate: []civil.Date{
				civil.DateOf(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
				civil.DateOf(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)),
				civil.DateOf(time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC)),
			},
			RecordCol: &NestedRecord{
				NestedStr:  "nested value",
				NestedInt:  100,
				NestedBool: true,
			},
			ArrayRecord: []ArrayRecordItem{
				{ItemName: "item1", ItemCount: 10},
				{ItemName: "item2", ItemCount: 20},
			},
		},
		{
			ID:           2,
			StrCol:       bigquery.NullString{StringVal: "another string", Valid: true},
			IntCol:       bigquery.NullInt64{Int64: -999, Valid: true},
			FloatCol:     bigquery.NullFloat64{Float64: -2.71828, Valid: true},
			BoolCol:      bigquery.NullBool{Bool: false, Valid: true},
			TsCol:        bigquery.NullTimestamp{Timestamp: now.Add(-48 * time.Hour), Valid: true},
			DateCol:      bigquery.NullDate{Date: civil.DateOf(time.Date(2023, 12, 31, 0, 0, 0, 0, time.UTC)), Valid: true},
			TimeCol:      bigquery.NullTime{Time: civil.TimeOf(time.Date(0, 1, 1, 9, 15, 30, 0, time.UTC)), Valid: true},
			ArrayStr:     []string{"single"},
			ArrayInt:     []int64{},
			ArrayFloat:   []float64{0.0},
			ArrayBool:    []bool{false},
			GeographyCol: bigquery.NullGeography{},
			ArrayRecord:  []ArrayRecordItem{},
		},
		{
			ID:       3,
			StrCol:   bigquery.NullString{StringVal: "", Valid: true}, // Empty string, not NULL
			IntCol:   bigquery.NullInt64{Int64: 0, Valid: true},
			FloatCol: bigquery.NullFloat64{Float64: 0.0, Valid: true},
			BoolCol:  bigquery.NullBool{Bool: false, Valid: true},
			TsCol:    bigquery.NullTimestamp{Timestamp: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true},
			DateCol:  bigquery.NullDate{Date: civil.DateOf(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)), Valid: true},
			TimeCol:  bigquery.NullTime{Time: civil.TimeOf(time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)), Valid: true},
			ArrayStr: []string{},
			ArrayInt: []int64{-1, 0, 1},
			ArrayDate: []civil.Date{
				civil.DateOf(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)),
			},
		},
	}

	inserter := table.Inserter()
	err = inserter.Put(ctx, testData)
	require.NoError(t, err, "should insert test data successfully")

	time.Sleep(2 * time.Second)

	count, err := source.helper.countRowsWithDataset(ctx, source.config.DatasetId, srcTable, "")
	require.NoError(t, err, "should be able to count rows")
	require.Equal(t, len(testData), count, "should have inserted all test rows")
	t.Logf("Inserted %d rows into source table", count)

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
	flowConnConfig.SnapshotStagingPath = stagingTestBucket + "/test_types"

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "all types replicated", srcTable, dstTable, "id")

	t.Log("Verifying data in ClickHouse")
	dstRows, err := s.GetRows(dstTable, "id,str_col,int_col,float_col,bool_col")
	require.NoError(t, err, "should fetch rows from ClickHouse")
	require.Len(t, dstRows.Records, len(testData), "should have all rows in destination")

	if len(dstRows.Records) > 0 {
		firstRow := dstRows.Records[0]
		require.Equal(t, int64(1), firstRow[0].Value(), "ID should match")
		require.Equal(t, "test string", firstRow[1].Value(), "string column should match")
		require.Equal(t, int64(42), firstRow[2].Value(), "int column should match")
		require.InDelta(t, 3.14159, firstRow[3].Value().(float64), 0.00001, "float column should match")
		require.Equal(t, true, firstRow[4].Value(), "bool column should match")
		t.Log("Basic type verification passed")
	}

	arrayRows, err := s.GetRows(dstTable, "id,array_str,array_int,array_bool")
	require.NoError(t, err, "should fetch array columns")
	if len(arrayRows.Records) > 0 {
		firstRow := arrayRows.Records[0]
		t.Logf("Array columns: str=%v, int=%v, bool=%v",
			firstRow[1].Value(), firstRow[2].Value(), firstRow[3].Value())
	}

	env.Cancel(ctx)
}

func (s BigQueryClickhouseSuite) Test_JSON_Support() {
	t := s.T()
	ctx := t.Context()

	t.Logf("ClickHouse database: %s", s.Peer().Config.(*protos.Peer_ClickhouseConfig).ClickhouseConfig.Database)

	source := s.Source().(*bigQuerySource)
	srcTable := "test_json_" + strings.ToLower(shared.RandomString(8))
	dstTable := srcTable + "_dst"

	t.Logf("Creating test table %s with JSON column", srcTable)

	// Define the table schema with JSON column
	schema := bigquery.Schema{
		{Name: "id", Type: bigquery.IntegerFieldType, Required: true},
		{Name: "name", Type: bigquery.StringFieldType, Required: true},
		{Name: "metadata", Type: bigquery.JSONFieldType, Required: false},
		{Name: "tags", Type: bigquery.StringFieldType, Repeated: true},
	}

	// Create the table
	table := source.client.DatasetInProject(source.config.ProjectId, source.config.DatasetId).Table(srcTable)
	err := table.Create(ctx, &bigquery.TableMetadata{
		Schema: schema,
		TableConstraints: &bigquery.TableConstraints{
			PrimaryKey: &bigquery.PrimaryKey{
				Columns: []string{"id"},
			},
		},
	})
	require.NoError(t, err, "should create table successfully")

	// Cleanup: delete table after test
	defer func() {
		if err := table.Delete(ctx); err != nil {
			t.Logf("Warning: failed to delete test table %s: %v", srcTable, err)
		}
	}()

	type TestRow struct {
		Name     string
		Metadata bigquery.NullJSON
		Tags     []string
		ID       int64
	}

	testData := []TestRow{
		{
			ID:   1,
			Name: "Product A",
			Metadata: bigquery.NullJSON{JSONVal: `{
				"price": 99.99,
				"in_stock": true,
				"category": "electronics",
				"specs": {"weight": 1.5, "color": "black"},
				"reviewCount": 42
			}`, Valid: true},
			Tags: []string{"featured", "sale"},
		},
		{
			ID:   2,
			Name: "Product B",
			Metadata: bigquery.NullJSON{JSONVal: `{
				"price": 149.50,
				"in_stock": false,
				"category": "furniture",
				"specs": {"dimensions": {"width": 100, "height": 200}}
			}`, Valid: true},
			Tags: []string{"new", "premium"},
		},
		{
			ID:       3,
			Name:     "Product C",
			Metadata: bigquery.NullJSON{},
			Tags:     []string{"clearance"},
		},
	}

	inserter := table.Inserter()
	err = inserter.Put(ctx, testData)
	require.NoError(t, err, "should insert test data successfully")

	time.Sleep(2 * time.Second)

	count, err := source.helper.countRowsWithDataset(ctx, source.config.DatasetId, srcTable, "")
	require.NoError(t, err, "should be able to count rows")
	require.Equal(t, len(testData), count, "should have inserted all test rows")
	t.Logf("Inserted %d rows into source table", count)

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
	flowConnConfig.SnapshotStagingPath = stagingTestBucket + "/test_json"

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "JSON data replicated", srcTable, dstTable, "id,name")

	t.Log("Verifying JSON data in ClickHouse")
	dstRows, err := s.GetRows(dstTable, "id,name,metadata")
	require.NoError(t, err, "should fetch rows from ClickHouse")
	require.Len(t, dstRows.Records, len(testData), "should have all rows in destination")

	if len(dstRows.Records) > 0 {
		firstRow := dstRows.Records[0]
		require.Equal(t, int64(1), firstRow[0].Value(), "first row ID should be 1")
		require.Equal(t, "Product A", firstRow[1].Value(), "first row name should match")

		jsonStr, ok := firstRow[2].Value().(string)
		require.True(t, ok, "JSON should be stored as string")
		require.Contains(t, jsonStr, "price", "JSON should contain price field")
		require.Contains(t, jsonStr, "99.99", "JSON should contain price value")
		require.Contains(t, jsonStr, "electronics", "JSON should contain category value")
		t.Logf("JSON content verified: %s", jsonStr)
	}

	env.Cancel(ctx)
}

func (s BigQueryClickhouseSuite) Test_GCS_Cleanup_After_Initial_Load() {
	t := s.T()
	ctx := t.Context()

	t.Logf("ClickHouse database: %s", s.Peer().Config.(*protos.Peer_ClickhouseConfig).ClickhouseConfig.Database)

	source := s.Source().(*bigQuerySource)
	srcTable := "test_gcs_cleanup_" + strings.ToLower(shared.RandomString(8))
	dstTable := srcTable + "_dst"

	t.Logf("Creating test table %s", srcTable)

	schema := bigquery.Schema{
		{Name: "id", Type: bigquery.IntegerFieldType, Required: true},
		{Name: "data", Type: bigquery.StringFieldType, Required: true},
		{Name: "value", Type: bigquery.IntegerFieldType, Required: false},
	}

	table := source.client.DatasetInProject(source.config.ProjectId, source.config.DatasetId).Table(srcTable)
	err := table.Create(ctx, &bigquery.TableMetadata{
		Schema: schema,
		TableConstraints: &bigquery.TableConstraints{
			PrimaryKey: &bigquery.PrimaryKey{
				Columns: []string{"id"},
			},
		},
	})
	require.NoError(t, err, "should create table successfully")

	defer func() {
		if err := table.Delete(ctx); err != nil {
			t.Logf("Warning: failed to delete test table %s: %v", srcTable, err)
		}
	}()

	type TestRow struct {
		Data  string `bigquery:"data"`
		ID    int64  `bigquery:"id"`
		Value int64  `bigquery:"value"`
	}

	testData := []TestRow{
		{ID: 1, Data: "test data 1", Value: 100},
		{ID: 2, Data: "test data 2", Value: 200},
		{ID: 3, Data: "test data 3", Value: 300},
		{ID: 4, Data: "test data 4", Value: 400},
		{ID: 5, Data: "test data 5", Value: 500},
	}

	inserter := table.Inserter()
	err = inserter.Put(ctx, testData)
	require.NoError(t, err, "should insert test data successfully")

	time.Sleep(2 * time.Second)

	count, err := source.helper.countRowsWithDataset(ctx, source.config.DatasetId, srcTable, "")
	require.NoError(t, err, "should be able to count rows")
	require.Equal(t, len(testData), count, "should have inserted all test rows")
	t.Logf("Inserted %d rows into source table", count)

	gcsStagingPath := fmt.Sprintf("%s/test_cleanup/%s", stagingTestBucket, srcTable)

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
	flowConnConfig.SnapshotStagingPath = gcsStagingPath

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "data replicated", srcTable, dstTable, "id")

	t.Log("Verifying data in ClickHouse")
	dstRows, err := s.GetRows(dstTable, "id,data,value")
	require.NoError(t, err, "should fetch rows from ClickHouse")
	require.Len(t, dstRows.Records, len(testData), "should have all rows in destination")

	env.Cancel(ctx)

	t.Log("Checking GCS staging path for exported objects")

	EnvWaitFor(t, env, time.Second*30, fmt.Sprintf("GCS path %s to be cleaned up", gcsStagingPath), func() bool {
		objectCount, err := source.helper.CountObjectsInGCSPath(ctx, gcsStagingPath)
		require.NoError(t, err, "should be able to count GCS objects")
		return objectCount > 0
	})
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
