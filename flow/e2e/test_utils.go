package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"

	"github.com/PeerDB-io/peer-flow/connectors"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	connsnowflake "github.com/PeerDB-io/peer-flow/connectors/snowflake"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

func init() {
	// it's okay if the .env file is not present
	// we will use the default values
	_ = godotenv.Load()
}

type Suite interface {
	e2eshared.Suite
	T() *testing.T
	Connector() *connpostgres.PostgresConnector
	Suffix() string
}

type RowSource interface {
	Suite
	GetRows(table, cols string) (*model.QRecordBatch, error)
}

type GenericSuite interface {
	RowSource
	Peer() *protos.Peer
	DestinationConnector() connectors.Connector
	DestinationTable(table string) string
}

func AttachSchema(s Suite, table string) string {
	return fmt.Sprintf("e2e_test_%s.%s", s.Suffix(), table)
}

func AddSuffix(s Suite, str string) string {
	return fmt.Sprintf("%s_%s", str, s.Suffix())
}

// Helper function to assert errors in go routines running concurrent to workflows
// This achieves two goals:
// 1. cancel workflow to avoid waiting on goroutine which has failed
// 2. get around t.FailNow being incorrect when called from non initial goroutine
func EnvNoError(t *testing.T, env WorkflowRun, err error) {
	t.Helper()

	if err != nil {
		env.Cancel()
		t.Fatal("UNEXPECTED ERROR", err.Error())
	}
}

func EnvTrue(t *testing.T, env WorkflowRun, val bool) {
	t.Helper()

	if !val {
		env.Cancel()
		t.Fatal("UNEXPECTED FALSE")
	}
}

func GetPgRows(conn *connpostgres.PostgresConnector, suffix string, table string, cols string) (*model.QRecordBatch, error) {
	pgQueryExecutor, err := conn.NewQRepQueryExecutor(context.Background(), "testflow", "testpart")
	if err != nil {
		return nil, err
	}

	return pgQueryExecutor.ExecuteAndProcessQuery(
		context.Background(),
		fmt.Sprintf(`SELECT %s FROM e2e_test_%s.%s ORDER BY id`, cols, suffix, connpostgres.QuoteIdentifier(table)),
	)
}

func RequireEqualTables(suite RowSource, table string, cols string) {
	t := suite.T()
	t.Helper()

	pgRows, err := GetPgRows(suite.Connector(), suite.Suffix(), table, cols)
	require.NoError(t, err)

	rows, err := suite.GetRows(table, cols)
	require.NoError(t, err)

	require.True(t, e2eshared.CheckEqualRecordBatches(t, pgRows, rows))
}

func EnvEqualTables(env WorkflowRun, suite RowSource, table string, cols string) {
	EnvEqualTablesWithNames(env, suite, table, table, cols)
}

func EnvEqualTablesWithNames(env WorkflowRun, suite RowSource, srcTable string, dstTable string, cols string) {
	t := suite.T()
	t.Helper()

	pgRows, err := GetPgRows(suite.Connector(), suite.Suffix(), srcTable, cols)
	EnvNoError(t, env, err)

	rows, err := suite.GetRows(dstTable, cols)
	EnvNoError(t, env, err)

	EnvEqualRecordBatches(t, env, pgRows, rows)
}

func EnvWaitForEqualTables(
	env WorkflowRun,
	suite RowSource,
	reason string,
	table string,
	cols string,
) {
	suite.T().Helper()
	EnvWaitForEqualTablesWithNames(env, suite, reason, table, table, cols)
}

func EnvWaitForEqualTablesWithNames(
	env WorkflowRun,
	suite RowSource,
	reason string,
	srcTable string,
	dstTable string,
	cols string,
) {
	t := suite.T()
	t.Helper()

	EnvWaitFor(t, env, 3*time.Minute, reason, func() bool {
		t.Helper()

		pgRows, err := GetPgRows(suite.Connector(), suite.Suffix(), srcTable, cols)
		if err != nil {
			t.Log(err)
			return false
		}

		rows, err := suite.GetRows(dstTable, cols)
		if err != nil {
			t.Log(err)
			return false
		}

		return e2eshared.CheckEqualRecordBatches(t, pgRows, rows)
	})
}

func EnvWaitForCount(
	env WorkflowRun,
	suite RowSource,
	reason string,
	dstTable string,
	cols string,
	expectedCount int,
) {
	t := suite.T()
	t.Helper()

	EnvWaitFor(t, env, 3*time.Minute, reason, func() bool {
		t.Helper()

		rows, err := suite.GetRows(dstTable, cols)
		if err != nil {
			t.Log(err)
			return false
		}

		return len(rows.Records) == expectedCount
	})
}

func RequireEnvCanceled(t *testing.T, env WorkflowRun) {
	t.Helper()
	EnvWaitForFinished(t, env, time.Minute)
	var panicErr *temporal.PanicError
	var canceledErr *temporal.CanceledError
	if err := env.Error(); err == nil {
		t.Fatal("Expected workflow to be canceled, not completed")
	} else if errors.As(err, &panicErr) {
		t.Fatalf("Workflow panic: %s %s", panicErr.Error(), panicErr.StackTrace())
	} else if !errors.As(err, &canceledErr) {
		t.Fatalf("Expected workflow to be canceled, not %v", err)
	}
}

func SetupCDCFlowStatusQuery(t *testing.T, env WorkflowRun, config *protos.FlowConnectionConfigs) {
	t.Helper()
	// errors expected while PeerFlowStatusQuery is setup
	counter := 0
	for {
		time.Sleep(time.Second)
		counter++
		response, err := env.Query(shared.FlowStatusQuery, config.FlowJobName)
		if err == nil {
			var status protos.FlowStatus
			if err := response.Get(&status); err != nil {
				t.Fatal(err)
			} else if status == protos.FlowStatus_STATUS_RUNNING || status == protos.FlowStatus_STATUS_COMPLETED {
				return
			} else if counter > 30 {
				env.Cancel()
				t.Fatal("UNEXPECTED STATUS TIMEOUT", status)
			}
		} else if counter > 15 {
			env.Cancel()
			t.Fatal("UNEXPECTED STATUS QUERY TIMEOUT", err.Error())
		} else if counter > 5 {
			// log the error for informational purposes
			t.Log(err.Error())
		}
	}
}

func CreateTableForQRep(conn *pgx.Conn, suffix string, tableName string) error {
	createMoodEnum := "CREATE TYPE mood AS ENUM ('happy', 'sad', 'angry');"

	tblFields := []string{
		"id UUID NOT NULL PRIMARY KEY",
		"card_id UUID",
		`"from" TIMESTAMP NOT NULL`,
		"price NUMERIC",
		"created_at TIMESTAMP NOT NULL",
		"updated_at TIMESTAMP NOT NULL",
		"transaction_hash BYTEA",
		"ownerable_type VARCHAR",
		"ownerable_id UUID",
		"user_nonce INTEGER",
		"transfer_type INTEGER DEFAULT 0 NOT NULL",
		"blockchain INTEGER NOT NULL",
		"deal_type VARCHAR",
		"deal_id UUID",
		"ethereum_transaction_id UUID",
		"ignore_price BOOLEAN DEFAULT false",
		"card_eth_value DOUBLE PRECISION",
		"paid_eth_price DOUBLE PRECISION",
		"card_bought_notified BOOLEAN DEFAULT false NOT NULL",
		"address NUMERIC(20,8)",
		"account_id UUID",
		"asset_id NUMERIC NOT NULL",
		"status INTEGER",
		"transaction_id UUID",
		"settled_at TIMESTAMP",
		"reference_id VARCHAR",
		"settle_at TIMESTAMP",
		"settlement_delay_reason INTEGER",
		"f1 text[]",
		"f2 bigint[]",
		"f3 int[]",
		"f4 varchar[]",
		"f5 jsonb",
		"f6 jsonb",
		"f7 jsonb",
		"f8 smallint",
		"f9 date[]",
		"f10 timestamp with time zone[]",
		"f11 timestamp without time zone[]",
		"f12 boolean[]",
		"f13 smallint[]",
		"my_date DATE",
		"old_date DATE",
		"my_time TIME",
		"my_mood mood",
		"myh HSTORE",
		`"geometryPoint" geometry(point)`,
		"geography_point geography(point)",
		"geometry_linestring geometry(linestring)",
		"geography_linestring geography(linestring)",
		"geometry_polygon geometry(polygon)",
		"geography_polygon geography(polygon)",
		"myreal REAL",
		"myreal2 REAL",
		"myreal3 REAL",
		"myinet INET",
		"mycidr CIDR",
		"mymac MACADDR",
	}
	tblFieldStr := strings.Join(tblFields, ",")
	_, enumErr := conn.Exec(context.Background(), createMoodEnum)
	if enumErr != nil &&
		!shared.IsSQLStateError(enumErr, pgerrcode.DuplicateObject, pgerrcode.UniqueViolation) {
		return enumErr
	}
	_, err := conn.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE e2e_test_%s.%s (
			%s
		);`, suffix, tableName, tblFieldStr))
	if err != nil {
		return fmt.Errorf("error creating table for qrep tests: %w", err)
	}

	return nil
}

func generate20MBJson() ([]byte, error) {
	xn := make(map[string]interface{}, 215000)
	for range 215000 {
		xn[uuid.New().String()] = uuid.New().String()
	}

	v, err := json.Marshal(xn)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func PopulateSourceTable(conn *pgx.Conn, suffix string, tableName string, rowCount int) error {
	var id0 string
	rows := make([]string, 0, rowCount)
	for i := range rowCount - 1 {
		id := uuid.New().String()
		if i == 0 {
			id0 = id
		}
		row := fmt.Sprintf(`
					(
						'%s', '%s', CURRENT_TIMESTAMP, 3.86487206688919, CURRENT_TIMESTAMP,
						CURRENT_TIMESTAMP, E'\\\\xDEADBEEF', 'type1', '%s',
						1, 0, 1, 'dealType1',
						'%s', '%s', false, 1.2345,
						1.2345, false, 200.12345678, '%s',
						200, 1, '%s', CURRENT_TIMESTAMP, 'refID',
						CURRENT_TIMESTAMP, 1, ARRAY['text1', 'text2'], ARRAY[123, 456], ARRAY[789, 012],
						ARRAY['varchar1', 'varchar2'], '{"key": -8.02139037433155}',
						'[{"key1": "value1", "key2": "value2", "key3": "value3"}]',
						'{"key": "value"}', 15,'{2023-09-09,2029-08-10}',
							'{"2024-01-15 17:00:00+00","2024-01-16 14:30:00+00"}',
							'{"2026-01-17 10:00:00","2026-01-18 13:45:00"}',
							'{true, false}',
							'{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}',
							CURRENT_DATE, CURRENT_TIME,'happy', '"a"=>"b"','POINT(1 2)','POINT(40.7128 -74.0060)',
						'LINESTRING(0 0, 1 1, 2 2)',
						'LINESTRING(-74.0060 40.7128, -73.9352 40.7306, -73.9123 40.7831)',
						'POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))','POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))',
						pi(), 1, 1.0,
						'10.0.0.0/32', '1.1.10.2'::cidr, 'a1:b2:c3:d4:e5:f6'
					)`,
			id, uuid.New().String(), uuid.New().String(),
			uuid.New().String(), uuid.New().String(), uuid.New().String(), uuid.New().String())
		rows = append(rows, row)
	}

	_, err := conn.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO e2e_test_%s.%s (
					id, card_id, "from", price, created_at,
					updated_at, transaction_hash, ownerable_type, ownerable_id,
					user_nonce, transfer_type, blockchain, deal_type,
					deal_id, ethereum_transaction_id, ignore_price, card_eth_value,
					paid_eth_price, card_bought_notified, address, account_id,
					asset_id, status, transaction_id, settled_at, reference_id,
					settle_at, settlement_delay_reason, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, my_date,
					my_time, my_mood, myh,
					"geometryPoint", geography_point,geometry_linestring, geography_linestring,geometry_polygon, geography_polygon,
					myreal, myreal2, myreal3,
					myinet, mycidr, mymac
			) VALUES %s;
	`, suffix, tableName, strings.Join(rows, ",")))
	if err != nil {
		return fmt.Errorf("error populating source table with initial data: %w", err)
	}

	// add a row where all the nullable fields are null
	_, err = conn.Exec(context.Background(), fmt.Sprintf(`
	INSERT INTO e2e_test_%s.%s (
			id, "from", created_at, updated_at,
			transfer_type, blockchain, card_bought_notified, asset_id
	) VALUES (
			'%s', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
			0, 1, false, 12345
	);
	`, suffix, tableName, uuid.New().String()))
	if err != nil {
		return err
	}

	// generate a 20 MB json and update id0's col f5 to it
	v, err := generate20MBJson()
	if err != nil {
		return err
	}
	_, err = conn.Exec(context.Background(), fmt.Sprintf(`
		UPDATE e2e_test_%s.%s SET f5 = $1 WHERE id = $2;
	`, suffix, tableName), v, id0)
	if err != nil {
		return err
	}

	// update my_date to a date before 1970
	_, err = conn.Exec(context.Background(), fmt.Sprintf(`
		UPDATE e2e_test_%s.%s SET old_date = '1950-01-01' WHERE id = $1;
	`, suffix, tableName), id0)
	if err != nil {
		return err
	}

	return nil
}

func CreateQRepWorkflowConfig(
	t *testing.T,
	flowJobName string,
	sourceTable string,
	dstTable string,
	query string,
	dest string,
	stagingPath string,
	setupDst bool,
	syncedAtCol string,
	isDeletedCol string,
) *protos.QRepConfig {
	t.Helper()

	return &protos.QRepConfig{
		FlowJobName:                flowJobName,
		WatermarkTable:             sourceTable,
		DestinationTableIdentifier: dstTable,
		SourceName:                 GeneratePostgresPeer(t).Name,
		DestinationName:            dest,
		Query:                      query,
		WatermarkColumn:            "updated_at",
		StagingPath:                stagingPath,
		WriteMode: &protos.QRepWriteMode{
			WriteType: protos.QRepWriteType_QREP_WRITE_MODE_APPEND,
		},
		NumRowsPerPartition:              1000,
		InitialCopyOnly:                  true,
		SyncedAtColName:                  syncedAtCol,
		SetupWatermarkTableOnDestination: setupDst,
		SoftDeleteColName:                isDeletedCol,
	}
}

func RunQRepFlowWorkflow(tc client.Client, config *protos.QRepConfig) WorkflowRun {
	return ExecutePeerflow(tc, peerflow.QRepFlowWorkflow, config, nil)
}

func RunXminFlowWorkflow(tc client.Client, config *protos.QRepConfig) WorkflowRun {
	return ExecutePeerflow(tc, peerflow.XminFlowWorkflow, config, nil)
}

func GetOwnersSchema() *qvalue.QRecordSchema {
	return &qvalue.QRecordSchema{
		Fields: []qvalue.QField{
			{Name: "id", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "card_id", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "from", Type: qvalue.QValueKindTimestamp, Nullable: true},
			{Name: "price", Type: qvalue.QValueKindNumeric, Nullable: true},
			{Name: "created_at", Type: qvalue.QValueKindTimestamp, Nullable: true},
			{Name: "updated_at", Type: qvalue.QValueKindTimestamp, Nullable: true},
			{Name: "transaction_hash", Type: qvalue.QValueKindBytes, Nullable: true},
			{Name: "ownerable_type", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "ownerable_id", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "user_nonce", Type: qvalue.QValueKindInt64, Nullable: true},
			{Name: "transfer_type", Type: qvalue.QValueKindInt64, Nullable: true},
			{Name: "blockchain", Type: qvalue.QValueKindInt64, Nullable: true},
			{Name: "deal_type", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "deal_id", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "ethereum_transaction_id", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "ignore_price", Type: qvalue.QValueKindBoolean, Nullable: true},
			{Name: "card_eth_value", Type: qvalue.QValueKindFloat64, Nullable: true},
			{Name: "paid_eth_price", Type: qvalue.QValueKindFloat64, Nullable: true},
			{Name: "card_bought_notified", Type: qvalue.QValueKindBoolean, Nullable: true},
			{Name: "address", Type: qvalue.QValueKindNumeric, Nullable: true},
			{Name: "account_id", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "asset_id", Type: qvalue.QValueKindNumeric, Nullable: true},
			{Name: "status", Type: qvalue.QValueKindInt64, Nullable: true},
			{Name: "transaction_id", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "settled_at", Type: qvalue.QValueKindTimestamp, Nullable: true},
			{Name: "reference_id", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "settle_at", Type: qvalue.QValueKindTimestamp, Nullable: true},
			{Name: "settlement_delay_reason", Type: qvalue.QValueKindInt64, Nullable: true},
			{Name: "f1", Type: qvalue.QValueKindArrayString, Nullable: true},
			{Name: "f2", Type: qvalue.QValueKindArrayInt64, Nullable: true},
			{Name: "f3", Type: qvalue.QValueKindArrayInt32, Nullable: true},
			{Name: "f4", Type: qvalue.QValueKindArrayString, Nullable: true},
			{Name: "f5", Type: qvalue.QValueKindJSON, Nullable: true},
			{Name: "f6", Type: qvalue.QValueKindJSON, Nullable: true},
			{Name: "f7", Type: qvalue.QValueKindJSON, Nullable: true},
			{Name: "f8", Type: qvalue.QValueKindInt16, Nullable: true},
			{Name: "f13", Type: qvalue.QValueKindArrayInt16, Nullable: true},
			{Name: "my_date", Type: qvalue.QValueKindDate, Nullable: true},
			{Name: "old_date", Type: qvalue.QValueKindDate, Nullable: true},
			{Name: "my_time", Type: qvalue.QValueKindTime, Nullable: true},
			{Name: "my_mood", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "geometryPoint", Type: qvalue.QValueKindGeometry, Nullable: true},
			{Name: "geometry_linestring", Type: qvalue.QValueKindGeometry, Nullable: true},
			{Name: "geometry_polygon", Type: qvalue.QValueKindGeometry, Nullable: true},
			{Name: "geography_point", Type: qvalue.QValueKindGeography, Nullable: true},
			{Name: "geography_linestring", Type: qvalue.QValueKindGeography, Nullable: true},
			{Name: "geography_polygon", Type: qvalue.QValueKindGeography, Nullable: true},
			{Name: "myreal", Type: qvalue.QValueKindFloat32, Nullable: true},
			{Name: "myreal2", Type: qvalue.QValueKindFloat32, Nullable: true},
			{Name: "myreal3", Type: qvalue.QValueKindFloat32, Nullable: true},
		},
	}
}

func GetOwnersSelectorStringsSF() [2]string {
	schema := GetOwnersSchema()
	pgFields := make([]string, 0, len(schema.Fields))
	sfFields := make([]string, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		pgFields = append(pgFields, fmt.Sprintf(`"%s"`, field.Name))
		if strings.HasPrefix(field.Name, "geo") {
			colName := connsnowflake.SnowflakeIdentifierNormalize(field.Name)

			// Have to apply a WKT transformation here,
			// else the sql driver we use receives the values as snowflake's OBJECT
			// which is troublesome to deal with. Now it receives it as string.
			sfFields = append(sfFields, fmt.Sprintf(`ST_ASWKT(%s) as %s`, colName, colName))
		} else {
			sfFields = append(sfFields, connsnowflake.SnowflakeIdentifierNormalize(field.Name))
		}
	}
	return [2]string{strings.Join(pgFields, ","), strings.Join(sfFields, ",")}
}

func ExpectedDestinationIdentifier(s GenericSuite, ident string) string {
	switch s.DestinationConnector().(type) {
	case *connsnowflake.SnowflakeConnector:
		return strings.ToUpper(ident)
	default:
		return ident
	}
}

func ExpectedDestinationTableName(s GenericSuite, table string) string {
	return ExpectedDestinationIdentifier(s, s.DestinationTable(table))
}

type testWriter struct {
	*testing.T
}

func (tw *testWriter) Write(p []byte) (int, error) {
	tw.T.Log(string(p))
	return len(p), nil
}

func NewTemporalClient(t *testing.T) client.Client {
	t.Helper()

	logger := slog.New(shared.NewSlogHandler(
		slog.NewJSONHandler(
			&testWriter{t},
			&slog.HandlerOptions{Level: slog.LevelWarn},
		),
	))

	tc, err := client.Dial(client.Options{
		HostPort: "localhost:7233",
		Logger:   logger,
	})
	if err != nil {
		t.Fatalf("Failed to connect temporal client: %v", err)
	}
	return tc
}

type WorkflowRun struct {
	client.WorkflowRun
	c client.Client
}

func ExecutePeerflow(tc client.Client, wf interface{}, args ...interface{}) WorkflowRun {
	return ExecuteWorkflow(tc, shared.PeerFlowTaskQueue, wf, args...)
}

func ExecuteWorkflow(tc client.Client, taskQueueID shared.TaskQueueID, wf interface{}, args ...interface{}) WorkflowRun {
	taskQueue := peerdbenv.PeerFlowTaskQueueName(taskQueueID)

	wr, err := tc.ExecuteWorkflow(
		context.Background(),
		client.StartWorkflowOptions{
			TaskQueue:                taskQueue,
			WorkflowExecutionTimeout: 5 * time.Minute,
		},
		wf,
		args...,
	)
	if err != nil {
		panic(err)
	}
	return WorkflowRun{
		WorkflowRun: wr,
		c:           tc,
	}
}

func (env WorkflowRun) Finished() bool {
	desc, err := env.c.DescribeWorkflowExecution(context.Background(), env.GetID(), "")
	if err != nil {
		return false
	}
	return desc.GetWorkflowExecutionInfo().GetStatus() != enums.WORKFLOW_EXECUTION_STATUS_RUNNING
}

func (env WorkflowRun) Error() error {
	if env.Finished() {
		return env.Get(context.Background(), nil)
	} else {
		return nil
	}
}

func (env WorkflowRun) Cancel() {
	_ = env.c.CancelWorkflow(context.Background(), env.GetID(), "")
}

func (env WorkflowRun) Query(queryType string, args ...interface{}) (converter.EncodedValue, error) {
	return env.c.QueryWorkflow(context.Background(), env.GetID(), "", queryType, args...)
}

func SignalWorkflow[T any](env WorkflowRun, signal model.TypedSignal[T], value T) {
	err := env.c.SignalWorkflow(context.Background(), env.GetID(), "", signal.Name, value)
	if err != nil {
		panic(err)
	}
}

func CompareTableSchemas(x *protos.TableSchema, y *protos.TableSchema) bool {
	xColNames := make([]string, 0, len(x.Columns))
	xColTypes := make([]string, 0, len(x.Columns))
	yColNames := make([]string, 0, len(y.Columns))
	yColTypes := make([]string, 0, len(y.Columns))
	xTypmods := make([]int32, 0, len(x.Columns))
	yTypmods := make([]int32, 0, len(y.Columns))

	for _, col := range x.Columns {
		xColNames = append(xColNames, col.Name)
		xColTypes = append(xColTypes, col.Type)
		xTypmods = append(xTypmods, col.TypeModifier)
	}
	for _, col := range y.Columns {
		yColNames = append(yColNames, col.Name)
		yColTypes = append(yColTypes, col.Type)
		yTypmods = append(yTypmods, col.TypeModifier)
	}

	return x.TableIdentifier == y.TableIdentifier ||
		x.IsReplicaIdentityFull == y.IsReplicaIdentityFull ||
		slices.Compare(x.PrimaryKeyColumns, y.PrimaryKeyColumns) == 0 ||
		slices.Compare(xColNames, yColNames) == 0 ||
		slices.Compare(xColTypes, yColTypes) == 0 ||
		slices.Compare(xTypmods, yTypmods) == 0
}

func RequireEqualRecordBatches(t *testing.T, q *model.QRecordBatch, other *model.QRecordBatch) {
	t.Helper()
	require.True(t, e2eshared.CheckEqualRecordBatches(t, q, other))
}

func EnvEqualRecordBatches(t *testing.T, env WorkflowRun, q *model.QRecordBatch, other *model.QRecordBatch) {
	t.Helper()
	EnvTrue(t, env, e2eshared.CheckEqualRecordBatches(t, q, other))
}

func EnvWaitFor(t *testing.T, env WorkflowRun, timeout time.Duration, reason string, f func() bool) {
	t.Helper()
	t.Log("WaitFor", reason, time.Now())

	deadline := time.Now().Add(timeout)
	for !f() {
		if time.Now().After(deadline) {
			env.Cancel()
			t.Fatal("UNEXPECTED TIMEOUT", reason, time.Now())
		}
		time.Sleep(time.Second)
	}
}

func EnvWaitForFinished(t *testing.T, env WorkflowRun, timeout time.Duration) {
	t.Helper()

	EnvWaitFor(t, env, timeout, "finish", func() bool {
		t.Helper()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		desc, err := env.c.DescribeWorkflowExecution(ctx, env.GetID(), "")
		if err != nil {
			t.Log("Not finished", err)
			return false
		}
		status := desc.GetWorkflowExecutionInfo().GetStatus()
		if status != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
			t.Log("Finished Status", status)
			return true
		}
		return false
	})
}

func EnvGetWorkflowState(t *testing.T, env WorkflowRun) peerflow.CDCFlowWorkflowState {
	t.Helper()
	var state peerflow.CDCFlowWorkflowState
	val, err := env.Query(shared.CDCFlowStateQuery)
	EnvNoError(t, env, err)
	EnvNoError(t, env, val.Get(&state))
	return state
}

func EnvGetFlowStatus(t *testing.T, env WorkflowRun) protos.FlowStatus {
	t.Helper()
	var flowStatus protos.FlowStatus
	val, err := env.Query(shared.FlowStatusQuery)
	EnvNoError(t, env, err)
	EnvNoError(t, env, val.Get(&flowStatus))
	return flowStatus
}
