package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/PeerDB-io/peer-flow/activities"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	connsnowflake "github.com/PeerDB-io/peer-flow/connectors/snowflake"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/PeerDB-io/peer-flow/shared/alerting"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

func RegisterWorkflowsAndActivities(t *testing.T, env *testsuite.TestWorkflowEnvironment) {
	t.Helper()

	conn, err := pgxpool.New(context.Background(), utils.GetPGConnectionString(GetTestPostgresConf()))
	if err != nil {
		t.Fatalf("unable to create catalog connection pool: %v", err)
	}

	// set a 300 second timeout for the workflow to execute a few runs.
	env.SetTestTimeout(300 * time.Second)

	env.RegisterWorkflow(peerflow.CDCFlowWorkflowWithConfig)
	env.RegisterWorkflow(peerflow.SyncFlowWorkflow)
	env.RegisterWorkflow(peerflow.SetupFlowWorkflow)
	env.RegisterWorkflow(peerflow.SnapshotFlowWorkflow)
	env.RegisterWorkflow(peerflow.NormalizeFlowWorkflow)
	env.RegisterWorkflow(peerflow.QRepFlowWorkflow)
	env.RegisterWorkflow(peerflow.XminFlowWorkflow)
	env.RegisterWorkflow(peerflow.QRepPartitionWorkflow)

	alerter, err := alerting.NewAlerter(conn)
	if err != nil {
		t.Fatalf("unable to create alerter: %v", err)
	}

	env.RegisterActivity(&activities.FlowableActivity{
		CatalogPool: conn,
		Alerter:     alerter,
	})
	env.RegisterActivity(&activities.SnapshotActivity{})
}

// Helper function to assert errors in go routines running concurrent to workflows
// This achieves two goals:
// 1. cancel workflow to avoid waiting on goroutine which has failed
// 2. get around t.FailNow being incorrect when called from non initial goroutine
func EnvNoError(t *testing.T, env *testsuite.TestWorkflowEnvironment, err error) {
	t.Helper()

	if err != nil {
		t.Error(err.Error())
		env.CancelWorkflow()
		runtime.Goexit()
	}
}

// See EnvNoError
func EnvEqual[T comparable](t *testing.T, env *testsuite.TestWorkflowEnvironment, x T, y T) {
	t.Helper()

	if x != y {
		t.Error("not equal", x, y)
		env.CancelWorkflow()
		runtime.Goexit()
	}
}

func EnvTrue(t *testing.T, env *testsuite.TestWorkflowEnvironment, val bool) {
	t.Helper()

	if !val {
		t.Error("assertion failed")
		env.CancelWorkflow()
		runtime.Goexit()
	}
}

func GetPgRows(pool *pgxpool.Pool, suffix string, table string, cols string) (*model.QRecordBatch, error) {
	pgQueryExecutor := connpostgres.NewQRepQueryExecutor(pool, context.Background(), "testflow", "testpart")
	pgQueryExecutor.SetTestEnv(true)

	return pgQueryExecutor.ExecuteAndProcessQuery(
		fmt.Sprintf(`SELECT %s FROM e2e_test_%s."%s" ORDER BY id`, cols, suffix, table),
	)
}

func RequireEqualTables(suite e2eshared.RowSource, table string, cols string) {
	t := suite.T()
	t.Helper()

	suffix := suite.Suffix()
	pool := suite.Pool()
	pgRows, err := GetPgRows(pool, suffix, table, cols)
	require.NoError(t, err)

	rows, err := suite.GetRows(table, cols)
	require.NoError(t, err)

	require.True(t, e2eshared.CheckEqualRecordBatches(t, pgRows, rows))
}

func EnvEqualTables(env *testsuite.TestWorkflowEnvironment, suite e2eshared.RowSource, table string, cols string) {
	t := suite.T()
	t.Helper()

	suffix := suite.Suffix()
	pool := suite.Pool()
	pgRows, err := GetPgRows(pool, suffix, table, cols)
	EnvNoError(t, env, err)

	rows, err := suite.GetRows(table, cols)
	EnvNoError(t, env, err)

	EnvEqualRecordBatches(t, env, pgRows, rows)
}

func EnvWaitForEqualTables(
	env *testsuite.TestWorkflowEnvironment,
	suite e2eshared.RowSource,
	reason string,
	table string,
	cols string,
) {
	t := suite.T()
	EnvWaitFor(t, env, time.Minute, reason, func(ctx context.Context) bool {
		suffix := suite.Suffix()
		pool := suite.Pool()
		pgRows, err := GetPgRows(pool, suffix, table, cols)
		if err != nil {
			return false
		}

		rows, err := suite.GetRows(table, cols)
		if err != nil {
			return false
		}

		return e2eshared.CheckEqualRecordBatches(t, pgRows, rows)
	})
}

func SetupCDCFlowStatusQuery(env *testsuite.TestWorkflowEnvironment,
	connectionGen FlowConnectionGenerationConfig,
) {
	// wait for PeerFlowStatusQuery to finish setup
	// sleep for 5 second to allow the workflow to start
	time.Sleep(5 * time.Second)
	for {
		response, err := env.QueryWorkflow(
			shared.CDCFlowStateQuery,
			connectionGen.FlowJobName,
		)
		if err == nil {
			var state peerflow.CDCFlowWorkflowState
			err = response.Get(&state)
			if err != nil {
				slog.Error(err.Error())
			}

			if *state.CurrentFlowState == protos.FlowStatus_STATUS_RUNNING {
				break
			}
		} else {
			// log the error for informational purposes
			slog.Error(err.Error())
		}
		time.Sleep(1 * time.Second)
	}
}

func NormalizeFlowCountQuery(env *testsuite.TestWorkflowEnvironment,
	connectionGen FlowConnectionGenerationConfig,
	minCount int,
) {
	// wait for PeerFlowStatusQuery to finish setup
	// sleep for 5 second to allow the workflow to start
	time.Sleep(5 * time.Second)
	for {
		response, err := env.QueryWorkflow(
			shared.CDCFlowStateQuery,
			connectionGen.FlowJobName,
		)
		if err == nil {
			var state peerflow.CDCFlowWorkflowState
			err = response.Get(&state)
			if err != nil {
				slog.Error(err.Error())
			}

			if len(state.NormalizeFlowStatuses) >= minCount {
				break
			}
		} else {
			// log the error for informational purposes
			slog.Error(err.Error())
		}
		time.Sleep(1 * time.Second)
	}
}

func CreateTableForQRep(pool *pgxpool.Pool, suffix string, tableName string) error {
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
		"address NUMERIC",
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
		"my_date DATE",
	}
	if strings.Contains(tableName, "sf") || strings.Contains(tableName, "bq") {
		tblFields = append(tblFields, `"geometryPoint" geometry(point)`,
			"geography_point geography(point)",
			"geometry_linestring geometry(linestring)",
			"geography_linestring geography(linestring)",
			"geometry_polygon geometry(polygon)",
			"geography_polygon geography(polygon)")
	}
	tblFieldStr := strings.Join(tblFields, ",")

	_, err := pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE e2e_test_%s.%s (
			%s
		);`, suffix, tableName, tblFieldStr))
	if err != nil {
		return err
	}

	return nil
}

func generate20MBJson() ([]byte, error) {
	xn := make(map[string]interface{}, 215000)
	for i := 0; i < 215000; i++ {
		xn[uuid.New().String()] = uuid.New().String()
	}

	v, err := json.Marshal(xn)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func PopulateSourceTable(pool *pgxpool.Pool, suffix string, tableName string, rowCount int) error {
	var ids []string
	var rows []string
	for i := 0; i < rowCount-1; i++ {
		id := uuid.New().String()
		ids = append(ids, id)
		geoValues := ""
		if strings.Contains(tableName, "sf") || strings.Contains(tableName, "bq") {
			geoValues = `,'POINT(1 2)','POINT(40.7128 -74.0060)',
			'LINESTRING(0 0, 1 1, 2 2)',
			'LINESTRING(-74.0060 40.7128, -73.9352 40.7306, -73.9123 40.7831)',
			'POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))','POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'`
		}
		row := fmt.Sprintf(`
					(
							'%s', '%s', CURRENT_TIMESTAMP, 3.86487206688919, CURRENT_TIMESTAMP,
							CURRENT_TIMESTAMP, E'\\\\xDEADBEEF', 'type1', '%s',
							1, 0, 1, 'dealType1',
							'%s', '%s', false, 1.2345,
							1.2345, false, 12345, '%s',
							12345, 1, '%s', CURRENT_TIMESTAMP, 'refID',
							CURRENT_TIMESTAMP, 1, ARRAY['text1', 'text2'], ARRAY[123, 456], ARRAY[789, 012],
							ARRAY['varchar1', 'varchar2'], '{"key": -8.02139037433155}',
							'[{"key1": "value1", "key2": "value2", "key3": "value3"}]',
							'{"key": "value"}', 15, CURRENT_DATE %s
					)`,
			id, uuid.New().String(), uuid.New().String(),
			uuid.New().String(), uuid.New().String(), uuid.New().String(), uuid.New().String(), geoValues)
		rows = append(rows, row)
	}

	geoColumns := ""
	if strings.Contains(tableName, "sf") || strings.Contains(tableName, "bq") {
		geoColumns = `,"geometryPoint", geography_point,` +
			"geometry_linestring, geography_linestring," +
			"geometry_polygon, geography_polygon"
	}
	_, err := pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO e2e_test_%s.%s (
					id, card_id, "from", price, created_at,
					updated_at, transaction_hash, ownerable_type, ownerable_id,
					user_nonce, transfer_type, blockchain, deal_type,
					deal_id, ethereum_transaction_id, ignore_price, card_eth_value,
					paid_eth_price, card_bought_notified, address, account_id,
					asset_id, status, transaction_id, settled_at, reference_id,
					settle_at, settlement_delay_reason, f1, f2, f3, f4, f5, f6, f7, f8, my_date
					%s
			) VALUES %s;
	`, suffix, tableName, geoColumns, strings.Join(rows, ",")))
	if err != nil {
		return err
	}

	// add a row where all the nullable fields are null
	_, err = pool.Exec(context.Background(), fmt.Sprintf(`
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

	// generate a 20 MB json and update id[0]'s col f5 to it
	v, err := generate20MBJson()
	if err != nil {
		return err
	}
	_, err = pool.Exec(context.Background(), fmt.Sprintf(`
		UPDATE e2e_test_%s.%s SET f5 = $1 WHERE id = $2;
	`, suffix, tableName), v, ids[0])
	if err != nil {
		return err
	}

	return nil
}

func CreateQRepWorkflowConfig(
	flowJobName string,
	sourceTable string,
	dstTable string,
	query string,
	dest *protos.Peer,
	stagingPath string,
	setupDst bool,
	syncedAtCol string,
) (*protos.QRepConfig, error) {
	connectionGen := QRepFlowConnectionGenerationConfig{
		FlowJobName:                flowJobName,
		WatermarkTable:             sourceTable,
		DestinationTableIdentifier: dstTable,
		PostgresPort:               PostgresPort,
		Destination:                dest,
		StagingPath:                stagingPath,
	}

	watermark := "updated_at"

	qrepConfig, err := connectionGen.GenerateQRepConfig(query, watermark)
	if err != nil {
		return nil, err
	}
	qrepConfig.InitialCopyOnly = true
	qrepConfig.SyncedAtColName = syncedAtCol
	qrepConfig.SetupWatermarkTableOnDestination = setupDst

	return qrepConfig, nil
}

func RunQrepFlowWorkflow(env *testsuite.TestWorkflowEnvironment, config *protos.QRepConfig) {
	state := peerflow.NewQRepFlowState()
	time.Sleep(5 * time.Second)
	env.ExecuteWorkflow(peerflow.QRepFlowWorkflow, config, state)
}

func RunXminFlowWorkflow(env *testsuite.TestWorkflowEnvironment, config *protos.QRepConfig) {
	state := peerflow.NewQRepFlowState()
	state.LastPartition.PartitionId = uuid.New().String()
	time.Sleep(5 * time.Second)
	env.ExecuteWorkflow(peerflow.XminFlowWorkflow, config, state)
}

func GetOwnersSchema() *model.QRecordSchema {
	return &model.QRecordSchema{
		Fields: []model.QField{
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
			{Name: "my_date", Type: qvalue.QValueKindDate, Nullable: true},
			{Name: "geometryPoint", Type: qvalue.QValueKindGeometry, Nullable: true},
			{Name: "geometry_linestring", Type: qvalue.QValueKindGeometry, Nullable: true},
			{Name: "geometry_polygon", Type: qvalue.QValueKindGeometry, Nullable: true},
			{Name: "geography_point", Type: qvalue.QValueKindGeography, Nullable: true},
			{Name: "geography_linestring", Type: qvalue.QValueKindGeography, Nullable: true},
			{Name: "geography_polygon", Type: qvalue.QValueKindGeography, Nullable: true},
		},
	}
}

func GetOwnersSelectorStringsSF() [2]string {
	schema := GetOwnersSchema()
	pgFields := make([]string, 0, len(schema.Fields))
	sfFields := make([]string, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		pgFields = append(pgFields, fmt.Sprintf(`"%s"`, field.Name))
		if strings.Contains(field.Name, "geo") {
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

func NewTemporalTestWorkflowEnvironment() *testsuite.TestWorkflowEnvironment {
	testSuite := &testsuite.WorkflowTestSuite{}

	logger := slog.New(logger.NewHandler(
		slog.NewJSONHandler(
			os.Stdout,
			&slog.HandlerOptions{Level: slog.LevelWarn},
		)))
	tLogger := NewTStructuredLogger(*logger)

	testSuite.SetLogger(tLogger)
	return testSuite.NewTestWorkflowEnvironment()
}

type TStructuredLogger struct {
	logger *slog.Logger
}

func NewTStructuredLogger(logger slog.Logger) *TStructuredLogger {
	return &TStructuredLogger{logger: &logger}
}

func (l *TStructuredLogger) keyvalsToFields(keyvals []interface{}) slog.Attr {
	var attrs []any
	for i := 0; i < len(keyvals); i += 1 {
		key := fmt.Sprintf("%v", keyvals[i])
		attrs = append(attrs, key)
	}
	return slog.Group("test-log", attrs...)
}

func (l *TStructuredLogger) Debug(msg string, keyvals ...interface{}) {
	l.logger.With(l.keyvalsToFields(keyvals)).Debug(msg)
}

func (l *TStructuredLogger) Info(msg string, keyvals ...interface{}) {
	l.logger.With(l.keyvalsToFields(keyvals)).Info(msg)
}

func (l *TStructuredLogger) Warn(msg string, keyvals ...interface{}) {
	l.logger.With(l.keyvalsToFields(keyvals)).Warn(msg)
}

func (l *TStructuredLogger) Error(msg string, keyvals ...interface{}) {
	l.logger.With(l.keyvalsToFields(keyvals)).Error(msg)
}

func CompareTableSchemas(x *protos.TableSchema, y *protos.TableSchema) bool {
	return x.TableIdentifier == y.TableIdentifier ||
		x.IsReplicaIdentityFull == y.IsReplicaIdentityFull ||
		slices.Compare(x.PrimaryKeyColumns, y.PrimaryKeyColumns) == 0 ||
		slices.Compare(x.ColumnNames, y.ColumnNames) == 0 ||
		slices.Compare(x.ColumnTypes, y.ColumnTypes) == 0
}

func RequireEqualRecordBatches(t *testing.T, q *model.QRecordBatch, other *model.QRecordBatch) {
	t.Helper()
	require.True(t, e2eshared.CheckEqualRecordBatches(t, q, other))
}

// See EnvNoError
func EnvEqualRecordBatches(t *testing.T, env *testsuite.TestWorkflowEnvironment, q *model.QRecordBatch, other *model.QRecordBatch) {
	t.Helper()

	if !e2eshared.CheckEqualRecordBatches(t, q, other) {
		env.CancelWorkflow()
		runtime.Goexit()
	}
}

func EnvWaitFor(t *testing.T, env *testsuite.TestWorkflowEnvironment, timeout time.Duration, reason string, f func(ctx context.Context) bool) {
	t.Helper()

	ctx, cleanup := context.WithTimeout(context.Background(), timeout)
	defer cleanup()
	deadline, _ := ctx.Deadline()
	for !f(ctx) {
		t.Log(time.Now(), deadline)
		if time.Now().Compare(deadline) >= 0 {
			t.Error("WaitFor timed out", reason)
			env.CancelWorkflow()
			runtime.Goexit()
		}
		time.Sleep(time.Second)
	}
}
