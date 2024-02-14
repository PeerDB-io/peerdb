package e2e_sqlserver

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
)

type PeerFlowE2ETestSuiteSQLServer struct {
	t *testing.T

	conn       *connpostgres.PostgresConnector
	sqlsHelper *SQLServerHelper
	suffix     string
}

func (s PeerFlowE2ETestSuiteSQLServer) T() *testing.T {
	return s.t
}

func (s PeerFlowE2ETestSuiteSQLServer) Conn() *pgx.Conn {
	return s.conn.Conn()
}

func (s PeerFlowE2ETestSuiteSQLServer) Connector() *connpostgres.PostgresConnector {
	return s.conn
}

func (s PeerFlowE2ETestSuiteSQLServer) Suffix() string {
	return s.suffix
}

func TestCDCFlowE2ETestSuiteSQLServer(t *testing.T) {
	e2eshared.RunSuite(t, SetupSuite, func(s PeerFlowE2ETestSuiteSQLServer) {
		e2e.TearDownPostgres(s)

		if s.sqlsHelper != nil {
			err := s.sqlsHelper.CleanUp()
			require.NoError(s.t, err)
		}
	})
}

func SetupSuite(t *testing.T) PeerFlowE2ETestSuiteSQLServer {
	t.Helper()

	err := godotenv.Load()
	if err != nil {
		// it's okay if the .env file is not present
		// we will use the default values
		t.Log("Unable to load .env file, using default values from env")
	}

	suffix := "sqls_" + strings.ToLower(shared.RandomString(8))
	conn, err := e2e.SetupPostgres(t, suffix)
	if err != nil {
		require.NoError(t, err)
	}

	var sqlsHelper *SQLServerHelper
	env := os.Getenv("ENABLE_SQLSERVER_TESTS")
	if env != "true" {
		sqlsHelper = nil
	} else {
		sqlsHelper, err = NewSQLServerHelper("test_sqlserver_peer")
		require.NoError(t, err)
	}

	return PeerFlowE2ETestSuiteSQLServer{
		t:          t,
		conn:       conn,
		sqlsHelper: sqlsHelper,
		suffix:     suffix,
	}
}

func (s PeerFlowE2ETestSuiteSQLServer) setupSQLServerTable(tableName string) {
	schema := getSimpleTableSchema()
	err := s.sqlsHelper.CreateTable(schema, tableName)
	require.NoError(s.t, err)
}

func (s PeerFlowE2ETestSuiteSQLServer) insertRowsIntoSQLServerTable(tableName string, numRows int) {
	schemaQualified := fmt.Sprintf("%s.%s", s.sqlsHelper.SchemaName, tableName)
	for i := 0; i < numRows; i++ {
		params := make(map[string]interface{})
		params["id"] = "test_id_" + strconv.Itoa(i)
		params["card_id"] = "test_card_id_" + strconv.Itoa(i)
		params["v_from"] = time.Now()
		params["price"] = 100.00
		params["status"] = 1

		_, err := s.sqlsHelper.E.NamedExec(
			context.Background(),
			"INSERT INTO "+schemaQualified+" (id, card_id, v_from, price, status) VALUES (:id, :card_id, :v_from, :price, :status)",
			params,
		)

		require.NoError(s.t, err)
	}
}

func (s PeerFlowE2ETestSuiteSQLServer) setupPGDestinationTable(tableName string) {
	ctx := context.Background()

	_, err := s.Conn().Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS e2e_test_%s.%s", s.suffix, tableName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(ctx,
		fmt.Sprintf("CREATE TABLE e2e_test_%s.%s (id TEXT, card_id TEXT, v_from TIMESTAMP, price NUMERIC, status INT)",
			s.suffix, tableName))
	require.NoError(s.t, err)
}

func getSimpleTableSchema() *model.QRecordSchema {
	return &model.QRecordSchema{
		Fields: []model.QField{
			{Name: "id", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "card_id", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "v_from", Type: qvalue.QValueKindTimestamp, Nullable: true},
			{Name: "price", Type: qvalue.QValueKindNumeric, Nullable: true},
			{Name: "status", Type: qvalue.QValueKindInt64, Nullable: true},
		},
	}
}

func (s PeerFlowE2ETestSuiteSQLServer) Test_Complete_QRep_Flow_SqlServer_Append() {
	if s.sqlsHelper == nil {
		s.t.Skip("Skipping SQL Server test")
	}

	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	numRows := 10
	tblName := "test_qrep_flow_avro_ss_append"
	srcTableName := fmt.Sprintf("%s.%s", s.sqlsHelper.SchemaName, tblName)

	s.setupSQLServerTable(tblName)
	s.insertRowsIntoSQLServerTable(tblName, numRows)

	s.setupPGDestinationTable(tblName)
	dstTableName := fmt.Sprintf("e2e_test_%s.%s", s.suffix, tblName)

	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE v_from BETWEEN {{.start}} AND {{.end}}",
		s.sqlsHelper.SchemaName, tblName)

	postgresPeer := e2e.GeneratePostgresPeer(e2e.PostgresPort)

	qrepConfig := &protos.QRepConfig{
		FlowJobName:                tblName,
		SourcePeer:                 s.sqlsHelper.GetPeer(),
		DestinationPeer:            postgresPeer,
		DestinationTableIdentifier: dstTableName,
		Query:                      query,
		WatermarkTable:             srcTableName,
		WatermarkColumn:            "v_from",
		NumRowsPerPartition:        5,
		InitialCopyOnly:            true,
		MaxParallelWorkers:         1,
		WaitBetweenBatchesSeconds:  5,
	}

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())

	err := env.GetWorkflowError()
	require.NoError(s.t, err)

	// Verify that the destination table has the same number of rows as the source table
	var numRowsInDest pgtype.Int8
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTableName)
	err = s.Conn().QueryRow(context.Background(), countQuery).Scan(&numRowsInDest)
	require.NoError(s.t, err)

	require.Equal(s.t, numRows, int(numRowsInDest.Int64))
}
