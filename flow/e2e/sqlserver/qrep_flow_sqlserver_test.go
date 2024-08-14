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
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
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

func (s PeerFlowE2ETestSuiteSQLServer) Peer() *protos.Peer {
	s.t.Helper()
	ret := &protos.Peer{
		Name: e2e.AddSuffix(s, "sqlspeer"),
		Type: protos.DBType_SQLSERVER,
		Config: &protos.Peer_SqlserverConfig{
			SqlserverConfig: s.sqlsHelper.config,
		},
	}
	e2e.CreatePeer(s.t, ret)
	return ret
}

func TestCDCFlowE2ETestSuiteSQLServer(t *testing.T) {
	t.Skip()
	e2eshared.RunSuite(t, SetupSuite)
}

func (s PeerFlowE2ETestSuiteSQLServer) Teardown() {
	e2e.TearDownPostgres(s)

	if s.sqlsHelper != nil {
		err := s.sqlsHelper.CleanUp()
		require.NoError(s.t, err)
	}
}

func SetupSuite(t *testing.T) PeerFlowE2ETestSuiteSQLServer {
	t.Helper()

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
		sqlsHelper, err = NewSQLServerHelper()
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
	for i := range numRows {
		params := map[string]interface{}{
			"id":      "test_id_" + strconv.Itoa(i),
			"card_id": "test_card_id_" + strconv.Itoa(i),
			"v_from":  time.Now(),
			"price":   100.00,
			"status":  1,
		}

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

func getSimpleTableSchema() *qvalue.QRecordSchema {
	return &qvalue.QRecordSchema{
		Fields: []qvalue.QField{
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

	tc := e2e.NewTemporalClient(s.t)

	numRows := 10
	tblName := "test_qrep_flow_avro_ss_append"
	srcTableName := fmt.Sprintf("%s.%s", s.sqlsHelper.SchemaName, tblName)

	s.setupSQLServerTable(tblName)
	s.insertRowsIntoSQLServerTable(tblName, numRows)

	s.setupPGDestinationTable(tblName)
	dstTableName := fmt.Sprintf("e2e_test_%s.%s", s.suffix, tblName)

	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE v_from BETWEEN {{.start}} AND {{.end}}",
		s.sqlsHelper.SchemaName, tblName)

	qrepConfig := &protos.QRepConfig{
		FlowJobName:                tblName,
		SourceName:                 s.Peer().Name,
		DestinationName:            e2e.GeneratePostgresPeer(s.t).Name,
		DestinationTableIdentifier: dstTableName,
		Query:                      query,
		WatermarkTable:             srcTableName,
		WatermarkColumn:            "v_from",
		NumRowsPerPartition:        5,
		InitialCopyOnly:            true,
		MaxParallelWorkers:         1,
		WaitBetweenBatchesSeconds:  5,
	}

	env := e2e.RunQRepFlowWorkflow(tc, qrepConfig)
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)
	require.NoError(s.t, env.Error())

	// Verify that the destination table has the same number of rows as the source table
	var numRowsInDest pgtype.Int8
	countQuery := "SELECT COUNT(*) FROM " + dstTableName
	err := s.Conn().QueryRow(context.Background(), countQuery).Scan(&numRowsInDest)
	require.NoError(s.t, err)

	require.Equal(s.t, numRows, int(numRowsInDest.Int64))
}
