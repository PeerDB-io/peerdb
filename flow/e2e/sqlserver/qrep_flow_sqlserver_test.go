package e2e_sqlserver

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
)

const sqlserverSuffix = "sqlserver"

type PeerFlowE2ETestSuiteSQLServer struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	pool       *pgxpool.Pool
	sqlsHelper *SQLServerHelper
}

func TestCDCFlowE2ETestSuiteSQLServer(t *testing.T) {
	suite.Run(t, new(PeerFlowE2ETestSuiteSQLServer))
}

// setup sql server connection
func (s *PeerFlowE2ETestSuiteSQLServer) setupSQLServer() {
	env := os.Getenv("ENABLE_SQLSERVER_TESTS")
	if env != "true" {
		s.sqlsHelper = nil
		return
	}

	sqlsHelper, err := NewSQLServerHelper("test_sqlserver_peer")
	require.NoError(s.T(), err)
	s.sqlsHelper = sqlsHelper
}

func (s *PeerFlowE2ETestSuiteSQLServer) SetupSuite() {
	err := godotenv.Load()
	if err != nil {
		// it's okay if the .env file is not present
		// we will use the default values
		log.Infof("Unable to load .env file, using default values from env")
	}

	log.SetReportCaller(true)

	pool, err := e2e.SetupPostgres(sqlserverSuffix)
	if err != nil {
		s.Fail("failed to setup postgres", err)
	}
	s.pool = pool

	s.setupSQLServer()
}

// Implement TearDownAllSuite interface to tear down the test suite
func (s *PeerFlowE2ETestSuiteSQLServer) TearDownSuite() {
	err := e2e.TearDownPostgres(s.pool, sqlserverSuffix)
	if err != nil {
		s.Fail("failed to drop Postgres schema", err)
	}

	if s.sqlsHelper != nil {
		err = s.sqlsHelper.CleanUp()
		if err != nil {
			s.Fail("failed to clean up sqlserver", err)
		}
	}
}

func (s *PeerFlowE2ETestSuiteSQLServer) setupSQLServerTable(tableName string) {
	schema := getSimpleTableSchema()
	err := s.sqlsHelper.CreateTable(schema, tableName)
	require.NoError(s.T(), err)
}

func (s *PeerFlowE2ETestSuiteSQLServer) insertRowsIntoSQLServerTable(tableName string, numRows int) {
	schemaQualified := fmt.Sprintf("%s.%s", s.sqlsHelper.SchemaName, tableName)
	for i := 0; i < numRows; i++ {
		params := make(map[string]interface{})
		params["id"] = "test_id_" + fmt.Sprintf("%d", i)
		params["card_id"] = "test_card_id_" + fmt.Sprintf("%d", i)
		params["v_from"] = time.Now()
		params["price"] = 100.00
		params["status"] = 1

		_, err := s.sqlsHelper.E.NamedExec(
			//nolint:lll
			"INSERT INTO "+schemaQualified+" (id, card_id, v_from, price, status) VALUES (:id, :card_id, :v_from, :price, :status)",
			params,
		)

		require.NoError(s.T(), err)
	}
}

func (s *PeerFlowE2ETestSuiteSQLServer) setupPGDestinationTable(tableName string) {
	ctx := context.Background()

	_, err := s.pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS e2e_test_%s.%s", sqlserverSuffix, tableName))
	require.NoError(s.T(), err)

	_, err = s.pool.Exec(ctx,
		fmt.Sprintf("CREATE TABLE e2e_test_%s.%s (id TEXT, card_id TEXT, v_from TIMESTAMP, price NUMERIC, status INT)",
			sqlserverSuffix, tableName))
	require.NoError(s.T(), err)
}

func getSimpleTableSchema() *model.QRecordSchema {
	return &model.QRecordSchema{
		Fields: []*model.QField{
			{Name: "id", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "card_id", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "v_from", Type: qvalue.QValueKindTimestamp, Nullable: true},
			{Name: "price", Type: qvalue.QValueKindNumeric, Nullable: true},
			{Name: "status", Type: qvalue.QValueKindInt64, Nullable: true},
		},
	}
}

func (s *PeerFlowE2ETestSuiteSQLServer) Test_Complete_QRep_Flow_SqlServer_Append() {
	if s.sqlsHelper == nil {
		s.T().Skip("Skipping SQL Server test")
	}

	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	numRows := 10
	tblName := "test_qrep_flow_avro_ss_append"
	srcTableName := fmt.Sprintf("%s.%s", s.sqlsHelper.SchemaName, tblName)

	s.setupSQLServerTable(tblName)
	s.insertRowsIntoSQLServerTable(tblName, numRows)

	s.setupPGDestinationTable(tblName)
	dstTableName := fmt.Sprintf("e2e_test_%s.%s", sqlserverSuffix, tblName)

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
		SyncMode:                   protos.QRepSyncMode_QREP_SYNC_MODE_MULTI_INSERT,
		MaxParallelWorkers:         1,
		WaitBetweenBatchesSeconds:  5,
	}

	e2e.RunQrepFlowWorkflow(s.WorkflowTestSuite, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())

	err := env.GetWorkflowError()
	s.NoError(err)

	// Verify that the destination table has the same number of rows as the source table
	var numRowsInDest int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTableName)
	err = s.pool.QueryRow(context.Background(), countQuery).Scan(&numRowsInDest)
	s.NoError(err)

	s.Equal(numRows, numRowsInDest)
}
