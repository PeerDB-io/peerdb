package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/stretchr/testify/require"
)

func (s *E2EPeerFlowTestSuite) setupSQLServerTable(tableName string) {
	schema := getSimpleTableSchema()
	err := s.sqlsHelper.CreateTable(schema, tableName)
	require.NoError(s.T(), err)
}

func (s *E2EPeerFlowTestSuite) insertRowsIntoSQLServerTable(tableName string, numRows int) {
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

func (s *E2EPeerFlowTestSuite) setupPGDestinationTable(schemaName, tableName string) {
	ctx := context.Background()
	_, err := s.pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schemaName))
	require.NoError(s.T(), err)

	_, err = s.pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", schemaName, tableName))
	require.NoError(s.T(), err)

	//nolint:lll
	_, err = s.pool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s.%s (id TEXT, card_id TEXT, v_from TIMESTAMP, price NUMERIC, status INT)", schemaName, tableName))
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

func (s *E2EPeerFlowTestSuite) Test_Complete_QRep_Flow_SqlServer_Append() {
	if s.sqlsHelper == nil {
		s.T().Skip("Skipping SQL Server test")
	}

	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	numRows := 10
	tblName := "test_qrep_flow_avro_ss_append"
	srcTableName := fmt.Sprintf("%s.%s", s.sqlsHelper.SchemaName, tblName)

	s.setupSQLServerTable(tblName)
	s.insertRowsIntoSQLServerTable(tblName, numRows)

	s.setupPGDestinationTable(s.sqlsHelper.SchemaName, tblName)
	dstTableName := fmt.Sprintf("%s.%s", s.sqlsHelper.SchemaName, tblName)

	//nolint:lll
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE v_from BETWEEN {{.start}} AND {{.end}}", s.sqlsHelper.SchemaName, tblName)

	postgresPeer := GeneratePostgresPeer(postgresPort)

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

	runQrepFlowWorkflow(env, qrepConfig)

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
