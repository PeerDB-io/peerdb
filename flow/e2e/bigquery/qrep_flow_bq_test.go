package e2e_bigquery

import (
	"context"
	"fmt"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/stretchr/testify/require"
)

func (s PeerFlowE2ETestSuiteBQ) setupSourceTable(tableName string, rowCount int) {
	err := e2e.CreateTableForQRep(s.pool, s.bqSuffix, tableName)
	require.NoError(s.t, err)
	err = e2e.PopulateSourceTable(s.pool, s.bqSuffix, tableName, rowCount)
	require.NoError(s.t, err)
}

func (s PeerFlowE2ETestSuiteBQ) setupBQDestinationTable(dstTable string) {
	schema := e2e.GetOwnersSchema()
	err := s.bqHelper.CreateTable(dstTable, schema)

	// fail if table creation fails
	require.NoError(s.t, err)

	s.t.Logf("created table on bigquery: %s.%s. %v", s.bqHelper.Config.DatasetId, dstTable, err)
}

func (s PeerFlowE2ETestSuiteBQ) compareTableContentsBQ(tableName string, colsString string) {
	// read rows from source table
	pgQueryExecutor := connpostgres.NewQRepQueryExecutor(s.pool, context.Background(), "testflow", "testpart")
	pgQueryExecutor.SetTestEnv(true)

	pgRows, err := pgQueryExecutor.ExecuteAndProcessQuery(
		fmt.Sprintf("SELECT %s FROM e2e_test_%s.%s ORDER BY id", colsString, s.bqSuffix, tableName),
	)
	require.NoError(s.t, err)

	// read rows from destination table
	qualifiedTableName := fmt.Sprintf("`%s.%s`", s.bqHelper.Config.DatasetId, tableName)
	bqSelQuery := fmt.Sprintf("SELECT %s FROM %s ORDER BY id", colsString, qualifiedTableName)
	s.t.Logf("running query on bigquery: %s", bqSelQuery)
	bqRows, err := s.bqHelper.ExecuteAndProcessQuery(bqSelQuery)
	require.NoError(s.t, err)

	e2e.RequireEqualRecordBatchs(s.t, pgRows, bqRows)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Complete_QRep_Flow_Avro() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	numRows := 10

	tblName := "test_qrep_flow_avro"
	s.setupSourceTable(tblName, numRows)
	s.setupBQDestinationTable(tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.bqSuffix, tblName)

	qrepConfig, err := e2e.CreateQRepWorkflowConfig("test_qrep_flow_avro",
		fmt.Sprintf("e2e_test_%s.%s", s.bqSuffix, tblName),
		tblName,
		query,
		s.bqHelper.Peer,
		"",
		false,
		"")
	require.NoError(s.t, err)
	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())

	err = env.GetWorkflowError()
	require.NoError(s.t, err)

	s.compareTableContentsBQ(tblName, "*")

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteBQ) Test_PeerDB_Columns_QRep_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	numRows := 10

	tblName := "test_columns_bq_qrep"
	s.setupSourceTable(tblName, numRows)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.bqSuffix, tblName)

	qrepConfig, err := e2e.CreateQRepWorkflowConfig("test_qrep_flow_avro",
		fmt.Sprintf("e2e_test_%s.%s", s.bqSuffix, tblName),
		tblName,
		query,
		s.bqHelper.Peer,
		"",
		true,
		"_PEERDB_SYNCED_AT")
	require.NoError(s.t, err)
	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())

	err = env.GetWorkflowError()
	require.NoError(s.t, err)

	err = s.checkPeerdbColumns(tblName, false)
	require.NoError(s.t, err)

	env.AssertExpectations(s.t)
}
