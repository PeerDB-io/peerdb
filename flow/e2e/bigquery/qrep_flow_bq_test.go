package e2e_bigquery

import (
	"context"
	"fmt"
	"sort"
	"strings"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/stretchr/testify/require"
)

func (s *PeerFlowE2ETestSuiteBQ) setupSourceTable(tableName string, rowCount int) {
	err := e2e.CreateSourceTableQRep(s.pool, bigquerySuffix, tableName)
	s.NoError(err)
	err = e2e.PopulateSourceTable(s.pool, bigquerySuffix, tableName, rowCount)
	s.NoError(err)
}

func (s *PeerFlowE2ETestSuiteBQ) setupBQDestinationTable(dstTable string) {
	schema := e2e.GetOwnersSchema()
	err := s.bqHelper.CreateTable(dstTable, schema)

	// fail if table creation fails
	require.NoError(s.T(), err)

	fmt.Printf("created table on bigquery: %s.%s. %v\n", s.bqHelper.Config.DatasetId, dstTable, err)
}

func (s *PeerFlowE2ETestSuiteBQ) compareTableSchemasBQ(tableName string) {
	// read rows from source table
	pgQueryExecutor := connpostgres.NewQRepQueryExecutor(s.pool, context.Background(), "testflow", "testpart")
	pgQueryExecutor.SetTestEnv(true)

	pgRows, err := pgQueryExecutor.ExecuteAndProcessQuery(
		fmt.Sprintf("SELECT * FROM e2e_test_%s.%s ORDER BY id", bigquerySuffix, tableName),
	)
	s.NoError(err)
	sort.Slice(pgRows.Schema.Fields, func(i int, j int) bool {
		return strings.Compare(pgRows.Schema.Fields[i].Name, pgRows.Schema.Fields[j].Name) == -1
	})

	// read rows from destination table
	qualifiedTableName := fmt.Sprintf("`%s.%s`", s.bqHelper.Config.DatasetId, tableName)
	bqRows, err := s.bqHelper.ExecuteAndProcessQuery(
		fmt.Sprintf("SELECT * FROM %s ORDER BY id", qualifiedTableName),
	)
	s.NoError(err)
	sort.Slice(bqRows.Schema.Fields, func(i int, j int) bool {
		return strings.Compare(bqRows.Schema.Fields[i].Name, bqRows.Schema.Fields[j].Name) == -1
	})

	s.True(pgRows.Schema.EqualNames(bqRows.Schema), "schemas from source and destination tables are not equal")
}

func (s *PeerFlowE2ETestSuiteBQ) compareTableContentsBQ(tableName string, colsString string) {
	// read rows from source table
	pgQueryExecutor := connpostgres.NewQRepQueryExecutor(s.pool, context.Background(), "testflow", "testpart")
	pgQueryExecutor.SetTestEnv(true)

	pgRows, err := pgQueryExecutor.ExecuteAndProcessQuery(
		fmt.Sprintf("SELECT %s FROM e2e_test_%s.%s ORDER BY id", colsString, bigquerySuffix, tableName),
	)
	s.NoError(err)

	// read rows from destination table
	qualifiedTableName := fmt.Sprintf("`%s.%s`", s.bqHelper.Config.DatasetId, tableName)
	bqRows, err := s.bqHelper.ExecuteAndProcessQuery(
		fmt.Sprintf("SELECT %s FROM %s ORDER BY id", colsString, qualifiedTableName),
	)
	s.NoError(err)

	s.True(pgRows.Equals(bqRows), "rows from source and destination tables are not equal")
}

func (s *PeerFlowE2ETestSuiteBQ) Test_Complete_QRep_Flow_Avro() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	numRows := 10

	tblName := "test_qrep_flow_avro"
	s.setupSourceTable(tblName, numRows)
	s.setupBQDestinationTable(tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		bigquerySuffix, tblName)

	qrepConfig, err := e2e.CreateQRepWorkflowConfig("test_qrep_flow_avro",
		fmt.Sprintf("e2e_test_%s.%s", bigquerySuffix, tblName),
		tblName,
		query,
		protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO,
		s.bqHelper.Peer,
		"peerdb_staging")
	s.NoError(err)
	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())

	// assert that error contains "invalid connection configs"
	err = env.GetWorkflowError()
	s.NoError(err)

	s.compareTableContentsBQ(tblName, "*")

	env.AssertExpectations(s.T())
}

// NOTE: Disabled due to large JSON tests being added: https://github.com/PeerDB-io/peerdb/issues/309

// Test_Complete_QRep_Flow tests a complete flow with data in the source table.
// The test inserts 10 rows into the source table and verifies that the data is
// // correctly synced to the destination table this runs a QRep Flow.
// func (s *E2EPeerFlowTestSuite) Test_Complete_QRep_Flow_Multi_Insert() {
// 	env := s.NewTestWorkflowEnvironment()
// 	registerWorkflowsAndActivities(env)

// 	numRows := 10

// 	tblName := "test_qrep_flow_multi_insert"
// 	s.setupSourceTable(tblName, numRows)
// 	s.setupBQDestinationTable(tblName)

// 	query := fmt.Sprintf("SELECT * FROM e2e_test.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}", tblName)

// 	qrepConfig := s.createQRepWorkflowConfig("test_qrep_flow_mi",
// 		"e2e_test."+tblName,
// 		tblName,
// 		query,
// 		protos.QRepSyncMode_QREP_SYNC_MODE_MULTI_INSERT,
// 		s.bqHelper.Peer)
// 	runQrepFlowWorkflow(env, qrepConfig)

// 	// Verify workflow completes without error
// 	s.True(env.IsWorkflowCompleted())

// 	// assert that error contains "invalid connection configs"
// 	err := env.GetWorkflowError()
// 	s.NoError(err)

// 	count, err := s.bqHelper.CountRows(tblName)
// 	s.NoError(err)

// 	s.Equal(numRows, count)

// 	env.AssertExpectations(s.T())
// }
