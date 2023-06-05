package e2e

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

func (s *E2EPeerFlowTestSuite) setupSourceTable(tableName string, rowCount int) {
	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE e2e_test.%s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`, tableName))
	s.NoError(err)

	// insert rows into the source table
	for i := 0; i < rowCount; i++ {
		_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO e2e_test.%s (key, value)
			VALUES ('key', 'value');
		`, tableName))
		s.NoError(err)
	}
}

func (s *E2EPeerFlowTestSuite) setupDestinationTable(dstTable string) {
	dstTableName := fmt.Sprintf("%s.%s", s.bqHelper.Config.DatasetId, dstTable)
	dstTableCmd := fmt.Sprintf("CREATE TABLE %s (id INT, key STRING, value STRING)", dstTableName)
	err := s.bqHelper.RunCommand(dstTableCmd)
	s.NoError(err)
}

func (s *E2EPeerFlowTestSuite) createWorkflowConfig(
	flowJobName string,
	sourceTable string,
	dstTable string,
	query string,
	syncMode protos.QRepSyncMode,
) *protos.QRepConfig {
	connectionGen := QRepFlowConnectionGenerationConfig{
		FlowJobName:                flowJobName,
		SourceTableIdentifier:      sourceTable,
		DestinationTableIdentifier: dstTable,
		PostgresPort:               postgresPort,
		BigQueryConfig:             s.bqHelper.Config,
	}

	watermark := "id"

	qrepConfig, err := connectionGen.GenerateQRepConfig(query, watermark, syncMode)
	s.NoError(err)

	qrepConfig.InitalCopyOnly = true

	return qrepConfig
}

// Test_Complete_QRep_Flow tests a complete flow with data in the source table.
// The test inserts 10 rows into the source table and verifies that the data is
// correctly synced to the destination table this runs a QRep Flow.
func (s *E2EPeerFlowTestSuite) Test_Complete_QRep_Flow_Multi_Insert() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	numRows := 10

	tblName := "test_qrep_flow_multi_insert"
	s.setupSourceTable(tblName, numRows)
	s.setupDestinationTable(tblName)

	qrepConfig := s.createWorkflowConfig("test_qrep_flow_mi",
		"e2e_test."+tblName,
		tblName,
		"SELECT * FROM e2e_test."+tblName,
		protos.QRepSyncMode_QREP_SYNC_MODE_MULTI_INSERT)
	env.ExecuteWorkflow(peerflow.QRepFlowWorkflow, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())

	// assert that error contains "invalid connection configs"
	err := env.GetWorkflowError()
	s.NoError(err)

	count, err := s.bqHelper.CountRows(tblName)
	s.NoError(err)

	s.Equal(numRows, count)

	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) Test_Complete_QRep_Flow_Avro() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	numRows := 10

	tblName := "test_qrep_flow_avro"
	s.setupSourceTable(tblName, numRows)
	s.setupDestinationTable(tblName)

	qrepConfig := s.createWorkflowConfig("test_qrep_flow_avro",
		"e2e_test."+tblName,
		tblName,
		"SELECT * FROM e2e_test."+tblName,
		protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO)
	env.ExecuteWorkflow(peerflow.QRepFlowWorkflow, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())

	// assert that error contains "invalid connection configs"
	err := env.GetWorkflowError()
	s.NoError(err)

	count, err := s.bqHelper.CountRows(tblName)
	s.NoError(err)

	s.Equal(numRows, count)

	env.AssertExpectations(s.T())
}
