package e2e

import (
	"context"
	"fmt"

	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

// Test_Complete_QRep_Flow tests a complete flow with data in the source table.
// The test inserts 10 rows into the source table and verifies that the data is
// correctly synced to the destination table this runs a QRep Flow.
func (s *E2EPeerFlowTestSuite) Test_Complete_QRep_Flow() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	_, err := s.pool.Exec(context.Background(), `
		CREATE TABLE e2e_test.test_qrep_flow (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`)
	s.NoError(err)

	// insert 100 rows into the source table
	rowCount := 10
	for i := 0; i < rowCount; i++ {
		_, err := s.pool.Exec(context.Background(), `
			INSERT INTO e2e_test.test_qrep_flow (key, value)
			VALUES ('key', 'value');
		`)
		s.NoError(err)
	}

	dstTableName := fmt.Sprintf("%s.%s", s.bqHelper.Config.DatasetId, "test_qrep_flow")
	dstTableCmd := fmt.Sprintf("CREATE TABLE %s (id INT, key STRING, value STRING)", dstTableName)
	err = s.bqHelper.RunCommand(dstTableCmd)
	s.NoError(err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:                "test_complete_qrep_flow",
		SourceTableIdentifier:      "e2e_test.test_qrep_flow",
		DestinationTableIdentifier: "test_qrep_flow",
		PostgresPort:               postgresPort,
		BigQueryConfig:             s.bqHelper.Config,
	}

	query := "SELECT * FROM e2e_test.test_qrep_flow"
	watermark := "id"

	qrepConfig, err := connectionGen.GenerateQRepConfig(query, watermark)
	s.NoError(err)

	qrepConfig.InitalCopyOnly = true
	env.ExecuteWorkflow(peerflow.QRepFlowWorkflow, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// assert that error contains "invalid connection configs"
	s.NoError(err)

	// verify that the data is correctly synced to the destination table
	// on the bigquery side by querying the number of rows in the table on the
	// bigquery side and comparing it to the number of rows in the source table (100)
	count, err := s.bqHelper.CountRows("test_qrep_flow")
	s.NoError(err)

	s.Equal(rowCount, count)

	env.AssertExpectations(s.T())
}
