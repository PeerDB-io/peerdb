package e2e

import (
	"context"
	"fmt"
	"os"
	"time"

	util "github.com/PeerDB-io/peer-flow/utils"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
	"github.com/stretchr/testify/require"
)

func (s *E2EPeerFlowTestSuite) setupEventHub() error {
	enableEHT := os.Getenv("ENABLE_EVENT_HUB_TESTS")
	if enableEHT == "" {
		return nil
	}

	pgConf := GetTestPostgresConf()
	helper, err := NewEventHubTestHelper(pgConf)
	if err != nil {
		return err
	}

	s.ehHelper = helper
	return nil
}

func (s *E2EPeerFlowTestSuite) Test_Complete_Simple_Flow_EH() {
	if s.ehHelper == nil {
		s.T().Skip("Skipping EventHub test")
	}

	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	ru, err := util.RandomUInt64()
	s.NoError(err)

	jobName := fmt.Sprintf("test_complete_single_col_flow_eh_%d", ru)
	schemaQualifiedName := fmt.Sprintf("e2e_test.%s", jobName)
	_, err = s.pool.Exec(context.Background(), `
		CREATE TABLE `+schemaQualifiedName+` (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`)
	s.NoError(err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      jobName,
		TableNameMapping: map[string]string{schemaQualifiedName: jobName},
		PostgresPort:     postgresPort,
		Destination:      s.ehHelper.GetPeer(),
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	peerFlowInput := peerflow.PeerFlowLimits{
		TotalSyncFlows: 2,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert 10 rows into the source table
	go func() {
		s.SetupPeerFlowStatusQuery(env, connectionGen)
		// insert 10 rows into the source table
		for i := 0; i < 10; i++ {
			testKey := fmt.Sprintf("test_key_%d", i)
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.pool.Exec(context.Background(), `
			INSERT INTO `+schemaQualifiedName+` (key, value) VALUES ($1, $2)
		`, testKey, testValue)
			s.NoError(err)
		}
		fmt.Println("Inserted 10 rows into the source table")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflowWithConfig, flowConnConfig, &peerFlowInput, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	// Verify that the destination table has 10 rows
	// make context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msgs, err := s.ehHelper.ConsumeAllMessages(ctx, jobName, 10)

	require.NoError(s.T(), err)

	require.Equal(s.T(), 10, len(msgs))

	env.AssertExpectations(s.T())
}
