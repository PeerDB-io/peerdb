package e2e_s3

import (
	"context"
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/e2e"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
	"github.com/stretchr/testify/require"
)

func (s *PeerFlowE2ETestSuiteS3) attachSchemaSuffix(tableName string) string {
	return fmt.Sprintf("e2e_test_%s.%s", s3Suffix, tableName)
}

func (s *PeerFlowE2ETestSuiteS3) attachSuffix(input string) string {
	return fmt.Sprintf("%s_%s", input, s3Suffix)
}

func (s *PeerFlowE2ETestSuiteS3) Test_Complete_Simple_Flow_S3() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_simple_flow_s3")
	dstTableName := fmt.Sprintf("%s.%s", "peerdb_test_s3", "test_simple_flow_s3")
	flowJobName := s.attachSuffix("test_simple_flow")
	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`, srcTableName))
	s.NoError(err)
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      flowJobName,
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.s3Helper.GetPeer(),
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows:   4,
		ExitAfterRecords: 20,
		MaxBatchSize:     5,
	}

	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		s.NoError(err)
		//insert 20 rows
		for i := 1; i <= 20; i++ {
			testKey := fmt.Sprintf("test_key_%d", i)
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s (key, value) VALUES ($1, $2)
		`, srcTableName), testKey, testValue)
			s.NoError(err)
		}
		s.NoError(err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fmt.Println("JobName: ", flowJobName)
	files, err := s.s3Helper.ListAllFiles(ctx, flowJobName)
	fmt.Println("Files in Test_Complete_Simple_Flow_S3: ", len(files))
	require.NoError(s.T(), err)

	require.Equal(s.T(), 4, len(files))

	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteS3) Test_Complete_Simple_Flow_GCS_Interop() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)
	setupErr := s.setupS3("gcs")
	if setupErr != nil {
		s.Fail("failed to setup S3", setupErr)
	}

	srcTableName := s.attachSchemaSuffix("test_simple_flow_gcs_interop")
	dstTableName := fmt.Sprintf("%s.%s", "peerdb_test_gcs_interop", "test_simple_flow_gcs_interop")
	flowJobName := s.attachSuffix("test_simple_flow")
	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`, srcTableName))
	s.NoError(err)
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      flowJobName,
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.s3Helper.GetPeer(),
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows:   4,
		ExitAfterRecords: 20,
		MaxBatchSize:     5,
	}

	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		s.NoError(err)
		//insert 20 rows
		for i := 1; i <= 20; i++ {
			testKey := fmt.Sprintf("test_key_%d", i)
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s (key, value) VALUES ($1, $2)
		`, srcTableName), testKey, testValue)
			s.NoError(err)
		}
		s.NoError(err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fmt.Println("JobName: ", flowJobName)
	files, err := s.s3Helper.ListAllFiles(ctx, flowJobName)
	fmt.Println("Files in Test_Complete_Simple_Flow_GCS: ", len(files))
	require.NoError(s.T(), err)

	require.Equal(s.T(), 4, len(files))

	env.AssertExpectations(s.T())
}
