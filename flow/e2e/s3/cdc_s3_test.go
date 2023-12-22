package e2e_s3

import (
	"context"
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/e2e"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
	"github.com/stretchr/testify/require"
)

func (s PeerFlowE2ETestSuiteS3) attachSchemaSuffix(tableName string) string {
	return fmt.Sprintf("e2e_test_%s.%s", s.suffix, tableName)
}

func (s PeerFlowE2ETestSuiteS3) attachSuffix(input string) string {
	return fmt.Sprintf("%s_%s", input, s.suffix)
}

func (s PeerFlowE2ETestSuiteS3) Test_Complete_Simple_Flow_S3() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env, s.t)

	helper, setupErr := setupS3("s3")
	if setupErr != nil {
		require.Fail(s.t, "failed to setup S3", setupErr)
	}
	s.s3Helper = helper

	srcTableName := s.attachSchemaSuffix("test_simple_flow_s3")
	dstTableName := fmt.Sprintf("%s.%s", "peerdb_test_s3", "test_simple_flow_s3")
	flowJobName := s.attachSuffix("test_simple_flow_s3")
	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`, srcTableName))
	require.NoError(s.t, err)
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      flowJobName,
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.s3Helper.GetPeer(),
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	require.NoError(s.t, err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows:   4,
		ExitAfterRecords: 20,
		MaxBatchSize:     5,
	}

	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		require.NoError(s.t, err)
		// insert 20 rows
		for i := 1; i <= 20; i++ {
			testKey := fmt.Sprintf("test_key_%d", i)
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s (key, value) VALUES ($1, $2)
		`, srcTableName), testKey, testValue)
			require.NoError(s.t, err)
		}
		require.NoError(s.t, err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Error(s.t, err)
	require.Contains(s.t, err.Error(), "continue as new")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fmt.Println("JobName: ", flowJobName)
	files, err := s.s3Helper.ListAllFiles(ctx, flowJobName)
	fmt.Println("Files in Test_Complete_Simple_Flow_S3: ", len(files))
	require.NoError(s.t, err)

	require.Equal(s.t, 4, len(files))

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteS3) Test_Complete_Simple_Flow_GCS_Interop() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env, s.t)
	helper, setupErr := setupS3("gcs")
	if setupErr != nil {
		require.Fail(s.t, "failed to setup S3", setupErr)
	}
	s.s3Helper = helper

	srcTableName := s.attachSchemaSuffix("test_simple_flow_gcs_interop")
	dstTableName := fmt.Sprintf("%s.%s", "peerdb_test_gcs_interop", "test_simple_flow_gcs_interop")
	flowJobName := s.attachSuffix("test_simple_flow_gcs_interop")
	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`, srcTableName))
	require.NoError(s.t, err)
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      flowJobName,
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.s3Helper.GetPeer(),
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	require.NoError(s.t, err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows:   4,
		ExitAfterRecords: 20,
		MaxBatchSize:     5,
	}

	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		require.NoError(s.t, err)
		// insert 20 rows
		for i := 1; i <= 20; i++ {
			testKey := fmt.Sprintf("test_key_%d", i)
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s (key, value) VALUES ($1, $2)
		`, srcTableName), testKey, testValue)
			require.NoError(s.t, err)
		}
		fmt.Println("Inserted 20 rows into the source table")
		require.NoError(s.t, err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Error(s.t, err)
	require.Contains(s.t, err.Error(), "continue as new")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fmt.Println("JobName: ", flowJobName)
	files, err := s.s3Helper.ListAllFiles(ctx, flowJobName)
	fmt.Println("Files in Test_Complete_Simple_Flow_GCS: ", len(files))
	require.NoError(s.t, err)

	require.Equal(s.t, 4, len(files))

	env.AssertExpectations(s.t)
}
