package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	util "github.com/PeerDB-io/peer-flow/utils"
	"github.com/stretchr/testify/require"
)

func (s *E2EPeerFlowTestSuite) setupS3() error {
	helper, err := NewS3TestHelper()
	if err != nil {
		return err
	}

	s.s3Helper = helper
	return nil
}

func (s *E2EPeerFlowTestSuite) Test_Complete_QRep_Flow_S3() {
	if s.s3Helper == nil {
		s.T().Skip("Skipping S3 test")
	}

	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	ru, err := util.RandomUInt64()
	s.NoError(err)

	jobName := fmt.Sprintf("test_complete_flow_s3_%d", ru)
	schemaQualifiedName := fmt.Sprintf("e2e_test.%s", jobName)
	_, err = s.pool.Exec(context.Background(), `
		CREATE TABLE `+schemaQualifiedName+` (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`)
	s.NoError(err)

	tblName := "test_qrep_flow_s3_1"
	s.setupSourceTable(tblName, 10)
	query := fmt.Sprintf("SELECT * FROM e2e_test.%s WHERE updated_at >= {{.start}} AND updated_at < {{.end}}", tblName)
	qrepConfig := s.createQRepWorkflowConfig(
		jobName,
		"e2e_test."+tblName,
		"e2e_dest_1",
		query,
		protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO,
		s.s3Helper.GetPeer(),
	)
	qrepConfig.StagingPath = s.s3Helper.s3Config.Url

	runQrepFlowWorkflow(env, qrepConfig)

	go func() {
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

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	s.NoError(err)

	// Verify destination has 1 file
	// make context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	files, err := s.s3Helper.ListAllFiles(ctx, jobName)

	require.NoError(s.T(), err)

	require.Equal(s.T(), 1, len(files))

	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) Test_Complete_QRep_Flow_S3_CTID() {
	if s.s3Helper == nil {
		s.T().Skip("Skipping S3 test")
	}

	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	ru, err := util.RandomUInt64()
	s.NoError(err)

	jobName := fmt.Sprintf("test_complete_flow_s3_ctid_%d", ru)
	schemaQualifiedName := fmt.Sprintf("e2e_test.%s", jobName)
	_, err = s.pool.Exec(context.Background(), `
		CREATE TABLE `+schemaQualifiedName+` (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`)
	s.NoError(err)

	tblName := "test_qrep_flow_s3_ctid"
	s.setupSourceTable(tblName, 20000)
	query := fmt.Sprintf("SELECT * FROM e2e_test.%s WHERE ctid BETWEEN {{.start}} AND {{.end}}", tblName)
	qrepConfig := s.createQRepWorkflowConfig(
		jobName,
		"e2e_test."+tblName,
		"e2e_dest_ctid",
		query,
		protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO,
		s.s3Helper.GetPeer(),
	)
	qrepConfig.StagingPath = s.s3Helper.s3Config.Url
	qrepConfig.NumRowsPerPartition = 2000
	qrepConfig.InitialCopyOnly = true
	qrepConfig.WatermarkColumn = "ctid"

	runQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	s.NoError(err)

	// Verify destination has 1 file
	// make context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	files, err := s.s3Helper.ListAllFiles(ctx, jobName)

	require.NoError(s.T(), err)

	require.Equal(s.T(), 10, len(files))

	env.AssertExpectations(s.T())
}
