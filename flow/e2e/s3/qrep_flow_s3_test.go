package e2e_s3

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
)

const s3Suffix = "s3"

type PeerFlowE2ETestSuiteS3 struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	pool     *pgxpool.Pool
	s3Helper *S3TestHelper
}

func TestPeerFlowE2ETestSuiteS3(t *testing.T) {
	suite.Run(t, new(PeerFlowE2ETestSuiteS3))
}

func (s *PeerFlowE2ETestSuiteS3) setupSourceTable(tableName string, rowCount int) {
	err := e2e.CreateTableForQRep(s.pool, s3Suffix, tableName)
	s.NoError(err)
	err = e2e.PopulateSourceTable(s.pool, s3Suffix, tableName, rowCount)
	s.NoError(err)
}

func (s *PeerFlowE2ETestSuiteS3) setupS3(mode string) error {
	switchToGCS := false
	if mode == "gcs" {
		switchToGCS = true
	}
	helper, err := NewS3TestHelper(switchToGCS)
	if err != nil {
		return err
	}

	s.s3Helper = helper
	return nil
}

func (s *PeerFlowE2ETestSuiteS3) SetupSuite() {
	err := godotenv.Load()
	if err != nil {
		// it's okay if the .env file is not present
		// we will use the default values
		slog.Info("Unable to load .env file, using default values from env")
	}

	pool, err := e2e.SetupPostgres(s3Suffix)
	if err != nil || pool == nil {
		s.Fail("failed to setup postgres", err)
	}
	s.pool = pool

	err = s.setupS3("s3")
	if err != nil {
		s.Fail("failed to setup S3", err)
	}
}

// Implement TearDownAllSuite interface to tear down the test suite
func (s *PeerFlowE2ETestSuiteS3) TearDownSuite() {
	err := e2e.TearDownPostgres(s.pool, s3Suffix)
	if err != nil {
		s.Fail("failed to drop Postgres schema", err)
	}

	if s.s3Helper != nil {
		err = s.s3Helper.CleanUp()
		if err != nil {
			s.Fail("failed to clean up s3", err)
		}
	}
}

func (s *PeerFlowE2ETestSuiteS3) Test_Complete_QRep_Flow_S3() {
	if s.s3Helper == nil {
		s.T().Skip("Skipping S3 test")
	}

	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.T(), env)

	jobName := "test_complete_flow_s3"
	schemaQualifiedName := fmt.Sprintf("e2e_test_%s.%s", s3Suffix, jobName)

	s.setupSourceTable(jobName, 10)
	query := fmt.Sprintf("SELECT * FROM %s WHERE updated_at >= {{.start}} AND updated_at < {{.end}}",
		schemaQualifiedName)
	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
		jobName,
		schemaQualifiedName,
		"e2e_dest_1",
		query,
		s.s3Helper.GetPeer(),
		"stage",
		false,
		"",
	)
	s.NoError(err)
	qrepConfig.StagingPath = s.s3Helper.s3Config.Url

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

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

func (s *PeerFlowE2ETestSuiteS3) Test_Complete_QRep_Flow_S3_CTID() {
	if s.s3Helper == nil {
		s.T().Skip("Skipping S3 test")
	}

	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.T(), env)

	jobName := "test_complete_flow_s3_ctid"
	schemaQualifiedName := fmt.Sprintf("e2e_test_%s.%s", s3Suffix, jobName)

	s.setupSourceTable(jobName, 20000)
	query := fmt.Sprintf("SELECT * FROM %s WHERE ctid BETWEEN {{.start}} AND {{.end}}", schemaQualifiedName)
	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
		jobName,
		schemaQualifiedName,
		"e2e_dest_ctid",
		query,
		s.s3Helper.GetPeer(),
		"stage",
		false,
		"",
	)
	s.NoError(err)
	qrepConfig.StagingPath = s.s3Helper.s3Config.Url
	qrepConfig.NumRowsPerPartition = 2000
	qrepConfig.InitialCopyOnly = true
	qrepConfig.WatermarkColumn = "ctid"

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

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
