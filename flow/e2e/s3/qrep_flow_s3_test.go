package e2e_s3

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/stretchr/testify/require"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/shared"
)

type PeerFlowE2ETestSuiteS3 struct {
	t *testing.T

	conn     *connpostgres.PostgresConnector
	s3Helper *S3TestHelper
	suffix   string
}

func (s PeerFlowE2ETestSuiteS3) T() *testing.T {
	return s.t
}

func (s PeerFlowE2ETestSuiteS3) Connector() *connpostgres.PostgresConnector {
	return s.conn
}

func (s PeerFlowE2ETestSuiteS3) Suffix() string {
	return s.suffix
}

func tearDownSuite(s PeerFlowE2ETestSuiteS3) {
	e2e.TearDownPostgres(s)

	err := s.s3Helper.CleanUp(context.Background())
	if err != nil {
		require.Fail(s.t, "failed to clean up s3", err)
	}
}

func TestPeerFlowE2ETestSuiteS3(t *testing.T) {
	e2eshared.RunSuite(t, SetupSuiteS3, tearDownSuite)
}

func TestPeerFlowE2ETestSuiteGCS(t *testing.T) {
	e2eshared.RunSuite(t, SetupSuiteGCS, tearDownSuite)
}

func (s PeerFlowE2ETestSuiteS3) setupSourceTable(tableName string, rowCount int) {
	err := e2e.CreateTableForQRep(s.conn.Conn(), s.suffix, tableName)
	require.NoError(s.t, err)
	err = e2e.PopulateSourceTable(s.conn.Conn(), s.suffix, tableName, rowCount)
	require.NoError(s.t, err)
}

func setupSuite(t *testing.T, gcs bool) PeerFlowE2ETestSuiteS3 {
	t.Helper()

	err := godotenv.Load()
	if err != nil {
		// it's okay if the .env file is not present
		// we will use the default values
		t.Log("Unable to load .env file, using default values from env")
	}

	suffix := "s3_" + strings.ToLower(shared.RandomString(8))
	conn, err := e2e.SetupPostgres(t, suffix)
	if err != nil || conn == nil {
		require.Fail(t, "failed to setup postgres", err)
	}

	helper, err := NewS3TestHelper(gcs)
	if err != nil {
		require.Fail(t, "failed to setup S3", err)
	}

	return PeerFlowE2ETestSuiteS3{
		t:        t,
		conn:     conn,
		s3Helper: helper,
		suffix:   suffix,
	}
}

func SetupSuiteS3(t *testing.T) PeerFlowE2ETestSuiteS3 {
	t.Helper()
	return setupSuite(t, false)
}

func SetupSuiteGCS(t *testing.T) PeerFlowE2ETestSuiteS3 {
	t.Helper()
	return setupSuite(t, true)
}

func (s PeerFlowE2ETestSuiteS3) Test_Complete_QRep_Flow_S3() {
	if s.s3Helper == nil {
		s.t.Skip("Skipping S3 test")
	}

	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	jobName := "test_complete_flow_s3"
	schemaQualifiedName := fmt.Sprintf("e2e_test_%s.%s", s.suffix, jobName)

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
	require.NoError(s.t, err)
	qrepConfig.StagingPath = s.s3Helper.s3Config.Url

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	require.NoError(s.t, err)

	// Verify destination has 1 file
	// make context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	files, err := s.s3Helper.ListAllFiles(ctx, jobName)

	require.NoError(s.t, err)

	require.Len(s.t, files, 1)
}

func (s PeerFlowE2ETestSuiteS3) Test_Complete_QRep_Flow_S3_CTID() {
	if s.s3Helper == nil {
		s.t.Skip("Skipping S3 test")
	}

	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	jobName := "test_complete_flow_s3_ctid"
	schemaQualifiedName := fmt.Sprintf("e2e_test_%s.%s", s.suffix, jobName)

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
	require.NoError(s.t, err)
	qrepConfig.StagingPath = s.s3Helper.s3Config.Url
	qrepConfig.NumRowsPerPartition = 2000
	qrepConfig.InitialCopyOnly = true
	qrepConfig.WatermarkColumn = "ctid"

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	require.NoError(s.t, err)

	// Verify destination has 1 file
	// make context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	files, err := s.s3Helper.ListAllFiles(ctx, jobName)

	require.NoError(s.t, err)

	require.Len(s.t, files, 10)
}
