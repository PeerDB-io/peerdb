package e2e_s3

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
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

func (s PeerFlowE2ETestSuiteS3) Peer() *protos.Peer {
	s.t.Helper()
	ret := &protos.Peer{
		Name: e2e.AddSuffix(s, "s3peer"),
		Type: protos.DBType_S3,
		Config: &protos.Peer_S3Config{
			S3Config: s.s3Helper.S3Config,
		},
	}
	e2e.CreatePeer(s.t, ret)
	return ret
}

func TestPeerFlowE2ETestSuiteS3(t *testing.T) {
	t.Skip("skipping AWS, CI credentials revoked") // TODO fix CI
	e2eshared.RunSuite(t, SetupSuiteS3)
}

func TestPeerFlowE2ETestSuiteGCS(t *testing.T) {
	e2eshared.RunSuite(t, SetupSuiteGCS)
}

func TestPeerFlowE2ETestSuiteMinIO(t *testing.T) {
	e2eshared.RunSuite(t, SetupSuiteMinIO)
}

func (s PeerFlowE2ETestSuiteS3) setupSourceTable(tableName string, rowCount int) {
	require.NoError(s.t, e2e.CreateTableForQRep(s.conn.Conn(), s.suffix, tableName))
	require.NoError(s.t, e2e.PopulateSourceTable(s.conn.Conn(), s.suffix, tableName, rowCount))
}

func setupSuite(t *testing.T, s3environment S3Environment) PeerFlowE2ETestSuiteS3 {
	t.Helper()

	suffix := "s3_" + strings.ToLower(shared.RandomString(8))
	conn, err := e2e.SetupPostgres(t, suffix)
	if err != nil || conn == nil {
		require.Fail(t, "failed to setup postgres", err)
	}

	helper, err := NewS3TestHelper(s3environment)
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

func (s PeerFlowE2ETestSuiteS3) Teardown() {
	e2e.TearDownPostgres(s)

	err := s.s3Helper.CleanUp(context.Background())
	if err != nil {
		require.Fail(s.t, "failed to clean up s3", err)
	}
}

func SetupSuiteS3(t *testing.T) PeerFlowE2ETestSuiteS3 {
	t.Helper()
	return setupSuite(t, Aws)
}

func SetupSuiteGCS(t *testing.T) PeerFlowE2ETestSuiteS3 {
	t.Helper()
	return setupSuite(t, Gcs)
}

func SetupSuiteMinIO(t *testing.T) PeerFlowE2ETestSuiteS3 {
	t.Helper()
	return setupSuite(t, Minio)
}

func (s PeerFlowE2ETestSuiteS3) Test_Complete_QRep_Flow_S3() {
	if s.s3Helper == nil {
		s.t.Skip("Skipping S3 test")
	}

	tc := e2e.NewTemporalClient(s.t)

	jobName := "test_complete_flow_s3"
	schemaQualifiedName := fmt.Sprintf("e2e_test_%s.%s", s.suffix, jobName)

	s.setupSourceTable(jobName, 10)
	query := fmt.Sprintf("SELECT * FROM %s WHERE updated_at >= {{.start}} AND updated_at < {{.end}}",
		schemaQualifiedName)
	qrepConfig := e2e.CreateQRepWorkflowConfig(
		s.t,
		jobName,
		schemaQualifiedName,
		"e2e_dest_1",
		query,
		s.Peer().Name,
		"stage",
		false,
		"",
		"",
	)
	qrepConfig.StagingPath = s.s3Helper.S3Config.Url

	env := e2e.RunQRepFlowWorkflow(tc, qrepConfig)
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)
	require.NoError(s.t, env.Error())

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

	tc := e2e.NewTemporalClient(s.t)

	jobName := "test_complete_flow_s3_ctid"
	schemaQualifiedName := fmt.Sprintf("e2e_test_%s.%s", s.suffix, jobName)

	s.setupSourceTable(jobName, 20000)
	query := fmt.Sprintf("SELECT * FROM %s WHERE ctid BETWEEN {{.start}} AND {{.end}}", schemaQualifiedName)
	qrepConfig := e2e.CreateQRepWorkflowConfig(
		s.t,
		jobName,
		schemaQualifiedName,
		"e2e_dest_ctid",
		query,
		s.Peer().Name,
		"stage",
		false,
		"",
		"",
	)
	qrepConfig.StagingPath = s.s3Helper.S3Config.Url
	qrepConfig.NumRowsPerPartition = 2000
	qrepConfig.InitialCopyOnly = true
	qrepConfig.WatermarkColumn = "ctid"

	env := e2e.RunQRepFlowWorkflow(tc, qrepConfig)
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)
	require.NoError(s.t, env.Error())

	// Verify destination has 1 file
	// make context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	files, err := s.s3Helper.ListAllFiles(ctx, jobName)

	require.NoError(s.t, err)

	require.Len(s.t, files, 10)
}
