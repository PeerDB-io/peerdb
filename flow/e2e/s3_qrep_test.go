package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
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

func (s PeerFlowE2ETestSuiteS3) Source() SuiteSource {
	return &PostgresSource{PostgresConnector: s.conn}
}

func (s PeerFlowE2ETestSuiteS3) Suffix() string {
	return s.suffix
}

func (s PeerFlowE2ETestSuiteS3) Peer() *protos.Peer {
	s.t.Helper()
	ret := &protos.Peer{
		Name: AddSuffix(s, "s3peer"),
		Type: protos.DBType_S3,
		Config: &protos.Peer_S3Config{
			S3Config: s.s3Helper.S3Config,
		},
	}
	CreatePeer(s.t, ret)
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
	require.NoError(s.t, CreateTableForQRep(s.t.Context(), s.conn.Conn(), s.suffix, tableName))
	require.NoError(s.t, PopulateSourceTable(s.t.Context(), s.conn.Conn(), s.suffix, tableName, rowCount))
}

func setupSuite(t *testing.T, s3environment S3Environment) PeerFlowE2ETestSuiteS3 {
	t.Helper()

	suffix := "s3_" + strings.ToLower(shared.RandomString(8))
	conn, err := SetupPostgres(t, suffix)
	if err != nil || conn == nil {
		require.Fail(t, "failed to setup postgres", err)
	}

	helper, err := NewS3TestHelper(t.Context(), s3environment)
	if err != nil {
		require.Fail(t, "failed to setup S3", err)
	}

	return PeerFlowE2ETestSuiteS3{
		t:        t,
		conn:     conn.PostgresConnector,
		s3Helper: helper,
		suffix:   suffix,
	}
}

func (s PeerFlowE2ETestSuiteS3) Teardown(ctx context.Context) {
	TearDownPostgres(ctx, s)

	if err := s.s3Helper.CleanUp(ctx); err != nil {
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

func SetupSuiteMinIO_TLS(t *testing.T) PeerFlowE2ETestSuiteS3 {
	t.Helper()
	return setupSuite(t, MinioTls)
}

func (s PeerFlowE2ETestSuiteS3) Test_Complete_QRep_Flow_S3() {
	if s.s3Helper == nil {
		s.t.Skip("Skipping S3 test")
	}

	tc := NewTemporalClient(s.t)

	tableName := "test_complete_flow_s3"
	jobName := AddSuffix(s, tableName)
	schemaQualifiedName := AttachSchema(s, tableName)

	s.setupSourceTable(tableName, 10)
	query := fmt.Sprintf("SELECT * FROM %s WHERE updated_at >= {{.start}} AND updated_at < {{.end}}",
		schemaQualifiedName)
	qrepConfig := CreateQRepWorkflowConfig(
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

	env := RunQRepFlowWorkflow(s.t, tc, qrepConfig)
	EnvWaitForFinished(s.t, env, 3*time.Minute)
	require.NoError(s.t, env.Error(s.t.Context()))

	// Verify destination has 1 file
	// make context with timeout
	ctx, cancel := context.WithTimeout(s.t.Context(), 10*time.Second)
	defer cancel()

	files, err := s.s3Helper.ListAllFiles(ctx, jobName)

	require.NoError(s.t, err)

	require.Len(s.t, files, 1)
}

func (s PeerFlowE2ETestSuiteS3) Test_Complete_QRep_Flow_S3_CTID() {
	if s.s3Helper == nil {
		s.t.Skip("Skipping S3 test")
	}

	tc := NewTemporalClient(s.t)

	tableName := "test_complete_flow_s3_ctid"
	jobName := AddSuffix(s, tableName)
	schemaQualifiedName := AttachSchema(s, tableName)

	s.setupSourceTable(tableName, 20000)
	query := fmt.Sprintf("SELECT * FROM %s WHERE ctid BETWEEN {{.start}} AND {{.end}}", schemaQualifiedName)
	qrepConfig := CreateQRepWorkflowConfig(
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

	env := RunQRepFlowWorkflow(s.t, tc, qrepConfig)
	EnvWaitForFinished(s.t, env, 3*time.Minute)
	require.NoError(s.t, env.Error(s.t.Context()))

	// Verify destination has 1 file
	// make context with timeout
	ctx, cancel := context.WithTimeout(s.t.Context(), 10*time.Second)
	defer cancel()

	files, err := s.s3Helper.ListAllFiles(ctx, jobName)

	require.NoError(s.t, err)

	require.Len(s.t, files, 10)
}
