package e2e_clickhouse

import (
	"context"
	"fmt"
	"log/slog"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// type PeerFlowE2ETestSuiteCH struct {
// 	got.G
// 	t *testing.T

// 	pgSuffix  string
// 	pool      *pgxpool.Pool
// 	chHelper  *ClickhouseTestHelper
// 	connector *connclickhouse.ClickhouseConnector
// }

// func SetupSuite(t *testing.T, g got.G) PeerFlowE2ETestSuiteCH {
// 	err := godotenv.Load()
// 	if err != nil {
// 		// it's okay if the .env file is not present
// 		// we will use the default values
// 		slog.Info("Unable to load .env file, using default values from env")
// 	}

// 	suffix := shared.RandomString(8)
// 	tsSuffix := time.Now().Format("20060102150405")
// 	pgSuffix := fmt.Sprintf("ch_%s_%s", strings.ToLower(suffix), tsSuffix)

// 	pool, err := e2e.SetupPostgres(pgSuffix)
// 	if err != nil || pool == nil {
// 		slog.Error("failed to setup Postgres", slog.Any("error", err))
// 		g.FailNow()
// 	}

// 	chHelper, err := NewClickhouseTestHelper()
// 	if err != nil {
// 		slog.Error("failed to setup Clickhouse", slog.Any("error", err))
// 		g.FailNow()
// 	}

// 	connector, err := connclickhouse.NewClickhouseConnector(
// 		context.Background(),
// 		chHelper.Config,
// 	)
// 	require.NoError(t, err)

// 	suite := PeerFlowE2ETestSuiteCH{
// 		G:         g,
// 		t:         t,
// 		pgSuffix:  pgSuffix,
// 		pool:      pool,
// 		chHelper:  chHelper,
// 		connector: connector,
// 	}

// 	return suite
// }

// // Implement TearDownAllSuite interface to tear down the test suite
// func (s PeerFlowE2ETestSuiteCH) tearDownSuite() {
// 	err := e2e.TearDownPostgres(s.pool, s.pgSuffix)
// 	if err != nil {
// 		slog.Error("failed to tear down Postgres", slog.Any("error", err))
// 		s.FailNow()
// 	}

// 	if s.chHelper != nil {
// 		err = s.chHelper.Cleanup()
// 		if err != nil {
// 			slog.Error("failed to tear down Clickhouse", slog.Any("error", err))
// 			s.FailNow()
// 		}
// 	}

// 	err = s.connector.Close()

// 	if err != nil {
// 		slog.Error("failed to close Clickhouse connector", slog.Any("error", err))
// 		s.FailNow()
// 	}
// }

// func (s PeerFlowE2ETestSuiteCH) attachSchemaSuffix(tableName string) string {
// 	return fmt.Sprintf("e2e_test_%s.%s", s.pgSuffix, tableName)
// }

// func TestPeerFlowE2ETestSuiteCH(t *testing.T) {
// 	got.Each(t, func(t *testing.T) PeerFlowE2ETestSuiteCH {
// 		g := got.New(t)

// 		// Concurrently run each test
// 		g.Parallel()

// 		suite := SetupSuite(t, g)

// 		g.Cleanup(func() {
// 			suite.tearDownSuite()
// 		})

// 		return suite
// 	})
// }

func (s PeerFlowE2ETestSuiteCH) setupSourceTable(tableName string, numRows int) {
	err := e2e.CreateSourceTableQRep(s.pool, s.pgSuffix, tableName)
	require.NoError(s.t, err)
	err = e2e.PopulateSourceTable(s.pool, s.pgSuffix, tableName, numRows)
	require.NoError(s.t, err)
}

func (s PeerFlowE2ETestSuiteCH) setupCHDestinationTable(dstTable string) {
	schema := e2e.GetOwnersSchema()

	err := s.chHelper.CreateTable(dstTable, schema)
	// fail if table creation fails
	if err != nil {
		require.FailNow(s.t, "unable to create table on clickhouse", err)
	}

	slog.Info(fmt.Sprintf("created table on clickhouse: %s.%s.", s.chHelper.testDatabaseName, dstTable))
}

func (s PeerFlowE2ETestSuiteCH) compareTableContentsCH(tableName string, selector string, caseSensitive bool) {
	// read rows from source table
	pgQueryExecutor := connpostgres.NewQRepQueryExecutor(s.pool, context.Background(), "testflow", "testpart")
	pgQueryExecutor.SetTestEnv(true)
	pgRows, err := pgQueryExecutor.ExecuteAndProcessQuery(
		fmt.Sprintf("SELECT %s FROM e2e_test_%s.%s ORDER BY id", selector, s.pgSuffix, tableName),
	)
	require.NoError(s.t, err)

	// read rows from destination table
	qualifiedTableName := fmt.Sprintf("%s.%s", s.chHelper.testDatabaseName, tableName)
	var chSelQuery string
	if caseSensitive {
		chSelQuery = fmt.Sprintf(`SELECT %s FROM %s ORDER BY "id"`, selector, qualifiedTableName)
	} else {
		chSelQuery = fmt.Sprintf(`SELECT %s FROM %s ORDER BY id`, selector, qualifiedTableName)
	}

	chRows, err := s.chHelper.ExecuteAndProcessQuery(chSelQuery)

	require.NoError(s.t, err)

	require.True(s.t, pgRows.Equals(chRows), "rows from source and destination tables are not equal")
}

// func (s PeerFlowE2ETestSuiteCH) Test_Complete_QRep_Flow_Avro_CH() {
// 	env := e2e.NewTemporalTestWorkflowEnvironment()
// 	e2e.RegisterWorkflowsAndActivities(env, s.t)

// 	numRows := 10

// 	tblName := "test_qrep_flow_avro_ch"
// 	s.setupSourceTable(tblName, numRows)
// 	s.setupCHDestinationTable(tblName)

// 	dstSchemaQualified := fmt.Sprintf("%s.%s", s.chHelper.testSchemaName, tblName)

// 	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
// 		s.pgSuffix, tblName)

// 	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
// 		"test_qrep_flow_avro_ch",
// 		fmt.Sprintf("e2e_test_%s.%s", s.pgSuffix, tblName),
// 		dstSchemaQualified,
// 		query,
// 		s.chHelper.Peer,
// 		"",
// 	)
// 	require.NoError(s.t, err)

// 	e2e.RunQrepFlowWorkflow(env, qrepConfig)

// 	// Verify workflow completes without error
// 	s.True(env.IsWorkflowCompleted())

// 	err = env.GetWorkflowError()
// 	require.NoError(s.t, err)

// 	sel := e2e.GetOwnersSelectorString()
// 	s.compareTableContentsCH(tblName, sel, true)

// 	env.AssertExpectations(s.t)
// }

func (s PeerFlowE2ETestSuiteCH) Test_Complete_QRep_Flow_Avro_CH_S3() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env, s.t)
	numRows := 10

	tblName := "test_qrep_flow_avro_ch_s3"
	s.setupSourceTable(tblName, numRows)
	s.setupCHDestinationTable(tblName)

	dstSchemaQualified := fmt.Sprintf("%s.%s", s.chHelper.testDatabaseName, tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.pgSuffix, tblName)

	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
		"test_qrep_flow_avro_ch",
		s.attachSchemaSuffix(tblName),
		dstSchemaQualified,
		query,
		s.chHelper.Peer,
		"",
	)
	require.NoError(s.t, err)
	qrepConfig.StagingPath = fmt.Sprintf("s3://peerdb-test-bucket/avro/%s", uuid.New())

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())

	err = env.GetWorkflowError()
	require.NoError(s.t, err)

	sel := e2e.GetOwnersSelectorString()
	s.compareTableContentsCH(tblName, sel, true)

	env.AssertExpectations(s.t)
}

// func (s PeerFlowE2ETestSuiteCH) Test_Complete_QRep_Flow_Avro_CH_S3_Integration() {
// 	env := e2e.NewTemporalTestWorkflowEnvironment()
// 	e2e.RegisterWorkflowsAndActivities(env, s.t)

// 	numRows := 10

// 	tblName := "test_qrep_flow_avro_ch_s3_int"
// 	s.setupSourceTable(tblName, numRows)
// 	s.setupCHDestinationTable(tblName)

// 	dstSchemaQualified := fmt.Sprintf("%s.%s", s.chHelper.testSchemaName, tblName)

// 	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
// 		s.pgSuffix, tblName)

// 	chPeer := s.chHelper.Peer
// 	chPeer.GetClickhouseConfig().S3Integration = "peerdb_s3_integration"

// 	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
// 		"test_qrep_flow_avro_ch_int",
// 		s.attachSchemaSuffix(tblName),
// 		dstSchemaQualified,
// 		query,

// 		chPeer,
// 		"",
// 	)
// 	require.NoError(s.t, err)
// 	qrepConfig.StagingPath = fmt.Sprintf("s3://peerdb-test-bucket/avro/%s", uuid.New())

// 	e2e.RunQrepFlowWorkflow(env, qrepConfig)

// 	// Verify workflow completes without error
// 	s.True(env.IsWorkflowCompleted())

// 	err = env.GetWorkflowError()
// 	require.NoError(s.t, err)

// 	sel := e2e.GetOwnersSelectorString()
// 	s.compareTableContentsCH(tblName, sel, true)

// 	env.AssertExpectations(s.t)
// }

// func (s PeerFlowE2ETestSuiteCH) Test_Complete_QRep_Flow_Avro_CH_Upsert_Simple() {
// 	env := e2e.NewTemporalTestWorkflowEnvironment()
// 	e2e.RegisterWorkflowsAndActivities(env, s.t)

// 	numRows := 10

// 	tblName := "test_qrep_flow_avro_ch_ups"
// 	s.setupSourceTable(tblName, numRows)
// 	s.setupCHDestinationTable(tblName)

// 	dstSchemaQualified := fmt.Sprintf("%s.%s", s.chHelper.testSchemaName, tblName)

// 	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
// 		s.pgSuffix, tblName)

// 	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
// 		"test_qrep_flow_avro_ch",
// 		fmt.Sprintf("e2e_test_%s.%s", s.pgSuffix, tblName),
// 		dstSchemaQualified,
// 		query,
// 		s.chHelper.Peer,
// 		"",
// 	)
// 	qrepConfig.WriteMode = &protos.QRepWriteMode{
// 		WriteType:        protos.QRepWriteType_QREP_WRITE_MODE_UPSERT,
// 		UpsertKeyColumns: []string{"id"},
// 	}
// 	require.NoError(s.t, err)

// 	e2e.RunQrepFlowWorkflow(env, qrepConfig)

// 	// Verify workflow completes without error
// 	s.True(env.IsWorkflowCompleted())

// 	err = env.GetWorkflowError()
// 	require.NoError(s.t, err)

// 	sel := e2e.GetOwnersSelectorString()
// 	s.compareTableContentsCH(tblName, sel, true)

// 	env.AssertExpectations(s.t)
// }

// func (s PeerFlowE2ETestSuiteCH) Test_Complete_QRep_Flow_Avro_CH_Upsert_XMIN() {
// 	env := e2e.NewTemporalTestWorkflowEnvironment()
// 	e2e.RegisterWorkflowsAndActivities(env, s.t)

// 	numRows := 10

// 	tblName := "test_qrep_flow_avro_ch_ups_xmin"
// 	s.setupSourceTable(tblName, numRows)
// 	s.setupCHDestinationTable(tblName)

// 	dstSchemaQualified := fmt.Sprintf("%s.%s", s.chHelper.testSchemaName, tblName)

// 	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s",
// 		s.pgSuffix, tblName)

// 	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
// 		"test_qrep_flow_avro_ch_xmin",
// 		fmt.Sprintf("e2e_test_%s.%s", s.pgSuffix, tblName),
// 		dstSchemaQualified,
// 		query,
// 		s.chHelper.Peer,
// 		"",
// 	)
// 	qrepConfig.WriteMode = &protos.QRepWriteMode{
// 		WriteType:        protos.QRepWriteType_QREP_WRITE_MODE_UPSERT,
// 		UpsertKeyColumns: []string{"id"},
// 	}
// 	qrepConfig.WatermarkColumn = "xmin"
// 	require.NoError(s.t, err)

// 	e2e.RunXminFlowWorkflow(env, qrepConfig)

// 	// Verify workflow completes without error
// 	s.True(env.IsWorkflowCompleted())

// 	err = env.GetWorkflowError()
// 	require.NoError(s.t, err)

// 	sel := e2e.GetOwnersSelectorString()
// 	s.compareTableContentsCH(tblName, sel, true)

// 	env.AssertExpectations(s.t)
// }
