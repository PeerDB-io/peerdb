package e2e_clickhouse

import (
	"context"
	"fmt"
	"log/slog"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func (s PeerFlowE2ETestSuiteCH) setupSourceTable(tableName string, numRows int) {
	err := e2e.CreateSourceTableQRep(s.pool, s.pgSuffix, tableName)
	require.NoError(s.t, err)
	err = e2e.PopulateSourceTable(s.pool, s.pgSuffix, tableName, numRows)
	require.NoError(s.t, err)
}

func (s PeerFlowE2ETestSuiteCH) setupCHDestinationTable(dstTable string) {
	schema := e2e.GetOwnersSchema()
	//TODO: write your own table creation logic for ch
	err := s.chHelper.CreateTable(dstTable, schema)
	// fail if table creation fails
	if err != nil {
		require.FailNow(s.t, "unable to create table on clickhouse", err)
	}

	slog.Info(fmt.Sprintf("created table on clickhouse: %s.%s.", s.chHelper.testSchemaName, dstTable))
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
	qualifiedTableName := fmt.Sprintf("%s.%s.%s", s.chHelper.testDatabaseName, s.chHelper.testSchemaName, tableName)
	var chSelQuery string
	if caseSensitive {
		chSelQuery = fmt.Sprintf(`SELECT %s FROM %s ORDER BY "id"`, selector, qualifiedTableName)
	} else {
		chSelQuery = fmt.Sprintf(`SELECT %s FROM %s ORDER BY id`, selector, qualifiedTableName)
	}
	fmt.Printf("running query on clickhouse: %s\n", chSelQuery)

	chRows, err := s.chHelper.ExecuteAndProcessQuery(chSelQuery)
	require.NoError(s.t, err)

	require.True(s.t, pgRows.Equals(chRows), "rows from source and destination tables are not equal")
}

func (s PeerFlowE2ETestSuiteCH) Test_Complete_QRep_Flow_Avro_CH() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env, s.t)

	numRows := 10

	tblName := "test_qrep_flow_avro_ch"
	s.setupSourceTable(tblName, numRows)
	s.setupCHDestinationTable(tblName)

	dstSchemaQualified := fmt.Sprintf("%s.%s", s.chHelper.testSchemaName, tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.pgSuffix, tblName)

	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
		"test_qrep_flow_avro_ch",
		fmt.Sprintf("e2e_test_%s.%s", s.pgSuffix, tblName),
		dstSchemaQualified,
		query,
		s.chHelper.Peer,
		"",
	)
	require.NoError(s.t, err)

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())

	err = env.GetWorkflowError()
	require.NoError(s.t, err)

	sel := e2e.GetOwnersSelectorString()
	s.compareTableContentsCH(tblName, sel, true)

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteCH) Test_Complete_QRep_Flow_Avro_CH_Upsert_Simple() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env, s.t)

	numRows := 10

	tblName := "test_qrep_flow_avro_ch_ups"
	s.setupSourceTable(tblName, numRows)
	s.setupCHDestinationTable(tblName)

	dstSchemaQualified := fmt.Sprintf("%s.%s", s.chHelper.testSchemaName, tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.pgSuffix, tblName)

	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
		"test_qrep_flow_avro_ch",
		fmt.Sprintf("e2e_test_%s.%s", s.pgSuffix, tblName),
		dstSchemaQualified,
		query,
		s.chHelper.Peer,
		"",
	)
	qrepConfig.WriteMode = &protos.QRepWriteMode{
		WriteType:        protos.QRepWriteType_QREP_WRITE_MODE_UPSERT,
		UpsertKeyColumns: []string{"id"},
	}
	require.NoError(s.t, err)

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())

	err = env.GetWorkflowError()
	require.NoError(s.t, err)

	sel := e2e.GetOwnersSelectorString()
	s.compareTableContentsCH(tblName, sel, true)

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteCH) Test_Complete_QRep_Flow_Avro_CH_S3() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env, s.t)

	numRows := 10

	tblName := "test_qrep_flow_avro_ch_s3"
	s.setupSourceTable(tblName, numRows)
	s.setupCHDestinationTable(tblName)

	dstSchemaQualified := fmt.Sprintf("%s.%s", s.chHelper.testSchemaName, tblName)

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

func (s PeerFlowE2ETestSuiteCH) Test_Complete_QRep_Flow_Avro_CH_Upsert_XMIN() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env, s.t)

	numRows := 10

	tblName := "test_qrep_flow_avro_ch_ups_xmin"
	s.setupSourceTable(tblName, numRows)
	s.setupCHDestinationTable(tblName)

	dstSchemaQualified := fmt.Sprintf("%s.%s", s.chHelper.testSchemaName, tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s",
		s.pgSuffix, tblName)

	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
		"test_qrep_flow_avro_ch_xmin",
		fmt.Sprintf("e2e_test_%s.%s", s.pgSuffix, tblName),
		dstSchemaQualified,
		query,
		s.chHelper.Peer,
		"",
	)
	qrepConfig.WriteMode = &protos.QRepWriteMode{
		WriteType:        protos.QRepWriteType_QREP_WRITE_MODE_UPSERT,
		UpsertKeyColumns: []string{"id"},
	}
	qrepConfig.WatermarkColumn = "xmin"
	require.NoError(s.t, err)

	e2e.RunXminFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())

	err = env.GetWorkflowError()
	require.NoError(s.t, err)

	sel := e2e.GetOwnersSelectorString()
	s.compareTableContentsCH(tblName, sel, true)

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteCH) Test_Complete_QRep_Flow_Avro_CH_S3_Integration() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env, s.t)

	numRows := 10

	tblName := "test_qrep_flow_avro_ch_s3_int"
	s.setupSourceTable(tblName, numRows)
	s.setupCHDestinationTable(tblName)

	dstSchemaQualified := fmt.Sprintf("%s.%s", s.chHelper.testSchemaName, tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.pgSuffix, tblName)

	chPeer := s.chHelper.Peer
	chPeer.GetClickhouseConfig().S3Integration = "peerdb_s3_integration"

	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
		"test_qrep_flow_avro_ch_int",
		s.attachSchemaSuffix(tblName),
		dstSchemaQualified,
		query,

		chPeer,
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
