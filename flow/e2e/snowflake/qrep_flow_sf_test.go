package e2e_snowflake

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

func (s PeerFlowE2ETestSuiteSF) setupSourceTable(tableName string, numRows int) {
	err := e2e.CreateTableForQRep(s.pool, s.pgSuffix, tableName)
	require.NoError(s.t, err)
	err = e2e.PopulateSourceTable(s.pool, s.pgSuffix, tableName, numRows)
	require.NoError(s.t, err)
}

func (s PeerFlowE2ETestSuiteSF) setupSFDestinationTable(dstTable string) {
	schema := e2e.GetOwnersSchema()
	err := s.sfHelper.CreateTable(dstTable, schema)
	// fail if table creation fails
	if err != nil {
		require.FailNow(s.t, "unable to create table on snowflake", err)
	}

	slog.Info(fmt.Sprintf("created table on snowflake: %s.%s.", s.sfHelper.testSchemaName, dstTable))
}

func (s PeerFlowE2ETestSuiteSF) compareTableContentsSF(tableName string, selector string, caseSensitive bool) {
	// read rows from source table
	pgQueryExecutor := connpostgres.NewQRepQueryExecutor(s.pool, context.Background(), "testflow", "testpart")
	pgQueryExecutor.SetTestEnv(true)
	pgRows, err := pgQueryExecutor.ExecuteAndProcessQuery(
		fmt.Sprintf("SELECT %s FROM e2e_test_%s.%s ORDER BY id", selector, s.pgSuffix, tableName),
	)
	require.NoError(s.t, err)

	// read rows from destination table
	qualifiedTableName := fmt.Sprintf("%s.%s.%s", s.sfHelper.testDatabaseName, s.sfHelper.testSchemaName, tableName)
	var sfSelQuery string
	if caseSensitive {
		sfSelQuery = fmt.Sprintf(`SELECT %s FROM %s ORDER BY "id"`, selector, qualifiedTableName)
	} else {
		sfSelQuery = fmt.Sprintf(`SELECT %s FROM %s ORDER BY id`, selector, qualifiedTableName)
	}
	fmt.Printf("running query on snowflake: %s\n", sfSelQuery)

	sfRows, err := s.sfHelper.ExecuteAndProcessQuery(sfSelQuery)
	require.NoError(s.t, err)

	require.True(s.t, pgRows.Equals(sfRows), "rows from source and destination tables are not equal")
}

func (s PeerFlowE2ETestSuiteSF) Test_Complete_QRep_Flow_Avro_SF() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env, s.t)

	numRows := 10

	tblName := "test_qrep_flow_avro_sf"
	s.setupSourceTable(tblName, numRows)
	s.setupSFDestinationTable(tblName)

	dstSchemaQualified := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.pgSuffix, tblName)

	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
		"test_qrep_flow_avro_sf",
		fmt.Sprintf("e2e_test_%s.%s", s.pgSuffix, tblName),
		dstSchemaQualified,
		query,
		s.sfHelper.Peer,
		"",
		false,
		"",
	)
	require.NoError(s.t, err)

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())

	err = env.GetWorkflowError()
	require.NoError(s.t, err)

	sel := e2e.GetOwnersSelectorString()
	s.compareTableContentsSF(tblName, sel, true)

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteSF) Test_Complete_QRep_Flow_Avro_SF_Upsert_Simple() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env, s.t)

	numRows := 10

	tblName := "test_qrep_flow_avro_sf_ups"
	s.setupSourceTable(tblName, numRows)
	s.setupSFDestinationTable(tblName)

	dstSchemaQualified := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.pgSuffix, tblName)

	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
		"test_qrep_flow_avro_sf",
		fmt.Sprintf("e2e_test_%s.%s", s.pgSuffix, tblName),
		dstSchemaQualified,
		query,
		s.sfHelper.Peer,
		"",
		false,
		"",
	)
	qrepConfig.WriteMode = &protos.QRepWriteMode{
		WriteType:        protos.QRepWriteType_QREP_WRITE_MODE_UPSERT,
		UpsertKeyColumns: []string{"id"},
	}
	require.NoError(s.t, err)

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())

	err = env.GetWorkflowError()
	require.NoError(s.t, err)

	sel := e2e.GetOwnersSelectorString()
	s.compareTableContentsSF(tblName, sel, true)

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteSF) Test_Complete_QRep_Flow_Avro_SF_S3() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env, s.t)

	numRows := 10

	tblName := "test_qrep_flow_avro_sf_s3"
	s.setupSourceTable(tblName, numRows)
	s.setupSFDestinationTable(tblName)

	dstSchemaQualified := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.pgSuffix, tblName)

	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
		"test_qrep_flow_avro_sf",
		s.attachSchemaSuffix(tblName),
		dstSchemaQualified,
		query,
		s.sfHelper.Peer,
		"",
		false,
		"",
	)
	require.NoError(s.t, err)
	qrepConfig.StagingPath = fmt.Sprintf("s3://peerdb-test-bucket/avro/%s", uuid.New())

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())

	err = env.GetWorkflowError()
	require.NoError(s.t, err)

	sel := e2e.GetOwnersSelectorString()
	s.compareTableContentsSF(tblName, sel, true)

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteSF) Test_Complete_QRep_Flow_Avro_SF_Upsert_XMIN() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env, s.t)

	numRows := 10

	tblName := "test_qrep_flow_avro_sf_ups_xmin"
	s.setupSourceTable(tblName, numRows)
	s.setupSFDestinationTable(tblName)

	dstSchemaQualified := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s",
		s.pgSuffix, tblName)

	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
		"test_qrep_flow_avro_sf_xmin",
		fmt.Sprintf("e2e_test_%s.%s", s.pgSuffix, tblName),
		dstSchemaQualified,
		query,
		s.sfHelper.Peer,
		"",
		false,
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
	require.True(s.t, env.IsWorkflowCompleted())

	err = env.GetWorkflowError()
	require.NoError(s.t, err)

	sel := e2e.GetOwnersSelectorString()
	s.compareTableContentsSF(tblName, sel, true)

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteSF) Test_Complete_QRep_Flow_Avro_SF_S3_Integration() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env, s.t)

	numRows := 10

	tblName := "test_qrep_flow_avro_sf_s3_int"
	s.setupSourceTable(tblName, numRows)
	s.setupSFDestinationTable(tblName)

	dstSchemaQualified := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.pgSuffix, tblName)

	sfPeer := s.sfHelper.Peer
	sfPeer.GetSnowflakeConfig().S3Integration = "peerdb_s3_integration"

	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
		"test_qrep_flow_avro_sf_int",
		s.attachSchemaSuffix(tblName),
		dstSchemaQualified,
		query,

		sfPeer,
		"",
		false,
		"",
	)
	require.NoError(s.t, err)
	qrepConfig.StagingPath = fmt.Sprintf("s3://peerdb-test-bucket/avro/%s", uuid.New())

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())

	err = env.GetWorkflowError()
	require.NoError(s.t, err)

	sel := e2e.GetOwnersSelectorString()
	s.compareTableContentsSF(tblName, sel, true)

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteSF) Test_PeerDB_Columns_QRep_SF() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env, s.t)

	numRows := 10

	tblName := "test_qrep_columns_sf"
	s.setupSourceTable(tblName, numRows)

	dstSchemaQualified := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.pgSuffix, tblName)

	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
		"test_columns_qrep_sf",
		fmt.Sprintf("e2e_test_%s.%s", s.pgSuffix, tblName),
		dstSchemaQualified,
		query,
		s.sfHelper.Peer,
		"",
		true,
		"_PEERDB_SYNCED_AT",
	)
	qrepConfig.WriteMode = &protos.QRepWriteMode{
		WriteType:        protos.QRepWriteType_QREP_WRITE_MODE_UPSERT,
		UpsertKeyColumns: []string{"id"},
	}
	require.NoError(s.t, err)

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())

	err = env.GetWorkflowError()
	require.NoError(s.t, err)

	err = s.sfHelper.checkSyncedAt(fmt.Sprintf(`SELECT "_PEERDB_SYNCED_AT" FROM %s.%s`,
		s.sfHelper.testSchemaName, tblName))
	require.NoError(s.t, err)

	env.AssertExpectations(s.t)
}
