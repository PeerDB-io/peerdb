package e2e_clickhouse

import (
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peer-flow/e2e"
)

//nolint:unparam
func (s PeerFlowE2ETestSuiteCH) setupSourceTable(tableName string, numRows int, fieldNames []string) {
	err := e2e.CreateTableForQRepNew(s.conn, s.pgSuffix, tableName, fieldNames)
	fmt.Printf("\n**************************** setupSourceTable created")
	require.NoError(s.t, err)
	err = e2e.PopulateSourceTableNew(s.conn, s.pgSuffix, tableName, numRows, fieldNames)
	require.NoError(s.t, err)
}

func (s PeerFlowE2ETestSuiteCH) setupCHDestinationTable(dstTable string, fieldNames []string) {
	schema := e2e.GetOwnersSchemaNew(fieldNames)
	//TODO: write your own table creation logic for ch or modify the one in chHelper()
	err := s.chHelper.CreateTable(dstTable, schema)
	// fail if table creation fails
	if err != nil {
		require.FailNow(s.t, "unable to create table on clickhouse", err)
	}

	slog.Info(fmt.Sprintf("created table on clickhouse: %s.%s.", s.chHelper.testDatabaseName, dstTable))
}

// func (s PeerFlowE2ETestSuiteCH) checkJSONValue(tableName, colName, fieldName, value string) error {
// 	res, err := s.chHelper.ExecuteAndProcessQuery(fmt.Sprintf("SELECT %s:%s FROM %s", colName, fieldName, tableName))
// 	if err != nil {
// 		return fmt.Errorf("json value check failed: %v", err)
// 	}

// 	if len(res.Records) == 0 {
// 		return fmt.Errorf("bad json: empty result set from %s", tableName)
// 	}

// 	jsonVal := res.Records[0][0].Value
// 	if jsonVal != value {
// 		return fmt.Errorf("bad json value in field %s of column %s: %v. expected: %v", fieldName, colName, jsonVal, value)
// 	}
// 	return nil
// }

func (s PeerFlowE2ETestSuiteCH) compareTableContentsWithDiffSelectorsCH(tableName, pgSelector, chSelector string) {
	pgRows, err := e2e.GetPgRows(s.conn, s.pgSuffix, tableName, pgSelector)
	require.NoError(s.t, err)

	chRows, err := s.GetRows(tableName, chSelector)
	require.NoError(s.t, err)

	e2e.RequireEqualRecordBatches(s.t, pgRows, chRows)
}

// func (s PeerFlowE2ETestSuiteCH) Test_Complete_QRep_Flow_Avro_CH() {
// 	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

// 	numRows := 10

// 	tblName := "test_qrep_flow_avro_ch"
// 	s.setupSourceTable(tblName, numRows)

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
// 		false,
// 		"",
// 	)
// 	qrepConfig.SetupWatermarkTableOnDestination = true
// 	require.NoError(s.t, err)

// 	e2e.RunQrepFlowWorkflow(env, qrepConfig)

// 	// Verify workflow completes without error
// 	require.True(s.t, env.IsWorkflowCompleted())

// 	err = env.GetWorkflowError()
// 	require.NoError(s.t, err)

// 	sel := e2e.GetOwnersSelectorStringsCH()
// 	s.compareTableContentsWithDiffSelectorsCH(tblName, sel[0], sel[1])

// 	err = s.checkJSONValue(dstSchemaQualified, "f7", "key", "\"value\"")
// 	require.NoError(s.t, err)
// }

// func (s PeerFlowE2ETestSuiteCH) Test_Complete_QRep_Flow_Avro_CH_Upsert_Simple() {
// 	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

// 	numRows := 10

// 	tblName := "test_qrep_flow_avro_ch_ups"
// 	s.setupSourceTable(tblName, numRows)

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
// 		false,
// 		"",
// 	)
// 	qrepConfig.WriteMode = &protos.QRepWriteMode{
// 		WriteType:        protos.QRepWriteType_QREP_WRITE_MODE_UPSERT,
// 		UpsertKeyColumns: []string{"id"},
// 	}
// 	qrepConfig.SetupWatermarkTableOnDestination = true
// 	require.NoError(s.t, err)

// 	e2e.RunQrepFlowWorkflow(env, qrepConfig)

// 	// Verify workflow completes without error
// 	require.True(s.t, env.IsWorkflowCompleted())

// 	err = env.GetWorkflowError()
// 	require.NoError(s.t, err)

// 	sel := e2e.GetOwnersSelectorStringsCH()
// 	s.compareTableContentsWithDiffSelectorsCH(tblName, sel[0], sel[1])
// }

func (s PeerFlowE2ETestSuiteCH) Test_Complete_QRep_Flow_Avro_CH_S3() {
	fieldNames := []string{"id", "ownerable_type", "created_at", "updated_at"}
	fmt.Printf("\n*********************************............Test_Complete_QRep_Flow_Avro_CH_S3")
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	numRows := 10

	tblName := "test_qrep_flow_avro_ch_s3"
	s.setupSourceTable(tblName, numRows, fieldNames)
	s.setupCHDestinationTable(tblName, fieldNames) //As currently qrep doesnot create destination table on its own
	fmt.Printf("\n*********************************............Test_Complete_QRep_Flow_Avro_CH_S3 1 setupSourceTable done\n")

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
		false,
		"",
	)
	fmt.Printf("\n*********************************............Test_Complete_QRep_Flow_Avro_CH_S3 2\n")
	require.NoError(s.t, err)
	qrepConfig.StagingPath = fmt.Sprintf("s3://peerdb-test-bucket/avro/%s", uuid.New())
	qrepConfig.SetupWatermarkTableOnDestination = true
	fmt.Printf("\n*********************************............Test_Complete_QRep_Flow_Avro_CH_S3 3\n")
	e2e.RunQrepFlowWorkflow(env, qrepConfig)
	fmt.Printf("\n*********************************............Test_Complete_QRep_Flow_Avro_CH_S3 4\n")
	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	fmt.Printf("\n*********************************............Test_Complete_QRep_Flow_Avro_CH_S3 5\n")
	err = env.GetWorkflowError()
	fmt.Printf("\n*********************************............Test_Complete_QRep_Flow_Avro_CH_S3 5.5\n %+v", err)
	require.NoError(s.t, err)
	fmt.Printf("\n*********************************............Test_Complete_QRep_Flow_Avro_CH_S3 6\n")
	sel := e2e.GetOwnersSelectorStringsCH(fieldNames)
	fmt.Printf("\n*********************************............Test_Complete_QRep_Flow_Avro_CH_S3 7\n")
	s.compareTableContentsWithDiffSelectorsCH(tblName, sel[0], sel[1])
	fmt.Printf("\n*********************************............Test_Complete_QRep_Flow_Avro_CH_S3 8\n")
}

// func (s PeerFlowE2ETestSuiteCH) Test_Complete_QRep_Flow_Avro_CH_Upsert_XMIN() {
// 	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

// 	numRows := 10

// 	tblName := "test_qrep_flow_avro_ch_ups_xmin"
// 	s.setupSourceTable(tblName, numRows)

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
// 		false,
// 		"",
// 	)
// 	qrepConfig.WriteMode = &protos.QRepWriteMode{
// 		WriteType:        protos.QRepWriteType_QREP_WRITE_MODE_UPSERT,
// 		UpsertKeyColumns: []string{"id"},
// 	}
// 	qrepConfig.WatermarkColumn = "xmin"
// 	qrepConfig.SetupWatermarkTableOnDestination = true
// 	require.NoError(s.t, err)

// 	e2e.RunXminFlowWorkflow(env, qrepConfig)

// 	// Verify workflow completes without error
// 	require.True(s.t, env.IsWorkflowCompleted())

// 	err = env.GetWorkflowError()
// 	require.NoError(s.t, err)

// 	sel := e2e.GetOwnersSelectorStringsCH()
// 	s.compareTableContentsWithDiffSelectorsCH(tblName, sel[0], sel[1])
// }

// func (s PeerFlowE2ETestSuiteCH) Test_Complete_QRep_Flow_Avro_CH_S3_Integration() {
// 	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

// 	numRows := 10

// 	tblName := "test_qrep_flow_avro_ch_s3_int"
// 	s.setupSourceTable(tblName, numRows)

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
// 		false,
// 		"",
// 	)
// 	require.NoError(s.t, err)
// 	qrepConfig.StagingPath = fmt.Sprintf("s3://peerdb-test-bucket/avro/%s", uuid.New())
// 	qrepConfig.SetupWatermarkTableOnDestination = true

// 	e2e.RunQrepFlowWorkflow(env, qrepConfig)

// 	// Verify workflow completes without error
// 	require.True(s.t, env.IsWorkflowCompleted())

// 	err = env.GetWorkflowError()
// 	require.NoError(s.t, err)

// 	sel := e2e.GetOwnersSelectorStringsCH()
// 	s.compareTableContentsWithDiffSelectorsCH(tblName, sel[0], sel[1])
// }

// func (s PeerFlowE2ETestSuiteCH) Test_PeerDB_Columns_QRep_CH() {
// 	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

// 	numRows := 10

// 	tblName := "test_qrep_columns_ch"
// 	s.setupSourceTable(tblName, numRows)

// 	dstSchemaQualified := fmt.Sprintf("%s.%s", s.chHelper.testSchemaName, tblName)

// 	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
// 		s.pgSuffix, tblName)

// 	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
// 		"test_columns_qrep_ch",
// 		fmt.Sprintf("e2e_test_%s.%s", s.pgSuffix, tblName),
// 		dstSchemaQualified,
// 		query,
// 		s.chHelper.Peer,
// 		"",
// 		true,
// 		"_PEERDB_SYNCED_AT",
// 	)
// 	qrepConfig.WriteMode = &protos.QRepWriteMode{
// 		WriteType:        protos.QRepWriteType_QREP_WRITE_MODE_UPSERT,
// 		UpsertKeyColumns: []string{"id"},
// 	}
// 	qrepConfig.SetupWatermarkTableOnDestination = true
// 	require.NoError(s.t, err)

// 	e2e.RunQrepFlowWorkflow(env, qrepConfig)

// 	// Verify workflow completes without error
// 	require.True(s.t, env.IsWorkflowCompleted())

// 	err = env.GetWorkflowError()
// 	require.NoError(s.t, err)

// 	err = s.chHelper.checkSyncedAt(fmt.Sprintf(`SELECT "_PEERDB_SYNCED_AT" FROM %s.%s`,
// 		s.chHelper.testSchemaName, tblName))
// 	require.NoError(s.t, err)
// }
