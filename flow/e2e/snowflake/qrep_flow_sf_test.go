package e2e_snowflake

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/generated/protos"
)

//nolint:unparam
func (s PeerFlowE2ETestSuiteSF) setupSourceTable(tableName string, numRows int) {
	err := e2e.CreateTableForQRep(s.Conn(), s.pgSuffix, tableName)
	require.NoError(s.t, err)
	err = e2e.PopulateSourceTable(s.Conn(), s.pgSuffix, tableName, numRows)
	require.NoError(s.t, err)
}

func (s PeerFlowE2ETestSuiteSF) checkJSONValue(tableName, colName, fieldName, value string) error {
	res, err := s.sfHelper.ExecuteAndProcessQuery(fmt.Sprintf("SELECT %s:%s FROM %s", colName, fieldName, tableName))
	if err != nil {
		return fmt.Errorf("json value check failed: %v", err)
	}

	if len(res.Records) == 0 {
		return fmt.Errorf("bad json: empty result set from %s", tableName)
	}

	jsonVal := res.Records[0][0].Value
	if jsonVal != value {
		return fmt.Errorf("bad json value in field %s of column %s: %v. expected: %v", fieldName, colName, jsonVal, value)
	}
	return nil
}

func (s PeerFlowE2ETestSuiteSF) compareTableContentsWithDiffSelectorsSF(tableName, pgSelector, sfSelector string) {
	pgRows, err := e2e.GetPgRows(s.conn, s.pgSuffix, tableName, pgSelector)
	require.NoError(s.t, err)

	sfRows, err := s.GetRows(tableName, sfSelector)
	require.NoError(s.t, err)

	e2e.RequireEqualRecordBatches(s.t, pgRows, sfRows)
}

func (s PeerFlowE2ETestSuiteSF) Test_Complete_QRep_Flow_Avro_SF() {
	tc := e2e.NewTemporalClient(s.t)

	numRows := 10

	tblName := "test_qrep_flow_avro_sf"
	s.setupSourceTable(tblName, numRows)

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
	qrepConfig.SetupWatermarkTableOnDestination = true
	require.NoError(s.t, err)

	env := e2e.RunQrepFlowWorkflow(tc, qrepConfig)
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "finish", env.Finished)
	require.NoError(s.t, env.Error())

	sel := e2e.GetOwnersSelectorStringsSF()
	s.compareTableContentsWithDiffSelectorsSF(tblName, sel[0], sel[1])

	err = s.checkJSONValue(dstSchemaQualified, "f7", "key", "\"value\"")
	require.NoError(s.t, err)
}

func (s PeerFlowE2ETestSuiteSF) Test_Complete_QRep_Flow_Avro_SF_Upsert_Simple() {
	tc := e2e.NewTemporalClient(s.t)

	numRows := 10

	tblName := "test_qrep_flow_avro_sf_ups"
	s.setupSourceTable(tblName, numRows)

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
	qrepConfig.SetupWatermarkTableOnDestination = true
	require.NoError(s.t, err)

	env := e2e.RunQrepFlowWorkflow(tc, qrepConfig)
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "finish", env.Finished)
	require.NoError(s.t, env.Error())

	sel := e2e.GetOwnersSelectorStringsSF()
	s.compareTableContentsWithDiffSelectorsSF(tblName, sel[0], sel[1])
}

func (s PeerFlowE2ETestSuiteSF) Test_Complete_QRep_Flow_Avro_SF_S3() {
	tc := e2e.NewTemporalClient(s.t)

	numRows := 10

	tblName := "test_qrep_flow_avro_sf_s3"
	s.setupSourceTable(tblName, numRows)

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
	qrepConfig.SetupWatermarkTableOnDestination = true

	env := e2e.RunQrepFlowWorkflow(tc, qrepConfig)
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "finish", env.Finished)
	require.NoError(s.t, env.Error())

	sel := e2e.GetOwnersSelectorStringsSF()
	s.compareTableContentsWithDiffSelectorsSF(tblName, sel[0], sel[1])
}

func (s PeerFlowE2ETestSuiteSF) Test_Complete_QRep_Flow_Avro_SF_Upsert_XMIN() {
	tc := e2e.NewTemporalClient(s.t)

	numRows := 10

	tblName := "test_qrep_flow_avro_sf_ups_xmin"
	s.setupSourceTable(tblName, numRows)

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
	qrepConfig.SetupWatermarkTableOnDestination = true
	require.NoError(s.t, err)

	env := e2e.RunXminFlowWorkflow(tc, qrepConfig)
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "finish", env.Finished)
	require.NoError(s.t, env.Error())

	sel := e2e.GetOwnersSelectorStringsSF()
	s.compareTableContentsWithDiffSelectorsSF(tblName, sel[0], sel[1])
}

func (s PeerFlowE2ETestSuiteSF) Test_Complete_QRep_Flow_Avro_SF_S3_Integration() {
	tc := e2e.NewTemporalClient(s.t)

	numRows := 10

	tblName := "test_qrep_flow_avro_sf_s3_int"
	s.setupSourceTable(tblName, numRows)

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
	qrepConfig.SetupWatermarkTableOnDestination = true

	env := e2e.RunQrepFlowWorkflow(tc, qrepConfig)
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "finish", env.Finished)
	require.NoError(s.t, env.Error())

	sel := e2e.GetOwnersSelectorStringsSF()
	s.compareTableContentsWithDiffSelectorsSF(tblName, sel[0], sel[1])
}

func (s PeerFlowE2ETestSuiteSF) Test_PeerDB_Columns_QRep_SF() {
	tc := e2e.NewTemporalClient(s.t)

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
	qrepConfig.SetupWatermarkTableOnDestination = true
	require.NoError(s.t, err)

	env := e2e.RunQrepFlowWorkflow(tc, qrepConfig)
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "finish", env.Finished)
	require.NoError(s.t, env.Error())

	err = s.sfHelper.checkSyncedAt(fmt.Sprintf(`SELECT "_PEERDB_SYNCED_AT" FROM %s.%s`,
		s.sfHelper.testSchemaName, tblName))
	require.NoError(s.t, err)
}
