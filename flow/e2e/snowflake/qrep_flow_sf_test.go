package e2e_snowflake

import (
	"context"
	"fmt"
	"testing"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/assert"
)

func (s *PeerFlowE2ETestSuiteSF) setupSourceTable(t *testing.T, tableName string, rowCount int) {
	err := e2e.CreateSourceTableQRep(s.pool, e2e.TestIdent(t), tableName)
	assert.NoError(t, err)
	err = e2e.PopulateSourceTable(s.pool, e2e.TestIdent(t), tableName, rowCount)
	assert.NoError(t, err)
}

func (s *PeerFlowE2ETestSuiteSF) setupSFDestinationTable(t *testing.T, dstTable string) {
	schema := e2e.GetOwnersSchema()
	err := s.sfHelper.CreateTable(dstTable, schema)

	// fail if table creation fails
	if err != nil {
		assert.FailNow(t, "unable to create table on snowflake", err)
	}

	fmt.Printf("created table on snowflake: %s.%s. %v\n", s.sfHelper.testSchemaName, dstTable, err)
}

func (s *PeerFlowE2ETestSuiteSF) compareTableContentsSF(t *testing.T, tableName string, selector string, caseSensitive bool) {
	// read rows from source table
	pgQueryExecutor := connpostgres.NewQRepQueryExecutor(s.pool, context.Background(), "testflow", "testpart")
	pgQueryExecutor.SetTestEnv(true)
	pgRows, err := pgQueryExecutor.ExecuteAndProcessQuery(
		fmt.Sprintf("SELECT %s FROM e2e_test_%s.%s ORDER BY id", selector, e2e.TestIdent(t), tableName),
	)
	require.NoError(t, err)

	// read rows from destination table
	qualifiedTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tableName)
	var sfSelQuery string
	if caseSensitive {
		sfSelQuery = fmt.Sprintf(`SELECT %s FROM %s ORDER BY "id"`, selector, qualifiedTableName)
	} else {
		sfSelQuery = fmt.Sprintf(`SELECT %s FROM %s ORDER BY id`, selector, qualifiedTableName)
	}
	fmt.Printf("running query on snowflake: %s\n", sfSelQuery)

	sfRows, err := s.sfHelper.ExecuteAndProcessQuery(sfSelQuery)
	require.NoError(t, err)

	t.Logf("compare %v = %v", pgRows, sfRows)

	assert.True(t, pgRows.Equals(sfRows), "rows from source and destination tables are not equal")
}

func (s *PeerFlowE2ETestSuiteSF) Test_Complete_QRep_Flow_Avro_SF(t *testing.T) {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	numRows := 10

	tblName := "test_qrep_flow_avro_sf"
	s.setupSourceTable(t, tblName, numRows)
	s.setupSFDestinationTable(t, tblName)

	dstSchemaQualified := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		e2e.TestIdent(t), tblName)

	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
		"test_qrep_flow_avro_sf",
		fmt.Sprintf("e2e_test_%s.%s", e2e.TestIdent(t), tblName),
		dstSchemaQualified,
		query,
		protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO,
		s.sfHelper.Peer,
		"",
	)
	assert.NoError(t, err)

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	assert.True(t, env.IsWorkflowCompleted())

	// assert that error contains "invalid connection configs"
	err = env.GetWorkflowError()
	assert.NoError(t, err)

	sel := e2e.GetOwnersSelectorString()
	s.compareTableContentsSF(t, tblName, sel, true)

	env.AssertExpectations(t)
}

func (s *PeerFlowE2ETestSuiteSF) Test_Complete_QRep_Flow_Avro_SF_Upsert_Simple(t *testing.T) {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	numRows := 10

	tblName := "test_qrep_flow_avro_sf_ups"
	s.setupSourceTable(t, tblName, numRows)
	s.setupSFDestinationTable(t, tblName)

	dstSchemaQualified := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		e2e.TestIdent(t), tblName)

	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
		"test_qrep_flow_avro_sf",
		fmt.Sprintf("e2e_test_%s.%s", e2e.TestIdent(t), tblName),
		dstSchemaQualified,
		query,
		protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO,
		s.sfHelper.Peer,
		"",
	)
	qrepConfig.WriteMode = &protos.QRepWriteMode{
		WriteType:        protos.QRepWriteType_QREP_WRITE_MODE_UPSERT,
		UpsertKeyColumns: []string{"id"},
	}
	assert.NoError(t, err)

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	assert.True(t, env.IsWorkflowCompleted())

	// assert that error contains "invalid connection configs"
	err = env.GetWorkflowError()
	assert.NoError(t, err)

	sel := e2e.GetOwnersSelectorString()
	s.compareTableContentsSF(t, tblName, sel, true)

	env.AssertExpectations(t)
}

func (s *PeerFlowE2ETestSuiteSF) Test_Complete_QRep_Flow_Avro_SF_S3(t *testing.T) {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	numRows := 10

	tblName := "test_qrep_flow_avro_sf_s3"
	s.setupSourceTable(t, tblName, numRows)
	s.setupSFDestinationTable(t, tblName)

	dstSchemaQualified := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		e2e.TestIdent(t), tblName)

	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
		"test_qrep_flow_avro_sf",
		s.attachSchemaSuffix(t, tblName),
		dstSchemaQualified,
		query,
		protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO,
		s.sfHelper.Peer,
		"",
	)
	assert.NoError(t, err)
	qrepConfig.StagingPath = fmt.Sprintf("s3://peerdb-test-bucket/avro/%s", uuid.New())

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	assert.True(t, env.IsWorkflowCompleted())

	err = env.GetWorkflowError()
	assert.NoError(t, err)

	sel := e2e.GetOwnersSelectorString()
	s.compareTableContentsSF(t, tblName, sel, true)

	env.AssertExpectations(t)
}
func (s *PeerFlowE2ETestSuiteSF) Test_Complete_QRep_Flow_Avro_SF_Upsert_XMIN(t *testing.T) {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	numRows := 10

	tblName := "test_qrep_flow_avro_sf_ups_xmin"
	s.setupSourceTable(t, tblName, numRows)
	s.setupSFDestinationTable(t, tblName)

	dstSchemaQualified := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE xmin::text::bigint BETWEEN {{.start}} AND {{.end}}",
		e2e.TestIdent(t), tblName)

	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
		"test_qrep_flow_avro_sf_xmin",
		fmt.Sprintf("e2e_test_%s.%s", e2e.TestIdent(t), tblName),
		dstSchemaQualified,
		query,
		protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO,
		s.sfHelper.Peer,
		"",
	)
	qrepConfig.WriteMode = &protos.QRepWriteMode{
		WriteType:        protos.QRepWriteType_QREP_WRITE_MODE_UPSERT,
		UpsertKeyColumns: []string{"id"},
	}
	qrepConfig.WatermarkColumn = "xmin"
	assert.NoError(t, err)

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	assert.True(t, env.IsWorkflowCompleted())

	err = env.GetWorkflowError()
	assert.NoError(t, err)

	sel := e2e.GetOwnersSelectorString()
	s.compareTableContentsSF(t, tblName, sel, true)

	env.AssertExpectations(t)
}

func (s *PeerFlowE2ETestSuiteSF) Test_Complete_QRep_Flow_Avro_SF_S3_Integration(t *testing.T) {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	numRows := 10

	tblName := "test_qrep_flow_avro_sf_s3_int"
	s.setupSourceTable(t, tblName, numRows)
	s.setupSFDestinationTable(t, tblName)

	dstSchemaQualified := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		e2e.TestIdent(t), tblName)

	sfPeer := s.sfHelper.Peer
	sfPeer.GetSnowflakeConfig().S3Integration = "peerdb_s3_integration"

	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
		"test_qrep_flow_avro_sf_int",
		s.attachSchemaSuffix(t, tblName),
		dstSchemaQualified,
		query,
		protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO,
		sfPeer,
		"",
	)
	assert.NoError(t, err)
	qrepConfig.StagingPath = fmt.Sprintf("s3://peerdb-test-bucket/avro/%s", uuid.New())

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	assert.True(t, env.IsWorkflowCompleted())

	err = env.GetWorkflowError()
	assert.NoError(t, err)

	sel := e2e.GetOwnersSelectorString()
	s.compareTableContentsSF(t, tblName, sel, true)

	env.AssertExpectations(t)
}
