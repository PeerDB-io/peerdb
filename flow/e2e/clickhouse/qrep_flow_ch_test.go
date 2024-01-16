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
