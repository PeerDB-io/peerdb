package e2e

import (
	"context"
	"fmt"
	"strings"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
	"github.com/google/uuid"
)

func (s *E2EPeerFlowTestSuite) setupSourceTable(tableName string, rowCount int) {
	tblFields := []string{
		"id UUID NOT NULL PRIMARY KEY",
		"card_id UUID",
		`from_v TIMESTAMP NOT NULL`,
		"price NUMERIC",
		"created_at TIMESTAMP NOT NULL",
		"updated_at TIMESTAMP NOT NULL",
		"transaction_hash BYTEA",
		"ownerable_type VARCHAR",
		"ownerable_id UUID",
		"user_nonce INTEGER",
		"transfer_type INTEGER DEFAULT 0 NOT NULL",
		"blockchain INTEGER NOT NULL",
		"deal_type VARCHAR",
		"deal_id UUID",
		"ethereum_transaction_id UUID",
		"ignore_price BOOLEAN DEFAULT false",
		"card_eth_value DOUBLE PRECISION",
		"paid_eth_price DOUBLE PRECISION",
		"card_bought_notified BOOLEAN DEFAULT false NOT NULL",
		"address NUMERIC",
		"account_id UUID",
		"asset_id NUMERIC NOT NULL",
		"status INTEGER",
		"transaction_id UUID",
		"settled_at TIMESTAMP",
		"reference_id VARCHAR",
		"settle_at TIMESTAMP",
		"settlement_delay_reason INTEGER",
	}

	tblFieldStr := strings.Join(tblFields, ",")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE e2e_test.%s (
			%s
		);`, tableName, tblFieldStr))
	s.NoError(err)

	fmt.Printf("created table on postgres: e2e_test.%s\n", tableName)

	// insert rows into the source table
	for i := 0; i < rowCount; i++ {
		_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO e2e_test.%s (
				id, card_id, from_v, price, created_at,
				updated_at, transaction_hash, ownerable_type, ownerable_id,
				user_nonce, transfer_type, blockchain, deal_type,
				deal_id, ethereum_transaction_id, ignore_price, card_eth_value,
				paid_eth_price, card_bought_notified, address, account_id,
				asset_id, status, transaction_id, settled_at, reference_id,
				settle_at, settlement_delay_reason
			) VALUES (
				'%s', '%s', CURRENT_TIMESTAMP, 3.86487206688919, CURRENT_TIMESTAMP,
				CURRENT_TIMESTAMP, E'\\\\xDEADBEEF', 'type1', '%s',
				1, 0, 1, 'dealType1',
				'%s', '%s', false, 1.2345,
				1.2345, false, 12345, '%s',
				12345, 1, '%s', CURRENT_TIMESTAMP, 'refID',
				CURRENT_TIMESTAMP, 1
			);
		`, tableName, uuid.New().String(), uuid.New().String(), uuid.New().String(),
			uuid.New().String(), uuid.New().String(), uuid.New().String(), uuid.New().String()))
		s.NoError(err)
	}
}

func (s *E2EPeerFlowTestSuite) setupDestinationTable(dstTable string) {
	dstTableName := fmt.Sprintf("%s.%s", s.bqHelper.Config.DatasetId, dstTable)
	colWithTypes := []string{
		"id STRING",
		"card_id STRING",
		"from_v TIMESTAMP",
		"price NUMERIC",
		"created_at TIMESTAMP",
		"updated_at TIMESTAMP",
		"transaction_hash BYTES",
		"ownerable_type STRING",
		"ownerable_id STRING",
		"user_nonce INT64",
		"transfer_type INT64",
		"blockchain INT64",
		"deal_type STRING",
		"deal_id STRING",
		"ethereum_transaction_id STRING",
		"ignore_price BOOL",
		"card_eth_value FLOAT64",
		"paid_eth_price FLOAT64",
		"card_bought_notified BOOL",
		"address NUMERIC",
		"account_id STRING",
		"asset_id NUMERIC",
		"status INT64",
		"transaction_id STRING",
		"settled_at TIMESTAMP",
		"reference_id STRING",
		"settle_at TIMESTAMP",
		"settlement_delay_reason INT64",
	}

	dstTableCmd := fmt.Sprintf(
		"CREATE TABLE %s (%s)",
		dstTableName,
		strings.Join(colWithTypes, ","),
	)
	err := s.bqHelper.RunCommand(dstTableCmd)

	// fail if table creation fails
	s.NoError(err)

	fmt.Printf("created table on bigquery: %s. %v\n", dstTableName, err)
}

func (s *E2EPeerFlowTestSuite) createWorkflowConfig(
	flowJobName string,
	sourceTable string,
	dstTable string,
	query string,
	syncMode protos.QRepSyncMode,
) *protos.QRepConfig {
	connectionGen := QRepFlowConnectionGenerationConfig{
		FlowJobName:                flowJobName,
		SourceTableIdentifier:      sourceTable,
		DestinationTableIdentifier: dstTable,
		PostgresPort:               postgresPort,
		Destination:                s.bqHelper.Peer,
	}

	watermark := "updated_at"

	qrepConfig, err := connectionGen.GenerateQRepConfig(query, watermark, syncMode)
	s.NoError(err)

	qrepConfig.InitalCopyOnly = true

	return qrepConfig
}

func (s *E2EPeerFlowTestSuite) compareTableContents(tableName string) {
	// read rows from source table
	pgQueryExecutor := connpostgres.NewQRepQueryExecutor(s.pool, context.Background())
	pgRows, err := pgQueryExecutor.ExecuteAndProcessQuery(
		fmt.Sprintf("SELECT * FROM e2e_test.%s ORDER BY id", tableName),
	)
	s.NoError(err)

	// read rows from destination table
	qualifiedTableName := fmt.Sprintf("`%s.%s`", s.bqHelper.Config.DatasetId, tableName)
	bqRows, err := s.bqHelper.ExecuteAndProcessQuery(
		fmt.Sprintf("SELECT * FROM %s ORDER BY id", qualifiedTableName),
	)
	s.NoError(err)

	s.True(pgRows.Equals(bqRows), "rows from source and destination tables are not equal")
}

// Test_Complete_QRep_Flow tests a complete flow with data in the source table.
// The test inserts 10 rows into the source table and verifies that the data is
// correctly synced to the destination table this runs a QRep Flow.
func (s *E2EPeerFlowTestSuite) Test_Complete_QRep_Flow_Multi_Insert() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	numRows := 10

	tblName := "test_qrep_flow_multi_insert"
	s.setupSourceTable(tblName, numRows)
	s.setupDestinationTable(tblName)

	qrepConfig := s.createWorkflowConfig("test_qrep_flow_mi",
		"e2e_test."+tblName,
		tblName,
		"SELECT * FROM e2e_test."+tblName,
		protos.QRepSyncMode_QREP_SYNC_MODE_MULTI_INSERT)
	env.ExecuteWorkflow(peerflow.QRepFlowWorkflow, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())

	// assert that error contains "invalid connection configs"
	err := env.GetWorkflowError()
	s.NoError(err)

	count, err := s.bqHelper.CountRows(tblName)
	s.NoError(err)

	s.Equal(numRows, count)

	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) Test_Complete_QRep_Flow_Avro() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	numRows := 10

	tblName := "test_qrep_flow_avro"
	s.setupSourceTable(tblName, numRows)
	s.setupDestinationTable(tblName)

	qrepConfig := s.createWorkflowConfig("test_qrep_flow_avro",
		"e2e_test."+tblName,
		tblName,
		"SELECT * FROM e2e_test."+tblName,
		protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO)
	env.ExecuteWorkflow(peerflow.QRepFlowWorkflow, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())

	// assert that error contains "invalid connection configs"
	err := env.GetWorkflowError()
	s.NoError(err)

	s.compareTableContents(tblName)

	env.AssertExpectations(s.T())
}
