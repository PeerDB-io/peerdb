package e2e

import (
	"context"
	"fmt"
	"strings"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
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

func getOwnersSchema() *model.QRecordSchema {
	return &model.QRecordSchema{
		Fields: []*model.QField{
			{Name: "id", Type: model.QValueKindString, Nullable: true},
			{Name: "card_id", Type: model.QValueKindString, Nullable: true},
			{Name: "from_v", Type: model.QValueKindETime, Nullable: true},
			{Name: "price", Type: model.QValueKindNumeric, Nullable: true},
			{Name: "created_at", Type: model.QValueKindETime, Nullable: true},
			{Name: "updated_at", Type: model.QValueKindETime, Nullable: true},
			{Name: "transaction_hash", Type: model.QValueKindBytes, Nullable: true},
			{Name: "ownerable_type", Type: model.QValueKindString, Nullable: true},
			{Name: "ownerable_id", Type: model.QValueKindString, Nullable: true},
			{Name: "user_nonce", Type: model.QValueKindInt64, Nullable: true},
			{Name: "transfer_type", Type: model.QValueKindInt64, Nullable: true},
			{Name: "blockchain", Type: model.QValueKindInt64, Nullable: true},
			{Name: "deal_type", Type: model.QValueKindString, Nullable: true},
			{Name: "deal_id", Type: model.QValueKindString, Nullable: true},
			{Name: "ethereum_transaction_id", Type: model.QValueKindString, Nullable: true},
			{Name: "ignore_price", Type: model.QValueKindBoolean, Nullable: true},
			{Name: "card_eth_value", Type: model.QValueKindFloat64, Nullable: true},
			{Name: "paid_eth_price", Type: model.QValueKindFloat64, Nullable: true},
			{Name: "card_bought_notified", Type: model.QValueKindBoolean, Nullable: true},
			{Name: "address", Type: model.QValueKindNumeric, Nullable: true},
			{Name: "account_id", Type: model.QValueKindString, Nullable: true},
			{Name: "asset_id", Type: model.QValueKindNumeric, Nullable: true},
			{Name: "status", Type: model.QValueKindInt64, Nullable: true},
			{Name: "transaction_id", Type: model.QValueKindString, Nullable: true},
			{Name: "settled_at", Type: model.QValueKindETime, Nullable: true},
			{Name: "reference_id", Type: model.QValueKindString, Nullable: true},
			{Name: "settle_at", Type: model.QValueKindETime, Nullable: true},
			{Name: "settlement_delay_reason", Type: model.QValueKindInt64, Nullable: true},
		},
	}
}

func (s *E2EPeerFlowTestSuite) setupDestinationTable(dstTable string) {
	schema := getOwnersSchema()
	err := s.bqHelper.CreateTable(dstTable, schema)

	// fail if table creation fails
	s.NoError(err)

	fmt.Printf("created table on bigquery: %s.%s. %v\n", s.bqHelper.Config.DatasetId, dstTable, err)
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
