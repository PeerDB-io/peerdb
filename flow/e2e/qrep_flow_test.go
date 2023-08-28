package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

func (s *E2EPeerFlowTestSuite) createSourceTable(tableName string) {
	tblFields := []string{
		"id UUID NOT NULL PRIMARY KEY",
		"card_id UUID",
		`"from" TIMESTAMP NOT NULL`,
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
		"f1 text[]",
		"f2 bigint[]",
		"f3 int[]",
		"f4 varchar[]",
		"f5 jsonb",
		"f6 jsonb",
		"f7 jsonb",
		"f8 smallint",
	}

	tblFieldStr := strings.Join(tblFields, ",")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE e2e_test.%s (
			%s
		);`, tableName, tblFieldStr))
	s.NoError(err)

	fmt.Printf("created table on postgres: e2e_test.%s\n", tableName)
}

func (s *E2EPeerFlowTestSuite) populateSourceTable(tableName string, rowCount int) {
	var ids []string
	var rows []string
	for i := 0; i < rowCount-1; i++ {
		id := uuid.New().String()
		ids = append(ids, id)
		row := fmt.Sprintf(`
					(
							'%s', '%s', CURRENT_TIMESTAMP, 3.86487206688919, CURRENT_TIMESTAMP,
							CURRENT_TIMESTAMP, E'\\\\xDEADBEEF', 'type1', '%s',
							1, 0, 1, 'dealType1',
							'%s', '%s', false, 1.2345,
							1.2345, false, 12345, '%s',
							12345, 1, '%s', CURRENT_TIMESTAMP, 'refID',
							CURRENT_TIMESTAMP, 1, ARRAY['text1', 'text2'], ARRAY[123, 456], ARRAY[789, 012],
							ARRAY['varchar1', 'varchar2'], '{"key": 8.5}',
							'[{"key1": "value1", "key2": "value2", "key3": "value3"}]',
							'{"key": "value"}', 15
					)`,
			id, uuid.New().String(), uuid.New().String(),
			uuid.New().String(), uuid.New().String(), uuid.New().String(), uuid.New().String())
		rows = append(rows, row)
	}

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO e2e_test.%s (
					id, card_id, "from", price, created_at,
					updated_at, transaction_hash, ownerable_type, ownerable_id,
					user_nonce, transfer_type, blockchain, deal_type,
					deal_id, ethereum_transaction_id, ignore_price, card_eth_value,
					paid_eth_price, card_bought_notified, address, account_id,
					asset_id, status, transaction_id, settled_at, reference_id,
					settle_at, settlement_delay_reason, f1, f2, f3, f4, f5, f6, f7, f8
			) VALUES %s;
	`, tableName, strings.Join(rows, ",")))
	require.NoError(s.T(), err)

	// add a row where all the nullable fields are null
	_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
	INSERT INTO e2e_test.%s (
			id, "from", created_at, updated_at,
			transfer_type, blockchain, card_bought_notified, asset_id
	) VALUES (
			'%s', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
			0, 1, false, 12345
	);
	`, tableName, uuid.New().String()))
	require.NoError(s.T(), err)

	// generate a 20 MB json and update id[0]'s col f5 to it
	v := s.generate20MBJson()
	_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		UPDATE e2e_test.%s SET f5 = '%s' WHERE id = '%s';
	`, tableName, v, ids[0]))
	require.NoError(s.T(), err)
}

func (s *E2EPeerFlowTestSuite) generate20MBJson() []byte {
	xn := make(map[string]interface{})
	for i := 0; i < 215000; i++ {
		xn[uuid.New().String()] = uuid.New().String()
	}

	v, err := json.Marshal(xn)
	require.NoError(s.T(), err)

	return v
}

func (s *E2EPeerFlowTestSuite) setupSourceTable(tableName string, rowCount int) {
	s.createSourceTable(tableName)
	s.populateSourceTable(tableName, rowCount)
}

func getOwnersSchema() *model.QRecordSchema {
	return &model.QRecordSchema{
		Fields: []*model.QField{
			{Name: "id", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "card_id", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "from", Type: qvalue.QValueKindTimestamp, Nullable: true},
			{Name: "price", Type: qvalue.QValueKindNumeric, Nullable: true},
			{Name: "created_at", Type: qvalue.QValueKindTimestamp, Nullable: true},
			{Name: "updated_at", Type: qvalue.QValueKindTimestamp, Nullable: true},
			{Name: "transaction_hash", Type: qvalue.QValueKindBytes, Nullable: true},
			{Name: "ownerable_type", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "ownerable_id", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "user_nonce", Type: qvalue.QValueKindInt64, Nullable: true},
			{Name: "transfer_type", Type: qvalue.QValueKindInt64, Nullable: true},
			{Name: "blockchain", Type: qvalue.QValueKindInt64, Nullable: true},
			{Name: "deal_type", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "deal_id", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "ethereum_transaction_id", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "ignore_price", Type: qvalue.QValueKindBoolean, Nullable: true},
			{Name: "card_eth_value", Type: qvalue.QValueKindFloat64, Nullable: true},
			{Name: "paid_eth_price", Type: qvalue.QValueKindFloat64, Nullable: true},
			{Name: "card_bought_notified", Type: qvalue.QValueKindBoolean, Nullable: true},
			{Name: "address", Type: qvalue.QValueKindNumeric, Nullable: true},
			{Name: "account_id", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "asset_id", Type: qvalue.QValueKindNumeric, Nullable: true},
			{Name: "status", Type: qvalue.QValueKindInt64, Nullable: true},
			{Name: "transaction_id", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "settled_at", Type: qvalue.QValueKindTimestamp, Nullable: true},
			{Name: "reference_id", Type: qvalue.QValueKindString, Nullable: true},
			{Name: "settle_at", Type: qvalue.QValueKindTimestamp, Nullable: true},
			{Name: "settlement_delay_reason", Type: qvalue.QValueKindInt64, Nullable: true},
			{Name: "f1", Type: qvalue.QValueKindArrayString, Nullable: true},
			{Name: "f2", Type: qvalue.QValueKindArrayInt64, Nullable: true},
			{Name: "f3", Type: qvalue.QValueKindArrayInt32, Nullable: true},
			{Name: "f4", Type: qvalue.QValueKindArrayString, Nullable: true},
			{Name: "f5", Type: qvalue.QValueKindJSON, Nullable: true},
			{Name: "f6", Type: qvalue.QValueKindJSON, Nullable: true},
			{Name: "f7", Type: qvalue.QValueKindJSON, Nullable: true},
			{Name: "f8", Type: qvalue.QValueKindInt16, Nullable: true},
		},
	}
}

func getOwnersSelectorString() string {
	schema := getOwnersSchema()
	var fields []string
	for _, field := range schema.Fields {
		// append quoted field name
		fields = append(fields, fmt.Sprintf(`"%s"`, field.Name))
	}
	return strings.Join(fields, ",")
}

func (s *E2EPeerFlowTestSuite) setupBQDestinationTable(dstTable string) {
	schema := getOwnersSchema()
	err := s.bqHelper.CreateTable(dstTable, schema)

	// fail if table creation fails
	require.NoError(s.T(), err)

	fmt.Printf("created table on bigquery: %s.%s. %v\n", s.bqHelper.Config.DatasetId, dstTable, err)
}

func (s *E2EPeerFlowTestSuite) setupSFDestinationTable(dstTable string) {
	schema := getOwnersSchema()
	err := s.sfHelper.CreateTable(dstTable, schema)

	// fail if table creation fails
	if err != nil {
		s.FailNow("unable to create table on snowflake", err)
	}

	fmt.Printf("created table on snowflake: %s.%s. %v\n", s.sfHelper.testSchemaName, dstTable, err)
}

func (s *E2EPeerFlowTestSuite) createQRepWorkflowConfig(
	flowJobName string,
	sourceTable string,
	dstTable string,
	query string,
	syncMode protos.QRepSyncMode,
	dest *protos.Peer,
	stagingPath string,
) *protos.QRepConfig {
	connectionGen := QRepFlowConnectionGenerationConfig{
		FlowJobName:                flowJobName,
		WatermarkTable:             sourceTable,
		DestinationTableIdentifier: dstTable,
		PostgresPort:               postgresPort,
		Destination:                dest,
		StagingPath:                stagingPath,
	}

	watermark := "updated_at"

	qrepConfig, err := connectionGen.GenerateQRepConfig(query, watermark, syncMode)
	s.NoError(err)

	qrepConfig.InitialCopyOnly = true

	return qrepConfig
}

func (s *E2EPeerFlowTestSuite) compareTableContentsBQ(tableName string, colsString string) {
	// read rows from source table
	pgQueryExecutor := connpostgres.NewQRepQueryExecutor(s.pool, context.Background(), "testflow", "testpart")
	pgQueryExecutor.SetTestEnv(true)

	pgRows, err := pgQueryExecutor.ExecuteAndProcessQuery(
		fmt.Sprintf("SELECT %s FROM e2e_test.%s ORDER BY id", colsString, tableName),
		"test_flow", "test part",
	)
	s.NoError(err)

	// read rows from destination table
	qualifiedTableName := fmt.Sprintf("`%s.%s`", s.bqHelper.Config.DatasetId, tableName)
	bqRows, err := s.bqHelper.ExecuteAndProcessQuery(
		fmt.Sprintf("SELECT %s FROM %s ORDER BY id", colsString, qualifiedTableName),
	)
	s.NoError(err)

	s.True(pgRows.Equals(bqRows), "rows from source and destination tables are not equal")
}

func (s *E2EPeerFlowTestSuite) compareTableContentsSF(tableName string, selector string, caseSensitive bool) {
	// read rows from source table
	pgQueryExecutor := connpostgres.NewQRepQueryExecutor(s.pool, context.Background(), "testflow", "testpart")
	pgQueryExecutor.SetTestEnv(true)
	pgRows, err := pgQueryExecutor.ExecuteAndProcessQuery(
		fmt.Sprintf("SELECT %s FROM e2e_test.%s ORDER BY id", selector, tableName),
		"test_flow", "test part",
	)
	require.NoError(s.T(), err)

	// read rows from destination table
	qualifiedTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tableName)
	var sfSelQuery string
	if caseSensitive {
		sfSelQuery = fmt.Sprintf(`SELECT %s FROM %s ORDER BY "id"`, selector, qualifiedTableName)
	} else {
		sfSelQuery = fmt.Sprintf(`SELECT %s FROM %s ORDER BY id`, selector, qualifiedTableName)
	}
	fmt.Printf("running query on snowflake: %s\n", sfSelQuery)

	// sleep for 1 min for debugging
	// time.Sleep(1 * time.Minute)

	sfRows, err := s.sfHelper.ExecuteAndProcessQuery(sfSelQuery)
	require.NoError(s.T(), err)

	s.True(pgRows.Equals(sfRows), "rows from source and destination tables are not equal")
}

func (s *E2EPeerFlowTestSuite) comparePGTables(srcSchemaQualified, dstSchemaQualified string) error {
	// Execute the two EXCEPT queries
	err := s.compareQuery(srcSchemaQualified, dstSchemaQualified)
	if err != nil {
		return err
	}

	err = s.compareQuery(dstSchemaQualified, srcSchemaQualified)
	if err != nil {
		return err
	}

	// If no error is returned, then the contents of the two tables are the same
	return nil
}

func (s *E2EPeerFlowTestSuite) compareQuery(schema1, schema2 string) error {
	query := fmt.Sprintf("SELECT * FROM %s EXCEPT SELECT * FROM %s", schema1, schema2)
	rows, _ := s.pool.Query(context.Background(), query)

	defer rows.Close()
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return err
		}

		columns := rows.FieldDescriptions()

		for i, value := range values {
			fmt.Printf("%s: %v\n", columns[i].Name, value)
		}
		fmt.Println("---")
	}

	return rows.Err()
}

// NOTE: Disabled due to large JSON tests being added: https://github.com/PeerDB-io/peerdb/issues/309

// Test_Complete_QRep_Flow tests a complete flow with data in the source table.
// The test inserts 10 rows into the source table and verifies that the data is
// // correctly synced to the destination table this runs a QRep Flow.
// func (s *E2EPeerFlowTestSuite) Test_Complete_QRep_Flow_Multi_Insert() {
// 	env := s.NewTestWorkflowEnvironment()
// 	registerWorkflowsAndActivities(env)

// 	numRows := 10

// 	tblName := "test_qrep_flow_multi_insert"
// 	s.setupSourceTable(tblName, numRows)
// 	s.setupBQDestinationTable(tblName)

// 	query := fmt.Sprintf("SELECT * FROM e2e_test.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}", tblName)

// 	qrepConfig := s.createQRepWorkflowConfig("test_qrep_flow_mi",
// 		"e2e_test."+tblName,
// 		tblName,
// 		query,
// 		protos.QRepSyncMode_QREP_SYNC_MODE_MULTI_INSERT,
// 		s.bqHelper.Peer)
// 	runQrepFlowWorkflow(env, qrepConfig)

// 	// Verify workflow completes without error
// 	s.True(env.IsWorkflowCompleted())

// 	// assert that error contains "invalid connection configs"
// 	err := env.GetWorkflowError()
// 	s.NoError(err)

// 	count, err := s.bqHelper.CountRows(tblName)
// 	s.NoError(err)

// 	s.Equal(numRows, count)

// 	env.AssertExpectations(s.T())
// }

func (s *E2EPeerFlowTestSuite) Test_Complete_QRep_Flow_Avro() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	numRows := 10

	tblName := "test_qrep_flow_avro"
	s.setupSourceTable(tblName, numRows)
	s.setupBQDestinationTable(tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}", tblName)

	qrepConfig := s.createQRepWorkflowConfig("test_qrep_flow_avro",
		"e2e_test."+tblName,
		tblName,
		query,
		protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO,
		s.bqHelper.Peer,
		"peerdb_staging")
	runQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())

	// assert that error contains "invalid connection configs"
	err := env.GetWorkflowError()
	s.NoError(err)

	s.compareTableContentsBQ(tblName, "*")

	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) Test_Complete_QRep_Flow_Avro_SF() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	numRows := 10

	tblName := "test_qrep_flow_avro_sf"
	s.setupSourceTable(tblName, numRows)
	s.setupSFDestinationTable(tblName)

	dstSchemaQualified := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}", tblName)

	qrepConfig := s.createQRepWorkflowConfig(
		"test_qrep_flow_avro_Sf",
		"e2e_test."+tblName,
		dstSchemaQualified,
		query,
		protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO,
		s.sfHelper.Peer,
		"",
	)

	runQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())

	// assert that error contains "invalid connection configs"
	err := env.GetWorkflowError()
	s.NoError(err)

	sel := getOwnersSelectorString()
	s.compareTableContentsSF(tblName, sel, true)

	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) Test_Complete_QRep_Flow_Avro_SF_Upsert_Simple() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	numRows := 10

	tblName := "test_qrep_flow_avro_sf_ups"
	s.setupSourceTable(tblName, numRows)
	s.setupSFDestinationTable(tblName)

	dstSchemaQualified := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test.%s WHERE updated_at >= {{.start}} AND updated_at < {{.end}}", tblName)

	qrepConfig := s.createQRepWorkflowConfig(
		"test_qrep_flow_avro_Sf",
		"e2e_test."+tblName,
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

	runQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())

	// assert that error contains "invalid connection configs"
	err := env.GetWorkflowError()
	s.NoError(err)

	sel := getOwnersSelectorString()
	s.compareTableContentsSF(tblName, sel, true)

	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) Test_Complete_QRep_Flow_Multi_Insert_PG() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	numRows := 10

	srcTable := "test_qrep_flow_avro_pg_1"
	s.setupSourceTable(srcTable, numRows)

	dstTable := "test_qrep_flow_avro_pg_2"
	s.createSourceTable(dstTable) // the name is misleading, but this is the destination table

	srcSchemaQualified := fmt.Sprintf("%s.%s", "e2e_test", srcTable)
	dstSchemaQualified := fmt.Sprintf("%s.%s", "e2e_test", dstTable)

	query := fmt.Sprintf("SELECT * FROM e2e_test.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}", srcTable)

	postgresPeer := GeneratePostgresPeer(postgresPort)

	qrepConfig := s.createQRepWorkflowConfig(
		"test_qrep_flow_avro_pg",
		srcSchemaQualified,
		dstSchemaQualified,
		query,
		protos.QRepSyncMode_QREP_SYNC_MODE_MULTI_INSERT,
		postgresPeer,
		"",
	)

	runQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())

	// assert that error contains "invalid connection configs"
	err := env.GetWorkflowError()
	s.NoError(err)

	err = s.comparePGTables(srcSchemaQualified, dstSchemaQualified)
	if err != nil {
		s.FailNow(err.Error())
	}

	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) Test_Complete_QRep_Flow_Avro_SF_S3() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	numRows := 10

	tblName := "test_qrep_flow_avro_sf_s3"
	s.setupSourceTable(tblName, numRows)
	s.setupSFDestinationTable(tblName)

	dstSchemaQualified := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test.%s WHERE updated_at >= {{.start}} AND updated_at < {{.end}}", tblName)

	qrepConfig := s.createQRepWorkflowConfig(
		"test_qrep_flow_avro_sf",
		"e2e_test."+tblName,
		dstSchemaQualified,
		query,
		protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO,
		s.sfHelper.Peer,
		"",
	)
	qrepConfig.StagingPath = fmt.Sprintf("s3://peerdb-test-bucket/avro/%s", uuid.New())

	runQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())

	// assert that error contains "invalid connection configs"
	err := env.GetWorkflowError()
	s.NoError(err)

	sel := getOwnersSelectorString()
	s.compareTableContentsSF(tblName, sel, true)

	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) Test_Complete_QRep_Flow_Avro_SF_S3_Integration() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	numRows := 10

	tblName := "test_qrep_flow_avro_sf_s3_int"
	s.setupSourceTable(tblName, numRows)
	s.setupSFDestinationTable(tblName)

	dstSchemaQualified := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test.%s WHERE updated_at >= {{.start}} AND updated_at < {{.end}}", tblName)

	sfPeer := s.sfHelper.Peer
	sfPeer.GetSnowflakeConfig().S3Integration = "peerdb_s3_integration"

	qrepConfig := s.createQRepWorkflowConfig(
		"test_qrep_flow_avro_sf_int",
		"e2e_test."+tblName,
		dstSchemaQualified,
		query,
		protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO,
		sfPeer,
		"",
	)
	qrepConfig.StagingPath = fmt.Sprintf("s3://peerdb-test-bucket/avro/%s", uuid.New())

	runQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())

	// assert that error contains "invalid connection configs"
	err := env.GetWorkflowError()
	s.NoError(err)

	sel := getOwnersSelectorString()
	s.compareTableContentsSF(tblName, sel, true)

	env.AssertExpectations(s.T())
}

func runQrepFlowWorkflow(env *testsuite.TestWorkflowEnvironment, config *protos.QRepConfig) {
	lastPartition := &protos.QRepPartition{
		PartitionId: "not-applicable-partition",
		Range:       nil,
	}
	numPartitionsProcessed := 0
	env.ExecuteWorkflow(peerflow.QRepFlowWorkflow, config, lastPartition, numPartitionsProcessed)
}
