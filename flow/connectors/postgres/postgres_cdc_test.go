package connpostgres

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/suite"
)

type PostgresCDCTestSuite struct {
	suite.Suite
	connector *PostgresConnector
}

func (suite *PostgresCDCTestSuite) failTestError(err error) {
	if err != nil {
		suite.FailNow(err.Error())
	}
}

func (suite *PostgresCDCTestSuite) dropTable(tableName string) {
	_, err := suite.connector.pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	suite.failTestError(err)
}

func (suite *PostgresCDCTestSuite) insertSimpleRecords(srcTableName string) {
	_, err := suite.connector.pool.Exec(context.Background(),
		fmt.Sprintf("INSERT INTO %s(id, name) VALUES (2, 'quick'), (4, 'brown'), (8, 'fox')", srcTableName))
	suite.failTestError(err)
}

func (suite *PostgresCDCTestSuite) validateInsertedSimpleRecords(records []model.Record, srcTableName string,
	dstTableName string) {
	suite.Equal(3, len(records))
	matchData := []model.RecordItems{
		{"id": qvalue.QValue{Kind: qvalue.QValueKindInt32, Value: int32(2)},
			"name": qvalue.QValue{Kind: qvalue.QValueKindString, Value: "quick"}},
		{"id": qvalue.QValue{Kind: qvalue.QValueKindInt32, Value: int32(4)},
			"name": qvalue.QValue{Kind: qvalue.QValueKindString, Value: "brown"}},
		{"id": qvalue.QValue{Kind: qvalue.QValueKindInt32, Value: int32(8)},
			"name": qvalue.QValue{Kind: qvalue.QValueKindString, Value: "fox"}},
	}
	for idx, record := range records {
		suite.IsType(&model.InsertRecord{}, record)
		insertRecord := record.(*model.InsertRecord)
		suite.Equal(srcTableName, insertRecord.SourceTableName)
		suite.Equal(dstTableName, insertRecord.DestinationTableName)
		suite.Equal(matchData[idx], insertRecord.Items)
	}
}

func (suite *PostgresCDCTestSuite) mutateSimpleRecords(srcTableName string) {
	mutateRecordsTx, err := suite.connector.pool.Begin(context.Background())
	suite.failTestError(err)
	defer func() {
		err := mutateRecordsTx.Rollback(context.Background())
		if err != pgx.ErrTxClosed {
			suite.failTestError(err)
		}
	}()

	_, err = mutateRecordsTx.Exec(context.Background(),
		fmt.Sprintf("UPDATE %s SET name = 'slow' WHERE id = 2", srcTableName))
	suite.failTestError(err)
	_, err = mutateRecordsTx.Exec(context.Background(), fmt.Sprintf("DELETE FROM %s WHERE id = 8", srcTableName))
	suite.failTestError(err)
	err = mutateRecordsTx.Commit(context.Background())
	suite.failTestError(err)
}

func (suite *PostgresCDCTestSuite) validateSimpleMutatedRecords(records []model.Record, srcTableName string,
	dstTableName string) {
	suite.Equal(2, len(records))

	suite.IsType(&model.UpdateRecord{}, records[0])
	updateRecord := records[0].(*model.UpdateRecord)
	suite.Equal(srcTableName, updateRecord.SourceTableName)
	suite.Equal(dstTableName, updateRecord.DestinationTableName)
	suite.Equal(model.RecordItems{}, updateRecord.OldItems)
	suite.Equal(model.RecordItems{"id": qvalue.QValue{Kind: qvalue.QValueKindInt32, Value: int32(2)},
		"name": qvalue.QValue{Kind: qvalue.QValueKindString, Value: "slow"}}, updateRecord.NewItems)

	suite.IsType(&model.DeleteRecord{}, records[1])
	deleteRecord := records[1].(*model.DeleteRecord)
	suite.Equal(srcTableName, deleteRecord.SourceTableName)
	suite.Equal(dstTableName, deleteRecord.DestinationTableName)
	suite.Equal(model.RecordItems{"id": qvalue.QValue{Kind: qvalue.QValueKindInt32, Value: int32(8)},
		"name": qvalue.QValue{Kind: qvalue.QValueKindInvalid, Value: nil}}, deleteRecord.Items)
}

func (suite *PostgresCDCTestSuite) randBytea(n int) []byte {
	b := make([]byte, n)
	//nolint:gosec
	_, err := rand.Read(b)
	suite.failTestError(err)
	return b
}

func (suite *PostgresCDCTestSuite) randString(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	b := make([]byte, n)
	for i := range b {
		//nolint:gosec
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func (suite *PostgresCDCTestSuite) insertToastRecords(srcTableName string) {
	insertRecordsTx, err := suite.connector.pool.Begin(context.Background())
	suite.failTestError(err)
	defer func() {
		err := insertRecordsTx.Rollback(context.Background())
		if err != pgx.ErrTxClosed {
			suite.failTestError(err)
		}
	}()

	for i := 0; i < 4; i++ {
		_, err := insertRecordsTx.Exec(context.Background(),
			fmt.Sprintf("INSERT INTO %s(n_t, lz4_t, n_b, lz4_b) VALUES ($1, $2, $3, $4)", srcTableName),
			suite.randString(32768), suite.randString(32768), suite.randBytea(32768), suite.randBytea(32768))
		suite.failTestError(err)
	}

	err = insertRecordsTx.Commit(context.Background())
	suite.failTestError(err)
}

func (suite *PostgresCDCTestSuite) validateInsertedToastRecords(records []model.Record, srcTableName string,
	dstTableName string) {
	suite.Equal(4, len(records))
	for idx, record := range records {
		suite.IsType(&model.InsertRecord{}, record)
		insertRecord := record.(*model.InsertRecord)
		suite.Equal(srcTableName, insertRecord.SourceTableName)
		suite.Equal(dstTableName, insertRecord.DestinationTableName)
		suite.Equal(5, len(insertRecord.Items))

		suite.Equal(int32(idx+1), insertRecord.Items["id"].Value.(int32))
		suite.Equal(32768, len(insertRecord.Items["n_t"].Value.(string)))
		suite.Equal(32768, len(insertRecord.Items["lz4_t"].Value.(string)))
		suite.Equal(32768, len(insertRecord.Items["n_b"].Value.([]byte)))
		suite.Equal(32768, len(insertRecord.Items["lz4_b"].Value.([]byte)))
	}
}

func (suite *PostgresCDCTestSuite) mutateToastRecords(srcTableName string) {
	mutateRecordsTx, err := suite.connector.pool.Begin(context.Background())
	suite.failTestError(err)
	defer func() {
		err := mutateRecordsTx.Rollback(context.Background())
		if err != pgx.ErrTxClosed {
			suite.failTestError(err)
		}
	}()

	_, err = mutateRecordsTx.Exec(context.Background(), fmt.Sprintf("UPDATE %s SET n_t = $1 WHERE id = 1",
		srcTableName),
		suite.randString(65536))
	suite.failTestError(err)
	_, err = mutateRecordsTx.Exec(context.Background(),
		fmt.Sprintf("UPDATE %s SET lz4_t = $1, n_b = $2, lz4_b = $3 WHERE id = 3", srcTableName),
		suite.randString(65536), suite.randBytea(65536), suite.randBytea(65536))
	suite.failTestError(err)
	_, err = mutateRecordsTx.Exec(context.Background(),
		fmt.Sprintf("UPDATE %s SET n_t = $1, lz4_t = $2, n_b = $3, lz4_b = $4 WHERE id = 4", srcTableName),
		suite.randString(65536), suite.randString(65536), suite.randBytea(65536), suite.randBytea(65536))
	suite.failTestError(err)
	_, err = mutateRecordsTx.Exec(context.Background(),
		fmt.Sprintf("DELETE FROM %s WHERE id = 3", srcTableName))
	suite.failTestError(err)

	err = mutateRecordsTx.Commit(context.Background())
	suite.failTestError(err)
}

func (suite *PostgresCDCTestSuite) validateMutatedToastRecords(records []model.Record, srcTableName string,
	dstTableName string) {
	suite.Equal(4, len(records))

	suite.IsType(&model.UpdateRecord{}, records[0])
	updateRecord := records[0].(*model.UpdateRecord)
	suite.Equal(srcTableName, updateRecord.SourceTableName)
	suite.Equal(dstTableName, updateRecord.DestinationTableName)
	suite.Equal(2, len(updateRecord.NewItems))
	suite.Equal(int32(1), updateRecord.NewItems["id"].Value.(int32))
	suite.Equal(qvalue.QValueKindString, updateRecord.NewItems["n_t"].Kind)
	suite.Equal(65536, len(updateRecord.NewItems["n_t"].Value.(string)))
	suite.Equal(3, len(updateRecord.UnchangedToastColumns))
	suite.True(updateRecord.UnchangedToastColumns["lz4_t"])
	suite.True(updateRecord.UnchangedToastColumns["n_b"])
	suite.True(updateRecord.UnchangedToastColumns["lz4_b"])

	suite.IsType(&model.UpdateRecord{}, records[1])
	updateRecord = records[1].(*model.UpdateRecord)
	suite.Equal(srcTableName, updateRecord.SourceTableName)
	suite.Equal(dstTableName, updateRecord.DestinationTableName)
	suite.Equal(4, len(updateRecord.NewItems))
	suite.Equal(qvalue.QValueKindInt32, updateRecord.NewItems["id"].Kind)
	suite.Equal(int32(3), updateRecord.NewItems["id"].Value.(int32))
	suite.Equal(qvalue.QValueKindString, updateRecord.NewItems["lz4_t"].Kind)
	suite.Equal(65536, len(updateRecord.NewItems["lz4_t"].Value.(string)))
	suite.Equal(qvalue.QValueKindBytes, updateRecord.NewItems["n_b"].Kind)
	suite.Equal(65536, len(updateRecord.NewItems["n_b"].Value.([]byte)))
	suite.Equal(qvalue.QValueKindBytes, updateRecord.NewItems["lz4_b"].Kind)
	suite.Equal(65536, len(updateRecord.NewItems["lz4_b"].Value.([]byte)))
	suite.Equal(1, len(updateRecord.UnchangedToastColumns))
	suite.True(updateRecord.UnchangedToastColumns["n_t"])

	suite.IsType(&model.UpdateRecord{}, records[2])
	updateRecord = records[2].(*model.UpdateRecord)
	suite.Equal(srcTableName, updateRecord.SourceTableName)
	suite.Equal(dstTableName, updateRecord.DestinationTableName)
	suite.Equal(5, len(updateRecord.NewItems))
	suite.Equal(int32(4), updateRecord.NewItems["id"].Value.(int32))
	suite.Equal(qvalue.QValueKindString, updateRecord.NewItems["n_t"].Kind)
	suite.Equal(65536, len(updateRecord.NewItems["n_t"].Value.(string)))
	suite.Equal(qvalue.QValueKindString, updateRecord.NewItems["lz4_t"].Kind)
	suite.Equal(65536, len(updateRecord.NewItems["lz4_t"].Value.(string)))
	suite.Equal(qvalue.QValueKindBytes, updateRecord.NewItems["n_b"].Kind)
	suite.Equal(65536, len(updateRecord.NewItems["n_b"].Value.([]byte)))
	suite.Equal(qvalue.QValueKindBytes, updateRecord.NewItems["lz4_b"].Kind)
	suite.Equal(65536, len(updateRecord.NewItems["lz4_b"].Value.([]byte)))
	suite.Equal(0, len(updateRecord.UnchangedToastColumns))

	suite.IsType(&model.DeleteRecord{}, records[3])
	deleteRecord := records[3].(*model.DeleteRecord)
	suite.Equal(srcTableName, deleteRecord.SourceTableName)
	suite.Equal(dstTableName, deleteRecord.DestinationTableName)
	suite.Equal(5, len(deleteRecord.Items))
	suite.Equal(int32(3), deleteRecord.Items["id"].Value.(int32))
	suite.Equal(qvalue.QValueKindInvalid, deleteRecord.Items["n_t"].Kind)
	suite.Nil(deleteRecord.Items["n_t"].Value)
	suite.Equal(qvalue.QValueKindInvalid, deleteRecord.Items["lz4_t"].Kind)
	suite.Nil(deleteRecord.Items["lz4_t"].Value)
	suite.Equal(qvalue.QValueKindInvalid, deleteRecord.Items["n_b"].Kind)
	suite.Nil(deleteRecord.Items["n_b"].Value)
	suite.Equal(qvalue.QValueKindInvalid, deleteRecord.Items["lz4_b"].Kind)
	suite.Nil(deleteRecord.Items["lz4_b"].Value)
}

func (suite *PostgresCDCTestSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())

	var err error
	suite.connector, err = NewPostgresConnector(context.Background(), &protos.PostgresConfig{
		Host:     "localhost",
		Port:     7132,
		User:     "postgres",
		Password: "postgres",
		Database: "postgres",
	})
	suite.failTestError(err)

	setupTx, err := suite.connector.pool.Begin(context.Background())
	suite.failTestError(err)
	defer func() {
		err := setupTx.Rollback(context.Background())
		if err != pgx.ErrTxClosed {
			suite.failTestError(err)
		}
	}()
	_, err = setupTx.Exec(context.Background(), "DROP SCHEMA IF EXISTS pgpeer_test CASCADE")
	suite.failTestError(err)
	_, err = setupTx.Exec(context.Background(), "CREATE SCHEMA pgpeer_test")
	suite.failTestError(err)
	err = setupTx.Commit(context.Background())
	suite.failTestError(err)
}

func (suite *PostgresCDCTestSuite) TearDownSuite() {
	teardownTx, err := suite.connector.pool.Begin(context.Background())
	suite.failTestError(err)
	defer func() {
		err := teardownTx.Rollback(context.Background())
		if err != pgx.ErrTxClosed {
			suite.failTestError(err)
		}
	}()
	_, err = teardownTx.Exec(context.Background(), "DROP SCHEMA IF EXISTS pgpeer_test CASCADE")
	suite.failTestError(err)
	err = teardownTx.Commit(context.Background())
	suite.failTestError(err)

	suite.True(suite.connector.ConnectionActive())
	err = suite.connector.Close()
	suite.failTestError(err)
	suite.False(suite.connector.ConnectionActive())
}

func (suite *PostgresCDCTestSuite) TestParseSchemaTable() {
	schemaTest1, err := parseSchemaTable("schema")
	suite.Nil(schemaTest1)
	suite.NotNil(err)

	schemaTest2, err := parseSchemaTable("schema.table")
	suite.Equal(&SchemaTable{
		Schema: "schema",
		Table:  "table",
	}, schemaTest2)
	suite.Equal("\"schema\".\"table\"", schemaTest2.String())
	suite.Nil(err)

	schemaTest3, err := parseSchemaTable("database.schema.table")
	suite.Nil(schemaTest3)
	suite.NotNil(err)
}

func (suite *PostgresCDCTestSuite) TestErrorForInvalidConfig() {
	connector, err := NewPostgresConnector(context.Background(), &protos.PostgresConfig{
		Host:     "fakehost",
		Port:     0,
		User:     "fakeuser",
		Password: "fakepassword",
		Database: "fakedatabase",
	})
	suite.Nil(connector)
	suite.NotNil(err)
}

// intended to test how activities react to a table that does not exist.
func (suite *PostgresCDCTestSuite) TestErrorForTableNotExist() {
	nonExistentFlowName := "non_existent_table_testing"
	nonExistentFlowSrcTableName := "pgpeer_test.non_existent_table"
	nonExistentFlowDstTableName := "non_existent_table_dst"

	ensurePullabilityOutput, err := suite.connector.EnsurePullability(&protos.EnsurePullabilityInput{
		FlowJobName:           nonExistentFlowName,
		SourceTableIdentifier: nonExistentFlowSrcTableName,
		PeerConnectionConfig:  nil, // not used by the connector itself.
	})
	suite.Nil(ensurePullabilityOutput)
	suite.Errorf(err, "error getting relation ID for table %s: no rows in result set", nonExistentFlowSrcTableName)

	tableNameMapping := map[string]string{
		nonExistentFlowSrcTableName: nonExistentFlowDstTableName,
	}

	getTblSchemaInput := &protos.GetTableSchemaBatchInput{
		TableIdentifiers:     []string{nonExistentFlowSrcTableName},
		PeerConnectionConfig: nil,
	}

	tableSchema, err := suite.connector.GetTableSchema(getTblSchemaInput)
	suite.Errorf(err, "error getting relation ID for table %s: no rows in result set", nonExistentFlowSrcTableName)
	suite.Nil(tableSchema)
	tableNameSchemaMapping := make(map[string]*protos.TableSchema)
	tableNameSchemaMapping[nonExistentFlowDstTableName] = &protos.TableSchema{
		TableIdentifier: nonExistentFlowSrcTableName,
		Columns: map[string]string{
			"id":   string(qvalue.QValueKindInt32),
			"name": string(qvalue.QValueKindString),
		},
		PrimaryKeyColumn: "id",
	}

	err = suite.connector.PullFlowCleanup(nonExistentFlowName)
	suite.Errorf(err, "error dropping replication slot:"+
		"ERROR: replication slot \"%s\" does not exist (SQLSTATE 42704)", nonExistentFlowName)

	// creating table and the replication slots for it, and dropping before pull records.
	_, err = suite.connector.pool.Exec(context.Background(),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(id INT PRIMARY KEY, name TEXT)", nonExistentFlowSrcTableName))
	suite.failTestError(err)
	ensurePullabilityOutput, err = suite.connector.EnsurePullability(&protos.EnsurePullabilityInput{
		FlowJobName:           nonExistentFlowName,
		SourceTableIdentifier: nonExistentFlowSrcTableName,
		PeerConnectionConfig:  nil, // not used by the connector itself.
	})
	suite.failTestError(err)
	tableRelID := ensurePullabilityOutput.TableIdentifier.GetPostgresTableIdentifier().RelId
	relIDTableNameMapping := map[uint32]string{
		tableRelID: nonExistentFlowSrcTableName,
	}
	err = suite.connector.SetupReplication(nil, &protos.SetupReplicationInput{
		FlowJobName:          nonExistentFlowName,
		TableNameMapping:     tableNameMapping,
		PeerConnectionConfig: nil, // not used by the connector itself.
	})
	suite.failTestError(err)
	suite.dropTable(nonExistentFlowSrcTableName)
	records, err := suite.connector.PullRecords(&model.PullRecordsRequest{
		FlowJobName:            nonExistentFlowName,
		LastSyncState:          nil,
		IdleTimeout:            5 * time.Second,
		MaxBatchSize:           100,
		SrcTableIDNameMapping:  relIDTableNameMapping,
		TableNameMapping:       tableNameMapping,
		TableNameSchemaMapping: tableNameSchemaMapping,
	})
	suite.Equal(0, len(records.Records))
	suite.Nil(err)

	err = suite.connector.PullFlowCleanup(nonExistentFlowName)
	suite.failTestError(err)
}

func (suite *PostgresCDCTestSuite) TestSimpleHappyFlow() {
	simpleHappyFlowName := "simple_happy_flow_testing_flow"
	simpleHappyFlowSrcTableName := "pgpeer_test.simple_table"
	simpleHappyFlowDstTableName := "simple_table_dst"

	_, err := suite.connector.pool.Exec(context.Background(),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(id INT PRIMARY KEY, name TEXT)", simpleHappyFlowSrcTableName))
	suite.failTestError(err)

	ensurePullabilityOutput, err := suite.connector.EnsurePullability(&protos.EnsurePullabilityInput{
		FlowJobName:           simpleHappyFlowName,
		SourceTableIdentifier: simpleHappyFlowSrcTableName,
		PeerConnectionConfig:  nil, // not used by the connector itself.
	})
	suite.failTestError(err)
	tableRelID := ensurePullabilityOutput.TableIdentifier.GetPostgresTableIdentifier().RelId

	relIDTableNameMapping := map[uint32]string{
		tableRelID: simpleHappyFlowSrcTableName,
	}
	tableNameMapping := map[string]string{
		simpleHappyFlowSrcTableName: simpleHappyFlowDstTableName,
	}
	err = suite.connector.SetupReplication(nil, &protos.SetupReplicationInput{
		FlowJobName:          simpleHappyFlowName,
		TableNameMapping:     tableNameMapping,
		PeerConnectionConfig: nil, // not used by the connector itself.
	})
	suite.failTestError(err)

	tableNameSchemaMapping := make(map[string]*protos.TableSchema)

	getTblSchemaInput := &protos.GetTableSchemaBatchInput{
		TableIdentifiers:     []string{simpleHappyFlowSrcTableName},
		PeerConnectionConfig: nil,
	}
	tableNameSchema, err := suite.connector.GetTableSchema(getTblSchemaInput)
	suite.failTestError(err)
	suite.Equal(&protos.TableSchema{
		TableIdentifier: simpleHappyFlowSrcTableName,
		Columns: map[string]string{
			"id":   string(qvalue.QValueKindInt32),
			"name": string(qvalue.QValueKindString),
		},
		PrimaryKeyColumn: "id",
	}, tableNameSchema)
	tableNameSchemaMapping[simpleHappyFlowDstTableName] =
		tableNameSchema.TableNameSchemaMapping[simpleHappyFlowSrcTableName]

	// pulling with no records.
	records, err := suite.connector.PullRecords(&model.PullRecordsRequest{
		FlowJobName:            simpleHappyFlowName,
		LastSyncState:          nil,
		IdleTimeout:            5 * time.Second,
		MaxBatchSize:           100,
		SrcTableIDNameMapping:  relIDTableNameMapping,
		TableNameMapping:       tableNameMapping,
		TableNameSchemaMapping: tableNameSchemaMapping,
	})
	suite.failTestError(err)
	suite.Equal(0, len(records.Records))
	suite.Equal(int64(0), records.FirstCheckPointID)
	suite.Equal(int64(0), records.LastCheckPointID)

	// pulling after inserting records.
	suite.insertSimpleRecords(simpleHappyFlowSrcTableName)
	records, err = suite.connector.PullRecords(&model.PullRecordsRequest{
		FlowJobName:            simpleHappyFlowName,
		LastSyncState:          nil,
		IdleTimeout:            5 * time.Second,
		MaxBatchSize:           100,
		SrcTableIDNameMapping:  relIDTableNameMapping,
		TableNameMapping:       tableNameMapping,
		TableNameSchemaMapping: tableNameSchemaMapping,
	})
	suite.failTestError(err)
	suite.validateInsertedSimpleRecords(records.Records, simpleHappyFlowSrcTableName, simpleHappyFlowDstTableName)
	suite.Greater(records.FirstCheckPointID, int64(0))
	suite.GreaterOrEqual(records.LastCheckPointID, records.FirstCheckPointID)
	currentCheckPointID := records.LastCheckPointID

	// pulling after mutating records.
	suite.mutateSimpleRecords(simpleHappyFlowSrcTableName)
	records, err = suite.connector.PullRecords(&model.PullRecordsRequest{
		FlowJobName: simpleHappyFlowName,
		LastSyncState: &protos.LastSyncState{
			Checkpoint:   records.LastCheckPointID,
			LastSyncedAt: nil,
		},
		IdleTimeout:            5 * time.Second,
		MaxBatchSize:           100,
		SrcTableIDNameMapping:  relIDTableNameMapping,
		TableNameMapping:       tableNameMapping,
		TableNameSchemaMapping: tableNameSchemaMapping,
	})
	suite.failTestError(err)
	suite.validateSimpleMutatedRecords(records.Records, simpleHappyFlowSrcTableName, simpleHappyFlowDstTableName)
	suite.GreaterOrEqual(records.FirstCheckPointID, currentCheckPointID)
	suite.GreaterOrEqual(records.LastCheckPointID, records.FirstCheckPointID)

	err = suite.connector.PullFlowCleanup(simpleHappyFlowName)
	suite.failTestError(err)

	suite.dropTable(simpleHappyFlowSrcTableName)
}

func (suite *PostgresCDCTestSuite) TestAllTypesHappyFlow() {
	allTypesHappyFlowName := "all_types_happy_flow_testing"
	allTypesHappyFlowSrcTableName := "pgpeer_test.all_types_table"
	allTypesHappyFlowDstTableName := "all_types_table_dst"

	_, err := suite.connector.pool.Exec(context.Background(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(id BIGINT PRIMARY KEY,
		 c1 BIGINT, c2 BIT, c3 VARBIT, c4 BOOLEAN, c6 BYTEA, c7 CHARACTER, c8 VARCHAR,
		 c9 CIDR, c11 DATE, c12 FLOAT, c13 DOUBLE PRECISION, c14 INET, c15 INTEGER,
		 c16 INTERVAL, c17 JSON, c18 JSONB, c21 MACADDR, c22 MONEY, c23 DECIMAL, c24 OID, c28 REAL,
		 c29 SMALLINT, c30 SMALLSERIAL, c31 SERIAL, c32 TEXT, c33 TIMESTAMP, c34 TIMESTAMPTZ,
		 c35 TIME, c36 TIMETZ, c37 TSQUERY, c38 TSVECTOR, c39 TXID_SNAPSHOT, c40 UUID, c41 XML)`,
			allTypesHappyFlowSrcTableName))
	suite.failTestError(err)

	ensurePullabilityOutput, err := suite.connector.EnsurePullability(&protos.EnsurePullabilityInput{
		FlowJobName:           allTypesHappyFlowName,
		SourceTableIdentifier: allTypesHappyFlowSrcTableName,
		PeerConnectionConfig:  nil, // not used by the connector itself.
	})
	suite.failTestError(err)
	tableRelID := ensurePullabilityOutput.TableIdentifier.GetPostgresTableIdentifier().RelId

	relIDTableNameMapping := map[uint32]string{
		tableRelID: allTypesHappyFlowSrcTableName,
	}
	tableNameMapping := map[string]string{
		allTypesHappyFlowSrcTableName: allTypesHappyFlowDstTableName,
	}
	err = suite.connector.SetupReplication(nil, &protos.SetupReplicationInput{
		FlowJobName:          allTypesHappyFlowName,
		TableNameMapping:     tableNameMapping,
		PeerConnectionConfig: nil, // not used by the connector itself.
	})
	suite.failTestError(err)

	tableNameSchemaMapping := make(map[string]*protos.TableSchema)
	getTblSchemaInput := &protos.GetTableSchemaBatchInput{
		TableIdentifiers:     []string{allTypesHappyFlowSrcTableName},
		PeerConnectionConfig: nil,
	}
	tableNameSchema, err := suite.connector.GetTableSchema(getTblSchemaInput)
	suite.failTestError(err)
	suite.Equal(&protos.TableSchema{
		TableIdentifier: allTypesHappyFlowSrcTableName,
		Columns: map[string]string{
			"id":  string(qvalue.QValueKindInt64),
			"c1":  string(qvalue.QValueKindInt64),
			"c2":  string(qvalue.QValueKindBit),
			"c3":  string(qvalue.QValueKindBit),
			"c4":  string(qvalue.QValueKindBoolean),
			"c6":  string(qvalue.QValueKindBytes),
			"c7":  string(qvalue.QValueKindString),
			"c8":  string(qvalue.QValueKindString),
			"c9":  string(qvalue.QValueKindString),
			"c11": string(qvalue.QValueKindDate),
			"c12": string(qvalue.QValueKindFloat64),
			"c13": string(qvalue.QValueKindFloat64),
			"c14": string(qvalue.QValueKindString),
			"c15": string(qvalue.QValueKindInt32),
			"c16": string(qvalue.QValueKindString),
			"c17": string(qvalue.QValueKindJSON),
			"c18": string(qvalue.QValueKindJSON),
			"c21": string(qvalue.QValueKindString),
			"c22": string(qvalue.QValueKindString),
			"c23": string(qvalue.QValueKindNumeric),
			"c24": string(qvalue.QValueKindString),
			"c28": string(qvalue.QValueKindFloat32),
			"c29": string(qvalue.QValueKindInt16),
			"c30": string(qvalue.QValueKindInt16),
			"c31": string(qvalue.QValueKindInt32),
			"c32": string(qvalue.QValueKindString),
			"c33": string(qvalue.QValueKindTimestamp),
			"c34": string(qvalue.QValueKindTimestampTZ),
			"c35": string(qvalue.QValueKindTime),
			"c36": string(qvalue.QValueKindTimeTZ),
			"c37": string(qvalue.QValueKindString),
			"c38": string(qvalue.QValueKindString),
			"c39": string(qvalue.QValueKindString),
			"c40": string(qvalue.QValueKindUUID),
			"c41": string(qvalue.QValueKindString),
		},
		PrimaryKeyColumn: "id",
	}, tableNameSchema)
	tableNameSchemaMapping[allTypesHappyFlowDstTableName] =
		tableNameSchema.TableNameSchemaMapping[allTypesHappyFlowSrcTableName]

	_, err = suite.connector.pool.Exec(context.Background(),
		fmt.Sprintf(`INSERT INTO %s SELECT 2, 2, b'1', b'101',
	 	 true, $1, 's', 'test', '1.1.10.2'::cidr,
		 CURRENT_DATE, 1.23, 1.234, '192.168.1.5'::inet, 1,
		 '5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds'::interval,
		 '{"sai":1}'::json, '{"sai":1}'::jsonb, '08:00:2b:01:02:03'::macaddr,
		 1.2, 1.23, 4::oid, 1.23, 1, 1, 1, 'test', now(), now(), now()::time, now()::timetz,
		 'fat & rat'::tsquery, 'a fat cat sat on a mat and ate a fat rat'::tsvector,
		 txid_current_snapshot(), '66073c38-b8df-4bdb-bbca-1c97596b8940'::uuid, xmlcomment('hello')`,
			allTypesHappyFlowSrcTableName),
		suite.randBytea(32))
	suite.failTestError(err)
	records, err := suite.connector.PullRecords(&model.PullRecordsRequest{
		FlowJobName:            allTypesHappyFlowName,
		LastSyncState:          nil,
		IdleTimeout:            5 * time.Second,
		MaxBatchSize:           100,
		SrcTableIDNameMapping:  relIDTableNameMapping,
		TableNameMapping:       tableNameMapping,
		TableNameSchemaMapping: tableNameSchemaMapping,
	})
	suite.failTestError(err)
	suite.Equal(1, len(records.Records))
	suite.Equal(35, len(records.Records[0].GetItems()))

	err = suite.connector.PullFlowCleanup(allTypesHappyFlowName)
	suite.failTestError(err)

	suite.dropTable(allTypesHappyFlowSrcTableName)
}

func (suite *PostgresCDCTestSuite) TestToastHappyFlow() {
	toastHappyFlowName := "toast_happy_flow_testing"
	toastHappyFlowSrcTableName := "pgpeer_test.toast_table"
	toastHappyFlowDstTableName := "toast_table_dst"

	_, err := suite.connector.pool.Exec(context.Background(),
		fmt.Sprintf(`CREATE TABLE %s(id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
		 n_t TEXT, lz4_t TEXT COMPRESSION LZ4, n_b BYTEA, lz4_b BYTEA COMPRESSION LZ4)`, toastHappyFlowSrcTableName))
	suite.failTestError(err)

	ensurePullabilityOutput, err := suite.connector.EnsurePullability(&protos.EnsurePullabilityInput{
		FlowJobName:           toastHappyFlowName,
		SourceTableIdentifier: toastHappyFlowSrcTableName,
		PeerConnectionConfig:  nil, // not used by the connector itself.
	})
	suite.failTestError(err)
	tableRelID := ensurePullabilityOutput.TableIdentifier.GetPostgresTableIdentifier().RelId

	relIDTableNameMapping := map[uint32]string{
		tableRelID: toastHappyFlowSrcTableName,
	}
	tableNameMapping := map[string]string{
		toastHappyFlowSrcTableName: toastHappyFlowDstTableName,
	}
	err = suite.connector.SetupReplication(nil, &protos.SetupReplicationInput{
		FlowJobName:          toastHappyFlowName,
		TableNameMapping:     tableNameMapping,
		PeerConnectionConfig: nil, // not used by the connector itself.
	})
	suite.failTestError(err)

	tableNameSchemaMapping := make(map[string]*protos.TableSchema)
	getTblSchemaInput := &protos.GetTableSchemaBatchInput{
		TableIdentifiers:     []string{toastHappyFlowSrcTableName},
		PeerConnectionConfig: nil,
	}
	tableNameSchema, err := suite.connector.GetTableSchema(getTblSchemaInput)
	suite.failTestError(err)
	suite.Equal(&protos.TableSchema{
		TableIdentifier: toastHappyFlowSrcTableName,
		Columns: map[string]string{
			"id":    string(qvalue.QValueKindInt32),
			"n_t":   string(qvalue.QValueKindString),
			"lz4_t": string(qvalue.QValueKindString),
			"n_b":   string(qvalue.QValueKindBytes),
			"lz4_b": string(qvalue.QValueKindBytes),
		},
		PrimaryKeyColumn: "id",
	}, tableNameSchema)
	tableNameSchemaMapping[toastHappyFlowDstTableName] =
		tableNameSchema.TableNameSchemaMapping[toastHappyFlowSrcTableName]

	suite.insertToastRecords(toastHappyFlowSrcTableName)
	records, err := suite.connector.PullRecords(&model.PullRecordsRequest{
		FlowJobName:            toastHappyFlowName,
		LastSyncState:          nil,
		IdleTimeout:            10 * time.Second,
		MaxBatchSize:           100,
		SrcTableIDNameMapping:  relIDTableNameMapping,
		TableNameMapping:       tableNameMapping,
		TableNameSchemaMapping: tableNameSchemaMapping,
	})
	suite.failTestError(err)
	suite.validateInsertedToastRecords(records.Records, toastHappyFlowSrcTableName, toastHappyFlowDstTableName)
	suite.Greater(records.FirstCheckPointID, int64(0))
	suite.GreaterOrEqual(records.LastCheckPointID, records.FirstCheckPointID)

	suite.mutateToastRecords(toastHappyFlowSrcTableName)
	records, err = suite.connector.PullRecords(&model.PullRecordsRequest{
		FlowJobName: toastHappyFlowName,
		LastSyncState: &protos.LastSyncState{
			Checkpoint:   records.LastCheckPointID,
			LastSyncedAt: nil,
		},
		IdleTimeout:            10 * time.Second,
		MaxBatchSize:           100,
		SrcTableIDNameMapping:  relIDTableNameMapping,
		TableNameMapping:       tableNameMapping,
		TableNameSchemaMapping: tableNameSchemaMapping,
	})
	suite.failTestError(err)
	suite.validateMutatedToastRecords(records.Records, toastHappyFlowSrcTableName, toastHappyFlowDstTableName)

	err = suite.connector.PullFlowCleanup(toastHappyFlowName)
	suite.failTestError(err)

	suite.dropTable(toastHappyFlowSrcTableName)
}

func TestPostgresTestSuite(t *testing.T) {
	suite.Run(t, new(PostgresCDCTestSuite))
}
