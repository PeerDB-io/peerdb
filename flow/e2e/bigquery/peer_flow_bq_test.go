package e2e_bigquery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/require"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

type PeerFlowE2ETestSuiteBQ struct {
	t *testing.T

	bqSuffix string
	conn     *connpostgres.PostgresConnector
	bqHelper *BigQueryTestHelper
}

func (s PeerFlowE2ETestSuiteBQ) T() *testing.T {
	return s.t
}

func (s PeerFlowE2ETestSuiteBQ) Conn() *pgx.Conn {
	return s.conn.Conn()
}

func (s PeerFlowE2ETestSuiteBQ) Connector() *connpostgres.PostgresConnector {
	return s.conn
}

func (s PeerFlowE2ETestSuiteBQ) Suffix() string {
	return s.bqSuffix
}

func (s PeerFlowE2ETestSuiteBQ) GetRows(tableName string, colsString string) (*model.QRecordBatch, error) {
	s.t.Helper()
	qualifiedTableName := fmt.Sprintf("`%s.%s`", s.bqHelper.Config.DatasetId, tableName)
	bqSelQuery := fmt.Sprintf("SELECT %s FROM %s ORDER BY id", colsString, qualifiedTableName)
	s.t.Logf("running query on bigquery: %s", bqSelQuery)
	return s.bqHelper.ExecuteAndProcessQuery(bqSelQuery)
}

func (s PeerFlowE2ETestSuiteBQ) GetRowsWhere(tableName string, colsString string, where string) (*model.QRecordBatch, error) {
	s.t.Helper()
	qualifiedTableName := fmt.Sprintf("`%s.%s`", s.bqHelper.Config.DatasetId, tableName)
	bqSelQuery := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY id", colsString, qualifiedTableName, where)
	s.t.Logf("running query on bigquery: %s", bqSelQuery)
	return s.bqHelper.ExecuteAndProcessQuery(bqSelQuery)
}

func TestPeerFlowE2ETestSuiteBQ(t *testing.T) {
	e2eshared.RunSuite(t, setupSuite, func(s PeerFlowE2ETestSuiteBQ) {
		e2e.TearDownPostgres(s)

		err := s.bqHelper.DropDataset(s.bqHelper.Config.DatasetId)
		if err != nil {
			s.t.Fatalf("failed to tear down bigquery: %v", err)
		}
	})
}

func (s PeerFlowE2ETestSuiteBQ) checkJSONValue(tableName, colName, fieldName, value string) error {
	res, err := s.bqHelper.ExecuteAndProcessQuery(fmt.Sprintf(
		"SELECT `%s`.%s FROM `%s.%s`;",
		colName, fieldName, s.bqHelper.Config.DatasetId, tableName))
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

func (s PeerFlowE2ETestSuiteBQ) attachSchemaSuffix(tableName string) string {
	return fmt.Sprintf("e2e_test_%s.%s", s.bqSuffix, tableName)
}

func (s PeerFlowE2ETestSuiteBQ) attachSuffix(input string) string {
	return fmt.Sprintf("%s_%s", input, s.bqSuffix)
}

func (s *PeerFlowE2ETestSuiteBQ) checkPeerdbColumns(dstQualified string, softDelete bool) error {
	qualifiedTableName := fmt.Sprintf("`%s.%s`", s.bqHelper.Config.DatasetId, dstQualified)
	selector := "`_PEERDB_SYNCED_AT`"
	if softDelete {
		selector += ", `_PEERDB_IS_DELETED`"
	}
	query := fmt.Sprintf("SELECT %s FROM %s",
		selector, qualifiedTableName)

	recordBatch, err := s.bqHelper.ExecuteAndProcessQuery(query)
	if err != nil {
		return err
	}

	recordCount := 0

	for _, record := range recordBatch.Records {
		for _, entry := range record {
			if entry.Kind == qvalue.QValueKindBoolean {
				isDeleteVal, ok := entry.Value.(bool)
				if !(ok && isDeleteVal) {
					return errors.New("peerdb column failed: _PEERDB_IS_DELETED is not true")
				}
				recordCount += 1
			}

			if entry.Kind == qvalue.QValueKindTimestamp {
				_, ok := entry.Value.(time.Time)
				if !ok {
					return errors.New("peerdb column failed: _PEERDB_SYNCED_AT is not valid")
				}

				recordCount += 1
			}
		}
	}

	if recordCount == 0 {
		return errors.New("peerdb column check failed: no records found")
	}

	return nil
}

// setupBigQuery sets up the bigquery connection.
func setupBigQuery(t *testing.T) *BigQueryTestHelper {
	t.Helper()

	bqHelper, err := NewBigQueryTestHelper()
	if err != nil {
		t.Fatalf("Failed to create helper: %v", err)
	}

	err = bqHelper.RecreateDataset()
	if err != nil {
		t.Fatalf("Failed to recreate dataset: %v", err)
	}

	return bqHelper
}

// Implement SetupAllSuite interface to setup the test suite
func setupSuite(t *testing.T) PeerFlowE2ETestSuiteBQ {
	t.Helper()

	err := godotenv.Load()
	if err != nil {
		// it's okay if the .env file is not present
		// we will use the default values
		t.Log("Unable to load .env file, using default values from env")
	}

	suffix := shared.RandomString(8)
	tsSuffix := time.Now().Format("20060102150405")
	bqSuffix := fmt.Sprintf("bq_%s_%s", strings.ToLower(suffix), tsSuffix)
	conn, err := e2e.SetupPostgres(t, bqSuffix)
	if err != nil || conn == nil {
		t.Fatalf("failed to setup postgres: %v", err)
	}

	bq := setupBigQuery(t)

	return PeerFlowE2ETestSuiteBQ{
		t:        t,
		bqSuffix: bqSuffix,
		conn:     conn,
		bqHelper: bq,
	}
}

func (s PeerFlowE2ETestSuiteBQ) Test_Invalid_Connection_Config() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflow, nil, nil)

	// Verify workflow completes
	require.True(s.t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()

	// assert that error contains "invalid connection configs"
	require.Contains(s.t, err.Error(), "invalid connection configs")
}

func (s PeerFlowE2ETestSuiteBQ) Test_Complete_Flow_No_Data() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	srcTableName := s.attachSchemaSuffix("test_no_data")
	dstTableName := "test_no_data"

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value VARCHAR(255) NOT NULL
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_complete_flow_no_data"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 1

	go func() {
		e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
		e2e.EnvWaitForEqualTables(env, s, "create table", dstTableName, "id,key,value")
		env.CancelWorkflow()
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Char_ColType_Error() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	srcTableName := s.attachSchemaSuffix("test_char_coltype")
	dstTableName := "test_char_coltype"

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value CHAR(255) NOT NULL
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_char_table"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 1

	go func() {
		e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
		e2e.EnvWaitForEqualTables(env, s, "create table", dstTableName, "id,key,value")
		env.CancelWorkflow()
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.RequireEnvCanceled(s.t, env)
}

// Test_Complete_Simple_Flow_BQ tests a complete flow with data in the source table.
// The test inserts 10 rows into the source table and verifies that the data is
// correctly synced to the destination table after sync flow completes.
func (s PeerFlowE2ETestSuiteBQ) Test_Complete_Simple_Flow_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	srcTableName := s.attachSchemaSuffix("test_simple_flow_bq")
	dstTableName := "test_simple_flow_bq"

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_complete_simple_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	go func() {
		e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
		// insert 10 rows into the source table
		for i := range 10 {
			testKey := fmt.Sprintf("test_key_%d", i)
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(key, value) VALUES ($1, $2)
		`, srcTableName), testKey, testValue)
			e2e.EnvNoError(s.t, env, err)
		}
		s.t.Log("Inserted 10 rows into the source table")

		e2e.EnvWaitForEqualTables(env, s, "normalize inserts", dstTableName, "id,key,value")
		env.CancelWorkflow()
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Toast_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	srcTableName := s.attachSchemaSuffix("test_toast_bq_1")
	dstTableName := "test_toast_bq_1"

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			t1 text,
			t2 text,
			k int
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_bq_1"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
		/*
			Executing a transaction which
			1. changes both toast column
			2. changes no toast column
			2. changes 1 toast column
		*/
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			BEGIN;
			INSERT INTO %s(t1,t2,k) SELECT random_string(9000),random_string(9000),
			1 FROM generate_series(1,2);
			UPDATE %s SET k=102 WHERE id=1;
			UPDATE %s SET t1='dummy' WHERE id=2;
			END;
		`, srcTableName, srcTableName, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Executed a transaction touching toast columns")

		e2e.EnvWaitForEqualTables(env, s, "normalize tx", dstTableName, "id,t1,t2,k")
		env.CancelWorkflow()
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Toast_Advance_1_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	srcTableName := s.attachSchemaSuffix("test_toast_bq_3")
	dstTableName := "test_toast_bq_3"

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			t1 text,
			t2 text,
			k int
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_bq_3"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
		// complex transaction with random DMLs on a table with toast columns
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			BEGIN;
			INSERT INTO %s(t1,t2,k) SELECT random_string(9000),random_string(9000),
			1 FROM generate_series(1,2);
			UPDATE %s SET k=102 WHERE id=1;
			UPDATE %s SET t1='dummy' WHERE id=2;
			UPDATE %s SET t2='dummy' WHERE id=2;
			DELETE FROM %s WHERE id=1;
			INSERT INTO %s(t1,t2,k) SELECT random_string(9000),random_string(9000),
			1 FROM generate_series(1,2);
			UPDATE %s SET k=1 WHERE id=1;
			UPDATE %s SET t1='dummy1',t2='dummy2' WHERE id=1;
			UPDATE %s SET t1='dummy3' WHERE id=3;
			DELETE FROM %s WHERE id=2;
			DELETE FROM %s WHERE id=3;
			DELETE FROM %s WHERE id=2;
			END;
		`, srcTableName, srcTableName, srcTableName, srcTableName, srcTableName, srcTableName,
			srcTableName, srcTableName, srcTableName, srcTableName, srcTableName, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Executed a transaction touching toast columns")

		e2e.EnvWaitForEqualTables(env, s, "normalizing tx", dstTableName, "id,t1,t2,k")
		env.CancelWorkflow()
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Toast_Advance_2_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	srcTableName := s.attachSchemaSuffix("test_toast_bq_4")
	dstTableName := "test_toast_bq_4"

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			t1 text,
			k int
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_bq_4"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
		// complex transaction with random DMLs on a table with toast columns
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			BEGIN;
			INSERT INTO %s(t1,k) SELECT random_string(9000),
			1 FROM generate_series(1,1);
			UPDATE %s SET t1=sub.t1 FROM (SELECT random_string(9000) t1
			FROM generate_series(1,1) ) sub WHERE id=1;
			UPDATE %s SET k=2 WHERE id=1;
			UPDATE %s SET k=3 WHERE id=1;
			UPDATE %s SET t1=sub.t1 FROM (SELECT random_string(9000) t1
			FROM generate_series(1,1)) sub WHERE id=1;
			UPDATE %s SET k=4 WHERE id=1;
			END;
		`, srcTableName, srcTableName, srcTableName, srcTableName, srcTableName, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Executed a transaction touching toast columns")
		e2e.EnvWaitForEqualTables(env, s, "normalizing tx", dstTableName, "id,t1,k")
		env.CancelWorkflow()
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Toast_Advance_3_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	srcTableName := s.attachSchemaSuffix("test_toast_bq_5")
	dstTableName := "test_toast_bq_5"

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			t1 text,
			t2 text,
			k int
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_bq_5"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
		/*
			transaction updating a single row
			multiple times with changed/unchanged toast columns
		*/
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			BEGIN;
			INSERT INTO %s(t1,t2,k) SELECT random_string(9000),random_string(9000),
			1 FROM generate_series(1,1);
			UPDATE %s SET k=102 WHERE id=1;
			UPDATE %s SET t1='dummy' WHERE id=1;
			UPDATE %s SET t2='dummy' WHERE id=1;
			END;
		`, srcTableName, srcTableName, srcTableName, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Executed a transaction touching toast columns")
		e2e.EnvWaitForEqualTables(env, s, "normalizing tx", dstTableName, "id,t1,t2,k")
		env.CancelWorkflow()
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Types_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	srcTableName := s.attachSchemaSuffix("test_types_bq")
	dstTableName := "test_types_bq"
	createMoodEnum := "CREATE TYPE mood AS ENUM ('happy', 'sad', 'angry');"
	var pgErr *pgconn.PgError
	_, enumErr := s.Conn().Exec(context.Background(), createMoodEnum)
	if errors.As(enumErr, &pgErr) && pgErr.Code != pgerrcode.DuplicateObject && !utils.IsUniqueError(pgErr) {
		require.NoError(s.t, enumErr)
	}

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (id serial PRIMARY KEY,c1 BIGINT,c2 BIT,c3 VARBIT,c4 BOOLEAN,
		c6 BYTEA,c7 CHARACTER,c8 varchar,c9 CIDR,c11 DATE,c12 FLOAT,c13 DOUBLE PRECISION,
		c14 INET,c15 INTEGER,c16 INTERVAL,c17 JSON,c18 JSONB,c21 MACADDR,c22 MONEY,
		c23 NUMERIC,c24 OID,c28 REAL,c29 SMALLINT,c30 SMALLSERIAL,c31 SERIAL,c32 TEXT,
		c33 TIMESTAMP,c34 TIMESTAMPTZ,c35 TIME, c36 TIMETZ,c37 TSQUERY,c38 TSVECTOR,
		c39 TXID_SNAPSHOT,c40 UUID,c41 XML, c42 INT[], c43 FLOAT[], c44 TEXT[], c45 mood, c46 HSTORE,
		c47 DATE[], c48 TIMESTAMPTZ[], c49 TIMESTAMP[], c50 BOOLEAN[], c51 SMALLINT[], c52 NUMERIC);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_types_bq"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
		/* test inserting various types*/
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s SELECT 2,2,b'1',b'101',
		true,random_bytea(32),'s','test','1.1.10.2'::cidr,
		CURRENT_DATE,1.23,1.234,'192.168.1.5'::inet,1,
		'5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds'::interval,
		'{"sai":-8.02139037433155}'::json,'{"sai":1}'::jsonb,'08:00:2b:01:02:03'::macaddr,
		1.2,1.23,4::oid,1.23,1,1,1,'test',now(),now(),now()::time,now()::timetz,
		'fat & rat'::tsquery,'a fat cat sat on a mat and ate a fat rat'::tsvector,
		txid_current_snapshot(),
		'66073c38-b8df-4bdb-bbca-1c97596b8940'::uuid,xmlcomment('hello'),
		ARRAY[10299301,2579827],
		ARRAY[0.0003, 8902.0092],
		ARRAY['hello','bye'],'happy',
		'key1=>value1, key2=>NULL'::hstore,
		'{2020-01-01, 2020-01-02}'::date[],
		'{"2020-01-01 01:01:01+00", "2020-01-02 01:01:01+00"}'::timestamptz[],
		'{"2020-01-01 01:01:01", "2020-01-02 01:01:01"}'::timestamp[],
		'{true, false}'::boolean[],
		'{1, 2}'::smallint[];
		`, srcTableName))
		e2e.EnvNoError(s.t, env, err)

		e2e.EnvWaitFor(s.t, env, 2*time.Minute, "normalize types", func() bool {
			noNulls, err := s.bqHelper.CheckNull(dstTableName, []string{
				"c41", "c1", "c2", "c3", "c4",
				"c6", "c39", "c40", "id", "c9", "c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18",
				"c21", "c22", "c23", "c24", "c28", "c29", "c30", "c31", "c33", "c34", "c35", "c36",
				"c37", "c38", "c7", "c8", "c32", "c42", "c43", "c44", "c45", "c46", "c47", "c48",
				"c49", "c50", "c51",
			})
			if err != nil {
				s.t.Log(err)
				return false
			}

			// check if JSON on bigquery side is a good JSON
			if err := s.checkJSONValue(dstTableName, "c17", "sai", "-8.021390374331551"); err != nil {
				return false
			}

			// check if HSTORE on bigquery side is a good JSON
			if err := s.checkJSONValue(dstTableName, "c46", "key1", "\"value1\""); err != nil {
				return false
			}
			if err := s.checkJSONValue(dstTableName, "c46", "key2", "null"); err != nil {
				return false
			}

			return noNulls
		})

		env.CancelWorkflow()
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteBQ) Test_NaN_Doubles_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	srcTableName := s.attachSchemaSuffix("test_nans_bq")
	dstTableName := "test_nans_bq"
	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (id serial PRIMARY KEY,c1 double precision,c2 double precision[]);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_nans_bq"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
		/* test inserting various types*/
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s SELECT 2, 'NaN'::double precision, '{NaN, Infinity, -Infinity}';
		`, srcTableName))
		e2e.EnvNoError(s.t, env, err)

		e2e.EnvWaitFor(s.t, env, 2*time.Minute, "normalize weird floats", func() bool {
			good, err := s.bqHelper.CheckDoubleValues(dstTableName, "c1", "c2")
			return err == nil && good
		})
		env.CancelWorkflow()
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Invalid_Geo_BQ_Avro_CDC() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	srcTableName := s.attachSchemaSuffix("test_invalid_geo_bq_avro_cdc")
	dstTableName := "test_invalid_geo_bq_avro_cdc"

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			line GEOMETRY(LINESTRING) NOT NULL,
			"polyPoly" GEOGRAPHY(POLYGON) NOT NULL
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_invalid_geo_bq_avro_cdc"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert 10 rows into the source table
	go func() {
		e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
		// insert 4 invalid shapes and 6 valid shapes into the source table
		for range 4 {
			_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s (line,"polyPoly") VALUES ($1,$2)
		`, srcTableName), "010200000001000000000000000000F03F0000000000000040",
				"0103000020e6100000010000000c0000001a8361d35dc64140afdb8d2b1bc3c9bf1b8ed4685fc641405ba64c"+
					"579dc2c9bf6a6ad95a5fc64140cd82767449c2c9bf9570fbf85ec641408a07944db9c2c9bf729a18a55ec6414021b8b748c7c2c9bfba46de4c"+
					"5fc64140f2567052abc2c9bf2df9c5925fc641409394e16573c2c9bf2df9c5925fc6414049eceda9afc1c9bfdd1cc1a05fc64140fe43faedebc0"+
					"c9bf4694f6065fc64140fe43faedebc0c9bfffe7305f5ec641406693d6f2ddc0c9bf1a8361d35dc64140afdb8d2b1bc3c9bf",
			)
			e2e.EnvNoError(s.t, env, err)
		}
		s.t.Log("Inserted 4 invalid geography rows into the source table")
		for range 6 {
			_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s (line,"polyPoly") VALUES ($1,$2)
		`, srcTableName), "010200000002000000000000000000F03F000000000000004000000000000008400000000000001040",
				"010300000001000000050000000000000000000000000000000000000000000000"+
					"00000000000000000000f03f000000000000f03f000000000000f03f0000000000"+
					"00f03f000000000000000000000000000000000000000000000000")
			e2e.EnvNoError(s.t, env, err)
		}
		s.t.Log("Inserted 6 valid geography rows and 10 total rows into source")

		e2e.EnvWaitFor(s.t, env, 2*time.Minute, "normalize shapes", func() bool {
			// We inserted 4 invalid shapes in each,
			// which should be filtered out as null on destination.
			lineCount, err := s.bqHelper.countRowsWithDataset(s.bqHelper.Config.DatasetId, dstTableName, "line")
			if err != nil {
				return false
			}

			polyCount, err := s.bqHelper.countRowsWithDataset(s.bqHelper.Config.DatasetId, dstTableName, "`polyPoly`")
			if err != nil {
				return false
			}

			if lineCount != 6 || polyCount != 6 {
				s.t.Logf("wrong counts, expect 6 lines 6 polies, not %d lines %d polies", lineCount, polyCount)
				return false
			} else {
				return true
			}
		})

		env.CancelWorkflow()
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Multi_Table_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	srcTable1Name := s.attachSchemaSuffix("test1_bq")
	dstTable1Name := "test1_bq"
	srcTable2Name := s.attachSchemaSuffix("test2_bq")
	dstTable2Name := "test2_bq"

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE %s (id serial primary key, c1 int, c2 text);
	CREATE TABLE %s(id serial primary key, c1 int, c2 text);
	`, srcTable1Name, srcTable2Name))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_multi_table_bq"),
		TableNameMapping: map[string]string{srcTable1Name: dstTable1Name, srcTable2Name: dstTable2Name},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
		/* inserting across multiple tables*/
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s (c1,c2) VALUES (1,'dummy_1');
		INSERT INTO %s (c1,c2) VALUES (-1,'dummy_-1');
		`, srcTable1Name, srcTable2Name))
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Executed an insert on two tables")

		e2e.EnvWaitFor(s.t, env, 2*time.Minute, "normalize both tables", func() bool {
			count1, err := s.bqHelper.countRows(dstTable1Name)
			if err != nil {
				return false
			}
			count2, err := s.bqHelper.countRows(dstTable2Name)
			if err != nil {
				return false
			}

			return count1 == 1 && count2 == 1
		})

		env.CancelWorkflow()
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.RequireEnvCanceled(s.t, env)
}

// TODO: not checking schema exactly, add later
func (s PeerFlowE2ETestSuiteBQ) Test_Simple_Schema_Changes_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	tableName := "test_simple_schema_changes"
	srcTableName := s.attachSchemaSuffix(tableName)

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			c1 BIGINT
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(tableName),
		TableNameMapping: map[string]string{srcTableName: tableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert and mutate schema repeatedly.
	go func() {
		// insert first row.
		e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1) VALUES (1)`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Inserted initial row in the source table")

		e2e.EnvWaitForEqualTables(env, s, "normalize insert", tableName, "id,c1")

		// alter source table, add column c2 and insert another row.
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s ADD COLUMN c2 BIGINT`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Altered source table, added column c2")
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,c2) VALUES (2,2)`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Inserted row with added c2 in the source table")

		// verify we got our two rows, if schema did not match up it will error.
		e2e.EnvWaitForEqualTables(env, s, "normalize altered row", tableName, "id,c1,c2")

		// alter source table, add column c3, drop column c2 and insert another row.
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s DROP COLUMN c2, ADD COLUMN c3 BIGINT`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Altered source table, dropped column c2 and added column c3")
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,c3) VALUES (3,3)`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Inserted row with added c3 in the source table")

		// verify we got our two rows, if schema did not match up it will error.
		e2e.EnvWaitForEqualTables(env, s, "normalize altered row", tableName, "id,c1,c3")

		// alter source table, drop column c3 and insert another row.
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s DROP COLUMN c3`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Altered source table, dropped column c3")
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1) VALUES (4)`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Inserted row after dropping all columns in the source table")

		// verify we got our two rows, if schema did not match up it will error.
		e2e.EnvWaitForEqualTables(env, s, "normalize drop column", tableName, "id,c1")

		env.CancelWorkflow()
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Composite_PKey_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	tableName := "test_simple_cpkey"
	srcTableName := s.attachSchemaSuffix("test_simple_cpkey")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT GENERATED ALWAYS AS IDENTITY,
			c1 INT GENERATED BY DEFAULT AS IDENTITY,
			c2 INT,
			t TEXT,
			PRIMARY KEY(id,t)
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_cpkey_flow"),
		TableNameMapping: map[string]string{srcTableName: tableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	go func() {
		e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
		// insert 10 rows into the source table
		for i := range 10 {
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c2,t) VALUES ($1,$2)
		`, srcTableName), i, testValue)
			e2e.EnvNoError(s.t, env, err)
		}
		s.t.Log("Inserted 10 rows into the source table")

		// verify we got our 10 rows
		e2e.EnvWaitForEqualTables(env, s, "normalize table", tableName, "id,c1,c2,t")

		_, err := s.Conn().Exec(context.Background(),
			fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=$1`, srcTableName), 1)
		e2e.EnvNoError(s.t, env, err)
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=$1`, srcTableName), 0)
		e2e.EnvNoError(s.t, env, err)

		e2e.EnvWaitForEqualTables(env, s, "normalize update", tableName, "id,c1,c2,t")

		env.CancelWorkflow()
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Composite_PKey_Toast_1_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	srcTableName := s.attachSchemaSuffix("test_cpkey_toast1")
	dstTableName := "test_cpkey_toast1"

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT GENERATED ALWAYS AS IDENTITY,
			c1 INT GENERATED BY DEFAULT AS IDENTITY,
			c2 INT,
			t TEXT,
			t2 TEXT,
			PRIMARY KEY(id,t)
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_cpkey_toast1_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	go func() {
		e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
		rowsTx, err := s.Conn().Begin(context.Background())
		e2e.EnvNoError(s.t, env, err)

		// insert 10 rows into the source table
		for i := range 10 {
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = rowsTx.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c2,t,t2) VALUES ($1,$2,random_string(9000))
		`, srcTableName), i, testValue)
			e2e.EnvNoError(s.t, env, err)
		}
		s.t.Log("Inserted 10 rows into the source table")

		_, err = rowsTx.Exec(context.Background(),
			fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=$1`, srcTableName), 1)
		e2e.EnvNoError(s.t, env, err)
		_, err = rowsTx.Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=$1`, srcTableName), 0)
		e2e.EnvNoError(s.t, env, err)

		err = rowsTx.Commit(context.Background())
		e2e.EnvNoError(s.t, env, err)
		e2e.EnvWaitForEqualTables(env, s, "normalize tx", dstTableName, "id,c1,c2,t,t2")
		env.CancelWorkflow()
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Composite_PKey_Toast_2_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	tableName := "test_cpkey_toast2"
	srcTableName := s.attachSchemaSuffix("test_cpkey_toast2")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT GENERATED ALWAYS AS IDENTITY,
			c1 INT GENERATED BY DEFAULT AS IDENTITY,
			c2 INT,
			t TEXT,
			t2 TEXT,
			PRIMARY KEY(id,t)
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_cpkey_toast2_flow"),
		TableNameMapping: map[string]string{srcTableName: tableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	go func() {
		e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)

		// insert 10 rows into the source table
		for i := range 10 {
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c2,t,t2) VALUES ($1,$2,random_string(9000))
		`, srcTableName), i, testValue)
			e2e.EnvNoError(s.t, env, err)
		}
		s.t.Log("Inserted 10 rows into the source table")

		e2e.EnvWaitForEqualTables(env, s, "normalize table", tableName, "id,c2,t,t2")
		_, err = s.Conn().Exec(context.Background(),
			fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=$1`, srcTableName), 1)
		e2e.EnvNoError(s.t, env, err)
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=$1`, srcTableName), 0)
		e2e.EnvNoError(s.t, env, err)
		e2e.EnvWaitForEqualTables(env, s, "normalize update", tableName, "id,c2,t,t2")

		env.CancelWorkflow()
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Columns_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	srcTableName := s.attachSchemaSuffix("test_peerdb_cols")
	dstTableName := "test_peerdb_cols_dst"
	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_peerdb_cols_mirror"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		SoftDelete:       true,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	go func() {
		e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
		// insert 1 row into the source table
		testKey := fmt.Sprintf("test_key_%d", 1)
		testValue := fmt.Sprintf("test_value_%d", 1)
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(key, value) VALUES ($1, $2)
		`, srcTableName), testKey, testValue)
		e2e.EnvNoError(s.t, env, err)

		// delete that row
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1
		`, srcTableName))
		e2e.EnvNoError(s.t, env, err)

		e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize insert/delete", func() bool {
			return s.checkPeerdbColumns(dstTableName, true) == nil
		})
		env.CancelWorkflow()
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Multi_Table_Multi_Dataset_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	srcTable1Name := s.attachSchemaSuffix("test1_bq")
	dstTable1Name := "test1_bq"
	secondDataset := s.bqHelper.Config.DatasetId + "_2"
	srcTable2Name := s.attachSchemaSuffix("test2_bq")
	dstTable2Name := "test2_bq"

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE %s(id serial primary key, c1 int, c2 text);
	CREATE TABLE %s(id serial primary key, c1 int, c2 text);
	`, srcTable1Name, srcTable2Name))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("test_multi_table_multi_dataset_bq"),
		TableNameMapping: map[string]string{
			srcTable1Name: dstTable1Name,
			srcTable2Name: fmt.Sprintf("%s.%s", secondDataset, dstTable2Name),
		},
		PostgresPort:   e2e.PostgresPort,
		Destination:    s.bqHelper.Peer,
		CdcStagingPath: "",
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
		/* inserting across multiple tables*/
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s (c1,c2) VALUES (1,'dummy_1');
		INSERT INTO %s (c1,c2) VALUES (-1,'dummy_-1');
		`, srcTable1Name, srcTable2Name))
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Executed an insert on two tables")

		e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize multi dataset", func() bool {
			count1, err := s.bqHelper.countRows(dstTable1Name)
			if err != nil {
				return false
			}
			count2, err := s.bqHelper.countRowsWithDataset(secondDataset, dstTable2Name, "")
			if err != nil {
				return false
			}

			return count1 == 1 && count2 == 1
		})
		env.CancelWorkflow()
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.RequireEnvCanceled(s.t, env)

	require.NoError(s.t, s.bqHelper.DropDataset(secondDataset))
}

func (s PeerFlowE2ETestSuiteBQ) Test_Soft_Delete_Basic() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	tableName := "test_softdel"
	srcName := "test_softdel_src"
	srcTableName := s.attachSchemaSuffix(srcName)

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			c1 INT,
			c2 INT,
			t TEXT
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix(tableName),
	}

	config := &protos.FlowConnectionConfigs{
		FlowJobName: connectionGen.FlowJobName,
		Destination: s.bqHelper.Peer,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: tableName,
			},
		},
		Source:            e2e.GeneratePostgresPeer(e2e.PostgresPort),
		CdcStagingPath:    connectionGen.CdcStagingPath,
		SoftDelete:        true,
		SoftDeleteColName: "_PEERDB_IS_DELETED",
		SyncedAtColName:   "_PEERDB_SYNCED_AT",
		MaxBatchSize:      100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	go func() {
		e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)

		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c1,c2,t) VALUES (1,2,random_string(9000))`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		e2e.EnvWaitForEqualTablesWithNames(env, s, "normalize insert", srcName, tableName, "id,c1,c2,t")
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			UPDATE %s SET c1=c1+4 WHERE id=1`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		e2e.EnvWaitForEqualTablesWithNames(env, s, "normalize update", srcName, tableName, "id,c1,c2,t")
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize delete", func() bool {
			pgRows, err := e2e.GetPgRows(s.conn, s.bqSuffix, srcName, "id,c1,c2,t")
			if err != nil {
				return false
			}
			rows, err := s.GetRowsWhere(tableName, "id,c1,c2,t", "NOT _PEERDB_IS_DELETED")
			if err != nil {
				return false
			}
			return e2eshared.CheckEqualRecordBatches(s.t, pgRows, rows)
		})

		env.CancelWorkflow()
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflow, config, nil)
	e2e.RequireEnvCanceled(s.t, env)

	newerSyncedAtQuery := fmt.Sprintf(
		"SELECT COUNT(*) FROM `%s.%s` WHERE _PEERDB_IS_DELETED",
		s.bqHelper.Config.DatasetId, tableName)
	numNewRows, err := s.bqHelper.RunInt64Query(newerSyncedAtQuery)
	require.NoError(s.t, err)
	require.Equal(s.t, int64(1), numNewRows)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Soft_Delete_IUD_Same_Batch() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	cmpTableName := s.attachSchemaSuffix("test_softdel_iud")
	srcTableName := cmpTableName + "_src"
	dstTableName := "test_softdel_iud"

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			c1 INT,
			c2 INT,
			t TEXT
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("test_softdel_iud"),
	}

	config := &protos.FlowConnectionConfigs{
		FlowJobName: connectionGen.FlowJobName,
		Destination: s.bqHelper.Peer,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: dstTableName,
			},
		},
		Source:            e2e.GeneratePostgresPeer(e2e.PostgresPort),
		CdcStagingPath:    connectionGen.CdcStagingPath,
		SoftDelete:        true,
		SoftDeleteColName: "_PEERDB_IS_DELETED",
		SyncedAtColName:   "_PEERDB_SYNCED_AT",
		MaxBatchSize:      100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	go func() {
		e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)

		insertTx, err := s.Conn().Begin(context.Background())
		e2e.EnvNoError(s.t, env, err)

		_, err = insertTx.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c1,c2,t) VALUES (1,2,random_string(9000))`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		_, err = insertTx.Exec(context.Background(), fmt.Sprintf(`
			UPDATE %s SET c1=c1+4 WHERE id=1`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		// since we delete stuff, create another table to compare with
		_, err = insertTx.Exec(context.Background(), fmt.Sprintf(`
			CREATE TABLE %s AS SELECT * FROM %s`, cmpTableName, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		_, err = insertTx.Exec(context.Background(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1`, srcTableName))
		e2e.EnvNoError(s.t, env, err)

		e2e.EnvNoError(s.t, env, insertTx.Commit(context.Background()))
		e2e.EnvWaitForEqualTables(env, s, "normalizing tx", "test_softdel_iud", "id,c1,c2,t")
		e2e.EnvWaitFor(s.t, env, 3*time.Minute, "checking soft delete", func() bool {
			newerSyncedAtQuery := fmt.Sprintf(
				"SELECT COUNT(*) FROM `%s.%s` WHERE _PEERDB_IS_DELETED",
				s.bqHelper.Config.DatasetId, dstTableName)
			numNewRows, err := s.bqHelper.RunInt64Query(newerSyncedAtQuery)
			e2e.EnvNoError(s.t, env, err)
			return numNewRows == 1
		})

		env.CancelWorkflow()
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflow, config, nil)
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Soft_Delete_UD_Same_Batch() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	srcName := "test_softdel_ud_src"
	srcTableName := s.attachSchemaSuffix(srcName)
	dstName := "test_softdel_ud"

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			c1 INT,
			c2 INT,
			t TEXT
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix(dstName),
	}

	config := &protos.FlowConnectionConfigs{
		FlowJobName: connectionGen.FlowJobName,
		Destination: s.bqHelper.Peer,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: dstName,
			},
		},
		Source:            e2e.GeneratePostgresPeer(e2e.PostgresPort),
		CdcStagingPath:    connectionGen.CdcStagingPath,
		SoftDelete:        true,
		SoftDeleteColName: "_PEERDB_IS_DELETED",
		SyncedAtColName:   "_PEERDB_SYNCED_AT",
		MaxBatchSize:      100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	go func() {
		e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)

		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c1,c2,t) VALUES (1,2,random_string(9000))`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		e2e.EnvWaitForEqualTablesWithNames(env, s, "normalize insert", srcName, dstName, "id,c1,c2,t")

		insertTx, err := s.Conn().Begin(context.Background())
		e2e.EnvNoError(s.t, env, err)
		_, err = insertTx.Exec(context.Background(), fmt.Sprintf(`
			UPDATE %s SET t=random_string(10000) WHERE id=1`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		_, err = insertTx.Exec(context.Background(), fmt.Sprintf(`
			UPDATE %s SET c1=c1+4 WHERE id=1`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		_, err = insertTx.Exec(context.Background(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		e2e.EnvNoError(s.t, env, insertTx.Commit(context.Background()))

		e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize transaction", func() bool {
			pgRows, err := e2e.GetPgRows(s.conn, s.bqSuffix, srcName, "id,c1,c2,t")
			e2e.EnvNoError(s.t, env, err)
			rows, err := s.GetRowsWhere(dstName, "id,c1,c2,t", "NOT _PEERDB_IS_DELETED")
			if err != nil {
				return false
			}
			return e2eshared.CheckEqualRecordBatches(s.t, pgRows, rows)
		})

		env.CancelWorkflow()
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflow, config, nil)
	e2e.RequireEnvCanceled(s.t, env)

	newerSyncedAtQuery := fmt.Sprintf(
		"SELECT COUNT(*) FROM `%s.%s` WHERE _PEERDB_IS_DELETED",
		s.bqHelper.Config.DatasetId, dstName)
	numNewRows, err := s.bqHelper.RunInt64Query(newerSyncedAtQuery)
	require.NoError(s.t, err)
	require.Equal(s.t, int64(0), numNewRows)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Soft_Delete_Insert_After_Delete() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)

	tableName := "test_softdel_iad"
	srcTableName := s.attachSchemaSuffix(tableName)

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
			c1 INT,
			c2 INT,
			t TEXT
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix(tableName),
	}

	config := &protos.FlowConnectionConfigs{
		FlowJobName: connectionGen.FlowJobName,
		Destination: s.bqHelper.Peer,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: tableName,
			},
		},
		Source:            e2e.GeneratePostgresPeer(e2e.PostgresPort),
		CdcStagingPath:    connectionGen.CdcStagingPath,
		SoftDelete:        true,
		SoftDeleteColName: "_PEERDB_IS_DELETED",
		SyncedAtColName:   "_PEERDB_SYNCED_AT",
		MaxBatchSize:      100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert and delete rows in the table.
	go func() {
		e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)

		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c1,c2,t) VALUES (1,2,random_string(9000))`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		e2e.EnvWaitForEqualTables(env, s, "normalize insert", tableName, "id,c1,c2,t")
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize delete", func() bool {
			pgRows, err := e2e.GetPgRows(s.conn, s.bqSuffix, tableName, "id,c1,c2,t")
			if err != nil {
				return false
			}
			rows, err := s.GetRowsWhere(tableName, "id,c1,c2,t", "NOT _PEERDB_IS_DELETED")
			if err != nil {
				return false
			}
			return e2eshared.CheckEqualRecordBatches(s.t, pgRows, rows)
		})
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(id,c1,c2,t) VALUES (1,3,4,random_string(10000))`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		e2e.EnvWaitForEqualTables(env, s, "normalize reinsert", tableName, "id,c1,c2,t")

		env.CancelWorkflow()
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflow, config, nil)
	e2e.RequireEnvCanceled(s.t, env)

	newerSyncedAtQuery := fmt.Sprintf(
		"SELECT COUNT(*) FROM `%s.%s` WHERE _PEERDB_IS_DELETED",
		s.bqHelper.Config.DatasetId, tableName)
	numNewRows, err := s.bqHelper.RunInt64Query(newerSyncedAtQuery)
	require.NoError(s.t, err)
	require.Equal(s.t, int64(0), numNewRows)
}
