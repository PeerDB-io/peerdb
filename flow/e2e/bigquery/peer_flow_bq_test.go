package e2e_bigquery

import (
	"context"
	"fmt"
	"testing"

	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
)

const bigquerySuffix = "bigquery"

type PeerFlowE2ETestSuiteBQ struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	pool     *pgxpool.Pool
	bqHelper *BigQueryTestHelper
}

func TestPeerFlowE2ETestSuiteBQ(t *testing.T) {
	suite.Run(t, new(PeerFlowE2ETestSuiteBQ))
}

func (s *PeerFlowE2ETestSuiteBQ) attachSchemaSuffix(tableName string) string {
	return fmt.Sprintf("e2e_test_%s.%s", bigquerySuffix, tableName)
}

func (s *PeerFlowE2ETestSuiteBQ) attachSuffix(input string) string {
	return fmt.Sprintf("%s_%s", input, bigquerySuffix)
}

// setupBigQuery sets up the bigquery connection.
func (s *PeerFlowE2ETestSuiteBQ) setupBigQuery() error {
	bqHelper, err := NewBigQueryTestHelper()
	if err != nil {
		return fmt.Errorf("failed to create bigquery helper: %w", err)
	}

	err = bqHelper.RecreateDataset()
	if err != nil {
		return fmt.Errorf("failed to recreate bigquery dataset: %w", err)
	}

	s.bqHelper = bqHelper
	return nil
}

// Implement SetupAllSuite interface to setup the test suite
func (s *PeerFlowE2ETestSuiteBQ) SetupSuite() {
	err := godotenv.Load()
	if err != nil {
		// it's okay if the .env file is not present
		// we will use the default values
		log.Infof("Unable to load .env file, using default values from env")
	}

	log.SetReportCaller(true)

	pool, err := e2e.SetupPostgres(bigquerySuffix)
	if err != nil {
		s.Fail("failed to setup postgres", err)
	}
	s.pool = pool

	err = s.setupBigQuery()
	if err != nil {
		s.Fail("failed to setup bigquery", err)
	}
}

// Implement TearDownAllSuite interface to tear down the test suite
func (s *PeerFlowE2ETestSuiteBQ) TearDownSuite() {
	err := e2e.TearDownPostgres(s.pool, bigquerySuffix)
	if err != nil {
		s.Fail("failed to drop Postgres schema", err)
	}

	err = s.bqHelper.DropDataset()
	if err != nil {
		s.Fail("failed to drop bigquery dataset", err)
	}
}

func (s *PeerFlowE2ETestSuiteBQ) Test_Invalid_Connection_Config() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	// TODO (kaushikiska): ensure flow name can only be alpha numeric and underscores.
	limits := peerflow.PeerFlowLimits{
		TotalSyncFlows: 1,
		MaxBatchSize:   1,
	}

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflowWithConfig, nil, &limits, nil)

	// Verify workflow completes
	s.True(env.IsWorkflowCompleted())
	err := env.GetWorkflowError()

	// assert that error contains "invalid connection configs"
	s.Error(err)
	s.Contains(err.Error(), "invalid connection configs")

	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteBQ) Test_Complete_Flow_No_Data() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_no_data")
	dstTableName := "test_no_data"

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value VARCHAR(255) NOT NULL
		);
	`, srcTableName))
	s.NoError(err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_complete_flow_no_data"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	limits := peerflow.PeerFlowLimits{
		TotalSyncFlows: 1,
		MaxBatchSize:   1,
	}

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteBQ) Test_Char_ColType_Error() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_char_coltype")
	dstTableName := "test_char_coltype"

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value CHAR(255) NOT NULL
		);
	`, srcTableName))
	s.NoError(err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_char_table"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	limits := peerflow.PeerFlowLimits{
		TotalSyncFlows: 1,
		MaxBatchSize:   1,
	}

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	env.AssertExpectations(s.T())
}

// Test_Complete_Simple_Flow_BQ tests a complete flow with data in the source table.
// The test inserts 10 rows into the source table and verifies that the data is
// correctly synced to the destination table after sync flow completes.
func (s *PeerFlowE2ETestSuiteBQ) Test_Complete_Simple_Flow_BQ() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_simple_flow_bq")
	dstTableName := "test_simple_flow_bq"

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`, srcTableName))
	s.NoError(err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_complete_simple_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	limits := peerflow.PeerFlowLimits{
		TotalSyncFlows: 2,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert 10 rows into the source table
	go func() {
		e2e.SetupPeerFlowStatusQuery(env, connectionGen)
		// insert 10 rows into the source table
		for i := 0; i < 10; i++ {
			testKey := fmt.Sprintf("test_key_%d", i)
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(key, value) VALUES ($1, $2)
		`, srcTableName), testKey, testValue)
			s.NoError(err)
		}
		fmt.Println("Inserted 10 rows into the source table")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	count, err := s.bqHelper.CountRows(dstTableName)
	s.NoError(err)
	s.Equal(10, count)

	// TODO: verify that the data is correctly synced to the destination table
	// on the bigquery side

	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteBQ) Test_Toast_BQ() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_toast_bq_1")
	dstTableName := "test_toast_bq_1"

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			t1 text,
			t2 text,
			k int
		);CREATE OR REPLACE FUNCTION random_string( int ) RETURNS TEXT as $$
		SELECT string_agg(substring('0123456789bcdfghjkmnpqrstvwxyz',
		round(random() * 30)::integer, 1), '') FROM generate_series(1, $1);
		$$ language sql;
	`, srcTableName))
	s.NoError(err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_bq_1"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	limits := peerflow.PeerFlowLimits{
		TotalSyncFlows: 1,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupPeerFlowStatusQuery(env, connectionGen)
		/*
			Executing a transaction which
			1. changes both toast column
			2. changes no toast column
			2. changes 1 toast column
		*/
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			BEGIN;
			INSERT INTO %s(t1,t2,k) SELECT random_string(9000),random_string(9000),
			1 FROM generate_series(1,2);
			UPDATE %s SET k=102 WHERE id=1;
			UPDATE %s SET t1='dummy' WHERE id=2;
			END;
		`, srcTableName, srcTableName, srcTableName))
		s.NoError(err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	s.compareTableContentsBQ(dstTableName, "id,t1,t2,k")
	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteBQ) Test_Toast_Nochanges_BQ() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_toast_bq_2")
	dstTableName := "test_toast_bq_2"

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			t1 text,
			t2 text,
			k int
		);CREATE OR REPLACE FUNCTION random_string( int ) RETURNS TEXT as $$
		SELECT string_agg(substring('0123456789bcdfghjkmnpqrstvwxyz',
		round(random() * 30)::integer, 1), '') FROM generate_series(1, $1);
		$$ language sql;
	`, srcTableName))
	s.NoError(err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_bq_2"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	limits := peerflow.PeerFlowLimits{
		TotalSyncFlows: 1,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupPeerFlowStatusQuery(env, connectionGen)
		/* transaction updating no rows */
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			BEGIN;
			UPDATE %s SET k=102 WHERE id=1;
			UPDATE %s SET t1='dummy' WHERE id=2;
			END;
		`, srcTableName, srcTableName))
		s.NoError(err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	s.compareTableContentsBQ(dstTableName, "id,t1,t2,k")
	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteBQ) Test_Toast_Advance_1_BQ() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_toast_bq_3")
	dstTableName := "test_toast_bq_3"

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			t1 text,
			t2 text,
			k int
		);CREATE OR REPLACE FUNCTION random_string( int ) RETURNS TEXT as $$
		SELECT string_agg(substring('0123456789bcdfghjkmnpqrstvwxyz',
		round(random() * 30)::integer, 1), '') FROM generate_series(1, $1);
		$$ language sql;
	`, srcTableName))
	s.NoError(err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_bq_3"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	limits := peerflow.PeerFlowLimits{
		TotalSyncFlows: 1,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupPeerFlowStatusQuery(env, connectionGen)
		//complex transaction with random DMLs on a table with toast columns
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
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
		s.NoError(err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	s.compareTableContentsBQ(dstTableName, "id,t1,t2,k")
	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteBQ) Test_Toast_Advance_2_BQ() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_toast_bq_4")
	dstTableName := "test_toast_bq_4"

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			t1 text,
			k int
		);CREATE OR REPLACE FUNCTION random_string( int ) RETURNS TEXT as $$
		SELECT string_agg(substring('0123456789bcdfghjkmnpqrstvwxyz',
		round(random() * 30)::integer, 1), '') FROM generate_series(1, $1);
		$$ language sql;
	`, srcTableName))
	s.NoError(err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_bq_4"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	limits := peerflow.PeerFlowLimits{
		TotalSyncFlows: 1,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupPeerFlowStatusQuery(env, connectionGen)
		//complex transaction with random DMLs on a table with toast columns
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
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
		s.NoError(err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	s.compareTableContentsBQ(dstTableName, "id,t1,k")
	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteBQ) Test_Toast_Advance_3_BQ() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_toast_bq_5")
	dstTableName := "test_toast_bq_5"

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			t1 text,
			t2 text,
			k int
		);CREATE OR REPLACE FUNCTION random_string( int ) RETURNS TEXT as $$
		SELECT string_agg(substring('0123456789bcdfghjkmnpqrstvwxyz',
		round(random() * 30)::integer, 1), '') FROM generate_series(1, $1);
		$$ language sql;
	`, srcTableName))
	s.NoError(err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_bq_5"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	limits := peerflow.PeerFlowLimits{
		TotalSyncFlows: 1,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupPeerFlowStatusQuery(env, connectionGen)
		/*
			transaction updating a single row
			multiple times with changed/unchanged toast columns
		*/
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			BEGIN;
			INSERT INTO %s(t1,t2,k) SELECT random_string(9000),random_string(9000),
			1 FROM generate_series(1,1);
			UPDATE %s SET k=102 WHERE id=1;
			UPDATE %s SET t1='dummy' WHERE id=1;
			UPDATE %s SET t2='dummy' WHERE id=1;
			END;
		`, srcTableName, srcTableName, srcTableName, srcTableName))
		s.NoError(err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	s.compareTableContentsBQ(dstTableName, "id,t1,t2,k")
	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteBQ) Test_Types_BQ() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_types_bq")
	dstTableName := "test_types_bq"

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (id serial PRIMARY KEY,c1 BIGINT,c2 BIT,c3 VARBIT,c4 BOOLEAN,
		c6 BYTEA,c7 CHARACTER,c8 varchar,c9 CIDR,c11 DATE,c12 FLOAT,c13 DOUBLE PRECISION,
		c14 INET,c15 INTEGER,c16 INTERVAL,c17 JSON,c18 JSONB,c21 MACADDR,c22 MONEY,
		c23 NUMERIC,c24 OID,c28 REAL,c29 SMALLINT,c30 SMALLSERIAL,c31 SERIAL,c32 TEXT,
		c33 TIMESTAMP,c34 TIMESTAMPTZ,c35 TIME, c36 TIMETZ,c37 TSQUERY,c38 TSVECTOR,
		c39 TXID_SNAPSHOT,c40 UUID,c41 XML, c42 INT[], c43 FLOAT[], c44 TEXT[]);
	CREATE OR REPLACE FUNCTION random_bytea(bytea_length integer)
		RETURNS bytea AS $body$
			SELECT decode(string_agg(lpad(to_hex(width_bucket(random(), 0, 1, 256)-1),2,'0') ,''), 'hex')
			FROM generate_series(1, $1);
		$body$
		LANGUAGE 'sql'
		VOLATILE
		SET search_path = 'pg_catalog';
	`, srcTableName))
	s.NoError(err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_types_bq"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	limits := peerflow.PeerFlowLimits{

		TotalSyncFlows: 1,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupPeerFlowStatusQuery(env, connectionGen)
		/* test inserting various types*/
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s SELECT 2,2,b'1',b'101',
		true,random_bytea(32),'s','test','1.1.10.2'::cidr,
		CURRENT_DATE,1.23,1.234,'192.168.1.5'::inet,1,
		'5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds'::interval,
		'{"sai":1}'::json,'{"sai":1}'::jsonb,'08:00:2b:01:02:03'::macaddr,
		1.2,1.23,4::oid,1.23,1,1,1,'test',now(),now(),now()::time,now()::timetz,
		'fat & rat'::tsquery,'a fat cat sat on a mat and ate a fat rat'::tsvector,
		txid_current_snapshot(),
		'66073c38-b8df-4bdb-bbca-1c97596b8940'::uuid,xmlcomment('hello'),
		ARRAY[10299301,2579827],
		ARRAY[0.0003, 8902.0092],
		ARRAY['hello','bye'];
		`, srcTableName))
		s.NoError(err)
		fmt.Println("Executed an insert with all types")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	noNulls, err := s.bqHelper.CheckNull(dstTableName, []string{"c41", "c1", "c2", "c3", "c4",
		"c6", "c39", "c40", "id", "c9", "c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18",
		"c21", "c22", "c23", "c24", "c28", "c29", "c30", "c31", "c33", "c34", "c35", "c36",
		"c37", "c38", "c7", "c8", "c32", "c42", "c43", "c44"})
	if err != nil {
		fmt.Println("error  %w", err)
	}
	// Make sure that there are no nulls
	s.True(noNulls)

	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteBQ) Test_Types_Avro_BQ() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_types_avro_bq")
	dstTableName := "test_types_avro_bq"

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (id serial PRIMARY KEY,c1 BIGINT,c2 BIT,c3 VARBIT,c4 BOOLEAN,
		c6 BYTEA,c7 CHARACTER,c8 varchar,c9 CIDR,c11 DATE,c12 FLOAT,c13 DOUBLE PRECISION,
		c14 INET,c15 INTEGER,c16 INTERVAL,c17 JSON,c18 JSONB,c21 MACADDR,c22 MONEY,
		c23 NUMERIC,c24 OID,c28 REAL,c29 SMALLINT,c30 SMALLSERIAL,c31 SERIAL,c32 TEXT,
		c33 TIMESTAMP,c34 TIMESTAMPTZ,c35 TIME, c36 TIMETZ,c37 TSQUERY,c38 TSVECTOR,
		c39 TXID_SNAPSHOT,c40 UUID,c41 XML, c42 INT[], c43 FLOAT[], c44 TEXT[]);
	CREATE OR REPLACE FUNCTION random_bytea(bytea_length integer)
		RETURNS bytea AS $body$
			SELECT decode(string_agg(lpad(to_hex(width_bucket(random(), 0, 1, 256)-1),2,'0') ,''), 'hex')
			FROM generate_series(1, $1);
		$body$
		LANGUAGE 'sql'
		VOLATILE
		SET search_path = 'pg_catalog';
	`, srcTableName))
	s.NoError(err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_types_avro_bq"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		CDCSyncMode:      protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO,
		CdcStagingPath:   "peerdb_staging",
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	limits := peerflow.PeerFlowLimits{

		TotalSyncFlows: 1,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupPeerFlowStatusQuery(env, connectionGen)
		/* test inserting various types*/
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s SELECT 2,2,b'1',b'101',
		true,random_bytea(32),'s','test','1.1.10.2'::cidr,
		CURRENT_DATE,1.23,1.234,'192.168.1.5'::inet,1,
		'5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds'::interval,
		'{"sai":1}'::json,'{"sai":1}'::jsonb,'08:00:2b:01:02:03'::macaddr,
		1.2,1.23,4::oid,1.23,1,1,1,'test',now(),now(),now()::time,now()::timetz,
		'fat & rat'::tsquery,'a fat cat sat on a mat and ate a fat rat'::tsvector,
		txid_current_snapshot(),
		'66073c38-b8df-4bdb-bbca-1c97596b8940'::uuid,xmlcomment('hello'),
		ARRAY[9301,239827],
		ARRAY[0.0003, 1039.0034],
		ARRAY['hello','bye'];
		`, srcTableName))
		s.NoError(err)
		fmt.Println("Executed an insert with all types")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	noNulls, err := s.bqHelper.CheckNull(dstTableName, []string{"c41", "c1", "c2", "c3", "c4",
		"c6", "c39", "c40", "id", "c9", "c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18",
		"c21", "c22", "c23", "c24", "c28", "c29", "c30", "c31", "c33", "c34", "c35", "c36",
		"c37", "c38", "c7", "c8", "c32", "c42", "c43", "c44"})
	if err != nil {
		fmt.Println("error  %w", err)
	}
	// Make sure that there are no nulls
	s.True(noNulls)

	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteBQ) Test_Simple_Flow_BQ_Avro_CDC() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_simple_flow_bq_avro_cdc")
	dstTableName := "test_simple_flow_bq_avro_cdc"

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`, srcTableName))
	s.NoError(err)
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_simple_flow_bq_avro_cdc"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		CDCSyncMode:      protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO,
		CdcStagingPath:   "peerdb_staging",
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	limits := peerflow.PeerFlowLimits{
		TotalSyncFlows: 2,
		MaxBatchSize:   100,
	}

	go func() {
		e2e.SetupPeerFlowStatusQuery(env, connectionGen)
		for i := 0; i < 10; i++ {
			testKey := fmt.Sprintf("test_key_%d", i)
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s (key, value) VALUES ($1, $2)
		`, srcTableName), testKey, testValue)
			s.NoError(err)
		}
		fmt.Println("Inserted 10 rows into the source table")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	count, err := s.bqHelper.CountRows(dstTableName)
	s.NoError(err)
	s.Equal(10, count)

	// TODO: verify that the data is correctly synced to the destination table
	// on the bigquery side

	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteBQ) Test_Multi_Table_BQ() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTable1Name := s.attachSchemaSuffix("test1_bq")
	dstTable1Name := "test1_bq"
	srcTable2Name := s.attachSchemaSuffix("test2_bq")
	dstTable2Name := "test2_bq"

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE %s (id serial primary key, c1 int, c2 text);
	CREATE TABLE %s(id serial primary key, c1 int, c2 text);
	`, srcTable1Name, srcTable2Name))
	s.NoError(err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_multi_table_bq"),
		TableNameMapping: map[string]string{srcTable1Name: dstTable1Name, srcTable2Name: dstTable2Name},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	limits := peerflow.PeerFlowLimits{
		TotalSyncFlows: 1,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupPeerFlowStatusQuery(env, connectionGen)
		/* inserting across multiple tables*/
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s (c1,c2) VALUES (1,'dummy_1');
		INSERT INTO %s (c1,c2) VALUES (-1,'dummy_-1');
		`, srcTable1Name, srcTable2Name))
		s.NoError(err)
		fmt.Println("Executed an insert on two tables")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.T(), env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	count1, err := s.bqHelper.CountRows(dstTable1Name)
	s.NoError(err)
	count2, err := s.bqHelper.CountRows(dstTable2Name)
	s.NoError(err)

	s.Equal(1, count1)
	s.Equal(1, count2)

	env.AssertExpectations(s.T())
}
