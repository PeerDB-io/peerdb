package e2e_snowflake

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
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
)

const snowflakeSuffix = "snowflake"

type PeerFlowE2ETestSuiteSF struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	pool     *pgxpool.Pool
	sfHelper *SnowflakeTestHelper
}

func TestPeerFlowE2ETestSuiteSF(t *testing.T) {
	suite.Run(t, new(PeerFlowE2ETestSuiteSF))
}

func (s *PeerFlowE2ETestSuiteSF) attachSchemaSuffix(tableName string) string {
	return fmt.Sprintf("e2e_test_%s.%s", snowflakeSuffix, tableName)
}

func (s *PeerFlowE2ETestSuiteSF) attachSuffix(input string) string {
	return fmt.Sprintf("%s_%s", input, snowflakeSuffix)
}

// setupSnowflake sets up the snowflake connection.
func (s *PeerFlowE2ETestSuiteSF) setupSnowflake() error {
	sfHelper, err := NewSnowflakeTestHelper()
	if err != nil {
		return fmt.Errorf("failed to create snowflake helper: %w", err)
	}

	s.sfHelper = sfHelper

	return nil
}

func (s *PeerFlowE2ETestSuiteSF) SetupSuite() {
	err := godotenv.Load()
	if err != nil {
		// it's okay if the .env file is not present
		// we will use the default values
		log.Infof("Unable to load .env file, using default values from env")
	}

	log.SetReportCaller(true)

	pool, err := e2e.SetupPostgres(snowflakeSuffix)
	if err != nil {
		s.Fail("failed to setup postgres", err)
	}
	s.pool = pool

	err = s.setupSnowflake()
	if err != nil {
		s.Fail("failed to setup snowflake", err)
	}
}

// Implement TearDownAllSuite interface to tear down the test suite
func (s *PeerFlowE2ETestSuiteSF) TearDownSuite() {
	err := e2e.TearDownPostgres(s.pool, snowflakeSuffix)
	if err != nil {
		s.Fail("failed to drop Postgres schema", err)
	}

	if s.sfHelper != nil {
		err = s.sfHelper.Cleanup()
		if err != nil {
			s.Fail("failed to clean up Snowflake", err)
		}
	}
}

func (s *PeerFlowE2ETestSuiteSF) Test_Complete_Simple_Flow_SF() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_simple_flow_sf")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_simple_flow_sf")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`, srcTableName))
	s.NoError(err)
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_simple_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
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

	count, err := s.sfHelper.CountRows("test_simple_flow_sf")
	s.NoError(err)
	s.Equal(10, count)

	// TODO: verify that the data is correctly synced to the destination table
	// on the bigquery side

	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteSF) Test_Complete_Simple_Flow_SF_Avro_CDC() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_simple_flow_sf_avro_cdc")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_simple_flow_sf_avro_cdc")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`, srcTableName))
	s.NoError(err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_simple_flow_avro"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
		CDCSyncMode:      protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO,
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

	count, err := s.sfHelper.CountRows("test_simple_flow_sf_avro_cdc")
	s.NoError(err)
	s.Equal(10, count)

	// TODO: verify that the data is correctly synced to the destination table
	// on the bigquery side

	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteSF) Test_Toast_SF() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_toast_sf_1")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_toast_sf_1")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE %s (
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
		FlowJobName:      s.attachSuffix("test_toast_sf_1"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
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
			INSERT INTO %s (t1,t2,k) SELECT random_string(9000),random_string(9000),
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

	s.compareTableContentsSF("test_toast_sf_1", `id,t1,t2,k`, false)
	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteSF) Test_Toast_Nochanges_SF() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_toast_sf_2")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_toast_sf_2")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE %s (
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
		FlowJobName:      s.attachSuffix("test_toast_sf_2"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
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

	s.compareTableContentsSF("test_toast_sf_2", `id,t1,t2,k`, false)
	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteSF) Test_Toast_Advance_1_SF() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_toast_sf_3")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_toast_sf_3")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE %s (
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
		FlowJobName:      s.attachSuffix("test_toast_sf_3"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	limits := peerflow.PeerFlowLimits{
		TotalSyncFlows: 2,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupPeerFlowStatusQuery(env, connectionGen)
		//complex transaction with random DMLs on a table with toast columns
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			BEGIN;
			INSERT INTO %s (t1,t2,k) SELECT random_string(9000),random_string(9000),
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

	s.compareTableContentsSF("test_toast_sf_3", `id,t1,t2,k`, false)
	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteSF) Test_Toast_Advance_2_SF() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_toast_sf_4")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_toast_sf_4")

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
		FlowJobName:      s.attachSuffix("test_toast_sf_4"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
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
			INSERT INTO %s (t1,k) SELECT random_string(9000),
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

	s.compareTableContentsSF("test_toast_sf_4", `id,t1,k`, false)
	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteSF) Test_Toast_Advance_3_SF() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_toast_sf_5")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_toast_sf_5")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE %s (
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
		FlowJobName:      s.attachSuffix("test_toast_sf_5"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
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
			INSERT INTO %s (t1,t2,k) SELECT random_string(9000),random_string(9000),
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

	s.compareTableContentsSF("test_toast_sf_5", `id,t1,t2,k`, false)
	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteSF) Test_Types_SF() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_types_sf")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_types_sf")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE %s (id serial PRIMARY KEY,c1 BIGINT,c2 BIT,c3 VARBIT,c4 BOOLEAN,
		c6 BYTEA,c7 CHARACTER,c8 varchar,c9 CIDR,c11 DATE,c12 FLOAT,c13 DOUBLE PRECISION,
		c14 INET,c15 INTEGER,c16 INTERVAL,c17 JSON,c18 JSONB,c21 MACADDR,c22 MONEY,
		c23 NUMERIC,c24 OID,c28 REAL,c29 SMALLINT,c30 SMALLSERIAL,c31 SERIAL,c32 TEXT,
		c33 TIMESTAMP,c34 TIMESTAMPTZ,c35 TIME, c36 TIMETZ,c37 TSQUERY,c38 TSVECTOR,
		c39 TXID_SNAPSHOT,c40 UUID,c41 XML);
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
		FlowJobName:      s.attachSuffix("test_types_sf"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
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
		'66073c38-b8df-4bdb-bbca-1c97596b8940'::uuid,xmlcomment('hello');
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

	noNulls, err := s.sfHelper.CheckNull("test_types_sf", []string{"c41", "c1", "c2", "c3", "c4",
		"c6", "c39", "c40", "id", "c9", "c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18",
		"c21", "c22", "c23", "c24", "c28", "c29", "c30", "c31", "c33", "c34", "c35", "c36",
		"c37", "c38", "c7", "c8", "c32"})
	if err != nil {
		fmt.Println("error  %w", err)
	}
	// Make sure that there are no nulls
	s.Equal(noNulls, true)

	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteSF) Test_Types_SF_Avro_CDC() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_types_sf_avro_cdc")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_types_sf_avro_cdc")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE %s (id serial PRIMARY KEY,c1 BIGINT,c2 BIT,c3 VARBIT,c4 BOOLEAN,
		c6 BYTEA,c7 CHARACTER,c8 varchar,c9 CIDR,c11 DATE,c12 FLOAT,c13 DOUBLE PRECISION,
		c14 INET,c15 INTEGER,c16 INTERVAL,c17 JSON,c18 JSONB,c21 MACADDR,c22 MONEY,
		c23 NUMERIC,c24 OID,c28 REAL,c29 SMALLINT,c30 SMALLSERIAL,c31 SERIAL,c32 TEXT,
		c33 TIMESTAMP,c34 TIMESTAMPTZ,c35 TIME, c36 TIMETZ,c37 TSQUERY,c38 TSVECTOR,
		c39 TXID_SNAPSHOT,c40 UUID,c41 XML);
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
		FlowJobName:      s.attachSuffix("test_types_sf"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
		CDCSyncMode:      protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO,
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
		'66073c38-b8df-4bdb-bbca-1c97596b8940'::uuid,xmlcomment('hello');
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

	noNulls, err := s.sfHelper.CheckNull("test_types_sf_avro_cdc", []string{"c41", "c1", "c2", "c3", "c4",
		"c6", "c39", "c40", "id", "c9", "c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18",
		"c21", "c22", "c23", "c24", "c28", "c29", "c30", "c31", "c33", "c34", "c35", "c36",
		"c37", "c38", "c7", "c8", "c32"})
	if err != nil {
		fmt.Println("error  %w", err)
	}
	// Make sure that there are no nulls
	s.Equal(noNulls, true)

	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteSF) Test_Multi_Table_SF() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTable1Name := s.attachSchemaSuffix("test1_sf")
	srcTable2Name := s.attachSchemaSuffix("test2_sf")
	dstTable1Name := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test1_sf")
	dstTable2Name := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test2_sf")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE %s (id serial primary key, c1 int, c2 text);
	CREATE TABLE %s (id serial primary key, c1 int, c2 text);
	`, srcTable1Name, srcTable2Name))
	s.NoError(err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_multi_table"),
		TableNameMapping: map[string]string{srcTable1Name: dstTable1Name, srcTable2Name: dstTable2Name},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
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
		fmt.Println("Executed an insert with all types")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	count1, err := s.sfHelper.CountRows("test1_sf")
	s.NoError(err)
	count2, err := s.sfHelper.CountRows("test2_sf")
	s.NoError(err)

	s.Equal(1, count1)
	s.Equal(1, count2)

	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteSF) Test_Simple_Schema_Changes_SF() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_simple_schema_changes")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_simple_schema_changes")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			c1 BIGINT
		);
	`, srcTableName))
	s.NoError(err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_simple_schema_changes"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	limits := peerflow.PeerFlowLimits{
		TotalSyncFlows: 10,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert and mutate schema repeatedly.
	go func() {
		// insert first row.
		e2e.SetupPeerFlowStatusQuery(env, connectionGen)
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1) VALUES ($1)`, srcTableName), 1)
		s.NoError(err)
		fmt.Println("Inserted initial row in the source table")

		// verify we got our first row.
		e2e.NormalizeFlowCountQuery(env, connectionGen, 2)
		s.compareTableSchemasSF("test_simple_schema_changes")
		s.compareTableContentsSF("test_simple_schema_changes", "id,c1", false)

		// alter source table, add column c2 and insert another row.
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s ADD COLUMN c2 BIGINT`, srcTableName))
		s.NoError(err)
		fmt.Println("Altered source table, added column c2")
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,c2) VALUES ($1,$2)`, srcTableName), 2, 2)
		s.NoError(err)
		fmt.Println("Inserted row with added c2 in the source table")

		// verify we got our two rows, if schema did not match up it will error.
		e2e.NormalizeFlowCountQuery(env, connectionGen, 4)
		s.compareTableSchemasSF("test_simple_schema_changes")
		s.compareTableContentsSF("test_simple_schema_changes", "id,c1,c2", false)

		// alter source table, add column c3, drop column c2 and insert another row.
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s DROP COLUMN c2, ADD COLUMN c3 BIGINT`, srcTableName))
		s.NoError(err)
		fmt.Println("Altered source table, dropped column c2 and added column c3")
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,c3) VALUES ($1,$2)`, srcTableName), 3, 3)
		s.NoError(err)
		fmt.Println("Inserted row with added c3 in the source table")

		// verify we got our two rows, if schema did not match up it will error.
		e2e.NormalizeFlowCountQuery(env, connectionGen, 6)
		s.compareTableSchemasSF("test_simple_schema_changes")
		s.compareTableContentsSF("test_simple_schema_changes", "id,c1,c3", false)

		// alter source table, drop column c3 and insert another row.
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s DROP COLUMN c3`, srcTableName))
		s.NoError(err)
		fmt.Println("Altered source table, dropped column c3")
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1) VALUES ($1)`, srcTableName), 4)
		s.NoError(err)
		fmt.Println("Inserted row after dropping all columns in the source table")

		// verify we got our two rows, if schema did not match up it will error.
		e2e.NormalizeFlowCountQuery(env, connectionGen, 8)
		s.compareTableSchemasSF("test_simple_schema_changes")
		s.compareTableContentsSF("test_simple_schema_changes", "id,c1", false)
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	env.AssertExpectations(s.T())
}
