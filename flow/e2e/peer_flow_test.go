package e2e

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/PeerDB-io/peer-flow/activities"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
)

type E2EPeerFlowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	pool     *pgxpool.Pool
	bqHelper *BigQueryTestHelper
}

func TestE2EPeerFlowTestSuite(t *testing.T) {
	suite.Run(t, new(E2EPeerFlowTestSuite))
}

const (
	postgresPort    = 7132
	postgresJdbcURL = "postgres://postgres:postgres@localhost:7132/postgres"
)

// setupPostgres sets up the postgres connection pool.
func (s *E2EPeerFlowTestSuite) setupPostgres() error {
	pool, err := pgxpool.New(context.Background(), postgresJdbcURL)
	if err != nil {
		return fmt.Errorf("failed to create postgres connection pool: %w", err)
	}

	s.pool = pool

	// drop the e2e_test schema if it exists
	_, err = s.pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS e2e_test CASCADE")
	if err != nil {
		return fmt.Errorf("failed to drop e2e_test schema: %w", err)
	}

	// create an e2e_test schema
	_, err = s.pool.Exec(context.Background(), "CREATE SCHEMA e2e_test")
	if err != nil {
		return fmt.Errorf("failed to create e2e_test schema: %w", err)
	}

	// drop all open slots
	_, err = s.pool.Exec(
		context.Background(),
		"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots",
	)
	if err != nil {
		return fmt.Errorf("failed to drop replication slots: %w", err)
	}

	// list all publications from pg_publication table
	rows, err := s.pool.Query(context.Background(), "SELECT pubname FROM pg_publication")
	if err != nil {
		return fmt.Errorf("failed to list publications: %w", err)
	}

	// drop all publications
	for rows.Next() {
		var pubName string
		err = rows.Scan(&pubName)
		if err != nil {
			return fmt.Errorf("failed to scan publication name: %w", err)
		}

		_, err = s.pool.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION %s", pubName))
		if err != nil {
			return fmt.Errorf("failed to drop publication %s: %w", pubName, err)
		}
	}

	return nil
}

// setupBigQuery sets up the bigquery connection.
func (s *E2EPeerFlowTestSuite) setupBigQuery() error {
	bqHelper, err := NewBigQueryTestHelper()
	if err != nil {
		return fmt.Errorf("failed to create bigquery helper: %w", err)
	}

	s.bqHelper = bqHelper
	return nil
}

// Implement SetupAllSuite interface to setup the test suite
func (s *E2EPeerFlowTestSuite) SetupSuite() {
	// seed the random number generator with current time

	rand.Seed(time.Now().UnixNano())

	err := s.setupPostgres()
	if err != nil {
		s.Fail("failed to setup postgres", err)
	}

	err = s.setupBigQuery()
	if err != nil {
		s.Fail("failed to setup bigquery", err)
	}

	err = s.bqHelper.RecreateDataset()
	if err != nil {
		s.Fail("failed to recreate bigquery dataset", err)
	}
}

// Implement TearDownAllSuite interface to tear down the test suite
func (s *E2EPeerFlowTestSuite) TearDownSuite() {
	// drop the e2e_test schema
	_, err := s.pool.Exec(context.Background(), "DROP SCHEMA e2e_test CASCADE")
	if err != nil {
		s.Fail("failed to drop e2e_test schema", err)
	}

	if s.pool != nil {
		s.pool.Close()
	}

	err = s.bqHelper.DropDataset()
	if err != nil {
		s.Fail("failed to drop bigquery dataset", err)
	}
}

func registerWorkflowsAndActivities(env *testsuite.TestWorkflowEnvironment) {
	// set a 300 second timeout for the workflow to execute a few runs.
	env.SetTestTimeout(300 * time.Second)

	env.RegisterWorkflow(peerflow.PeerFlowWorkflow)
	env.RegisterWorkflow(peerflow.SyncFlowWorkflow)
	env.RegisterWorkflow(peerflow.SetupFlowWorkflow)
	env.RegisterWorkflow(peerflow.NormalizeFlowWorkflow)
	env.RegisterWorkflow(peerflow.QRepFlowWorkflow)
	env.RegisterWorkflow(peerflow.QRepPartitionWorkflow)
	env.RegisterActivity(&activities.FetchConfigActivity{})
	env.RegisterActivity(&activities.FlowableActivity{})
}

func (s *E2EPeerFlowTestSuite) Test_Invalid_Connection_Config() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	env.OnActivity("FetchConfig", mock.Anything, mock.Anything).Return(nil, nil)

	// TODO (kaushikiska): ensure flow name can only be alpha numeric and underscores.
	peerFlowInput := peerflow.PeerFlowWorkflowInput{
		PeerFlowName:   "invalid_connection_config",
		CatalogJdbcURL: postgresJdbcURL,
		TotalSyncFlows: 1,
		MaxBatchSize:   1,
	}

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflow, &peerFlowInput)

	// Verify workflow completes
	s.True(env.IsWorkflowCompleted())
	err := env.GetWorkflowError()

	// assert that error contains "invalid connection configs"
	s.Error(err)
	s.Contains(err.Error(), "invalid connection configs")

	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) Test_Complete_Flow_No_Data() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	_, err := s.pool.Exec(context.Background(), `
		CREATE TABLE e2e_test.test (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value VARCHAR(255) NOT NULL
		);
	`)
	s.NoError(err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "test_complete_flow_no_data",
		TableNameMapping: map[string]string{"e2e_test.test": "test"},
		PostgresPort:     postgresPort,
		BigQueryConfig:   s.bqHelper.Config,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	env.OnActivity("FetchConfig", mock.Anything, mock.Anything).Return(flowConnConfig, nil)

	peerFlowInput := peerflow.PeerFlowWorkflowInput{
		PeerFlowName:   connectionGen.FlowJobName,
		CatalogJdbcURL: postgresJdbcURL,
		TotalSyncFlows: 1,
		MaxBatchSize:   1,
	}

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflow, &peerFlowInput)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// assert that error contains "invalid connection configs"
	s.NoError(err)

	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) Test_Char_ColType_Error() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	_, err := s.pool.Exec(context.Background(), `
		CREATE TABLE e2e_test.test_char_table (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value CHAR(255) NOT NULL
		);
	`)
	s.NoError(err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "test_char_table",
		TableNameMapping: map[string]string{"e2e_test.test_char_table": "test"},
		PostgresPort:     postgresPort,
		BigQueryConfig:   s.bqHelper.Config,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	env.OnActivity("FetchConfig", mock.Anything, mock.Anything).Return(flowConnConfig, nil)

	peerFlowInput := peerflow.PeerFlowWorkflowInput{
		PeerFlowName:   connectionGen.FlowJobName,
		CatalogJdbcURL: postgresJdbcURL,
		TotalSyncFlows: 1,
		MaxBatchSize:   1,
	}

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflow, &peerFlowInput)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// assert that error contains "invalid connection configs"
	s.NoError(err)

	env.AssertExpectations(s.T())
}

// Test_Complete_Simple_Flow tests a complete flow with data in the source table.
// The test inserts 10 rows into the source table and verifies that the data is
// correctly synced to the destination table after sync flow completes.
func (s *E2EPeerFlowTestSuite) Test_Complete_Simple_Flow() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	_, err := s.pool.Exec(context.Background(), `
		CREATE TABLE e2e_test.test_simple_flow (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`)
	s.NoError(err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "test_complete_single_col_flow",
		TableNameMapping: map[string]string{"e2e_test.test_simple_flow": "test_simple_flow"},
		PostgresPort:     postgresPort,
		BigQueryConfig:   s.bqHelper.Config,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	env.OnActivity("FetchConfig", mock.Anything, mock.Anything).Return(flowConnConfig, nil)

	peerFlowInput := peerflow.PeerFlowWorkflowInput{
		PeerFlowName:   connectionGen.FlowJobName,
		CatalogJdbcURL: postgresJdbcURL,
		TotalSyncFlows: 2,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert 10 rows into the source table
	go func() {
		// wait for PeerFlowStatusQuery to finish setup
		// sleep for 5 second to allow the workflow to start
		time.Sleep(5 * time.Second)
		for {
			response, err := env.QueryWorkflow(
				peerflow.PeerFlowStatusQuery,
				connectionGen.FlowJobName,
			)
			if err == nil {
				var state peerflow.PeerFlowState
				err = response.Get(&state)
				s.NoError(err)

				if state.SetupComplete {
					fmt.Println("query indicates setup is complete")
					break
				}
			} else {
				// log the error for informational purposes
				fmt.Println(err)
			}
			time.Sleep(1 * time.Second)
		}

		// insert 10 rows into the source table
		for i := 0; i < 10; i++ {
			test_key := fmt.Sprintf("test_key_%d", i)
			test_value := fmt.Sprintf("test_value_%d", i)
			_, err = s.pool.Exec(context.Background(), `
			INSERT INTO e2e_test.test_simple_flow (key, value) VALUES ($1, $2)
		`, test_key, test_value)
			s.NoError(err)
		}
		fmt.Println("Inserted 10 rows into the source table")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflow, &peerFlowInput)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// assert that error contains "invalid connection configs"
	s.NoError(err)

	// TODO: verify that the data is correctly synced to the destination table
	// on the bigquery side

	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) Test_Toast() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	_, err := s.pool.Exec(context.Background(), `
	
		CREATE TABLE e2e_test.test_toast (
			id SERIAL PRIMARY KEY,
			t1 text,
			t2 text,
			k int
		);CREATE OR REPLACE FUNCTION random_string( int ) RETURNS TEXT as $$
		SELECT string_agg(substring('0123456789bcdfghjkmnpqrstvwxyz', 
		round(random() * 30)::integer, 1), '') FROM generate_series(1, $1);
		$$ language sql;
	`)
	s.NoError(err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "test_toast",
		TableNameMapping: map[string]string{"e2e_test.test_toast": "test_toast"},
		PostgresPort:     postgresPort,
		BigQueryConfig:   s.bqHelper.Config,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	env.OnActivity("FetchConfig", mock.Anything, mock.Anything).Return(flowConnConfig, nil)

	peerFlowInput := peerflow.PeerFlowWorkflowInput{
		PeerFlowName:   connectionGen.FlowJobName,
		CatalogJdbcURL: postgresJdbcURL,
		TotalSyncFlows: 1,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		// wait for PeerFlowStatusQuery to finish setup
		// sleep for 5 second to allow the workflow to start
		time.Sleep(5 * time.Second)
		for {
			response, err := env.QueryWorkflow(
				peerflow.PeerFlowStatusQuery,
				connectionGen.FlowJobName,
			)
			if err == nil {
				var state peerflow.PeerFlowState
				err = response.Get(&state)
				s.NoError(err)

				if state.SetupComplete {
					fmt.Println("query indicates setup is complete")
					break
				}
			} else {
				// log the error for informational purposes
				fmt.Println(err)
			}
			time.Sleep(1 * time.Second)
		}

		/*
			Executing a transaction which
			1. changes both toast column
			2. changes no toast column
			2. changes 1 toast column
		*/
		_, err = s.pool.Exec(context.Background(), `
			BEGIN;
			INSERT INTO e2e_test.test_toast(t1,t2,k) SELECT random_string(9000),random_string(9000),
			1 FROM generate_series(1,2);
			UPDATE e2e_test.test_toast SET k=102 WHERE id=1;
			UPDATE e2e_test.test_toast SET t1='dummy' WHERE id=2;
			END;
		`)
		s.NoError(err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflow, &peerFlowInput)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// assert that error contains "invalid connection configs"
	s.NoError(err)

	noNullsT1, err := s.bqHelper.CheckNull("test_toast", []string{"t1"})
	noNullsT2, err := s.bqHelper.CheckNull("test_toast", []string{"t2"})
	if err != nil {
		fmt.Println("error  %w", err)
	}
	// Make sure that there are no nulls
	s.Equal(noNullsT1, true)
	s.Equal(noNullsT2, true)

	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) Test_Types() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	_, err := s.pool.Exec(context.Background(), `
	
	CREATE TABLE e2e_test.test_types(id serial PRIMARY KEY,c1 BIGINT,c2 BIT,c3 VARBIT,c4 BOOLEAN,
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
	`)
	s.NoError(err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "test_types",
		TableNameMapping: map[string]string{"e2e_test.test_types": "test_types"},
		PostgresPort:     postgresPort,
		BigQueryConfig:   s.bqHelper.Config,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	env.OnActivity("FetchConfig", mock.Anything, mock.Anything).Return(flowConnConfig, nil)

	peerFlowInput := peerflow.PeerFlowWorkflowInput{
		PeerFlowName:   connectionGen.FlowJobName,
		CatalogJdbcURL: postgresJdbcURL,
		TotalSyncFlows: 1,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		// wait for PeerFlowStatusQuery to finish setup
		// sleep for 5 second to allow the workflow to start
		time.Sleep(5 * time.Second)
		for {
			response, err := env.QueryWorkflow(
				peerflow.PeerFlowStatusQuery,
				connectionGen.FlowJobName,
			)
			if err == nil {
				var state peerflow.PeerFlowState
				err = response.Get(&state)
				s.NoError(err)

				if state.SetupComplete {
					fmt.Println("query indicates setup is complete")
					break
				}
			} else {
				// log the error for informational purposes
				fmt.Println(err)
			}
			time.Sleep(1 * time.Second)
		}

		/*
			Executing a transaction which
			1. changes both toast column
			2. changes no toast column
			2. changes 1 toast column
		*/
		_, err = s.pool.Exec(context.Background(), `
		INSERT INTO e2e_test.test_types SELECT 2,2,b'1',b'101',
		true,random_bytea(32),'s','test','1.1.10.2'::cidr,
		CURRENT_DATE,1.23,1.234,'192.168.1.5'::inet,1,
		'5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds'::interval,
		'{"sai":1}'::json,'{"sai":1}'::jsonb,'08:00:2b:01:02:03'::macaddr,
		1.2,1.23,4::oid,1.23,1,1,1,'test',now(),now(),now()::time,now()::timetz,
		'fat & rat'::tsquery,'a fat cat sat on a mat and ate a fat rat'::tsvector,
		txid_current_snapshot(),
		'66073c38-b8df-4bdb-bbca-1c97596b8940'::uuid,xmlcomment('hello');
		`)
		s.NoError(err)
		fmt.Println("Executed an insert with all types")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflow, &peerFlowInput)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// assert that error contains "invalid connection configs"
	s.NoError(err)

	noNulls, err := s.bqHelper.CheckNull("test_types", []string{"c41", "c1", "c2", "c3", "c4",
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
