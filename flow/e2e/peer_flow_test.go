package e2e

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/PeerDB-io/peer-flow/activities"
	util "github.com/PeerDB-io/peer-flow/utils"
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
	sfHelper *SnowflakeTestHelper
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

	err = bqHelper.RecreateDataset()
	if err != nil {
		return fmt.Errorf("failed to recreate bigquery dataset: %w", err)
	}

	s.bqHelper = bqHelper
	return nil
}

// setupSnowflake sets up the snowflake connection.
func (s *E2EPeerFlowTestSuite) setupSnowflake() error {
	runID, err := util.RandomUInt64()
	if err != nil {
		return fmt.Errorf("failed to generate random uint64: %w", err)
	}

	testSchemaName := fmt.Sprintf("e2e_test_%d", runID)

	sfHelper, err := NewSnowflakeTestHelper(testSchemaName)
	if err != nil {
		return fmt.Errorf("failed to create snowflake helper: %w", err)
	}

	err = sfHelper.RecreateSchema()
	if err != nil {
		return fmt.Errorf("failed to recreate snowflake schema: %w", err)
	}
	s.sfHelper = sfHelper

	// for every test, drop the _PEERDB_INTERNAL schema
	s.sfHelper.client.DropSchema("_PEERDB_INTERNAL")

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

	err = s.setupSnowflake()
	if err != nil {
		s.Fail("failed to setup snowflake", err)
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

	if s.sfHelper != nil {
		err = s.sfHelper.DropSchema()
		if err != nil {
			s.Fail("failed to drop snowflake schema", err)
		}
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
		Destination:      s.bqHelper.Peer,
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
		Destination:      s.bqHelper.Peer,
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

// Test_Complete_Simple_Flow_BQ tests a complete flow with data in the source table.
// The test inserts 10 rows into the source table and verifies that the data is
// correctly synced to the destination table after sync flow completes.
func (s *E2EPeerFlowTestSuite) Test_Complete_Simple_Flow_BQ() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	_, err := s.pool.Exec(context.Background(), `
		CREATE TABLE e2e_test.test_simple_flow_bq (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`)
	s.NoError(err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "test_complete_single_col_flow",
		TableNameMapping: map[string]string{"e2e_test.test_simple_flow_bq": "test_simple_flow_bq"},
		PostgresPort:     postgresPort,
		Destination:      s.bqHelper.Peer,
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
		s.SetupPeerFlowStatusQuery(env, connectionGen)
		// insert 10 rows into the source table
		for i := 0; i < 10; i++ {
			testKey := fmt.Sprintf("test_key_%d", i)
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.pool.Exec(context.Background(), `
			INSERT INTO e2e_test.test_simple_flow_bq (key, value) VALUES ($1, $2)
		`, testKey, testValue)
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

func (s *E2EPeerFlowTestSuite) Test_Toast_BQ() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	_, err := s.pool.Exec(context.Background(), `

		CREATE TABLE e2e_test.test_toast_bq_1 (
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
		FlowJobName:      "test_toast_bq_1",
		TableNameMapping: map[string]string{"e2e_test.test_toast_bq_1": "test_toast_bq_1"},
		PostgresPort:     postgresPort,
		Destination:      s.bqHelper.Peer,
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
		s.SetupPeerFlowStatusQuery(env, connectionGen)
		/*
			Executing a transaction which
			1. changes both toast column
			2. changes no toast column
			2. changes 1 toast column
		*/
		_, err = s.pool.Exec(context.Background(), `
			BEGIN;
			INSERT INTO e2e_test.test_toast_bq_1(t1,t2,k) SELECT random_string(9000),random_string(9000),
			1 FROM generate_series(1,2);
			UPDATE e2e_test.test_toast_bq_1 SET k=102 WHERE id=1;
			UPDATE e2e_test.test_toast_bq_1 SET t1='dummy' WHERE id=2;
			END;
		`)
		s.NoError(err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflow, &peerFlowInput)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	s.NoError(err)

	s.compareTableContentsBQ("test_toast_bq_1", "id,t1,t2,k")
	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) Test_Toast_Nochanges_BQ() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	_, err := s.pool.Exec(context.Background(), `

		CREATE TABLE e2e_test.test_toast_bq_2 (
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
		FlowJobName:      "test_toast_bq_2",
		TableNameMapping: map[string]string{"e2e_test.test_toast_bq_2": "test_toast_bq_2"},
		PostgresPort:     postgresPort,
		Destination:      s.bqHelper.Peer,
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
		s.SetupPeerFlowStatusQuery(env, connectionGen)
		/* transaction updating no rows */
		_, err = s.pool.Exec(context.Background(), `
			BEGIN;
			UPDATE e2e_test.test_toast_bq_2 SET k=102 WHERE id=1;
			UPDATE e2e_test.test_toast_bq_2 SET t1='dummy' WHERE id=2;
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

	s.compareTableContentsBQ("test_toast_bq_2", "id,t1,t2,k")
	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) Test_Toast_Advance_1_BQ() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	_, err := s.pool.Exec(context.Background(), `

		CREATE TABLE e2e_test.test_toast_bq_3 (
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
		FlowJobName:      "test_toast_bq_3",
		TableNameMapping: map[string]string{"e2e_test.test_toast_bq_3": "test_toast_bq_3"},
		PostgresPort:     postgresPort,
		Destination:      s.bqHelper.Peer,
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
		s.SetupPeerFlowStatusQuery(env, connectionGen)
		//complex transaction with random DMLs on a table with toast columns
		_, err = s.pool.Exec(context.Background(), `
			BEGIN;
			INSERT INTO e2e_test.test_toast_bq_3(t1,t2,k) SELECT random_string(9000),random_string(9000),
			1 FROM generate_series(1,2);
			UPDATE e2e_test.test_toast_bq_3 SET k=102 WHERE id=1;
			UPDATE e2e_test.test_toast_bq_3 SET t1='dummy' WHERE id=2;
			UPDATE e2e_test.test_toast_bq_3 SET t2='dummy' WHERE id=2;
			DELETE FROM e2e_test.test_toast_bq_3 WHERE id=1;
			INSERT INTO e2e_test.test_toast_bq_3(t1,t2,k) SELECT random_string(9000),random_string(9000),
			1 FROM generate_series(1,2);
			UPDATE e2e_test.test_toast_bq_3 SET k=1 WHERE id=1;
			UPDATE e2e_test.test_toast_bq_3 SET t1='dummy1',t2='dummy2' WHERE id=1;
			UPDATE e2e_test.test_toast_bq_3 SET t1='dummy3' WHERE id=3;
			DELETE FROM e2e_test.test_toast_bq_3 WHERE id=2;
			DELETE FROM e2e_test.test_toast_bq_3 WHERE id=3;
			DELETE FROM e2e_test.test_toast_bq_3 WHERE id=2;
			END;
		`)
		s.NoError(err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflow, &peerFlowInput)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	s.NoError(err)

	s.compareTableContentsBQ("test_toast_bq_3", "id,t1,t2,k")
	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) SetupPeerFlowStatusQuery(env *testsuite.TestWorkflowEnvironment,
	connectionGen FlowConnectionGenerationConfig) {
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
}

func (s *E2EPeerFlowTestSuite) Test_Toast_Advance_2_BQ() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	_, err := s.pool.Exec(context.Background(), `

		CREATE TABLE e2e_test.test_toast_bq_4 (
			id SERIAL PRIMARY KEY,
			t1 text,
			k int
		);CREATE OR REPLACE FUNCTION random_string( int ) RETURNS TEXT as $$
		SELECT string_agg(substring('0123456789bcdfghjkmnpqrstvwxyz',
		round(random() * 30)::integer, 1), '') FROM generate_series(1, $1);
		$$ language sql;
	`)
	s.NoError(err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "test_toast_bq_4",
		TableNameMapping: map[string]string{"e2e_test.test_toast_bq_4": "test_toast_bq_4"},
		PostgresPort:     postgresPort,
		Destination:      s.bqHelper.Peer,
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
		s.SetupPeerFlowStatusQuery(env, connectionGen)
		//complex transaction with random DMLs on a table with toast columns
		_, err = s.pool.Exec(context.Background(), `
			BEGIN;
			INSERT INTO e2e_test.test_toast_bq_4(t1,k) SELECT random_string(9000),
			1 FROM generate_series(1,1);
			UPDATE e2e_test.test_toast_bq_4 SET t1=sub.t1 FROM (SELECT random_string(9000) t1
			FROM generate_series(1,1) ) sub WHERE id=1;
			UPDATE e2e_test.test_toast_bq_4 SET k=2 WHERE id=1;
			UPDATE e2e_test.test_toast_bq_4 SET k=3 WHERE id=1;
			UPDATE e2e_test.test_toast_bq_4 SET t1=sub.t1 FROM (SELECT random_string(9000) t1
			FROM generate_series(1,1)) sub WHERE id=1;
			UPDATE e2e_test.test_toast_bq_4 SET k=4 WHERE id=1;
			END;
		`)
		s.NoError(err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflow, &peerFlowInput)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	s.NoError(err)

	s.compareTableContentsBQ("test_toast_bq_4", "id,t1,k")
	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) Test_Toast_Advance_3_BQ() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	_, err := s.pool.Exec(context.Background(), `

		CREATE TABLE e2e_test.test_toast_bq_5 (
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
		FlowJobName:      "test_toast_bq_5",
		TableNameMapping: map[string]string{"e2e_test.test_toast_bq_5": "test_toast_bq_5"},
		PostgresPort:     postgresPort,
		Destination:      s.bqHelper.Peer,
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
		s.SetupPeerFlowStatusQuery(env, connectionGen)
		/*
			transaction updating a single row
			multiple times with changed/unchanged toast columns
		*/
		_, err = s.pool.Exec(context.Background(), `
			BEGIN;
			INSERT INTO e2e_test.test_toast_bq_5(t1,t2,k) SELECT random_string(9000),random_string(9000),
			1 FROM generate_series(1,1);
			UPDATE e2e_test.test_toast_bq_5 SET k=102 WHERE id=1;
			UPDATE e2e_test.test_toast_bq_5 SET t1='dummy' WHERE id=1;
			UPDATE e2e_test.test_toast_bq_5 SET t2='dummy' WHERE id=1;
			END;
		`)
		s.NoError(err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflow, &peerFlowInput)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	s.NoError(err)

	s.compareTableContentsBQ("test_toast_bq_5", "id,t1,t2,k")
	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) Test_Types_BQ() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	_, err := s.pool.Exec(context.Background(), `

	CREATE TABLE e2e_test.test_types_bq(id serial PRIMARY KEY,c1 BIGINT,c2 BIT,c3 VARBIT,c4 BOOLEAN,
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
		FlowJobName:      "test_types_bq",
		TableNameMapping: map[string]string{"e2e_test.test_types_bq": "test_types_bq"},
		PostgresPort:     postgresPort,
		Destination:      s.bqHelper.Peer,
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
		s.SetupPeerFlowStatusQuery(env, connectionGen)
		/* test inserting various types*/
		_, err = s.pool.Exec(context.Background(), `
		INSERT INTO e2e_test.test_types_bq SELECT 2,2,b'1',b'101',
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

	noNulls, err := s.bqHelper.CheckNull("test_types_bq", []string{"c41", "c1", "c2", "c3", "c4",
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

func (s *E2EPeerFlowTestSuite) Test_Multi_Table_BQ() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	_, err := s.pool.Exec(context.Background(), `
	CREATE TABLE e2e_test.test1_bq(id serial primary key, c1 int, c2 text);
	CREATE TABLE e2e_test.test2_bq(id serial primary key, c1 int, c2 text);
	`)
	s.NoError(err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "test_multi_table_bq",
		TableNameMapping: map[string]string{"e2e_test.test1_bq": "test1_bq", "e2e_test.test2_bq": "test2_bq"},
		PostgresPort:     postgresPort,
		Destination:      s.bqHelper.Peer,
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
		s.SetupPeerFlowStatusQuery(env, connectionGen)
		/* inserting across multiple tables*/
		_, err = s.pool.Exec(context.Background(), `
		INSERT INTO e2e_test.test1_bq(c1,c2) VALUES (1,'dummy_1');
		INSERT INTO e2e_test.test2_bq(c1,c2) VALUES (-1,'dummy_-1');
		`)
		s.NoError(err)
		fmt.Println("Executed an insert with all types")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflow, &peerFlowInput)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	count1, err := s.bqHelper.CountRows("test1_bq")
	s.NoError(err)
	count2, err := s.bqHelper.CountRows("test2_bq")
	s.NoError(err)

	s.Equal(1, count1)
	s.Equal(1, count2)

	env.AssertExpectations(s.T())
}

// tests for snowflake

func (s *E2EPeerFlowTestSuite) Test_Complete_Simple_Flow_SF() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	_, err := s.pool.Exec(context.Background(), `
		CREATE TABLE e2e_test.test_simple_flow_sf (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`)
	s.NoError(err)
	tableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_simple_flow_sf")
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "test_complete_single_col_flow",
		TableNameMapping: map[string]string{"e2e_test.test_simple_flow_sf": tableName},
		PostgresPort:     postgresPort,
		Destination:      s.sfHelper.Peer,
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
		s.SetupPeerFlowStatusQuery(env, connectionGen)
		// insert 10 rows into the source table
		for i := 0; i < 10; i++ {
			testKey := fmt.Sprintf("test_key_%d", i)
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.pool.Exec(context.Background(), `
			INSERT INTO e2e_test.test_simple_flow_sf (key, value) VALUES ($1, $2)
		`, testKey, testValue)
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

	count, err := s.sfHelper.CountRows("test_simple_flow_sf")
	s.NoError(err)
	s.Equal(10, count)

	// TODO: verify that the data is correctly synced to the destination table
	// on the bigquery side

	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) Test_Toast_SF() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	_, err := s.pool.Exec(context.Background(), `

		CREATE TABLE e2e_test.test_toast_sf_1 (
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

	tableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_toast_sf_1")
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "test_toast_sf_1",
		TableNameMapping: map[string]string{"e2e_test.test_toast_sf_1": tableName},
		PostgresPort:     postgresPort,
		Destination:      s.sfHelper.Peer,
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
		s.SetupPeerFlowStatusQuery(env, connectionGen)
		/*
			Executing a transaction which
			1. changes both toast column
			2. changes no toast column
			2. changes 1 toast column
		*/
		_, err = s.pool.Exec(context.Background(), `
			BEGIN;
			INSERT INTO e2e_test.test_toast_sf_1(t1,t2,k) SELECT random_string(9000),random_string(9000),
			1 FROM generate_series(1,2);
			UPDATE e2e_test.test_toast_sf_1 SET k=102 WHERE id=1;
			UPDATE e2e_test.test_toast_sf_1 SET t1='dummy' WHERE id=2;
			END;
		`)
		s.NoError(err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflow, &peerFlowInput)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	s.NoError(err)

	s.compareTableContentsSF("test_toast_sf_1", "id,t1,t2,k")
	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) Test_Toast_Nochanges_SF() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	_, err := s.pool.Exec(context.Background(), `

		CREATE TABLE e2e_test.test_toast_sf_2 (
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

	tableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_toast_sf_2")
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "test_toast_sf_2",
		TableNameMapping: map[string]string{"e2e_test.test_toast_sf_2": tableName},
		PostgresPort:     postgresPort,
		Destination:      s.sfHelper.Peer,
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
		s.SetupPeerFlowStatusQuery(env, connectionGen)
		/* transaction updating no rows */
		_, err = s.pool.Exec(context.Background(), `
			BEGIN;
			UPDATE e2e_test.test_toast_sf_2 SET k=102 WHERE id=1;
			UPDATE e2e_test.test_toast_sf_2 SET t1='dummy' WHERE id=2;
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

	s.compareTableContentsSF("test_toast_sf_2", "id,t1,t2,k")
	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) Test_Toast_Advance_1_SF() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	_, err := s.pool.Exec(context.Background(), `

		CREATE TABLE e2e_test.test_toast_sf_3 (
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

	tableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_toast_sf_3")
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "test_toast_sf_3",
		TableNameMapping: map[string]string{"e2e_test.test_toast_sf_3": tableName},
		PostgresPort:     postgresPort,
		Destination:      s.sfHelper.Peer,
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
	// and execute a transaction touching toast columns
	go func() {
		s.SetupPeerFlowStatusQuery(env, connectionGen)
		//complex transaction with random DMLs on a table with toast columns
		_, err = s.pool.Exec(context.Background(), `
			BEGIN;
			INSERT INTO e2e_test.test_toast_sf_3(t1,t2,k) SELECT random_string(9000),random_string(9000),
			1 FROM generate_series(1,2);
			UPDATE e2e_test.test_toast_sf_3 SET k=102 WHERE id=1;
			UPDATE e2e_test.test_toast_sf_3 SET t1='dummy' WHERE id=2;
			UPDATE e2e_test.test_toast_sf_3 SET t2='dummy' WHERE id=2;
			DELETE FROM e2e_test.test_toast_sf_3 WHERE id=1;
			INSERT INTO e2e_test.test_toast_sf_3(t1,t2,k) SELECT random_string(9000),random_string(9000),
			1 FROM generate_series(1,2);
			UPDATE e2e_test.test_toast_sf_3 SET k=1 WHERE id=1;
			UPDATE e2e_test.test_toast_sf_3 SET t1='dummy1',t2='dummy2' WHERE id=1;
			UPDATE e2e_test.test_toast_sf_3 SET t1='dummy3' WHERE id=3;
			DELETE FROM e2e_test.test_toast_sf_3 WHERE id=2;
			DELETE FROM e2e_test.test_toast_sf_3 WHERE id=3;
			DELETE FROM e2e_test.test_toast_sf_3 WHERE id=2;
			END;
		`)
		s.NoError(err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflow, &peerFlowInput)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	s.NoError(err)

	s.compareTableContentsSF("test_toast_sf_3", "id,t1,t2,k")
	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) Test_Toast_Advance_2_SF() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	_, err := s.pool.Exec(context.Background(), `

		CREATE TABLE e2e_test.test_toast_sf_4 (
			id SERIAL PRIMARY KEY,
			t1 text,
			k int
		);CREATE OR REPLACE FUNCTION random_string( int ) RETURNS TEXT as $$
		SELECT string_agg(substring('0123456789bcdfghjkmnpqrstvwxyz',
		round(random() * 30)::integer, 1), '') FROM generate_series(1, $1);
		$$ language sql;
	`)
	s.NoError(err)

	tableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_toast_sf_4")
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "test_toast_sf_4",
		TableNameMapping: map[string]string{"e2e_test.test_toast_sf_4": tableName},
		PostgresPort:     postgresPort,
		Destination:      s.sfHelper.Peer,
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
		s.SetupPeerFlowStatusQuery(env, connectionGen)
		//complex transaction with random DMLs on a table with toast columns
		_, err = s.pool.Exec(context.Background(), `
			BEGIN;
			INSERT INTO e2e_test.test_toast_sf_4(t1,k) SELECT random_string(9000),
			1 FROM generate_series(1,1);
			UPDATE e2e_test.test_toast_sf_4 SET t1=sub.t1 FROM (SELECT random_string(9000) t1
			FROM generate_series(1,1) ) sub WHERE id=1;
			UPDATE e2e_test.test_toast_sf_4 SET k=2 WHERE id=1;
			UPDATE e2e_test.test_toast_sf_4 SET k=3 WHERE id=1;
			UPDATE e2e_test.test_toast_sf_4 SET t1=sub.t1 FROM (SELECT random_string(9000) t1
			FROM generate_series(1,1)) sub WHERE id=1;
			UPDATE e2e_test.test_toast_sf_4 SET k=4 WHERE id=1;
			END;
		`)
		s.NoError(err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflow, &peerFlowInput)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	s.NoError(err)

	s.compareTableContentsSF("test_toast_sf_4", "id,t1,k")
	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) Test_Toast_Advance_3_SF() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	_, err := s.pool.Exec(context.Background(), `

		CREATE TABLE e2e_test.test_toast_sf_5 (
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

	tableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_toast_sf_5")
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "test_toast_sf_5",
		TableNameMapping: map[string]string{"e2e_test.test_toast_sf_5": tableName},
		PostgresPort:     postgresPort,
		Destination:      s.sfHelper.Peer,
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
		s.SetupPeerFlowStatusQuery(env, connectionGen)
		/*
			transaction updating a single row
			multiple times with changed/unchanged toast columns
		*/
		_, err = s.pool.Exec(context.Background(), `
			BEGIN;
			INSERT INTO e2e_test.test_toast_sf_5(t1,t2,k) SELECT random_string(9000),random_string(9000),
			1 FROM generate_series(1,1);
			UPDATE e2e_test.test_toast_sf_5 SET k=102 WHERE id=1;
			UPDATE e2e_test.test_toast_sf_5 SET t1='dummy' WHERE id=1;
			UPDATE e2e_test.test_toast_sf_5 SET t2='dummy' WHERE id=1;
			END;
		`)
		s.NoError(err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflow, &peerFlowInput)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	s.NoError(err)

	s.compareTableContentsSF("test_toast_sf_5", "id,t1,t2,k")
	env.AssertExpectations(s.T())
}

func (s *E2EPeerFlowTestSuite) Test_Types_SF() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	_, err := s.pool.Exec(context.Background(), `

	CREATE TABLE e2e_test.test_types_sf(id serial PRIMARY KEY,c1 BIGINT,c2 BIT,c3 VARBIT,c4 BOOLEAN,
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

	tableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_types_sf")
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "test_types_sf",
		TableNameMapping: map[string]string{"e2e_test.test_types_sf": tableName},
		PostgresPort:     postgresPort,
		Destination:      s.sfHelper.Peer,
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
		s.SetupPeerFlowStatusQuery(env, connectionGen)
		/* test inserting various types*/
		_, err = s.pool.Exec(context.Background(), `
		INSERT INTO e2e_test.test_types_sf SELECT 2,2,b'1',b'101',
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

func (s *E2EPeerFlowTestSuite) Test_Multi_Table_SF() {
	env := s.NewTestWorkflowEnvironment()
	registerWorkflowsAndActivities(env)

	_, err := s.pool.Exec(context.Background(), `
	CREATE TABLE e2e_test.test1_sf(id serial primary key, c1 int, c2 text);
	CREATE TABLE e2e_test.test2_sf(id serial primary key, c1 int, c2 text);
	`)
	s.NoError(err)

	table1 := fmt.Sprintf(s.sfHelper.testSchemaName + ".test1_sf")
	table2 := fmt.Sprintf(s.sfHelper.testSchemaName + ".test2_sf")
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "test_multi_table_sf",
		TableNameMapping: map[string]string{"e2e_test.test1_sf": table1, "e2e_test.test2_sf": table2},
		PostgresPort:     postgresPort,
		Destination:      s.sfHelper.Peer,
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
		s.SetupPeerFlowStatusQuery(env, connectionGen)
		/* inserting across multiple tables*/
		_, err = s.pool.Exec(context.Background(), `
		INSERT INTO e2e_test.test1_sf(c1,c2) VALUES (1,'dummy_1');
		INSERT INTO e2e_test.test2_sf(c1,c2) VALUES (-1,'dummy_-1');
		`)
		s.NoError(err)
		fmt.Println("Executed an insert with all types")
	}()

	env.ExecuteWorkflow(peerflow.PeerFlowWorkflow, &peerFlowInput)

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
