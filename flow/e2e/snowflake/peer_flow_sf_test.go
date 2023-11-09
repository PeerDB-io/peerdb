package e2e_snowflake

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	connsnowflake "github.com/PeerDB-io/peer-flow/connectors/snowflake"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/sdk/testsuite"
)

const snowflakeSuffix = "snowflake"

type PeerFlowE2ETestSuiteSF struct {
	testsuite.WorkflowTestSuite

	pool      *pgxpool.Pool
	sfHelper  *SnowflakeTestHelper
	connector *connsnowflake.SnowflakeConnector
}

func TestPeerFlowE2ETestSuiteSF(t *testing.T) {

	t.Run("group", func(t *testing.T) {
		e2etype := reflect.TypeOf((*PeerFlowE2ETestSuiteSF)(nil))
		methodcount := e2etype.NumMethod()
		for methodid := 0; methodid < methodcount; methodid += 1 {
			method := e2etype.Method(methodid)
			if strings.HasPrefix(method.Name, "Test_") {
				t.Run(method.Name, func(t *testing.T) {
					t.Parallel()
					e2e := new(PeerFlowE2ETestSuiteSF)
					if e2e.SetupSuite(t) == nil {
						method.Func.Call([]reflect.Value{reflect.ValueOf(e2e), reflect.ValueOf(t)})
						e2e.TearDownSuite(t)
					}
				})
			}
		}
	})
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

func (s *PeerFlowE2ETestSuiteSF) SetupSuite(t *testing.T) error {
	err := godotenv.Load()
	if err != nil {
		// it's okay if the .env file is not present
		// we will use the default values
		log.Infof("Unable to load .env file, using default values from env")
	}

	log.SetReportCaller(true)

	pool, err := e2e.SetupPostgres(snowflakeSuffix)
	if err != nil {
		t.Error("failed to setup postgres", err)
		return err
	}
	s.pool = pool

	err = s.setupSnowflake()
	if err != nil {
		t.Error("failed to setup snowflake", err)
		return err
	}

	s.connector, err = connsnowflake.NewSnowflakeConnector(context.Background(),
		s.sfHelper.Config)
	if err != nil {
		t.Error(err)
		return err
	}

	return nil
}

// Implement TearDownAllSuite interface to tear down the test suite
func (s *PeerFlowE2ETestSuiteSF) TearDownSuite(t *testing.T) {
	err := e2e.TearDownPostgres(s.pool, snowflakeSuffix)
	if err != nil {
		t.Error("failed to drop Postgres schema", err)
	}

	if s.sfHelper != nil {
		err = s.sfHelper.Cleanup()
		if err != nil {
			t.Error("failed to clean up Snowflake", err)
		}
	}

	err = s.connector.Close()
	if err != nil {
		t.Error(err)
	}
}

func (s *PeerFlowE2ETestSuiteSF) Test_Complete_Simple_Flow_SF(t *testing.T) {
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
	assert.NoError(t, err)
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_simple_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	assert.NoError(t, err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows: 2,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert 10 rows into the source table
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		// insert 10 rows into the source table
		for i := 0; i < 10; i++ {
			testKey := fmt.Sprintf("test_key_%d", i)
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s (key, value) VALUES ($1, $2)
		`, srcTableName), testKey, testValue)
			assert.NoError(t, err)
		}
		fmt.Println("Inserted 10 rows into the source table")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	assert.True(t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "continue as new")

	count, err := s.sfHelper.CountRows("test_simple_flow_sf")
	assert.NoError(t, err)
	assert.Equal(t, 10, count)

	// TODO: verify that the data is correctly synced to the destination table
	// on the bigquery side

	env.AssertExpectations(t)
}

func (s *PeerFlowE2ETestSuiteSF) Test_Complete_Simple_Flow_SF_Avro_CDC(t *testing.T) {
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
	assert.NoError(t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_simple_flow_avro"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
		CDCSyncMode:      protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	assert.NoError(t, err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows: 2,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert 10 rows into the source table
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		// insert 10 rows into the source table
		for i := 0; i < 10; i++ {
			testKey := fmt.Sprintf("test_key_%d", i)
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s (key, value) VALUES ($1, $2)
		`, srcTableName), testKey, testValue)
			assert.NoError(t, err)
		}
		fmt.Println("Inserted 10 rows into the source table")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	assert.True(t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "continue as new")

	count, err := s.sfHelper.CountRows("test_simple_flow_sf_avro_cdc")
	assert.NoError(t, err)
	assert.Equal(t, 10, count)

	// TODO: verify that the data is correctly synced to the destination table
	// on the bigquery side

	env.AssertExpectations(t)
}

func (s *PeerFlowE2ETestSuiteSF) Test_Invalid_Geo_SF_Avro_CDC(t *testing.T) {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_invalid_geo_sf_avro_cdc")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_invalid_geo_sf_avro_cdc")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			line GEOMETRY(LINESTRING) NOT NULL,
			poly GEOGRAPHY(POLYGON) NOT NULL
		);
	`, srcTableName))
	assert.NoError(t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_invalid_geo_sf_avro_cdc"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
		CDCSyncMode:      protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	assert.NoError(t, err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows: 2,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert 10 rows into the source table
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		// insert 10 rows into the source table
		for i := 0; i < 4; i++ {
			_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s (line,poly) VALUES ($1,$2)
		`, srcTableName), "010200000001000000000000000000F03F0000000000000040",
				"0103000020e6100000010000000c0000001a8361d35dc64140afdb8d2b1bc3c9bf1b8ed4685fc641405ba64c"+
					"579dc2c9bf6a6ad95a5fc64140cd82767449c2c9bf9570fbf85ec641408a07944db9c2c9bf729a18a55ec6414021b8b748c7c2c9bfba46de4c"+
					"5fc64140f2567052abc2c9bf2df9c5925fc641409394e16573c2c9bf2df9c5925fc6414049eceda9afc1c9bfdd1cc1a05fc64140fe43faedebc0"+
					"c9bf4694f6065fc64140fe43faedebc0c9bfffe7305f5ec641406693d6f2ddc0c9bf1a8361d35dc64140afdb8d2b1bc3c9bf",
			)
			assert.NoError(t, err)
		}
		fmt.Println("Inserted 4 invalid geography rows into the source table")
		for i := 4; i < 10; i++ {
			_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s (line,poly) VALUES ($1,$2)
		`, srcTableName), "010200000002000000000000000000F03F000000000000004000000000000008400000000000001040",
				"010300000001000000050000000000000000000000000000000000000000000000"+
					"00000000000000000000f03f000000000000f03f000000000000f03f0000000000"+
					"00f03f000000000000000000000000000000000000000000000000")
			assert.NoError(t, err)
		}
		fmt.Println("Inserted 6 valid geography rows and 10 total rows into source")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	assert.True(t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "continue as new")

	// We inserted 4 invalid shapes in each.
	// They should have filtered out as null on destination
	lineCount, err := s.sfHelper.CountNonNullRows("test_invalid_geo_sf_avro_cdc", "line")
	assert.NoError(t, err)
	assert.Equal(t, 6, lineCount)

	polyCount, err := s.sfHelper.CountNonNullRows("test_invalid_geo_sf_avro_cdc", "poly")
	assert.NoError(t, err)
	assert.Equal(t, 6, polyCount)

	// TODO: verify that the data is correctly synced to the destination table
	// on the bigquery side

	env.AssertExpectations(t)
}

func (s *PeerFlowE2ETestSuiteSF) Test_Toast_SF(t *testing.T) {
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
	assert.NoError(t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_sf_1"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	assert.NoError(t, err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows: 1,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		/*
			Executing a transaction which
			1. changes both toast column
			2. changes no toast column
			3. changes 1 toast column
		*/
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			BEGIN;
			INSERT INTO %s (t1,t2,k) SELECT random_string(9000),random_string(9000),
			1 FROM generate_series(1,2);
			UPDATE %s SET k=102 WHERE id=1;
			UPDATE %s SET t1='dummy' WHERE id=2;
			END;
		`, srcTableName, srcTableName, srcTableName))
		assert.NoError(t, err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	assert.True(t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "continue as new")

	s.compareTableContentsSF(t, "test_toast_sf_1", `id,t1,t2,k`, false)
	env.AssertExpectations(t)
}

func (s *PeerFlowE2ETestSuiteSF) Test_Toast_Nochanges_SF(t *testing.T) {
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
	assert.NoError(t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_sf_2"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	assert.NoError(t, err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows: 1,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		/* transaction updating no rows */
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			BEGIN;
			UPDATE %s SET k=102 WHERE id=1;
			UPDATE %s SET t1='dummy' WHERE id=2;
			END;
		`, srcTableName, srcTableName))
		assert.NoError(t, err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	assert.True(t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "continue as new")

	s.compareTableContentsSF(t, "test_toast_sf_2", `id,t1,t2,k`, false)
	env.AssertExpectations(t)
}

func (s *PeerFlowE2ETestSuiteSF) Test_Toast_Advance_1_SF(t *testing.T) {
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
	assert.NoError(t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_sf_3"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	assert.NoError(t, err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows: 2,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
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
		assert.NoError(t, err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	assert.True(t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "continue as new")

	s.compareTableContentsSF(t, "test_toast_sf_3", `id,t1,t2,k`, false)
	env.AssertExpectations(t)
}

func (s *PeerFlowE2ETestSuiteSF) Test_Toast_Advance_2_SF(t *testing.T) {
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
	assert.NoError(t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_sf_4"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	assert.NoError(t, err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows: 1,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
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
		assert.NoError(t, err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	assert.True(t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "continue as new")

	s.compareTableContentsSF(t, "test_toast_sf_4", `id,t1,k`, false)
	env.AssertExpectations(t)
}

func (s *PeerFlowE2ETestSuiteSF) Test_Toast_Advance_3_SF(t *testing.T) {
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
	assert.NoError(t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_sf_5"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	assert.NoError(t, err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows: 1,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
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
		assert.NoError(t, err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	assert.True(t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "continue as new")

	s.compareTableContentsSF(t, "test_toast_sf_5", `id,t1,t2,k`, false)
	env.AssertExpectations(t)
}

func (s *PeerFlowE2ETestSuiteSF) Test_Types_SF(t *testing.T) {
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
		c39 TXID_SNAPSHOT,c40 UUID,c41 XML, c42 GEOMETRY(POINT), c43 GEOGRAPHY(POINT),
		c44 GEOGRAPHY(POLYGON), c45 GEOGRAPHY(LINESTRING), c46 GEOMETRY(LINESTRING), c47 GEOMETRY(POLYGON));
	CREATE OR REPLACE FUNCTION random_bytea(bytea_length integer)
		RETURNS bytea AS $body$
			SELECT decode(string_agg(lpad(to_hex(width_bucket(random(), 0, 1, 256)-1),2,'0') ,''), 'hex')
			FROM generate_series(1, $1);
		$body$
		LANGUAGE 'sql'
		VOLATILE
		SET search_path = 'pg_catalog';
	`, srcTableName))
	assert.NoError(t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_types_sf"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	assert.NoError(t, err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows: 1,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
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
		'POINT(1 2)','POINT(40.7128 -74.0060)','POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))',
		'LINESTRING(-74.0060 40.7128, -73.9352 40.7306, -73.9123 40.7831)','LINESTRING(0 0, 1 1, 2 2)',
		'POLYGON((-74.0060 40.7128, -73.9352 40.7306, -73.9123 40.7831, -74.0060 40.7128))';
		`, srcTableName))
		assert.NoError(t, err)
		fmt.Println("Executed an insert with all types")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	assert.True(t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "continue as new")

	noNulls, err := s.sfHelper.CheckNull("test_types_sf", []string{"c41", "c1", "c2", "c3", "c4",
		"c6", "c39", "c40", "id", "c9", "c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18",
		"c21", "c22", "c23", "c24", "c28", "c29", "c30", "c31", "c33", "c34", "c35", "c36",
		"c37", "c38", "c7", "c8", "c32", "c42", "c43", "c44", "c45", "c46"})
	if err != nil {
		fmt.Println("error  %w", err)
	}
	// Make sure that there are no nulls
	assert.Equal(t, noNulls, true)

	env.AssertExpectations(t)
}

func (s *PeerFlowE2ETestSuiteSF) Test_Types_SF_Avro_CDC(t *testing.T) {
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
		c39 TXID_SNAPSHOT,c40 UUID,c41 XML, c42 GEOMETRY(POINT), c43 GEOGRAPHY(POINT),
		c44 GEOGRAPHY(POLYGON), c45 GEOGRAPHY(LINESTRING), c46 GEOMETRY(LINESTRING), c47 GEOMETRY(POLYGON));
	CREATE OR REPLACE FUNCTION random_bytea(bytea_length integer)
		RETURNS bytea AS $body$
			SELECT decode(string_agg(lpad(to_hex(width_bucket(random(), 0, 1, 256)-1),2,'0') ,''), 'hex')
			FROM generate_series(1, $1);
		$body$
		LANGUAGE 'sql'
		VOLATILE
		SET search_path = 'pg_catalog';
	`, srcTableName))
	assert.NoError(t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_types_sf"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
		CDCSyncMode:      protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	assert.NoError(t, err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows: 1,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
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
		'POINT(1 2)','POINT(40.7128 -74.0060)','POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))',
		'LINESTRING(-74.0060 40.7128, -73.9352 40.7306, -73.9123 40.7831)','LINESTRING(0 0, 1 1, 2 2)',
		'POLYGON((-74.0060 40.7128, -73.9352 40.7306, -73.9123 40.7831, -74.0060 40.7128))';
		`, srcTableName))
		assert.NoError(t, err)
		fmt.Println("Executed an insert with all types")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	assert.True(t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "continue as new")

	noNulls, err := s.sfHelper.CheckNull("test_types_sf_avro_cdc", []string{"c41", "c1", "c2", "c3", "c4",
		"c6", "c39", "c40", "id", "c9", "c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18",
		"c21", "c22", "c23", "c24", "c28", "c29", "c30", "c31", "c33", "c34", "c35", "c36",
		"c37", "c38", "c7", "c8", "c32", "c42", "c43", "c44", "c45", "c46"})
	if err != nil {
		fmt.Println("error  %w", err)
	}
	// Make sure that there are no nulls
	assert.Equal(t, noNulls, true)

	env.AssertExpectations(t)
}

func (s *PeerFlowE2ETestSuiteSF) Test_Multi_Table_SF(t *testing.T) {
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
	assert.NoError(t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_multi_table"),
		TableNameMapping: map[string]string{srcTable1Name: dstTable1Name, srcTable2Name: dstTable2Name},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	assert.NoError(t, err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows: 1,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		/* inserting across multiple tables*/
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s (c1,c2) VALUES (1,'dummy_1');
		INSERT INTO %s (c1,c2) VALUES (-1,'dummy_-1');
		`, srcTable1Name, srcTable2Name))
		assert.NoError(t, err)
		fmt.Println("Executed an insert with all types")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	assert.True(t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	count1, err := s.sfHelper.CountRows("test1_sf")
	assert.NoError(t, err)
	count2, err := s.sfHelper.CountRows("test2_sf")
	assert.NoError(t, err)

	assert.Equal(t, 1, count1)
	assert.Equal(t, 1, count2)

	env.AssertExpectations(t)
}

func (s *PeerFlowE2ETestSuiteSF) Test_Simple_Schema_Changes_SF(t *testing.T) {
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
	assert.NoError(t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_simple_schema_changes"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	assert.NoError(t, err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows: 10,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert and mutate schema repeatedly.
	go func() {
		// insert first row.
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1) VALUES ($1)`, srcTableName), 1)
		assert.NoError(t, err)
		fmt.Println("Inserted initial row in the source table")

		// verify we got our first row.
		e2e.NormalizeFlowCountQuery(env, connectionGen, 2)
		expectedTableSchema := &protos.TableSchema{
			TableIdentifier: strings.ToUpper(dstTableName),
			Columns: map[string]string{
				"ID":                 string(qvalue.QValueKindNumeric),
				"C1":                 string(qvalue.QValueKindNumeric),
				"_PEERDB_IS_DELETED": string(qvalue.QValueKindBoolean),
			},
		}
		output, err := s.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
			TableIdentifiers: []string{dstTableName},
		})
		assert.NoError(t, err)
		assert.Equal(t, expectedTableSchema, output.TableNameSchemaMapping[dstTableName])
		s.compareTableContentsSF(t, "test_simple_schema_changes", "id,c1", false)

		// alter source table, add column c2 and insert another row.
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s ADD COLUMN c2 BIGINT`, srcTableName))
		assert.NoError(t, err)
		fmt.Println("Altered source table, added column c2")
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,c2) VALUES ($1,$2)`, srcTableName), 2, 2)
		assert.NoError(t, err)
		fmt.Println("Inserted row with added c2 in the source table")

		// verify we got our two rows, if schema did not match up it will error.
		e2e.NormalizeFlowCountQuery(env, connectionGen, 4)
		expectedTableSchema = &protos.TableSchema{
			TableIdentifier: strings.ToUpper(dstTableName),
			Columns: map[string]string{
				"ID":                 string(qvalue.QValueKindNumeric),
				"C1":                 string(qvalue.QValueKindNumeric),
				"C2":                 string(qvalue.QValueKindNumeric),
				"_PEERDB_IS_DELETED": string(qvalue.QValueKindBoolean),
			},
		}
		output, err = s.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
			TableIdentifiers: []string{dstTableName},
		})
		assert.NoError(t, err)
		assert.Equal(t, expectedTableSchema, output.TableNameSchemaMapping[dstTableName])
		s.compareTableContentsSF(t, "test_simple_schema_changes", "id,c1,c2", false)

		// alter source table, add column c3, drop column c2 and insert another row.
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s DROP COLUMN c2, ADD COLUMN c3 BIGINT`, srcTableName))
		assert.NoError(t, err)
		fmt.Println("Altered source table, dropped column c2 and added column c3")
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,c3) VALUES ($1,$2)`, srcTableName), 3, 3)
		assert.NoError(t, err)
		fmt.Println("Inserted row with added c3 in the source table")

		// verify we got our two rows, if schema did not match up it will error.
		e2e.NormalizeFlowCountQuery(env, connectionGen, 6)
		expectedTableSchema = &protos.TableSchema{
			TableIdentifier: strings.ToUpper(dstTableName),
			Columns: map[string]string{
				"ID":                 string(qvalue.QValueKindNumeric),
				"C1":                 string(qvalue.QValueKindNumeric),
				"C2":                 string(qvalue.QValueKindNumeric),
				"C3":                 string(qvalue.QValueKindNumeric),
				"_PEERDB_IS_DELETED": string(qvalue.QValueKindBoolean),
			},
		}
		output, err = s.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
			TableIdentifiers: []string{dstTableName},
		})
		assert.NoError(t, err)
		assert.Equal(t, expectedTableSchema, output.TableNameSchemaMapping[dstTableName])
		s.compareTableContentsSF(t, "test_simple_schema_changes", "id,c1,c3", false)

		// alter source table, drop column c3 and insert another row.
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s DROP COLUMN c3`, srcTableName))
		assert.NoError(t, err)
		fmt.Println("Altered source table, dropped column c3")
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1) VALUES ($1)`, srcTableName), 4)
		assert.NoError(t, err)
		fmt.Println("Inserted row after dropping all columns in the source table")

		// verify we got our two rows, if schema did not match up it will error.
		e2e.NormalizeFlowCountQuery(env, connectionGen, 8)
		expectedTableSchema = &protos.TableSchema{
			TableIdentifier: strings.ToUpper(dstTableName),
			Columns: map[string]string{
				"ID":                 string(qvalue.QValueKindNumeric),
				"C1":                 string(qvalue.QValueKindNumeric),
				"C2":                 string(qvalue.QValueKindNumeric),
				"C3":                 string(qvalue.QValueKindNumeric),
				"_PEERDB_IS_DELETED": string(qvalue.QValueKindBoolean),
			},
		}
		output, err = s.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
			TableIdentifiers: []string{dstTableName},
		})
		assert.NoError(t, err)
		assert.Equal(t, expectedTableSchema, output.TableNameSchemaMapping[dstTableName])
		s.compareTableContentsSF(t, "test_simple_schema_changes", "id,c1", false)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	assert.True(t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "continue as new")

	env.AssertExpectations(t)
}

func (s *PeerFlowE2ETestSuiteSF) Test_Composite_PKey_SF(t *testing.T) {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_simple_cpkey")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_simple_cpkey")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT GENERATED ALWAYS AS IDENTITY,
			c1 INT GENERATED BY DEFAULT AS IDENTITY,
			c2 INT,
			t TEXT,
			PRIMARY KEY(id,t)
		);
	`, srcTableName))
	assert.NoError(t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_cpkey_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	assert.NoError(t, err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows: 5,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		// insert 10 rows into the source table
		for i := 0; i < 10; i++ {
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c2,t) VALUES ($1,$2)
		`, srcTableName), i, testValue)
			assert.NoError(t, err)
		}
		fmt.Println("Inserted 10 rows into the source table")

		// verify we got our 10 rows
		e2e.NormalizeFlowCountQuery(env, connectionGen, 2)
		s.compareTableContentsSF(t, "test_simple_cpkey", "id,c1,c2,t", false)

		_, err := s.pool.Exec(context.Background(),
			fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=$1`, srcTableName), 1)
		assert.NoError(t, err)
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=$1`, srcTableName), 0)
		assert.NoError(t, err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	assert.True(t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "continue as new")

	// verify our updates and delete happened
	s.compareTableContentsSF(t, "test_simple_cpkey", "id,c1,c2,t", false)

	env.AssertExpectations(t)
}

func (s *PeerFlowE2ETestSuiteSF) Test_Composite_PKey_Toast_1_SF(t *testing.T) {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_cpkey_toast1")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_cpkey_toast1")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT GENERATED ALWAYS AS IDENTITY,
			c1 INT GENERATED BY DEFAULT AS IDENTITY,
			c2 INT,
			t TEXT,
			t2 TEXT,
			PRIMARY KEY(id,t)
		);CREATE OR REPLACE FUNCTION random_string( int ) RETURNS TEXT as $$
		SELECT string_agg(substring('0123456789bcdfghjkmnpqrstvwxyz',
		round(random() * 30)::integer, 1), '') FROM generate_series(1, $1);
		$$ language sql;
	`, srcTableName))
	assert.NoError(t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_cpkey_toast1_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	assert.NoError(t, err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows: 2,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		rowsTx, err := s.pool.Begin(context.Background())
		assert.NoError(t, err)

		// insert 10 rows into the source table
		for i := 0; i < 10; i++ {
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = rowsTx.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c2,t,t2) VALUES ($1,$2,random_string(9000))
		`, srcTableName), i, testValue)
			assert.NoError(t, err)
		}
		fmt.Println("Inserted 10 rows into the source table")

		_, err = rowsTx.Exec(context.Background(),
			fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=$1`, srcTableName), 1)
		assert.NoError(t, err)
		_, err = rowsTx.Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=$1`, srcTableName), 0)
		assert.NoError(t, err)

		err = rowsTx.Commit(context.Background())
		assert.NoError(t, err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	assert.True(t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "continue as new")

	// verify our updates and delete happened
	s.compareTableContentsSF(t, "test_cpkey_toast1", "id,c1,c2,t,t2", false)

	env.AssertExpectations(t)
}

func (s *PeerFlowE2ETestSuiteSF) Test_Composite_PKey_Toast_2_SF(t *testing.T) {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_cpkey_toast2")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_cpkey_toast2")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT GENERATED ALWAYS AS IDENTITY,
			c1 INT GENERATED BY DEFAULT AS IDENTITY,
			c2 INT,
			t TEXT,
			t2 TEXT,
			PRIMARY KEY(id,t)
		);CREATE OR REPLACE FUNCTION random_string( int ) RETURNS TEXT as $$
		SELECT string_agg(substring('0123456789bcdfghjkmnpqrstvwxyz',
		round(random() * 30)::integer, 1), '') FROM generate_series(1, $1);
		$$ language sql;
	`, srcTableName))
	assert.NoError(t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_cpkey_toast2_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	assert.NoError(t, err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows: 4,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)

		// insert 10 rows into the source table
		for i := 0; i < 10; i++ {
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c2,t,t2) VALUES ($1,$2,random_string(9000))
		`, srcTableName), i, testValue)
			assert.NoError(t, err)
		}
		fmt.Println("Inserted 10 rows into the source table")

		e2e.NormalizeFlowCountQuery(env, connectionGen, 2)
		_, err = s.pool.Exec(context.Background(),
			fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=$1`, srcTableName), 1)
		assert.NoError(t, err)
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=$1`, srcTableName), 0)
		assert.NoError(t, err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	assert.True(t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "continue as new")

	// verify our updates and delete happened
	s.compareTableContentsSF(t, "test_cpkey_toast2", "id,c1,c2,t,t2", false)

	env.AssertExpectations(t)
}
