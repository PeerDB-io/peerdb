package e2e_snowflake

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	connsnowflake "github.com/PeerDB-io/peer-flow/connectors/snowflake"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/require"
)

type PeerFlowE2ETestSuiteSF struct {
	t *testing.T

	pgSuffix  string
	pool      *pgxpool.Pool
	sfHelper  *SnowflakeTestHelper
	connector *connsnowflake.SnowflakeConnector
}

func (s PeerFlowE2ETestSuiteSF) T() *testing.T {
	return s.t
}

func (s PeerFlowE2ETestSuiteSF) Pool() *pgxpool.Pool {
	return s.pool
}

func (s PeerFlowE2ETestSuiteSF) Suffix() string {
	return s.pgSuffix
}

func (s PeerFlowE2ETestSuiteSF) GetRows(tableName string, sfSelector string) (*model.QRecordBatch, error) {
	qualifiedTableName := fmt.Sprintf(`%s.%s.%s`, s.sfHelper.testDatabaseName, s.sfHelper.testSchemaName, tableName)
	sfSelQuery := fmt.Sprintf(`SELECT %s FROM %s ORDER BY id`, sfSelector, qualifiedTableName)
	s.t.Logf("running query on snowflake: %s", sfSelQuery)
	return s.sfHelper.ExecuteAndProcessQuery(sfSelQuery)
}

func TestPeerFlowE2ETestSuiteSF(t *testing.T) {
	e2eshared.RunSuite(t, SetupSuite, func(s PeerFlowE2ETestSuiteSF) {
		err := e2e.TearDownPostgres(s.pool, s.pgSuffix)
		if err != nil {
			slog.Error("failed to tear down Postgres", slog.Any("error", err))
			s.t.FailNow()
		}

		if s.sfHelper != nil {
			err = s.sfHelper.Cleanup()
			if err != nil {
				slog.Error("failed to tear down Snowflake", slog.Any("error", err))
				s.t.FailNow()
			}
		}

		err = s.connector.Close()
		if err != nil {
			slog.Error("failed to close Snowflake connector", slog.Any("error", err))
			s.t.FailNow()
		}
	})
}

func (s PeerFlowE2ETestSuiteSF) attachSchemaSuffix(tableName string) string {
	return fmt.Sprintf("e2e_test_%s.%s", s.pgSuffix, tableName)
}

func (s PeerFlowE2ETestSuiteSF) attachSuffix(input string) string {
	return fmt.Sprintf("%s_%s", input, s.pgSuffix)
}

func SetupSuite(t *testing.T) PeerFlowE2ETestSuiteSF {
	t.Helper()

	err := godotenv.Load()
	if err != nil {
		// it's okay if the .env file is not present
		// we will use the default values
		slog.Info("Unable to load .env file, using default values from env")
	}

	suffix := shared.RandomString(8)
	tsSuffix := time.Now().Format("20060102150405")
	pgSuffix := fmt.Sprintf("sf_%s_%s", strings.ToLower(suffix), tsSuffix)

	pool, err := e2e.SetupPostgres(pgSuffix)
	if err != nil || pool == nil {
		slog.Error("failed to setup Postgres", slog.Any("error", err))
		t.FailNow()
	}

	sfHelper, err := NewSnowflakeTestHelper()
	if err != nil {
		slog.Error("failed to setup Snowflake", slog.Any("error", err))
		t.FailNow()
	}

	connector, err := connsnowflake.NewSnowflakeConnector(
		context.Background(),
		sfHelper.Config,
	)
	require.NoError(t, err)

	suite := PeerFlowE2ETestSuiteSF{
		t:         t,
		pgSuffix:  pgSuffix,
		pool:      pool,
		sfHelper:  sfHelper,
		connector: connector,
	}

	return suite
}

func (s PeerFlowE2ETestSuiteSF) Test_Complete_Simple_Flow_SF() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_simple_flow_sf")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_simple_flow_sf")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_simple_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	require.NoError(s.t, err)

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 20,
		MaxBatchSize:     100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert 20 rows into the source table
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		// insert 20 rows into the source table
		for i := 0; i < 20; i++ {
			testKey := fmt.Sprintf("test_key_%d", i)
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s (key, value) VALUES ($1, $2)
		`, srcTableName), testKey, testValue)
			e2e.EnvNoError(s.t, env, err)
		}
		s.t.Log("Inserted 20 rows into the source table")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	count, err := s.sfHelper.CountRows("test_simple_flow_sf")
	require.NoError(s.t, err)
	require.Equal(s.t, 20, count)

	// check the number of rows where _PEERDB_SYNCED_AT is newer than 5 mins ago
	// it should match the count.
	newerSyncedAtQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s WHERE _PEERDB_SYNCED_AT > CURRENT_TIMESTAMP() - INTERVAL '30 MINUTE'
	`, dstTableName)
	numNewRows, err := s.sfHelper.RunIntQuery(newerSyncedAtQuery)
	require.NoError(s.t, err)
	require.Equal(s.t, 20, numNewRows)

	// TODO: verify that the data is correctly synced to the destination table
	// on the Snowflake side

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteSF) Test_Flow_ReplicaIdentity_Index_No_Pkey() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_replica_identity_no_pkey")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_replica_identity_no_pkey")

	// Create a table without a primary key and create a named unique index
	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
		CREATE UNIQUE INDEX unique_idx_on_id_key ON %s (id, key);
		ALTER TABLE %s REPLICA IDENTITY USING INDEX unique_idx_on_id_key;
	`, srcTableName, srcTableName, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_simple_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	require.NoError(s.t, err)

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 20,
		MaxBatchSize:     100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert 20 rows into the source table
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		// insert 20 rows into the source table
		for i := 0; i < 20; i++ {
			testKey := fmt.Sprintf("test_key_%d", i)
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s (id, key, value) VALUES ($1, $2, $3)
		`, srcTableName), i, testKey, testValue)
			e2e.EnvNoError(s.t, env, err)
		}
		s.t.Log("Inserted 20 rows into the source table")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	count, err := s.sfHelper.CountRows("test_replica_identity_no_pkey")
	require.NoError(s.t, err)
	require.Equal(s.t, 20, count)

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteSF) Test_Invalid_Geo_SF_Avro_CDC() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_invalid_geo_sf_avro_cdc")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_invalid_geo_sf_avro_cdc")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			line GEOMETRY(LINESTRING) NOT NULL,
			poly GEOGRAPHY(POLYGON) NOT NULL
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_invalid_geo_sf_avro_cdc"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	require.NoError(s.t, err)

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 10,
		MaxBatchSize:     100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert 10 rows into the source table
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		// insert 4 invalid shapes and 6 valid shapes into the source table
		for i := 0; i < 4; i++ {
			_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s (line,poly) VALUES ($1,$2)
		`, srcTableName), "010200000001000000000000000000F03F0000000000000040",
				"0103000020e6100000010000000c0000001a8361d35dc64140afdb8d2b1bc3c9bf1b8ed4685fc641405ba64c"+
					"579dc2c9bf6a6ad95a5fc64140cd82767449c2c9bf9570fbf85ec641408a07944db9c2c9bf729a18a55ec6414021b8b748c7c2c9bfba46de4c"+
					"5fc64140f2567052abc2c9bf2df9c5925fc641409394e16573c2c9bf2df9c5925fc6414049eceda9afc1c9bfdd1cc1a05fc64140fe43faedebc0"+
					"c9bf4694f6065fc64140fe43faedebc0c9bfffe7305f5ec641406693d6f2ddc0c9bf1a8361d35dc64140afdb8d2b1bc3c9bf",
			)
			e2e.EnvNoError(s.t, env, err)
		}
		s.t.Log("Inserted 4 invalid geography rows into the source table")
		for i := 4; i < 10; i++ {
			_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s (line,poly) VALUES ($1,$2)
		`, srcTableName), "010200000002000000000000000000F03F000000000000004000000000000008400000000000001040",
				"010300000001000000050000000000000000000000000000000000000000000000"+
					"00000000000000000000f03f000000000000f03f000000000000f03f0000000000"+
					"00f03f000000000000000000000000000000000000000000000000")
			e2e.EnvNoError(s.t, env, err)
		}
		s.t.Log("Inserted 6 valid geography rows and 10 total rows into source")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	// We inserted 4 invalid shapes in each.
	// They should have been filtered out as null on destination
	lineCount, err := s.sfHelper.CountNonNullRows("test_invalid_geo_sf_avro_cdc", "line")
	require.NoError(s.t, err)
	require.Equal(s.t, 6, lineCount)

	polyCount, err := s.sfHelper.CountNonNullRows("test_invalid_geo_sf_avro_cdc", "poly")
	require.NoError(s.t, err)
	require.Equal(s.t, 6, polyCount)

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteSF) Test_Toast_SF() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_toast_sf_1")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_toast_sf_1")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			t1 text,
			t2 text,
			k int
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_sf_1"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	require.NoError(s.t, err)

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 4,
		MaxBatchSize:     100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
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
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	e2e.RequireEqualTables(s, "test_toast_sf_1", `id,t1,t2,k`)
	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteSF) Test_Toast_Nochanges_SF() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_toast_sf_2")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_toast_sf_2")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
    CREATE TABLE IF NOT EXISTS %s (
        id SERIAL PRIMARY KEY,
        t1 text,
        t2 text,
        k int
    );
`, srcTableName))
	slog.Info(fmt.Sprintf("Creating table '%s', err: %v", srcTableName, err))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_sf_2"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	require.NoError(s.t, err)

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 0,
		MaxBatchSize:     100,
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		/* transaction updating no rows */
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			BEGIN;
			UPDATE %s SET k=102 WHERE id=1;
			UPDATE %s SET t1='dummy' WHERE id=2;
			END;
		`, srcTableName, srcTableName))
		e2e.EnvNoError(s.t, env, err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	e2e.RequireEqualTables(s, "test_toast_sf_2", `id,t1,t2,k`)
	env.AssertExpectations(s.t)
	wg.Wait()
}

func (s PeerFlowE2ETestSuiteSF) Test_Toast_Advance_1_SF() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_toast_sf_3")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_toast_sf_3")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			t1 text,
			t2 text,
			k int
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_sf_3"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	require.NoError(s.t, err)

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 11,
		MaxBatchSize:     100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		// complex transaction with random DMLs on a table with toast columns
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
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	e2e.RequireEqualTables(s, "test_toast_sf_3", `id,t1,t2,k`)
	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteSF) Test_Toast_Advance_2_SF() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_toast_sf_4")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_toast_sf_4")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			t1 text,
			k int
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_sf_4"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	require.NoError(s.t, err)

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 6,
		MaxBatchSize:     100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		// complex transaction with random DMLs on a table with toast columns
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
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	e2e.RequireEqualTables(s, "test_toast_sf_4", `id,t1,k`)
	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteSF) Test_Toast_Advance_3_SF() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_toast_sf_5")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_toast_sf_5")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			t1 text,
			t2 text,
			k int
	);
`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_sf_5"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	require.NoError(s.t, err)

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 4,
		MaxBatchSize:     100,
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
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	e2e.RequireEqualTables(s, "test_toast_sf_5", `id,t1,t2,k`)
	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteSF) Test_Types_SF() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_types_sf")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_types_sf")
	createMoodEnum := "CREATE TYPE mood AS ENUM ('happy', 'sad', 'angry');"
	var pgErr *pgconn.PgError
	_, enumErr := s.pool.Exec(context.Background(), createMoodEnum)
	if errors.As(enumErr, &pgErr) && pgErr.Code != pgerrcode.DuplicateObject {
		require.NoError(s.t, enumErr)
	}
	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (id serial PRIMARY KEY,c1 BIGINT,c2 BIT,c3 VARBIT,c4 BOOLEAN,
		c6 BYTEA,c7 CHARACTER,c8 varchar,c9 CIDR,c11 DATE,c12 FLOAT,c13 DOUBLE PRECISION,
		c14 INET,c15 INTEGER,c16 INTERVAL,c17 JSON,c18 JSONB,c21 MACADDR,c22 MONEY,
		c23 NUMERIC,c24 OID,c28 REAL,c29 SMALLINT,c30 SMALLSERIAL,c31 SERIAL,c32 TEXT,
		c33 TIMESTAMP,c34 TIMESTAMPTZ,c35 TIME, c36 TIMETZ,c37 TSQUERY,c38 TSVECTOR,
		c39 TXID_SNAPSHOT,c40 UUID,c41 XML, c42 GEOMETRY(POINT), c43 GEOGRAPHY(POINT),
		c44 GEOGRAPHY(POLYGON), c45 GEOGRAPHY(LINESTRING), c46 GEOMETRY(LINESTRING), c47 GEOMETRY(POLYGON), c48 mood);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_types_sf"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	require.NoError(s.t, err)

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 1,
		MaxBatchSize:     100,
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
		'POLYGON((-74.0060 40.7128, -73.9352 40.7306, -73.9123 40.7831, -74.0060 40.7128))', 'happy';
		`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	noNulls, err := s.sfHelper.CheckNull("test_types_sf", []string{
		"c41", "c1", "c2", "c3", "c4",
		"c6", "c39", "c40", "id", "c9", "c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18",
		"c21", "c22", "c23", "c24", "c28", "c29", "c30", "c31", "c33", "c34", "c35", "c36",
		"c37", "c38", "c7", "c8", "c32", "c42", "c43", "c44", "c45", "c46", "c47", "c48",
	})
	if err != nil {
		s.t.Log(err)
	}

	// check if JSON on snowflake side is a good JSON
	err = s.checkJSONValue(dstTableName, "c17", "sai", "1")
	require.NoError(s.t, err)

	// Make sure that there are no nulls
	require.Equal(s.t, noNulls, true)

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteSF) Test_Multi_Table_SF() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTable1Name := s.attachSchemaSuffix("test1_sf")
	srcTable2Name := s.attachSchemaSuffix("test2_sf")
	dstTable1Name := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test1_sf")
	dstTable2Name := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test2_sf")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (id serial primary key, c1 int, c2 text);
	CREATE TABLE IF NOT EXISTS %s (id serial primary key, c1 int, c2 text);
	`, srcTable1Name, srcTable2Name))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_multi_table"),
		TableNameMapping: map[string]string{srcTable1Name: dstTable1Name, srcTable2Name: dstTable2Name},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	require.NoError(s.t, err)

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 2,
		MaxBatchSize:     100,
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
		e2e.EnvNoError(s.t, env, err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	count1, err := s.sfHelper.CountRows("test1_sf")
	require.NoError(s.t, err)
	count2, err := s.sfHelper.CountRows("test2_sf")
	require.NoError(s.t, err)

	require.Equal(s.t, 1, count1)
	require.Equal(s.t, 1, count2)

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteSF) Test_Simple_Schema_Changes_SF() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_simple_schema_changes")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_simple_schema_changes")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			c1 BIGINT
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_simple_schema_changes"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	require.NoError(s.t, err)

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 1,
		MaxBatchSize:     100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert and mutate schema repeatedly.
	go func() {
		// insert first row.
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1) VALUES ($1)`, srcTableName), 1)
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Inserted initial row in the source table")

		// verify we got our first row.
		e2e.NormalizeFlowCountQuery(env, connectionGen, 2)
		expectedTableSchema := &protos.TableSchema{
			TableIdentifier: strings.ToUpper(dstTableName),
			ColumnNames: []string{
				"ID",
				"C1",
				"_PEERDB_IS_DELETED",
				"_PEERDB_SYNCED_AT",
			},
			ColumnTypes: []string{
				string(qvalue.QValueKindNumeric),
				string(qvalue.QValueKindNumeric),
				string(qvalue.QValueKindBoolean),
				string(qvalue.QValueKindTimestamp),
			},
		}
		output, err := s.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
			TableIdentifiers: []string{dstTableName},
		})
		e2e.EnvNoError(s.t, env, err)
		e2e.EnvEqual(s.t, env, expectedTableSchema, output.TableNameSchemaMapping[dstTableName])
		e2e.EnvEqualTables(env, s, "test_simple_schema_changes", "id,c1")

		// alter source table, add column c2 and insert another row.
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s ADD COLUMN c2 BIGINT`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Altered source table, added column c2")
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,c2) VALUES ($1,$2)`, srcTableName), 2, 2)
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Inserted row with added c2 in the source table")

		// verify we got our two rows, if schema did not match up it will error.
		e2e.NormalizeFlowCountQuery(env, connectionGen, 4)
		expectedTableSchema = &protos.TableSchema{
			TableIdentifier: strings.ToUpper(dstTableName),
			ColumnNames: []string{
				"ID",
				"C1",
				"C2",
				"_PEERDB_IS_DELETED",
				"_PEERDB_SYNCED_AT",
			},
			ColumnTypes: []string{
				string(qvalue.QValueKindNumeric),
				string(qvalue.QValueKindNumeric),
				string(qvalue.QValueKindNumeric),
				string(qvalue.QValueKindBoolean),
				string(qvalue.QValueKindTimestamp),
			},
		}
		output, err = s.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
			TableIdentifiers: []string{dstTableName},
		})
		e2e.EnvNoError(s.t, env, err)
		e2e.EnvEqual(s.t, env, expectedTableSchema, output.TableNameSchemaMapping[dstTableName])
		e2e.EnvEqualTables(env, s, "test_simple_schema_changes", "id,c1,c2")

		// alter source table, add column c3, drop column c2 and insert another row.
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s DROP COLUMN c2, ADD COLUMN c3 BIGINT`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Altered source table, dropped column c2 and added column c3")
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,c3) VALUES ($1,$2)`, srcTableName), 3, 3)
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Inserted row with added c3 in the source table")

		// verify we got our two rows, if schema did not match up it will error.
		e2e.NormalizeFlowCountQuery(env, connectionGen, 6)
		expectedTableSchema = &protos.TableSchema{
			TableIdentifier: strings.ToUpper(dstTableName),
			ColumnNames: []string{
				"ID",
				"C1",
				"C2",
				"C3",
				"_PEERDB_IS_DELETED",
				"_PEERDB_SYNCED_AT",
			},
			ColumnTypes: []string{
				string(qvalue.QValueKindNumeric),
				string(qvalue.QValueKindNumeric),
				string(qvalue.QValueKindNumeric),
				string(qvalue.QValueKindNumeric),
				string(qvalue.QValueKindBoolean),
				string(qvalue.QValueKindTimestamp),
			},
		}
		output, err = s.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
			TableIdentifiers: []string{dstTableName},
		})
		e2e.EnvNoError(s.t, env, err)
		e2e.EnvEqual(s.t, env, expectedTableSchema, output.TableNameSchemaMapping[dstTableName])
		e2e.EnvEqualTables(env, s, "test_simple_schema_changes", "id,c1,c3")

		// alter source table, drop column c3 and insert another row.
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s DROP COLUMN c3`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Altered source table, dropped column c3")
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1) VALUES ($1)`, srcTableName), 4)
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Inserted row after dropping all columns in the source table")

		// verify we got our two rows, if schema did not match up it will error.
		e2e.NormalizeFlowCountQuery(env, connectionGen, 8)
		expectedTableSchema = &protos.TableSchema{
			TableIdentifier: strings.ToUpper(dstTableName),
			ColumnNames: []string{
				"ID",
				"C1",
				"C2",
				"C3",
				"_PEERDB_IS_DELETED",
				"_PEERDB_SYNCED_AT",
			},
			ColumnTypes: []string{
				string(qvalue.QValueKindNumeric),
				string(qvalue.QValueKindNumeric),
				string(qvalue.QValueKindNumeric),
				string(qvalue.QValueKindNumeric),
				string(qvalue.QValueKindBoolean),
				string(qvalue.QValueKindTimestamp),
			},
		}
		output, err = s.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
			TableIdentifiers: []string{dstTableName},
		})
		e2e.EnvNoError(s.t, env, err)
		e2e.EnvEqual(s.t, env, expectedTableSchema, output.TableNameSchemaMapping[dstTableName])
		e2e.EnvEqualTables(env, s, "test_simple_schema_changes", "id,c1")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteSF) Test_Composite_PKey_SF() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

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
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_cpkey_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	require.NoError(s.t, err)

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 10,
		MaxBatchSize:     100,
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
			e2e.EnvNoError(s.t, env, err)
		}
		s.t.Log("Inserted 10 rows into the source table")

		// verify we got our 10 rows
		e2e.NormalizeFlowCountQuery(env, connectionGen, 2)
		e2e.EnvEqualTables(env, s, "test_simple_cpkey", "id,c1,c2,t")

		_, err := s.pool.Exec(context.Background(),
			fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=$1`, srcTableName), 1)
		e2e.EnvNoError(s.t, env, err)
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=$1`, srcTableName), 0)
		e2e.EnvNoError(s.t, env, err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	// verify our updates and delete happened
	e2e.RequireEqualTables(s, "test_simple_cpkey", "id,c1,c2,t")

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteSF) Test_Composite_PKey_Toast_1_SF() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

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
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_cpkey_toast1_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	require.NoError(s.t, err)

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 20,
		MaxBatchSize:     100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		rowsTx, err := s.pool.Begin(context.Background())
		e2e.EnvNoError(s.t, env, err)

		// insert 10 rows into the source table
		for i := 0; i < 10; i++ {
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
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	// verify our updates and delete happened
	e2e.RequireEqualTables(s, "test_cpkey_toast1", "id,c1,c2,t,t2")

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteSF) Test_Composite_PKey_Toast_2_SF() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

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
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_cpkey_toast2_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	require.NoError(s.t, err)

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 10,
		MaxBatchSize:     100,
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
			e2e.EnvNoError(s.t, env, err)
		}
		s.t.Log("Inserted 10 rows into the source table")

		e2e.NormalizeFlowCountQuery(env, connectionGen, 2)
		_, err = s.pool.Exec(context.Background(),
			fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=$1`, srcTableName), 1)
		e2e.EnvNoError(s.t, env, err)
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=$1`, srcTableName), 0)
		e2e.EnvNoError(s.t, env, err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	// verify our updates and delete happened
	e2e.RequireEqualTables(s, "test_cpkey_toast2", "id,c1,c2,t,t2")

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteSF) Test_Column_Exclusion() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_exclude_sf")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_exclude_sf")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
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
		FlowJobName: s.attachSuffix("test_exclude_flow"),
	}

	config := &protos.FlowConnectionConfigs{
		FlowJobName: connectionGen.FlowJobName,
		Destination: s.sfHelper.Peer,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: dstTableName,
				Exclude:                    []string{"c2"},
			},
		},
		Source:          e2e.GeneratePostgresPeer(e2e.PostgresPort),
		CdcStagingPath:  connectionGen.CdcStagingPath,
		SyncedAtColName: "_PEERDB_SYNCED_AT",
	}

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 10,
		MaxBatchSize:     100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)

		// insert 10 rows into the source table
		for i := 0; i < 10; i++ {
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c2,t,t2) VALUES ($1,$2,random_string(100))
		`, srcTableName), i, testValue)
			e2e.EnvNoError(s.t, env, err)
		}
		s.t.Log("Inserted 10 rows into the source table")

		e2e.NormalizeFlowCountQuery(env, connectionGen, 2)
		_, err = s.pool.Exec(context.Background(),
			fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=$1`, srcTableName), 1)
		e2e.EnvNoError(s.t, env, err)
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=$1`, srcTableName), 0)
		e2e.EnvNoError(s.t, env, err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, config, &limits, nil)
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()
	require.Contains(s.t, err.Error(), "continue as new")

	query := fmt.Sprintf("SELECT * FROM %s.%s.test_exclude_sf ORDER BY id",
		s.sfHelper.testDatabaseName, s.sfHelper.testSchemaName)
	sfRows, err := s.sfHelper.ExecuteAndProcessQuery(query)
	require.NoError(s.t, err)

	for _, field := range sfRows.Schema.Fields {
		require.NotEqual(s.t, field.Name, "c2")
	}
	require.Equal(s.t, 5, len(sfRows.Schema.Fields))
	require.Equal(s.t, 10, len(sfRows.Records))
}

func (s PeerFlowE2ETestSuiteSF) Test_Soft_Delete_Basic() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	cmpTableName := s.attachSchemaSuffix("test_softdel")
	srcTableName := fmt.Sprintf("%s_src", cmpTableName)
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_softdel")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			c1 INT,
			c2 INT,
			t TEXT
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("test_softdel"),
	}

	config := &protos.FlowConnectionConfigs{
		FlowJobName: connectionGen.FlowJobName,
		Destination: s.sfHelper.Peer,
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
	}

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 3,
		MaxBatchSize:     100,
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	go func() {
		defer wg.Done()
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)

		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c1,c2,t) VALUES (1,2,random_string(9000))`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		e2e.NormalizeFlowCountQuery(env, connectionGen, 1)
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			UPDATE %s SET c1=c1+4 WHERE id=1`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		e2e.NormalizeFlowCountQuery(env, connectionGen, 2)
		// since we delete stuff, create another table to compare with
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			CREATE TABLE %s AS SELECT * FROM %s`, cmpTableName, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, config, &limits, nil)
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()
	require.Contains(s.t, err.Error(), "continue as new")

	wg.Wait()

	// verify our updates and delete happened
	e2e.RequireEqualTables(s, "test_softdel", "id,c1,c2,t")

	newerSyncedAtQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s WHERE _PEERDB_IS_DELETED = TRUE`, dstTableName)
	numNewRows, err := s.sfHelper.RunIntQuery(newerSyncedAtQuery)
	require.NoError(s.t, err)
	require.Equal(s.t, 1, numNewRows)
}

func (s PeerFlowE2ETestSuiteSF) Test_Soft_Delete_IUD_Same_Batch() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	cmpTableName := s.attachSchemaSuffix("test_softdel_iud")
	srcTableName := fmt.Sprintf("%s_src", cmpTableName)
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_softdel_iud")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
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
		Destination: s.sfHelper.Peer,
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
	}

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 3,
		MaxBatchSize:     100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)

		insertTx, err := s.pool.Begin(context.Background())
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
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, config, &limits, nil)
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()
	require.Contains(s.t, err.Error(), "continue as new")

	// verify our updates and delete happened
	e2e.RequireEqualTables(s, "test_softdel_iud", "id,c1,c2,t")

	newerSyncedAtQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s WHERE _PEERDB_IS_DELETED = TRUE`, dstTableName)
	numNewRows, err := s.sfHelper.RunIntQuery(newerSyncedAtQuery)
	require.NoError(s.t, err)
	require.Equal(s.t, 1, numNewRows)
}

func (s PeerFlowE2ETestSuiteSF) Test_Soft_Delete_UD_Same_Batch() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	cmpTableName := s.attachSchemaSuffix("test_softdel_ud")
	srcTableName := fmt.Sprintf("%s_src", cmpTableName)
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_softdel_ud")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			c1 INT,
			c2 INT,
			t TEXT
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("test_softdel_ud"),
	}

	config := &protos.FlowConnectionConfigs{
		FlowJobName: connectionGen.FlowJobName,
		Destination: s.sfHelper.Peer,
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
	}

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 4,
		MaxBatchSize:     100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)

		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c1,c2,t) VALUES (1,2,random_string(9000))`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		e2e.NormalizeFlowCountQuery(env, connectionGen, 1)

		insertTx, err := s.pool.Begin(context.Background())
		e2e.EnvNoError(s.t, env, err)
		_, err = insertTx.Exec(context.Background(), fmt.Sprintf(`
			UPDATE %s SET t=random_string(10000) WHERE id=1`, srcTableName))
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
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, config, &limits, nil)
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()
	require.Contains(s.t, err.Error(), "continue as new")

	// verify our updates and delete happened
	e2e.RequireEqualTables(s, "test_softdel_ud", "id,c1,c2,t")

	newerSyncedAtQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s WHERE _PEERDB_IS_DELETED = TRUE`, dstTableName)
	numNewRows, err := s.sfHelper.RunIntQuery(newerSyncedAtQuery)
	require.NoError(s.t, err)
	require.Equal(s.t, 1, numNewRows)
}

func (s PeerFlowE2ETestSuiteSF) Test_Soft_Delete_Insert_After_Delete() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_softdel_iad")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_softdel_iad")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
			c1 INT,
			c2 INT,
			t TEXT
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("test_softdel_iad"),
	}

	config := &protos.FlowConnectionConfigs{
		FlowJobName: connectionGen.FlowJobName,
		Destination: s.sfHelper.Peer,
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
	}

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 3,
		MaxBatchSize:     100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert and delete rows in the table.
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)

		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c1,c2,t) VALUES (1,2,random_string(9000))`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		e2e.NormalizeFlowCountQuery(env, connectionGen, 1)
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		e2e.NormalizeFlowCountQuery(env, connectionGen, 2)
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(id,c1,c2,t) VALUES (1,3,4,random_string(10000))`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, config, &limits, nil)
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()
	require.Contains(s.t, err.Error(), "continue as new")

	// verify our updates and delete happened
	e2e.RequireEqualTables(s, "test_softdel_iad", "id,c1,c2,t")

	newerSyncedAtQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s WHERE _PEERDB_IS_DELETED = TRUE`, dstTableName)
	numNewRows, err := s.sfHelper.RunIntQuery(newerSyncedAtQuery)
	require.NoError(s.t, err)
	require.Equal(s.t, 0, numNewRows)
}

func (s PeerFlowE2ETestSuiteSF) Test_Supported_Mixed_Case_Table_SF() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("testMixedCase")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "testMixedCase")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS e2e_test_%s."%s" (
			"pulseArmor" SERIAL PRIMARY KEY,
			"highGold" TEXT NOT NULL,
			"eVe" TEXT NOT NULL,
			id SERIAL
		);
	`, s.pgSuffix, "testMixedCase"))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_mixed_case"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.sfHelper.Peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	require.NoError(s.t, err)

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 20,
		MaxBatchSize:     100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert 20 rows into the source table
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		// insert 20 rows into the source table
		for i := 0; i < 20; i++ {
			testKey := fmt.Sprintf("test_key_%d", i)
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO e2e_test_%s."%s"("highGold","eVe") VALUES ($1, $2)
		`, s.pgSuffix, "testMixedCase"), testKey, testValue)
			e2e.EnvNoError(s.t, env, err)
		}
		s.t.Log("Inserted 20 rows into the source table")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	s.compareTableContentsWithDiffSelectorsSF("testMixedCase", `"pulseArmor","highGold","eVe",id`,
		`"pulseArmor","highGold","eVe",id`, true)

	env.AssertExpectations(s.t)
}
