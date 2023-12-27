package e2e_bigquery

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/require"
	"github.com/ysmood/got"
)

type PeerFlowE2ETestSuiteBQ struct {
	got.G
	t *testing.T

	bqSuffix string
	pool     *pgxpool.Pool
	bqHelper *BigQueryTestHelper
}

func TestPeerFlowE2ETestSuiteBQ(t *testing.T) {
	got.Each(t, func(t *testing.T) PeerFlowE2ETestSuiteBQ {
		t.Helper()

		g := got.New(t)
		g.Parallel()

		suite := setupSuite(t, g)

		g.Cleanup(func() {
			suite.tearDownSuite()
		})

		return suite
	})
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
		for _, entry := range record.Entries {
			if entry.Kind == qvalue.QValueKindBoolean {
				isDeleteVal, ok := entry.Value.(bool)
				if !(ok && isDeleteVal) {
					return fmt.Errorf("peerdb column failed: _PEERDB_IS_DELETED is not true")
				}
				recordCount += 1
			}

			if entry.Kind == qvalue.QValueKindTimestamp {
				_, ok := entry.Value.(time.Time)
				if !ok {
					return fmt.Errorf("peerdb column failed: _PEERDB_SYNCED_AT is not valid")
				}

				recordCount += 1
			}
		}
	}

	if recordCount == 0 {
		return fmt.Errorf("peerdb column check failed: no records found")
	}

	return nil
}

// setupBigQuery sets up the bigquery connection.
func setupBigQuery(t *testing.T) *BigQueryTestHelper {
	t.Helper()

	bqHelper, err := NewBigQueryTestHelper()
	if err != nil {
		slog.Error("Error in test", slog.Any("error", err))
		t.FailNow()
	}

	err = bqHelper.RecreateDataset()
	if err != nil {
		slog.Error("Error in test", slog.Any("error", err))
		t.FailNow()
	}

	return bqHelper
}

// Implement SetupAllSuite interface to setup the test suite
func setupSuite(t *testing.T, g got.G) PeerFlowE2ETestSuiteBQ {
	t.Helper()

	err := godotenv.Load()
	if err != nil {
		// it's okay if the .env file is not present
		// we will use the default values
		slog.Info("Unable to load .env file, using default values from env")
	}

	suffix := shared.RandomString(8)
	tsSuffix := time.Now().Format("20060102150405")
	bqSuffix := fmt.Sprintf("bq_%s_%s", strings.ToLower(suffix), tsSuffix)
	pool, err := e2e.SetupPostgres(bqSuffix)
	if err != nil || pool == nil {
		slog.Error("failed to setup postgres", slog.Any("error", err))
		t.FailNow()
	}

	bq := setupBigQuery(t)

	return PeerFlowE2ETestSuiteBQ{
		G:        g,
		t:        t,
		bqSuffix: bqSuffix,
		pool:     pool,
		bqHelper: bq,
	}
}

// Implement TearDownAllSuite interface to tear down the test suite
func (s PeerFlowE2ETestSuiteBQ) tearDownSuite() {
	err := e2e.TearDownPostgres(s.pool, s.bqSuffix)
	if err != nil {
		slog.Error("failed to tear down postgres", slog.Any("error", err))
		s.FailNow()
	}

	err = s.bqHelper.DropDataset(s.bqHelper.datasetName)
	if err != nil {
		slog.Error("failed to tear down bigquery", slog.Any("error", err))
		s.FailNow()
	}
}

func (s PeerFlowE2ETestSuiteBQ) Test_Invalid_Connection_Config() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	// TODO (kaushikiska): ensure flow name can only be alpha numeric and underscores.
	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 0,
		MaxBatchSize:     1,
	}

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, nil, &limits, nil)

	// Verify workflow completes
	s.True(env.IsWorkflowCompleted())
	err := env.GetWorkflowError()

	// assert that error contains "invalid connection configs"
	require.Contains(s.t, err.Error(), "invalid connection configs")

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Complete_Flow_No_Data() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_no_data")
	dstTableName := "test_no_data"

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
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

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	require.NoError(s.t, err)

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 0,
		MaxBatchSize:     1,
	}

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Char_ColType_Error() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_char_coltype")
	dstTableName := "test_char_coltype"

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
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

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	require.NoError(s.t, err)

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 0,
		MaxBatchSize:     1,
	}

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	env.AssertExpectations(s.t)
}

// Test_Complete_Simple_Flow_BQ tests a complete flow with data in the source table.
// The test inserts 10 rows into the source table and verifies that the data is
// correctly synced to the destination table after sync flow completes.
func (s PeerFlowE2ETestSuiteBQ) Test_Complete_Simple_Flow_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_simple_flow_bq")
	dstTableName := "test_simple_flow_bq"

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
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
		// insert 10 rows into the source table
		for i := 0; i < 10; i++ {
			testKey := fmt.Sprintf("test_key_%d", i)
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(key, value) VALUES ($1, $2)
		`, srcTableName), testKey, testValue)
			require.NoError(s.t, err)
		}
		fmt.Println("Inserted 10 rows into the source table")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	count, err := s.bqHelper.countRows(dstTableName)
	require.NoError(s.t, err)
	s.Equal(10, count)

	// TODO: verify that the data is correctly synced to the destination table
	// on the bigquery side

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Toast_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_toast_bq_1")
	dstTableName := "test_toast_bq_1"

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
		FlowJobName:      s.attachSuffix("test_toast_bq_1"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
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
			INSERT INTO %s(t1,t2,k) SELECT random_string(9000),random_string(9000),
			1 FROM generate_series(1,2);
			UPDATE %s SET k=102 WHERE id=1;
			UPDATE %s SET t1='dummy' WHERE id=2;
			END;
		`, srcTableName, srcTableName, srcTableName))
		require.NoError(s.t, err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	s.compareTableContentsBQ(dstTableName, "id,t1,t2,k")
	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Toast_Nochanges_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_toast_bq_2")
	dstTableName := "test_toast_bq_2"

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
		FlowJobName:      s.attachSuffix("test_toast_bq_2"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	require.NoError(s.t, err)

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 0,
		MaxBatchSize:     100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	done := make(chan struct{})
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		/* transaction updating no rows */
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			BEGIN;
			UPDATE %s SET k=102 WHERE id=1;
			UPDATE %s SET t1='dummy' WHERE id=2;
			END;
		`, srcTableName, srcTableName))
		require.NoError(s.t, err)
		fmt.Println("Executed a transaction touching toast columns")
		done <- struct{}{}
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	s.compareTableContentsBQ(dstTableName, "id,t1,t2,k")
	env.AssertExpectations(s.t)
	<-done
}

func (s PeerFlowE2ETestSuiteBQ) Test_Toast_Advance_1_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_toast_bq_3")
	dstTableName := "test_toast_bq_3"

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
		FlowJobName:      s.attachSuffix("test_toast_bq_3"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
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
		require.NoError(s.t, err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	s.compareTableContentsBQ(dstTableName, "id,t1,t2,k")
	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Toast_Advance_2_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_toast_bq_4")
	dstTableName := "test_toast_bq_4"

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
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
		require.NoError(s.t, err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	s.compareTableContentsBQ(dstTableName, "id,t1,k")
	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Toast_Advance_3_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_toast_bq_5")
	dstTableName := "test_toast_bq_5"

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
		FlowJobName:      s.attachSuffix("test_toast_bq_5"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
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
			INSERT INTO %s(t1,t2,k) SELECT random_string(9000),random_string(9000),
			1 FROM generate_series(1,1);
			UPDATE %s SET k=102 WHERE id=1;
			UPDATE %s SET t1='dummy' WHERE id=1;
			UPDATE %s SET t2='dummy' WHERE id=1;
			END;
		`, srcTableName, srcTableName, srcTableName, srcTableName))
		require.NoError(s.t, err)
		fmt.Println("Executed a transaction touching toast columns")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	s.compareTableContentsBQ(dstTableName, "id,t1,t2,k")
	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Types_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_types_bq")
	dstTableName := "test_types_bq"

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (id serial PRIMARY KEY,c1 BIGINT,c2 BIT,c3 VARBIT,c4 BOOLEAN,
		c6 BYTEA,c7 CHARACTER,c8 varchar,c9 CIDR,c11 DATE,c12 FLOAT,c13 DOUBLE PRECISION,
		c14 INET,c15 INTEGER,c16 INTERVAL,c17 JSON,c18 JSONB,c21 MACADDR,c22 MONEY,
		c23 NUMERIC,c24 OID,c28 REAL,c29 SMALLINT,c30 SMALLSERIAL,c31 SERIAL,c32 TEXT,
		c33 TIMESTAMP,c34 TIMESTAMPTZ,c35 TIME, c36 TIMETZ,c37 TSQUERY,c38 TSVECTOR,
		c39 TXID_SNAPSHOT,c40 UUID,c41 XML, c42 INT[], c43 FLOAT[], c44 TEXT[]);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_types_bq"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
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
		ARRAY[10299301,2579827],
		ARRAY[0.0003, 8902.0092],
		ARRAY['hello','bye'];
		`, srcTableName))
		require.NoError(s.t, err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	noNulls, err := s.bqHelper.CheckNull(dstTableName, []string{
		"c41", "c1", "c2", "c3", "c4",
		"c6", "c39", "c40", "id", "c9", "c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18",
		"c21", "c22", "c23", "c24", "c28", "c29", "c30", "c31", "c33", "c34", "c35", "c36",
		"c37", "c38", "c7", "c8", "c32", "c42", "c43", "c44",
	})
	if err != nil {
		fmt.Println("error  %w", err)
	}
	// Make sure that there are no nulls
	s.True(noNulls)

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Multi_Table_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTable1Name := s.attachSchemaSuffix("test1_bq")
	dstTable1Name := "test1_bq"
	srcTable2Name := s.attachSchemaSuffix("test2_bq")
	dstTable2Name := "test2_bq"

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
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
		require.NoError(s.t, err)
		fmt.Println("Executed an insert on two tables")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	count1, err := s.bqHelper.countRows(dstTable1Name)
	require.NoError(s.t, err)
	count2, err := s.bqHelper.countRows(dstTable2Name)
	require.NoError(s.t, err)

	s.Equal(1, count1)
	s.Equal(1, count2)

	env.AssertExpectations(s.t)
}

// TODO: not checking schema exactly, add later
func (s PeerFlowE2ETestSuiteBQ) Test_Simple_Schema_Changes_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_simple_schema_changes")
	dstTableName := "test_simple_schema_changes"

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
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
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
		require.NoError(s.t, err)
		fmt.Println("Inserted initial row in the source table")

		// verify we got our first row.
		e2e.NormalizeFlowCountQuery(env, connectionGen, 2)
		s.compareTableContentsBQ("test_simple_schema_changes", "id,c1")

		// alter source table, add column c2 and insert another row.
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s ADD COLUMN c2 BIGINT`, srcTableName))
		require.NoError(s.t, err)
		fmt.Println("Altered source table, added column c2")
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,c2) VALUES ($1,$2)`, srcTableName), 2, 2)
		require.NoError(s.t, err)
		fmt.Println("Inserted row with added c2 in the source table")

		// verify we got our two rows, if schema did not match up it will error.
		e2e.NormalizeFlowCountQuery(env, connectionGen, 4)
		s.compareTableContentsBQ("test_simple_schema_changes", "id,c1,c2")

		// alter source table, add column c3, drop column c2 and insert another row.
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s DROP COLUMN c2, ADD COLUMN c3 BIGINT`, srcTableName))
		require.NoError(s.t, err)
		fmt.Println("Altered source table, dropped column c2 and added column c3")
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,c3) VALUES ($1,$2)`, srcTableName), 3, 3)
		require.NoError(s.t, err)
		fmt.Println("Inserted row with added c3 in the source table")

		// verify we got our two rows, if schema did not match up it will error.
		e2e.NormalizeFlowCountQuery(env, connectionGen, 6)
		s.compareTableContentsBQ("test_simple_schema_changes", "id,c1,c3")

		// alter source table, drop column c3 and insert another row.
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s DROP COLUMN c3`, srcTableName))
		require.NoError(s.t, err)
		fmt.Println("Altered source table, dropped column c3")
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1) VALUES ($1)`, srcTableName), 4)
		require.NoError(s.t, err)
		fmt.Println("Inserted row after dropping all columns in the source table")

		// verify we got our two rows, if schema did not match up it will error.
		e2e.NormalizeFlowCountQuery(env, connectionGen, 8)
		s.compareTableContentsBQ("test_simple_schema_changes", "id,c1")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Composite_PKey_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_simple_cpkey")
	dstTableName := "test_simple_cpkey"

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
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
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
			require.NoError(s.t, err)
		}
		fmt.Println("Inserted 10 rows into the source table")

		// verify we got our 10 rows
		e2e.NormalizeFlowCountQuery(env, connectionGen, 2)
		s.compareTableContentsBQ(dstTableName, "id,c1,c2,t")

		_, err := s.pool.Exec(context.Background(),
			fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=$1`, srcTableName), 1)
		require.NoError(s.t, err)
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=$1`, srcTableName), 0)
		require.NoError(s.t, err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	s.compareTableContentsBQ(dstTableName, "id,c1,c2,t")

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Composite_PKey_Toast_1_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_cpkey_toast1")
	dstTableName := "test_cpkey_toast1"

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
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
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
		require.NoError(s.t, err)

		// insert 10 rows into the source table
		for i := 0; i < 10; i++ {
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = rowsTx.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c2,t,t2) VALUES ($1,$2,random_string(9000))
		`, srcTableName), i, testValue)
			require.NoError(s.t, err)
		}
		fmt.Println("Inserted 10 rows into the source table")

		_, err = rowsTx.Exec(context.Background(),
			fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=$1`, srcTableName), 1)
		require.NoError(s.t, err)
		_, err = rowsTx.Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=$1`, srcTableName), 0)
		require.NoError(s.t, err)

		err = rowsTx.Commit(context.Background())
		require.NoError(s.t, err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	// verify our updates and delete happened
	s.compareTableContentsBQ(dstTableName, "id,c1,c2,t,t2")

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Composite_PKey_Toast_2_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_cpkey_toast2")
	dstTableName := "test_cpkey_toast2"

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
		Destination:      s.bqHelper.Peer,
		CdcStagingPath:   "",
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
			require.NoError(s.t, err)
		}
		fmt.Println("Inserted 10 rows into the source table")

		e2e.NormalizeFlowCountQuery(env, connectionGen, 2)
		_, err = s.pool.Exec(context.Background(),
			fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=$1`, srcTableName), 1)
		require.NoError(s.t, err)
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=$1`, srcTableName), 0)
		require.NoError(s.t, err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	// verify our updates and delete happened
	s.compareTableContentsBQ(dstTableName, "id,c1,c2,t,t2")

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Columns_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_peerdb_cols")
	dstTableName := "test_peerdb_cols_dst"
	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
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

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	require.NoError(s.t, err)

	limits := peerflow.CDCFlowLimits{
		ExitAfterRecords: 2,
		MaxBatchSize:     100,
	}

	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		// insert 1 row into the source table
		testKey := fmt.Sprintf("test_key_%d", 1)
		testValue := fmt.Sprintf("test_value_%d", 1)
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(key, value) VALUES ($1, $2)
		`, srcTableName), testKey, testValue)
		require.NoError(s.t, err)

		// delete that row
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1
		`, srcTableName))
		require.NoError(s.t, err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	err = s.checkPeerdbColumns(dstTableName, true)
	require.NoError(s.t, err)

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Multi_Table_Multi_Dataset_BQ() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTable1Name := s.attachSchemaSuffix("test1_bq")
	dstTable1Name := "test1_bq"
	secondDataset := fmt.Sprintf("%s_2", s.bqHelper.datasetName)
	srcTable2Name := s.attachSchemaSuffix("test2_bq")
	dstTable2Name := "test2_bq"

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
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
		require.NoError(s.t, err)
		fmt.Println("Executed an insert on two tables")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	count1, err := s.bqHelper.countRows(dstTable1Name)
	require.NoError(s.t, err)
	count2, err := s.bqHelper.countRowsWithDataset(secondDataset, dstTable2Name)
	require.NoError(s.t, err)

	s.Equal(1, count1)
	s.Equal(1, count2)

	err = s.bqHelper.DropDataset(secondDataset)
	require.NoError(s.t, err)

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Soft_Delete_Basic() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	cmpTableName := s.attachSchemaSuffix("test_softdel")
	srcTableName := fmt.Sprintf("%s_src", cmpTableName)
	dstTableName := "test_softdel"

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
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)

		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c1,c2,t) VALUES (1,2,random_string(9000))`, srcTableName))
		require.NoError(s.t, err)
		e2e.NormalizeFlowCountQuery(env, connectionGen, 1)
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			UPDATE %s SET c1=c1+4 WHERE id=1`, srcTableName))
		require.NoError(s.t, err)
		e2e.NormalizeFlowCountQuery(env, connectionGen, 2)
		// since we delete stuff, create another table to compare with
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			CREATE TABLE %s AS SELECT * FROM %s`, cmpTableName, srcTableName))
		require.NoError(s.t, err)
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1`, srcTableName))
		require.NoError(s.t, err)

		wg.Done()
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, config, &limits, nil)
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()
	require.Contains(s.t, err.Error(), "continue as new")

	wg.Wait()

	// verify our updates and delete happened
	s.compareTableContentsBQ("test_softdel", "id,c1,c2,t")

	newerSyncedAtQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM`+"`%s.%s`"+`WHERE _PEERDB_IS_DELETED = TRUE`,
		s.bqHelper.datasetName, dstTableName)
	numNewRows, err := s.bqHelper.RunInt64Query(newerSyncedAtQuery)
	require.NoError(s.t, err)
	s.Eq(1, numNewRows)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Soft_Delete_IUD_Same_Batch() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	cmpTableName := s.attachSchemaSuffix("test_softdel_iud")
	srcTableName := fmt.Sprintf("%s_src", cmpTableName)
	dstTableName := "test_softdel_iud"

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
		require.NoError(s.t, err)

		_, err = insertTx.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c1,c2,t) VALUES (1,2,random_string(9000))`, srcTableName))
		require.NoError(s.t, err)
		_, err = insertTx.Exec(context.Background(), fmt.Sprintf(`
			UPDATE %s SET c1=c1+4 WHERE id=1`, srcTableName))
		require.NoError(s.t, err)
		// since we delete stuff, create another table to compare with
		_, err = insertTx.Exec(context.Background(), fmt.Sprintf(`
			CREATE TABLE %s AS SELECT * FROM %s`, cmpTableName, srcTableName))
		require.NoError(s.t, err)
		_, err = insertTx.Exec(context.Background(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1`, srcTableName))
		require.NoError(s.t, err)

		require.NoError(s.t, insertTx.Commit(context.Background()))
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, config, &limits, nil)
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()
	require.Contains(s.t, err.Error(), "continue as new")

	// verify our updates and delete happened
	s.compareTableContentsBQ("test_softdel_iud", "id,c1,c2,t")

	newerSyncedAtQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM`+"`%s.%s`"+`WHERE _PEERDB_IS_DELETED = TRUE`,
		s.bqHelper.datasetName, dstTableName)
	numNewRows, err := s.bqHelper.RunInt64Query(newerSyncedAtQuery)
	require.NoError(s.t, err)
	s.Eq(1, numNewRows)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Soft_Delete_UD_Same_Batch() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	cmpTableName := s.attachSchemaSuffix("test_softdel_ud")
	srcTableName := fmt.Sprintf("%s_src", cmpTableName)
	dstTableName := "test_softdel_ud"

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
		require.NoError(s.t, err)
		e2e.NormalizeFlowCountQuery(env, connectionGen, 1)

		insertTx, err := s.pool.Begin(context.Background())
		require.NoError(s.t, err)
		_, err = insertTx.Exec(context.Background(), fmt.Sprintf(`
			UPDATE %s SET t=random_string(10000) WHERE id=1`, srcTableName))
		require.NoError(s.t, err)
		_, err = insertTx.Exec(context.Background(), fmt.Sprintf(`
			UPDATE %s SET c1=c1+4 WHERE id=1`, srcTableName))
		require.NoError(s.t, err)
		// since we delete stuff, create another table to compare with
		_, err = insertTx.Exec(context.Background(), fmt.Sprintf(`
			CREATE TABLE %s AS SELECT * FROM %s`, cmpTableName, srcTableName))
		require.NoError(s.t, err)
		_, err = insertTx.Exec(context.Background(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1`, srcTableName))
		require.NoError(s.t, err)

		require.NoError(s.t, insertTx.Commit(context.Background()))
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, config, &limits, nil)
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()
	require.Contains(s.t, err.Error(), "continue as new")

	// verify our updates and delete happened
	s.compareTableContentsBQ("test_softdel_ud", "id,c1,c2,t")

	newerSyncedAtQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM`+"`%s.%s`"+`WHERE _PEERDB_IS_DELETED = TRUE`,
		s.bqHelper.datasetName, dstTableName)
	numNewRows, err := s.bqHelper.RunInt64Query(newerSyncedAtQuery)
	require.NoError(s.t, err)
	s.Eq(1, numNewRows)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Soft_Delete_Insert_After_Delete() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_softdel_iad")
	dstTableName := "test_softdel_iad"

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
		require.NoError(s.t, err)
		e2e.NormalizeFlowCountQuery(env, connectionGen, 1)
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1`, srcTableName))
		require.NoError(s.t, err)
		e2e.NormalizeFlowCountQuery(env, connectionGen, 2)
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(id,c1,c2,t) VALUES (1,3,4,random_string(10000))`, srcTableName))
		require.NoError(s.t, err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, config, &limits, nil)
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()
	require.Contains(s.t, err.Error(), "continue as new")

	// verify our updates and delete happened
	s.compareTableContentsBQ("test_softdel_iad", "id,c1,c2,t")

	newerSyncedAtQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM`+"`%s.%s`"+`WHERE _PEERDB_IS_DELETED = TRUE`,
		s.bqHelper.datasetName, dstTableName)
	numNewRows, err := s.bqHelper.RunInt64Query(newerSyncedAtQuery)
	require.NoError(s.t, err)
	s.Eq(0, numNewRows)
}
