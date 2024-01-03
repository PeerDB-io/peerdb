package e2e_postgres

import (
	"context"
	"fmt"
	"sync"

	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func (s PeerFlowE2ETestSuitePG) attachSchemaSuffix(tableName string) string {
	return fmt.Sprintf("e2e_test_%s.%s", s.suffix, tableName)
}

func (s PeerFlowE2ETestSuitePG) attachSuffix(input string) string {
	return fmt.Sprintf("%s_%s", input, s.suffix)
}

func (s PeerFlowE2ETestSuitePG) checkPeerdbColumns(dstSchemaQualified string, rowID int8) error {
	query := fmt.Sprintf(`SELECT "_PEERDB_IS_DELETED","_PEERDB_SYNCED_AT" FROM %s WHERE id = %d`,
		dstSchemaQualified, rowID)
	var isDeleted pgtype.Bool
	var syncedAt pgtype.Timestamp
	err := s.pool.QueryRow(context.Background(), query).Scan(&isDeleted, &syncedAt)
	if err != nil {
		return fmt.Errorf("failed to query row: %w", err)
	}

	if !isDeleted.Bool {
		return fmt.Errorf("isDeleted is not true")
	}

	if !syncedAt.Valid {
		return fmt.Errorf("syncedAt is not valid")
	}

	return nil
}

func (s PeerFlowE2ETestSuitePG) Test_Simple_Flow_PG() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_simple_flow")
	dstTableName := s.attachSchemaSuffix("test_simple_flow_dst")

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
		Destination:      s.peer,
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
			e2e.EnvNoError(s.t, env, err)
		}
		s.t.Log("Inserted 10 rows into the source table")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	err = s.comparePGTables(srcTableName, dstTableName, "id,key,value")
	require.NoError(s.t, err)

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuitePG) Test_Simple_Schema_Changes_PG() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_simple_schema_changes")
	dstTableName := s.attachSchemaSuffix("test_simple_schema_changes_dst")

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
		Destination:      s.peer,
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
			TableIdentifier:   dstTableName,
			ColumnNames:       []string{"id", "c1"},
			ColumnTypes:       []string{string(qvalue.QValueKindInt64), string(qvalue.QValueKindInt64)},
			PrimaryKeyColumns: []string{"id"},
		}
		output, err := s.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
			TableIdentifiers: []string{dstTableName},
		})
		e2e.EnvNoError(s.t, env, err)
		require.Equal(s.t, expectedTableSchema, output.TableNameSchemaMapping[dstTableName])
		err = s.comparePGTables(srcTableName, dstTableName, "id,c1")
		e2e.EnvNoError(s.t, env, err)

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
			TableIdentifier: dstTableName,
			ColumnNames:     []string{"id", "c1", "c2"},
			ColumnTypes: []string{
				string(qvalue.QValueKindInt64),
				string(qvalue.QValueKindInt64),
				string(qvalue.QValueKindInt64),
			},
			PrimaryKeyColumns: []string{"id"},
		}
		output, err = s.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
			TableIdentifiers: []string{dstTableName},
		})
		e2e.EnvNoError(s.t, env, err)
		require.Equal(s.t, expectedTableSchema, output.TableNameSchemaMapping[dstTableName])
		err = s.comparePGTables(srcTableName, dstTableName, "id,c1,c2")
		e2e.EnvNoError(s.t, env, err)

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
			TableIdentifier: dstTableName,
			ColumnNames:     []string{"id", "c1", "c2", "c3"},
			ColumnTypes: []string{
				string(qvalue.QValueKindInt64),
				string(qvalue.QValueKindInt64),
				string(qvalue.QValueKindInt64),
				string(qvalue.QValueKindInt64),
			},
			PrimaryKeyColumns: []string{"id"},
		}
		output, err = s.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
			TableIdentifiers: []string{dstTableName},
		})
		e2e.EnvNoError(s.t, env, err)
		require.Equal(s.t, expectedTableSchema, output.TableNameSchemaMapping[dstTableName])
		err = s.comparePGTables(srcTableName, dstTableName, "id,c1,c3")
		e2e.EnvNoError(s.t, env, err)

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
			TableIdentifier: dstTableName,
			ColumnNames:     []string{"id", "c1", "c2", "c3"},
			ColumnTypes: []string{
				string(qvalue.QValueKindInt64),
				string(qvalue.QValueKindInt64),
				string(qvalue.QValueKindInt64),
				string(qvalue.QValueKindInt64),
			},
			PrimaryKeyColumns: []string{"id"},
		}
		output, err = s.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
			TableIdentifiers: []string{dstTableName},
		})
		e2e.EnvNoError(s.t, env, err)
		require.Equal(s.t, expectedTableSchema, output.TableNameSchemaMapping[dstTableName])
		err = s.comparePGTables(srcTableName, dstTableName, "id,c1")
		e2e.EnvNoError(s.t, env, err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuitePG) Test_Composite_PKey_PG() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_simple_cpkey")
	dstTableName := s.attachSchemaSuffix("test_simple_cpkey_dst")

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
		Destination:      s.peer,
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
		err = s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t")
		e2e.EnvNoError(s.t, env, err)

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

	err = s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t")
	require.NoError(s.t, err)

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuitePG) Test_Composite_PKey_Toast_1_PG() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_cpkey_toast1")
	randomString := s.attachSchemaSuffix("random_string")
	dstTableName := s.attachSchemaSuffix("test_cpkey_toast1_dst")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT GENERATED ALWAYS AS IDENTITY,
			c1 INT GENERATED BY DEFAULT AS IDENTITY,
			c2 INT,
			t TEXT,
			t2 TEXT,
			PRIMARY KEY(id,t)
		);CREATE OR REPLACE FUNCTION %s( int ) RETURNS TEXT as $$
		SELECT string_agg(substring('0123456789bcdfghjkmnpqrstvwxyz',
		round(random() * 30)::integer, 1), '') FROM generate_series(1, $1);
		$$ language sql;
	`, srcTableName, randomString))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_cpkey_toast1_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.peer,
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
			INSERT INTO %s(c2,t,t2) VALUES ($1,$2,%s(9000))
		`, srcTableName, randomString), i, testValue)
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
	err = s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t,t2")
	require.NoError(s.t, err)

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuitePG) Test_Composite_PKey_Toast_2_PG() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_cpkey_toast2")
	randomString := s.attachSchemaSuffix("random_string")
	dstTableName := s.attachSchemaSuffix("test_cpkey_toast2_dst")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT GENERATED ALWAYS AS IDENTITY,
			c1 INT GENERATED BY DEFAULT AS IDENTITY,
			c2 INT,
			t TEXT,
			t2 TEXT,
			PRIMARY KEY(id,t)
		);CREATE OR REPLACE FUNCTION %s( int ) RETURNS TEXT as $$
		SELECT string_agg(substring('0123456789bcdfghjkmnpqrstvwxyz',
		round(random() * 30)::integer, 1), '') FROM generate_series(1, $1);
		$$ language sql;
	`, srcTableName, randomString))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_cpkey_toast2_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.peer,
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
			INSERT INTO %s(c2,t,t2) VALUES ($1,$2,%s(9000))
		`, srcTableName, randomString), i, testValue)
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
	err = s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t,t2")
	require.NoError(s.t, err)

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuitePG) Test_PeerDB_Columns() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_peerdb_cols")
	dstTableName := s.attachSchemaSuffix("test_peerdb_cols_dst")

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
		Destination:      s.peer,
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
		e2e.EnvNoError(s.t, env, err)

		// delete that row
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1
		`, srcTableName))
		e2e.EnvNoError(s.t, env, err)
		s.t.Log("Inserted and deleted a row for peerdb column check")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())

	err = env.GetWorkflowError()
	// allow only continue as new error
	require.Contains(s.t, err.Error(), "continue as new")
	checkErr := s.checkPeerdbColumns(dstTableName, 1)
	require.NoError(s.t, checkErr)
	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuitePG) Test_Soft_Delete_Basic() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	cmpTableName := s.attachSchemaSuffix("test_softdel")
	srcTableName := fmt.Sprintf("%s_src", cmpTableName)
	dstTableName := s.attachSchemaSuffix("test_softdel_dst")

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
		Destination: s.peer,
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
	err = s.comparePGTables(cmpTableName, dstTableName, "id,c1,c2,t")
	require.NoError(s.t, err)

	softDeleteQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s WHERE "_PEERDB_IS_DELETED"=TRUE`,
		dstTableName)
	numRows, err := s.countRowsInQuery(softDeleteQuery)
	require.NoError(s.t, err)
	require.Equal(s.t, int64(1), numRows)
}

func (s PeerFlowE2ETestSuitePG) Test_Soft_Delete_IUD_Same_Batch() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	cmpTableName := s.attachSchemaSuffix("test_softdel_iud")
	srcTableName := fmt.Sprintf("%s_src", cmpTableName)
	dstTableName := s.attachSchemaSuffix("test_softdel_iud_dst")

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
		Destination: s.peer,
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
	err = s.comparePGTables(cmpTableName, dstTableName, "id,c1,c2,t")
	require.NoError(s.t, err)

	softDeleteQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s WHERE "_PEERDB_IS_DELETED"=TRUE`,
		dstTableName)
	numRows, err := s.countRowsInQuery(softDeleteQuery)
	require.NoError(s.t, err)
	require.Equal(s.t, int64(1), numRows)
}

func (s PeerFlowE2ETestSuitePG) Test_Soft_Delete_UD_Same_Batch() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	cmpTableName := s.attachSchemaSuffix("test_softdel_ud")
	srcTableName := fmt.Sprintf("%s_src", cmpTableName)
	dstTableName := s.attachSchemaSuffix("test_softdel_ud_dst")

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
		Destination: s.peer,
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
	err = s.comparePGTables(cmpTableName, dstTableName, "id,c1,c2,t")
	require.NoError(s.t, err)

	softDeleteQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s WHERE "_PEERDB_IS_DELETED"=TRUE`,
		dstTableName)
	numRows, err := s.countRowsInQuery(softDeleteQuery)
	require.NoError(s.t, err)
	require.Equal(s.t, int64(1), numRows)
}

func (s PeerFlowE2ETestSuitePG) Test_Soft_Delete_Insert_After_Delete() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	srcTableName := s.attachSchemaSuffix("test_softdel_iad")
	dstTableName := s.attachSchemaSuffix("test_softdel_iad_dst")

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
		Destination: s.peer,
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
	err = s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t")
	require.NoError(s.t, err)

	softDeleteQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s WHERE "_PEERDB_IS_DELETED"=TRUE`,
		dstTableName)
	numRows, err := s.countRowsInQuery(softDeleteQuery)
	require.NoError(s.t, err)
	require.Equal(s.t, int64(0), numRows)
}
