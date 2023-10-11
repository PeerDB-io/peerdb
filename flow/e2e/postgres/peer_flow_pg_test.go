package e2e_postgres

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

func (s *PeerFlowE2ETestSuitePG) attachSchemaSuffix(tableName string) string {
	return fmt.Sprintf("e2e_test_%s.%s", postgresSuffix, tableName)
}

func (s *PeerFlowE2ETestSuitePG) attachSuffix(input string) string {
	return fmt.Sprintf("%s_%s", input, postgresSuffix)
}

func (s *PeerFlowE2ETestSuitePG) Test_Simple_Flow_PG() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_simple_flow")
	dstTableName := s.attachSchemaSuffix("test_simple_flow_dst")

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
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
		Destination:      s.peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

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
			INSERT INTO %s(key, value) VALUES ($1, $2)
		`, srcTableName), testKey, testValue)
			s.NoError(err)
		}
		fmt.Println("Inserted 10 rows into the source table")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	err = s.comparePGTables(srcTableName, dstTableName, "id,key,value")
	s.NoError(err)

	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuitePG) Test_Simple_Schema_Changes_PG() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_simple_schema_changes")
	dstTableName := s.attachSchemaSuffix("test_simple_schema_changes_dst")

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
		Destination:      s.peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

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
		s.NoError(err)
		fmt.Println("Inserted initial row in the source table")

		// verify we got our first row.
		e2e.NormalizeFlowCountQuery(env, connectionGen, 2)
		expectedTableSchema := &protos.TableSchema{
			TableIdentifier: dstTableName,
			Columns: map[string]string{
				"id": string(qvalue.QValueKindInt64),
				"c1": string(qvalue.QValueKindInt64),
			},
			PrimaryKeyColumn: "id",
		}
		output, err := s.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
			TableIdentifiers: []string{dstTableName},
		})
		s.NoError(err)
		s.Equal(expectedTableSchema, output.TableNameSchemaMapping[dstTableName])
		err = s.comparePGTables(srcTableName, dstTableName, "id,c1")
		s.NoError(err)

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
		expectedTableSchema = &protos.TableSchema{
			TableIdentifier: dstTableName,
			Columns: map[string]string{
				"id": string(qvalue.QValueKindInt64),
				"c1": string(qvalue.QValueKindInt64),
				"c2": string(qvalue.QValueKindInt64),
			},
			PrimaryKeyColumn: "id",
		}
		output, err = s.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
			TableIdentifiers: []string{dstTableName},
		})
		s.NoError(err)
		s.Equal(expectedTableSchema, output.TableNameSchemaMapping[dstTableName])
		err = s.comparePGTables(srcTableName, dstTableName, "id,c1,c2")
		s.NoError(err)

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
		expectedTableSchema = &protos.TableSchema{
			TableIdentifier: dstTableName,
			Columns: map[string]string{
				"id": string(qvalue.QValueKindInt64),
				"c1": string(qvalue.QValueKindInt64),
				"c2": string(qvalue.QValueKindInt64),
				"c3": string(qvalue.QValueKindInt64),
			},
			PrimaryKeyColumn: "id",
		}
		output, err = s.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
			TableIdentifiers: []string{dstTableName},
		})
		s.NoError(err)
		s.Equal(expectedTableSchema, output.TableNameSchemaMapping[dstTableName])
		err = s.comparePGTables(srcTableName, dstTableName, "id,c1,c3")
		s.NoError(err)

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
		expectedTableSchema = &protos.TableSchema{
			TableIdentifier: dstTableName,
			Columns: map[string]string{
				"id": string(qvalue.QValueKindInt64),
				"c1": string(qvalue.QValueKindInt64),
				"c2": string(qvalue.QValueKindInt64),
				"c3": string(qvalue.QValueKindInt64),
			},
			PrimaryKeyColumn: "id",
		}
		output, err = s.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
			TableIdentifiers: []string{dstTableName},
		})
		s.NoError(err)
		s.Equal(expectedTableSchema, output.TableNameSchemaMapping[dstTableName])
		err = s.comparePGTables(srcTableName, dstTableName, "id,c1")
		s.NoError(err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuitePG) Test_Composite_PKey_PG() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

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
	s.NoError(err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_cpkey_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

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
			INSERT INTO %s(c2,t) VALUES ($1,$2)
		`, srcTableName), i, testValue)
			s.NoError(err)
		}
		fmt.Println("Inserted 10 rows into the source table")

		// verify we got our 10 rows
		e2e.NormalizeFlowCountQuery(env, connectionGen, 2)
		err = s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t")
		s.NoError(err)

		_, err := s.pool.Exec(context.Background(),
			fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=$1`, srcTableName), 1)
		s.NoError(err)
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=$1`, srcTableName), 0)
		s.NoError(err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	err = s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t")
	s.NoError(err)

	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuitePG) Test_Composite_PKey_Toast_1_PG() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_cpkey_toast1")
	dstTableName := s.attachSchemaSuffix("test_cpkey_toast1_dst")

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
	s.NoError(err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_cpkey_toast1_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.peer,
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows: 2,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		rowsTx, err := s.pool.Begin(context.Background())
		s.NoError(err)

		// insert 10 rows into the source table
		for i := 0; i < 10; i++ {
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = rowsTx.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c2,t,t2) VALUES ($1,$2,random_string(9000))
		`, srcTableName), i, testValue)
			s.NoError(err)
		}
		fmt.Println("Inserted 10 rows into the source table")

		_, err = rowsTx.Exec(context.Background(),
			fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=$1`, srcTableName), 1)
		s.NoError(err)
		_, err = rowsTx.Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=$1`, srcTableName), 0)
		s.NoError(err)

		err = rowsTx.Commit(context.Background())
		s.NoError(err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	// verify our updates and delete happened
	err = s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t,t2")
	s.NoError(err)

	env.AssertExpectations(s.T())
}
