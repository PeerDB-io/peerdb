package e2e_clickhouse

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

func TestPeerFlowE2ETestSuiteCH(t *testing.T) {
	e2eshared.RunSuite(t, SetupSuite)
}

func (s ClickHouseSuite) attachSchemaSuffix(tableName string) string {
	return fmt.Sprintf("e2e_test_%s.%s", s.suffix, tableName)
}

func (s ClickHouseSuite) attachSuffix(input string) string {
	return fmt.Sprintf("%s_%s", input, s.suffix)
}

func (s ClickHouseSuite) Test_Addition_Removal() {
	tc := e2e.NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_table_add_remove")
	addedSrcTableName := s.attachSchemaSuffix("test_table_add_remove_added")
	dstTableName := "test_table_add_remove_target"
	addedDstTableName := "test_table_add_remove_target_added"

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL
		);
	`, srcTableName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id SERIAL PRIMARY KEY,
		key TEXT NOT NULL
	);
	`, addedSrcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhousetableremoval"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 1

	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)

	getFlowStatus := func() protos.FlowStatus {
		var flowStatus protos.FlowStatus
		val, err := env.Query(shared.FlowStatusQuery)
		e2e.EnvNoError(s.t, env, err)
		err = val.Get(&flowStatus)
		e2e.EnvNoError(s.t, env, err)

		return flowStatus
	}

	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s (key) VALUES ('test');
	`, srcTableName))
	require.NoError(s.t, err)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "first insert", "test_table_add_remove", dstTableName, "id,key")
	e2e.SignalWorkflow(env, model.FlowSignal, model.PauseSignal)
	e2e.EnvWaitFor(s.t, env, 4*time.Minute, "pausing for add table", func() bool {
		flowStatus := getFlowStatus()
		return flowStatus == protos.FlowStatus_STATUS_PAUSED
	})

	_, err = s.Conn().Exec(context.Background(),
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
	 WHERE query LIKE '%START_REPLICATION%' AND query LIKE '%clickhousetableremoval%' AND backend_type='walsender'`)
	require.NoError(s.t, err)

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "waiting for replication to stop", func() bool {
		rows, err := s.Conn().Query(context.Background(), `
		SELECT pid FROM pg_stat_activity
		WHERE query LIKE '%START_REPLICATION%' AND query LIKE '%clickhousetableremoval%' AND backend_type='walsender'
		`)
		require.NoError(s.t, err)
		defer rows.Close()
		return !rows.Next()
	})

	e2e.SignalWorkflow(env, model.CDCDynamicPropertiesSignal, &protos.CDCFlowConfigUpdate{
		AdditionalTables: []*protos.TableMapping{
			{
				SourceTableIdentifier:      addedSrcTableName,
				DestinationTableIdentifier: addedDstTableName,
			},
		},
	})

	e2e.EnvWaitFor(s.t, env, 4*time.Minute, "adding table", func() bool {
		flowStatus := getFlowStatus()
		return flowStatus == protos.FlowStatus_STATUS_RUNNING
	})

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s (key) VALUES ('test');
	`, addedSrcTableName))
	require.NoError(s.t, err)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "first insert to added table", "test_table_add_remove_added", addedDstTableName, "id,key")
	e2e.SignalWorkflow(env, model.FlowSignal, model.PauseSignal)
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "pausing again for removing table", func() bool {
		flowStatus := getFlowStatus()
		return flowStatus == protos.FlowStatus_STATUS_PAUSED
	})

	_, err = s.Conn().Exec(context.Background(),
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
	 WHERE query LIKE '%START_REPLICATION%' AND query LIKE '%clickhousetableremoval%' AND backend_type='walsender'`)
	require.NoError(s.t, err)

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "waiting for replication to stop", func() bool {
		rows, err := s.Conn().Query(context.Background(), `
		SELECT pid FROM pg_stat_activity
		WHERE query LIKE '%START_REPLICATION%' AND query LIKE '%clickhousetableremoval%' AND backend_type='walsender'
		`)
		require.NoError(s.t, err)
		defer rows.Close()
		return !rows.Next()
	})

	e2e.SignalWorkflow(env, model.CDCDynamicPropertiesSignal, &protos.CDCFlowConfigUpdate{
		RemovedTables: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: dstTableName,
			},
		},
	})

	e2e.EnvWaitFor(s.t, env, 4*time.Minute, "removing table", func() bool {
		flowStatus := getFlowStatus()
		return flowStatus == protos.FlowStatus_STATUS_RUNNING
	})

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
	INSERT INTO %s (key) VALUES ('test');
	`, srcTableName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
	INSERT INTO %s (key) VALUES ('test');
	`, addedSrcTableName))
	require.NoError(s.t, err)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "second insert to added table", "test_table_add_remove_added", addedDstTableName, "id,key")

	rows, err := s.GetRows(dstTableName, "id")
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 1, "expected no new rows in removed table")
	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Nullable() {
	srcTableName := "test_nullable"
	srcFullName := s.attachSchemaSuffix("test_nullable")
	dstTableName := "test_nullable_dst"

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			val TEXT,
			n NUMERIC,
			t TIMESTAMP
		);
	`, srcFullName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
	INSERT INTO %s (key) VALUES ('init');
	`, srcFullName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_nullable"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_NULLABLE": "true"}

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,key,val,n,t")

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
	INSERT INTO %s (key) VALUES ('cdc');
	`, srcFullName))
	require.NoError(s.t, err)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,key,val,n,t")

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Date32() {
	srcTableName := "test_date32"
	srcFullName := s.attachSchemaSuffix("test_date32")
	dstTableName := "test_date32_dst"

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			d DATE NOT NULL
		);
	`, srcFullName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
	INSERT INTO %s (key,d) VALUES ('init','1935-01-01');
	`, srcFullName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_date32"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,key,d")

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
	INSERT INTO %s (key,d) VALUES ('cdc','1935-01-01');
	`, srcFullName))
	require.NoError(s.t, err)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,key,d")

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Schema_Changes_CH() {
	tc := e2e.NewTemporalClient(s.t)

	tableName := "test_schema_changes"
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
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 100

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert and mutate schema repeatedly.
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	// insert first row.
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1) VALUES (1)`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Inserted initial row in the source table")

	e2e.EnvWaitForEqualTables(env, s, "normalize insert", tableName, "id,c1")

	// alter source table, add column c2 and insert another row.
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s ADD COLUMN "myC2" BIGINT`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Altered source table, added column myC2")
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,"myC2") VALUES (2,2)`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Inserted row with added myC2 in the source table")

	// verify we got our two rows, if schema did not match up it will error.
	e2e.EnvWaitForEqualTables(env, s, "normalize altered row", tableName, "id,c1,\"myC2\"")

	// alter source table, add column c3, drop column c2 and insert another row.
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s DROP COLUMN "myC2", ADD COLUMN c3 FLOAT`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Altered source table, dropped column myC2 and added column c3")
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,c3) VALUES (3,3.5)`, srcTableName))
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

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}
