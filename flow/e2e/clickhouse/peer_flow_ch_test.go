package e2e_clickhouse

import (
	"context"
	"embed"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	connclickhouse "github.com/PeerDB-io/peer-flow/connectors/clickhouse"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

//go:embed test_data/*
var testData embed.FS

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

func (s ClickHouseSuite) Test_NullableMirrorSetting() {
	srcTableName := "test_nullable_mirror"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_nullable_mirror_dst"

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
		FlowJobName:      s.attachSuffix("ch_nullable_mirror"),
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

func (s ClickHouseSuite) Test_NullableColumnSetting() {
	srcTableName := "test_nullable_column"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_nullable_column_dst"

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
		FlowJobName:      s.attachSuffix("ch_nullable_column"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.DoInitialSnapshot = true
	for _, tm := range flowConnConfig.TableMappings {
		tm.Columns = []*protos.ColumnSetting{
			{SourceName: "key", NullableEnabled: true},
			{SourceName: "val", NullableEnabled: true},
			{SourceName: "n", NullableEnabled: true},
			{SourceName: "t", NullableEnabled: true},
		}
	}

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

func (s ClickHouseSuite) Test_Update_PKey_Env_Disabled() {
	srcTableName := "test_update_pkey_disabled"
	srcFullName := s.attachSchemaSuffix("test_update_pkey_disabled")
	dstTableName := "test_update_pkey_disabled_dst"

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY,
			key TEXT NOT NULL
		);
	`, srcFullName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
	INSERT INTO %s (id,key) VALUES (1,'init');
	`, srcFullName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_pkey_update_disabled"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_CLICKHOUSE_ENABLE_PRIMARY_UPDATE": "false"}

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,key")

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
	UPDATE %s SET id = 2, key = 'update' WHERE id = 1;
	`, srcFullName))
	require.NoError(s.t, err)

	e2e.EnvWaitFor(s.t, env, time.Minute, "waiting for duplicate row", func() bool {
		rows, err := s.GetRows(dstTableName, "id")
		require.NoError(s.t, err)
		return len(rows.Records) == 2
	})

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Update_PKey_Env_Enabled() {
	srcTableName := "test_update_pkey_enabled"
	srcFullName := s.attachSchemaSuffix("test_update_pkey_enabled")
	dstTableName := "test_update_pkey_enabled_dst"

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY,
			key TEXT NOT NULL
		);
	`, srcFullName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
	INSERT INTO %s (id,key) VALUES (1,'init');
	`, srcFullName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_pkey_update_enabled"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_CLICKHOUSE_ENABLE_PRIMARY_UPDATE": "true"}

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,key")

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
	UPDATE %s SET id = 2, key = 'update' WHERE id = 1;
	`, srcFullName))
	require.NoError(s.t, err)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,key")

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Replident_Full_Unchanged_TOAST_Updates() {
	srcTableName := "test_replident_full_toast"
	srcFullName := s.attachSchemaSuffix("test_replident_full_toast")
	dstTableName := "test_replident_full_toast_dst"

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s(
		id INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
		c1 INT,
		c2 INT,
		t TEXT);
	ALTER TABLE %s REPLICA IDENTITY FULL`, srcFullName, srcFullName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_test_replident_full_toast"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	content, err := testData.ReadFile("test_data/big_data.json")
	require.NoError(s.t, err)
	contentStr := string(content)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
	INSERT INTO %s (c1,c2,t) VALUES ($1,$2,$3)`, srcFullName), 1, 2, contentStr)
	require.NoError(s.t, err)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial insert", srcTableName, dstTableName, "id,c1,c2,t")

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
	UPDATE %s SET c1=$1 WHERE id=$2`, srcFullName), 3, 1)
	require.NoError(s.t, err)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on update", srcTableName, dstTableName, "id,c1,c2,t")

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) WeirdTable(tableName string) {
	srcTableName := tableName
	srcFullName := s.attachSchemaSuffix(fmt.Sprintf("\"%s\"", tableName))
	dstTableName := tableName

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL
		);
	`, srcFullName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf("INSERT INTO %s (key) VALUES ('init')", srcFullName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_test_weird_table_" + strings.ReplaceAll(tableName, "-", "_")),
		TableNameMapping: map[string]string{s.attachSchemaSuffix(tableName): dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.DoInitialSnapshot = true
	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,key")

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf("INSERT INTO %s (key) VALUES ('cdc')", srcFullName))
	require.NoError(s.t, err)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,key")

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)

	env = e2e.ExecuteWorkflow(tc, shared.PeerFlowTaskQueue, peerflow.DropFlowWorkflow, &protos.DropFlowInput{
		FlowJobName:           flowConnConfig.FlowJobName,
		DropFlowStats:         false,
		FlowConnectionConfigs: flowConnConfig,
	})
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)
	// now test weird names with rename based resync
	ch, err := connclickhouse.Connect(context.Background(), nil, s.Peer().GetClickhouseConfig())
	require.NoError(s.t, err)
	require.NoError(s.t, ch.Exec(context.Background(), fmt.Sprintf("DROP TABLE `%s`", dstTableName)))
	require.NoError(s.t, ch.Close())
	flowConnConfig.Resync = true
	env = e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,key")
	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)

	env = e2e.ExecuteWorkflow(tc, shared.PeerFlowTaskQueue, peerflow.DropFlowWorkflow, &protos.DropFlowInput{
		FlowJobName:           flowConnConfig.FlowJobName,
		DropFlowStats:         false,
		FlowConnectionConfigs: flowConnConfig,
	})
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)
	// now test weird names with exchange based resync
	ch, err = connclickhouse.Connect(context.Background(), nil, s.Peer().GetClickhouseConfig())
	require.NoError(s.t, err)
	require.NoError(s.t, ch.Exec(context.Background(), fmt.Sprintf("TRUNCATE TABLE `%s`", dstTableName)))
	require.NoError(s.t, ch.Close())
	env = e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,key")
	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_WeirdTable_Keyword() {
	s.WeirdTable("table")
}

func (s ClickHouseSuite) Test_WeirdTable_Dash() {
	s.t.SkipNow() // TODO fix avro errors by sanitizing names
	s.WeirdTable("table-group")
}

// large NUMERICs (precision >76) are mapped to String on CH, test
func (s ClickHouseSuite) Test_Large_Numeric() {
	srcFullName := s.attachSchemaSuffix("lnumeric")
	dstTableName := "lnumeric"

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s(
			id INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
			c1 NUMERIC(76,0),
			c2 NUMERIC(78,0)
		);
	`, srcFullName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf("INSERT INTO %s(c1,c2) VALUES($1,$2)", srcFullName),
		strings.Repeat("7", 76), strings.Repeat("9", 78))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_test_large_numerics"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id,c1,c2", 1)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf("INSERT INTO %s(c1,c2) VALUES($1,$2)", srcFullName),
		strings.Repeat("7", 76), strings.Repeat("9", 78))
	require.NoError(s.t, err)

	e2e.EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id,c1,c2", 2)

	rows, err := s.GetRows(dstTableName, "c1,c2")
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 2, "expected 2 rows")
	for _, row := range rows.Records {
		require.Len(s.t, row, 2, "expected 2 columns")
		require.Equal(s.t, qvalue.QValueKindNumeric, row[0].Kind(), "expected NUMERIC(76,0) to be Decimal")
		require.Equal(s.t, qvalue.QValueKindString, row[1].Kind(), "expected NUMERIC(78,0) to be String")
		c1, ok := row[0].Value().(decimal.Decimal)
		require.True(s.t, ok, "expected NUMERIC(76,0) to be Decimal")
		require.Equal(s.t, strings.Repeat("7", 76), c1.String(), "expected NUMERIC(76,0) to be 7s")
		c2, ok := row[1].Value().(string)
		require.True(s.t, ok, "expected NUMERIC(78,0) to be String")
		require.Equal(s.t, strings.Repeat("9", 78), c2, "expected NUMERIC(78,0) to be 9s")
	}

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}

// Unbounded NUMERICs (no precision, scale specified) are mapped to String on CH if FF enabled, Decimal if not
func (s ClickHouseSuite) testNumericFF(ffValue bool) {
	nines := strings.Repeat("9", 38)
	dstTableName := fmt.Sprintf("unumeric_ff_%v", ffValue)
	srcFullName := s.attachSchemaSuffix(dstTableName)

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s(
			id INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
			c numeric
		);
	`, srcFullName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf("INSERT INTO %s(c) VALUES($1)", srcFullName), nines)
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(fmt.Sprintf("clickhouse_test_unbounded_numerics_ff_%v", ffValue)),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": strconv.FormatBool(ffValue)}
	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id,c", 1)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf("INSERT INTO %s(c) VALUES($1)", srcFullName), nines)
	require.NoError(s.t, err)

	e2e.EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id,c", 2)

	rows, err := s.GetRows(dstTableName, "c")
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 2, "expected 2 rows")
	for _, row := range rows.Records {
		require.Len(s.t, row, 1, "expected 1 column")
		if ffValue {
			c, ok := row[0].Value().(string)
			require.True(s.t, ok, "expected unbounded NUMERIC to be String")
			require.Equal(s.t, nines, c, "expected unbounded NUMERIC to be 9s")
		} else {
			c, ok := row[0].Value().(decimal.Decimal)
			require.True(s.t, ok, "expected unbounded NUMERIC to be Decimal")
			require.Equal(s.t, nines, c.String(), "expected unbounded NUMERIC to be 9s")
		}
	}

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Unbounded_Numeric_With_FF() {
	s.testNumericFF(true)
}

func (s ClickHouseSuite) Test_Unbounded_Numeric_Without_FF() {
	s.testNumericFF(false)
}

func (s ClickHouseSuite) Test_Types_CH() {
	srcTableName := "test_types"
	srcFullName := s.attachSchemaSuffix("test_types")
	dstTableName := "test_types"
	createMoodEnum := "CREATE TYPE mood AS ENUM ('happy', 'sad', 'angry');"
	_, enumErr := s.Conn().Exec(context.Background(), createMoodEnum)
	if enumErr != nil &&
		!shared.IsSQLStateError(enumErr, pgerrcode.DuplicateObject, pgerrcode.UniqueViolation) {
		require.NoError(s.t, enumErr)
	}

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %[1]s(id serial PRIMARY KEY,c1 BIGINT,c2 BIT,c3 VARBIT,c4 BOOLEAN,
		c6 BYTEA,c7 CHARACTER,c8 varchar,c9 CIDR,c11 DATE,c12 FLOAT,c13 DOUBLE PRECISION,
		c14 INET,c15 INTEGER,c16 INTERVAL,c17 JSON,c18 JSONB,c21 MACADDR,c22 MONEY,
		c23 NUMERIC,c24 OID,c28 REAL,c29 SMALLINT,c30 SMALLSERIAL,c31 SERIAL,c32 TEXT,
		c33 TIMESTAMP,c34 TIMESTAMPTZ,c35 TIME,c36 TIMETZ,c37 TSQUERY,c38 TSVECTOR,
		c39 TXID_SNAPSHOT,c40 UUID,c42 INT[], c43 FLOAT[], c44 TEXT[], c45 mood, c46 HSTORE,
		c47 DATE[], c48 TIMESTAMPTZ[], c49 TIMESTAMP[], c50 BOOLEAN[], c51 SMALLINT[]);
		INSERT INTO %[1]s SELECT 2,2,b'1',b'101',
		true,random_bytea(32),'s','test','1.1.10.2'::cidr,
		CURRENT_DATE,1.23,1.234,'10.0.0.0/32'::inet,1,
		'5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds'::interval,
		'{"sai":-8.02139037433155}'::json,'{"sai":1}'::jsonb,'08:00:2b:01:02:03'::macaddr,
		1.2,123456789012345678901234567890.123456789012345678901234567890,
		4::oid,1.23,1,1,1,'test',now(),now(),now()::time,now()::timetz,
		'fat & rat'::tsquery,'a fat cat sat on a mat and ate a fat rat'::tsvector,
		txid_current_snapshot(),
		'66073c38-b8df-4bdb-bbca-1c97596b8940'::uuid,
		ARRAY[10299301,2579827],
		ARRAY[0.0003, 8902.0092],
		ARRAY['hello','bye'],'happy',
		'key1=>value1, key2=>NULL'::hstore,
		'{2020-01-01, 2020-01-02}'::date[],
		'{"2020-01-01 01:01:01+00", "2020-01-02 01:01:01+00"}'::timestamptz[],
		'{"2020-01-01 01:01:01", "2020-01-02 01:01:01"}'::timestamp[],
		'{true, false}'::boolean[],
		'{1, 2}'::smallint[];`, srcFullName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_test_types"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	e2e.EnvWaitForCount(env, s, "waiting for initial snapshot count", dstTableName, "id", 1)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "check comparable types 1", srcTableName, dstTableName,
		"id,c1,c4,c7,c8,c11,c12,c13,c15,c23,c28,c29,c30,c31,c32,c33,c34,c35,c36")

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s SELECT 3,2,b'1',b'101',
		true,random_bytea(32),'s','test','1.1.10.2'::cidr,
		CURRENT_DATE,1.23,1.234,'10.0.0.0/32'::inet,1,
		'5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds'::interval,
		'{"sai":-8.02139037433155}'::json,'{"sai":1}'::jsonb,'08:00:2b:01:02:03'::macaddr,
		1.2,123456789012345678901234567890.123456789012345678901234567890,
		4::oid,1.23,1,1,1,'test',now(),now(),now()::time,now()::timetz,
		'fat & rat'::tsquery,'a fat cat sat on a mat and ate a fat rat'::tsvector,
		txid_current_snapshot(),
		'66073c38-b8df-4bdb-bbca-1c97596b8940'::uuid,
		ARRAY[10299301,2579827],
		ARRAY[0.0003, 8902.0092],
		ARRAY['hello','bye'],'happy',
		'key1=>value1, key2=>NULL'::hstore,
		'{2020-01-01, 2020-01-02}'::date[],
		'{"2020-01-01 01:01:01+00", "2020-01-02 01:01:01+00"}'::timestamptz[],
		'{"2020-01-01 01:01:01", "2020-01-02 01:01:01"}'::timestamp[],
		'{true, false}'::boolean[],
		'{1, 2}'::smallint[];`, srcFullName))
	require.NoError(s.t, err)
	e2e.EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id", 2)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "check comparable types 2", srcTableName, dstTableName,
		"id,c1,c4,c7,c8,c11,c12,c13,c15,c23,c28,c29,c30,c31,c32,c33,c34,c35,c36")

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		UPDATE %[1]s SET c1=3,c32='testery' WHERE id=2;
		UPDATE %[1]s SET c33=now(),c34=now(),c35=now()::TIME,c36=now()::TIMETZ WHERE id=3;
		INSERT INTO %[1]s SELECT 4,2,b'1',b'101',
		true,random_bytea(32),'s','test','1.1.10.2'::cidr,
		CURRENT_DATE,1.23,1.234,'10.0.0.0/32'::inet,1,
		'5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds'::interval,
		'{"sai":-8.02139037433155}'::json,'{"sai":1}'::jsonb,'08:00:2b:01:02:03'::macaddr,
		1.2,123456789012345678901234567890.123456789012345678901234567890,
		4::oid,1.23,1,1,1,'test',now(),now(),now()::time,now()::timetz,
		'fat & rat'::tsquery,'a fat cat sat on a mat and ate a fat rat'::tsvector,
		txid_current_snapshot(),
		'66073c38-b8df-4bdb-bbca-1c97596b8940'::uuid,
		ARRAY[10299301,2579827],
		ARRAY[0.0003, 8902.0092],
		ARRAY['hello','bye'],'happy',
		'key1=>value1, key2=>NULL'::hstore,
		'{2020-01-01, 2020-01-02}'::date[],
		'{"2020-01-01 01:01:01+00", "2020-01-02 01:01:01+00"}'::timestamptz[],
		'{"2020-01-01 01:01:01", "2020-01-02 01:01:01"}'::timestamp[],
		'{true, false}'::boolean[],
		'{1, 2}'::smallint[];`, srcFullName))
	require.NoError(s.t, err)
	e2e.EnvWaitForCount(env, s, "waiting for CDC count again", dstTableName, "id", 3)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "check comparable types 3", srcTableName, dstTableName,
		"id,c1,c4,c7,c8,c11,c12,c13,c15,c23,c28,c29,c30,c31,c32,c33,c34,c35,c36")

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}
