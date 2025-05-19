package e2e_clickhouse

import (
	"embed"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	connclickhouse "github.com/PeerDB-io/peerdb/flow/connectors/clickhouse"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/shared"
	peerflow "github.com/PeerDB-io/peerdb/flow/workflows"
)

//go:embed test_data/*
var testData embed.FS

func TestPeerFlowE2ETestSuitePG_CH(t *testing.T) {
	e2eshared.RunSuite(t, SetupSuite(t, func(t *testing.T) (*e2e.PostgresSource, string, error) {
		t.Helper()
		suffix := "pgch_" + strings.ToLower(shared.RandomString(8))
		source, err := e2e.SetupPostgres(t, suffix)
		return source, suffix, err
	}))
}

func TestPeerFlowE2ETestSuiteMySQL_CH(t *testing.T) {
	e2eshared.RunSuite(t, SetupSuite(t, func(t *testing.T) (*e2e.MySqlSource, string, error) {
		t.Helper()
		suffix := "mych_" + strings.ToLower(shared.RandomString(8))
		source, err := e2e.SetupMySQL(t, suffix)
		return source, suffix, err
	}))
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

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			"key" TEXT NOT NULL
		);
	`, srcTableName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			"key" TEXT NOT NULL
		);
	`, addedSrcTableName)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhousetableremoval"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.MaxBatchSize = 1

	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)

	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s ("key") VALUES ('test')`, srcTableName)))
	e2e.EnvWaitForEqualTablesWithNames(env, s, "first insert", "test_table_add_remove", dstTableName, "id,\"key\"")
	e2e.SignalWorkflow(s.t.Context(), env, model.FlowSignal, model.PauseSignal)
	e2e.EnvWaitFor(s.t, env, 4*time.Minute, "pausing for add table", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_PAUSED
	})

	if pgconn, ok := s.source.Connector().(*connpostgres.PostgresConnector); ok {
		conn := pgconn.Conn()
		_, err := conn.Exec(s.t.Context(),
			`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
			 WHERE query LIKE '%START_REPLICATION%' AND query LIKE '%clickhousetableremoval%' AND backend_type='walsender'`)
		require.NoError(s.t, err)

		e2e.EnvWaitFor(s.t, env, 3*time.Minute, "waiting for replication to stop", func() bool {
			rows, err := conn.Query(s.t.Context(),
				`SELECT pid FROM pg_stat_activity
				WHERE query LIKE '%START_REPLICATION%' AND query LIKE '%clickhousetableremoval%' AND backend_type='walsender'`)
			require.NoError(s.t, err)
			defer rows.Close()
			return !rows.Next()
		})
	}

	runID := e2e.EnvGetRunID(s.t, env)
	e2e.SignalWorkflow(s.t.Context(), env, model.CDCDynamicPropertiesSignal, &protos.CDCFlowConfigUpdate{
		AdditionalTables: []*protos.TableMapping{
			{
				SourceTableIdentifier:      addedSrcTableName,
				DestinationTableIdentifier: addedDstTableName,
			},
		},
	})

	e2e.EnvWaitFor(s.t, env, 4*time.Minute, "adding table", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	afterAddRunID := e2e.EnvGetRunID(s.t, env)
	require.NotEqual(s.t, runID, afterAddRunID)

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s ("key") VALUES ('test')`, addedSrcTableName)))
	e2e.EnvWaitForEqualTablesWithNames(env, s, "first insert to added table", "test_table_add_remove_added", addedDstTableName, "id,\"key\"")
	e2e.SignalWorkflow(s.t.Context(), env, model.FlowSignal, model.PauseSignal)
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "pausing again for removing table", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_PAUSED
	})

	if pgconn, ok := s.source.Connector().(*connpostgres.PostgresConnector); ok {
		conn := pgconn.Conn()
		_, err := conn.Exec(s.t.Context(),
			`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
			 WHERE query LIKE '%START_REPLICATION%' AND query LIKE '%clickhousetableremoval%' AND backend_type='walsender'`)
		require.NoError(s.t, err)

		e2e.EnvWaitFor(s.t, env, 3*time.Minute, "waiting for replication to stop", func() bool {
			rows, err := conn.Query(s.t.Context(),
				`SELECT pid FROM pg_stat_activity
				WHERE query LIKE '%START_REPLICATION%' AND query LIKE '%clickhousetableremoval%' AND backend_type='walsender'`)
			require.NoError(s.t, err)
			defer rows.Close()
			return !rows.Next()
		})
	}

	e2e.SignalWorkflow(s.t.Context(), env, model.CDCDynamicPropertiesSignal, &protos.CDCFlowConfigUpdate{
		RemovedTables: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: dstTableName,
			},
		},
	})

	e2e.EnvWaitFor(s.t, env, 4*time.Minute, "removing table", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	afterRemoveRunID := e2e.EnvGetRunID(s.t, env)
	require.NotEqual(s.t, runID, afterRemoveRunID)

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s ("key") VALUES ('test')`, srcTableName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s ("key") VALUES ('test')`, addedSrcTableName)))

	e2e.EnvWaitForEqualTablesWithNames(env, s, "second insert to added table", "test_table_add_remove_added", addedDstTableName, "id,\"key\"")

	rows, err := s.GetRows(dstTableName, "id")
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 1, "expected no new rows in removed table")
	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_NullableMirrorSetting() {
	srcTableName := "test_nullable_mirror"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_nullable_mirror_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			ky TEXT NOT NULL,
			val TEXT,
			n NUMERIC,
			t TIMESTAMP
		);
	`, srcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (ky) VALUES ('init')`, srcFullName)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("ch_nullable_mirror"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_NULLABLE": "true"}

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,ky,val,n,t")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (ky) VALUES ('cdc')`, srcFullName)))

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,ky,val,n,t")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_NullableColumnSetting() {
	srcTableName := "test_nullable_column"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_nullable_column_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			ky TEXT NOT NULL,
			val TEXT,
			n NUMERIC,
			t TIMESTAMP
		);
	`, srcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (ky) VALUES ('init')`, srcFullName)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("ch_nullable_column"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	for _, tm := range flowConnConfig.TableMappings {
		tm.Columns = []*protos.ColumnSetting{
			{SourceName: "ky", NullableEnabled: true},
			{SourceName: "val", NullableEnabled: true},
			{SourceName: "n", NullableEnabled: true},
			{SourceName: "t", NullableEnabled: true},
		}
	}

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,ky,val,n,t")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (ky) VALUES ('cdc')`, srcFullName)))

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,ky,val,n,t")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Update_PKey_Env_Disabled() {
	srcTableName := "test_update_pkey_disabled"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_update_pkey_disabled_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY,
			"key" TEXT NOT NULL
		);
	`, srcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (id,"key") VALUES (1,'init')`, srcFullName)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_pkey_update_disabled"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_CLICKHOUSE_ENABLE_PRIMARY_UPDATE": "false"}

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\"")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`UPDATE %s SET id=2, "key"='update' WHERE id=1`, srcFullName)))

	e2e.EnvWaitFor(s.t, env, time.Minute, "waiting for duplicate row", func() bool {
		rows, err := s.GetRows(dstTableName, "id")
		require.NoError(s.t, err)
		return len(rows.Records) == 2
	})

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Update_PKey_Env_Enabled() {
	srcTableName := "test_update_pkey_enabled"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_update_pkey_enabled_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY,
			"key" TEXT NOT NULL
		);
	`, srcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (id,"key") VALUES (1,'init')`, srcFullName)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_pkey_update_enabled"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_CLICKHOUSE_ENABLE_PRIMARY_UPDATE": "true"}

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\"")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`UPDATE %s SET id=2, "key"='update' WHERE id=1`, srcFullName)))

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,\"key\"")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Chunking_Normalize() {
	srcTableName := "test_update_pkey_chunking_enabled"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_update_pkey_chunking_enabled_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY,
			"key" TEXT NOT NULL
		);
	`, srcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO %s (id,"key") VALUES (1,'init'),(2,'two'),(3,'tri'),(4,'cry')`, srcFullName)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_pkey_update_chunking_enabled"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{
		"PEERDB_CLICKHOUSE_ENABLE_PRIMARY_UPDATE":            "true",
		"PEERDB_CLICKHOUSE_INITIAL_LOAD_PARTS_PER_PARTITION": "2",
		"PEERDB_CLICKHOUSE_NORMALIZATION_PARTS":              "3",
	}

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\"")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`UPDATE %s SET id=id+10, "key"='update'`, srcFullName)))

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,\"key\"")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Replident_Full_Unchanged_TOAST_Updates() {
	srcTableName := "test_replident_full_toast"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_replident_full_toast_dst"

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
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
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	content, err := testData.ReadFile("test_data/big_data.json")
	require.NoError(s.t, err)
	contentStr := string(content)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	INSERT INTO %s (c1,c2,t) VALUES ($1,$2,$3)`, srcFullName), 1, 2, contentStr)
	require.NoError(s.t, err)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial insert", srcTableName, dstTableName, "id,c1,c2,t")

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	UPDATE %s SET c1=$1 WHERE id=$2`, srcFullName), 3, 1)
	require.NoError(s.t, err)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on update", srcTableName, dstTableName, "id,c1,c2,t")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) WeirdTable(tableName string) {
	srcTableName := tableName
	srcFullName := s.attachSchemaSuffix(fmt.Sprintf("\"%s\"", tableName))
	dstTableName := tableName

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			"excludedColumn" TEXT
		);
	`, srcFullName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf("INSERT INTO %s (key, \"excludedColumn\") VALUES ('init','excluded')", srcFullName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("clickhouse_test_weird_table_" + strings.ReplaceAll(
			strings.ToLower(tableName), "-", "_")),
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      s.attachSchemaSuffix(tableName),
				DestinationTableIdentifier: dstTableName,
				Exclude:                    []string{"excludedColumn"},
			},
		},
		Destination: s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\"")

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf("INSERT INTO %s (key, \"excludedColumn\") VALUES ('cdc','excluded')", srcFullName))
	require.NoError(s.t, err)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,\"key\"")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)

	env = e2e.ExecuteWorkflow(s.t.Context(), tc, shared.PeerFlowTaskQueue, peerflow.DropFlowWorkflow, &protos.DropFlowInput{
		FlowJobName:           flowConnConfig.FlowJobName,
		DropFlowStats:         false,
		FlowConnectionConfigs: flowConnConfig,
	})
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)
	// now test weird names with rename based resync
	ch, err := connclickhouse.Connect(s.t.Context(), nil, s.Peer().GetClickhouseConfig())
	require.NoError(s.t, err)
	require.NoError(s.t, ch.Exec(s.t.Context(), fmt.Sprintf("DROP TABLE `%s`", dstTableName)))
	require.NoError(s.t, ch.Close())
	flowConnConfig.Resync = true
	env = e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\"")
	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)

	env = e2e.ExecuteWorkflow(s.t.Context(), tc, shared.PeerFlowTaskQueue, peerflow.DropFlowWorkflow, &protos.DropFlowInput{
		FlowJobName:           flowConnConfig.FlowJobName,
		DropFlowStats:         false,
		FlowConnectionConfigs: flowConnConfig,
	})
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)
	// now test weird names with exchange based resync
	ch, err = connclickhouse.Connect(s.t.Context(), nil, s.Peer().GetClickhouseConfig())
	require.NoError(s.t, err)
	require.NoError(s.t, ch.Exec(s.t.Context(), fmt.Sprintf("TRUNCATE TABLE `%s`", dstTableName)))
	require.NoError(s.t, ch.Close())
	env = e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\"")
	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_WeirdTable_Keyword() {
	s.WeirdTable("table")
}

func (s ClickHouseSuite) Test_WeirdTable_MixedCase() {
	s.WeirdTable("myMixedCaseTable")
}

func (s ClickHouseSuite) Test_WeirdTable_Dash() {
	s.t.SkipNow() // TODO fix avro errors by sanitizing names
	s.WeirdTable("table-group")
}

// large NUMERICs (precision >76) are mapped to String on CH, test
func (s ClickHouseSuite) Test_Large_Numeric() {
	srcFullName := s.attachSchemaSuffix("lnumeric")
	dstTableName := "lnumeric"

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s(
			id INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
			c1 NUMERIC(76,0),
			c2 NUMERIC(78,0)
		);
	`, srcFullName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf("INSERT INTO %s(c1,c2) VALUES($1,$2)", srcFullName),
		strings.Repeat("7", 76), strings.Repeat("9", 78))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_test_large_numerics"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id,c1,c2", 1)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf("INSERT INTO %s(c1,c2) VALUES($1,$2)", srcFullName),
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

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

// Unbounded NUMERICs (no precision, scale specified) are mapped to String on CH if FF enabled, Decimal if not
func (s ClickHouseSuite) testNumericFF(ffValue bool) {
	nines := strings.Repeat("9", 38)
	dstTableName := fmt.Sprintf("unumeric_ff_%v", ffValue)
	srcFullName := s.attachSchemaSuffix(dstTableName)

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s(
			id INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
			c numeric
		);
	`, srcFullName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf("INSERT INTO %s(c) VALUES($1)", srcFullName), nines)
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(fmt.Sprintf("clickhouse_test_unbounded_numerics_ff_%v", ffValue)),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": strconv.FormatBool(ffValue)}
	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id,c", 1)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf("INSERT INTO %s(c) VALUES($1)", srcFullName), nines)
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

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Unbounded_Numeric_With_FF() {
	s.testNumericFF(true)
}

func (s ClickHouseSuite) Test_Unbounded_Numeric_Without_FF() {
	s.testNumericFF(false)
}

const binaryFormatTestcase = "\x00\x010123\x7f\xff"

// PEERDB_CLICKHOUSE_BINARY_FORMAT
func (s ClickHouseSuite) testBinaryFormat(format string, expected string) {
	dstTableName := "binary_format_" + format
	srcFullName := s.attachSchemaSuffix(dstTableName)

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s(
			id INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
			val bytea
		);
	`, srcFullName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf("INSERT INTO %s(val) VALUES($1)", srcFullName), []byte(binaryFormatTestcase))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("ch_binary_format_" + format),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_CLICKHOUSE_BINARY_FORMAT": format}
	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id,val", 1)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf("INSERT INTO %s(val) VALUES($1)", srcFullName), []byte(binaryFormatTestcase))
	require.NoError(s.t, err)
	e2e.EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id,val", 2)

	rows, err := s.GetRows(dstTableName, "val")
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 2, "expected 2 rows")
	for _, row := range rows.Records {
		require.Len(s.t, row, 1, "expected 1 column")
		require.Equal(s.t, expected, row[0].Value())
	}

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Binary_Format_Raw() {
	s.testBinaryFormat("raw", binaryFormatTestcase)
}

func (s ClickHouseSuite) Test_Binary_Format_Hex() {
	s.testBinaryFormat("hex", "0001303132337FFF")
}

func (s ClickHouseSuite) Test_Binary_Format_Base64() {
	s.testBinaryFormat("base64", "AAEwMTIzf/8=")
}

func (s ClickHouseSuite) Test_Types_CH() {
	if _, ok := s.source.(*e2e.PostgresSource); !ok {
		s.t.Skip("only applies to postgres")
	}

	srcTableName := "test_types"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_types"
	createMoodEnum := "CREATE TYPE mood AS ENUM ('happy', 'sad', 'angry');"
	if _, err := s.Conn().Exec(s.t.Context(), createMoodEnum); err != nil &&
		!shared.IsSQLStateError(err, pgerrcode.DuplicateObject, pgerrcode.UniqueViolation) {
		require.NoError(s.t, err)
	}

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %[1]s(id serial PRIMARY KEY,c1 BIGINT,c2 BIT,c3 VARBIT,c4 BOOLEAN,
		c6 BYTEA,c7 CHARACTER,c8 varchar,c9 CIDR,c11 DATE,c12 FLOAT,c13 DOUBLE PRECISION,
		c14 INET,c15 INTEGER,c16 INTERVAL,c17 JSON,c18 JSONB,c21 MACADDR,c22 MONEY,
		c23 NUMERIC,c24 OID,c28 REAL,c29 SMALLINT,c30 SMALLSERIAL,c31 SERIAL,c32 TEXT,
		c33 TIMESTAMP,c34 TIMESTAMPTZ,c35 TIME,c36 TIMETZ,c37 TSQUERY,c38 TSVECTOR,
		c39 TXID_SNAPSHOT,c40 UUID, c41 mood[], c42 INT[], c43 FLOAT[], c44 TEXT[], c45 mood, c46 HSTORE,
		c47 DATE[], c48 TIMESTAMPTZ[], c49 TIMESTAMP[], c50 BOOLEAN[], c51 SMALLINT[], c52 UUID[]);
		INSERT INTO %[1]s SELECT 2,2,b'1',b'101',
		true,random_bytes(32),'s','test','1.1.10.2'::cidr,
		CURRENT_DATE,1.23,1.234,'10.0.0.0/32'::inet,1,
		'5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds'::interval,
		'{"sai":-8.02139037433155}'::json,'{"sai":1}'::jsonb,'08:00:2b:01:02:03'::macaddr,
		1.2,123456789012345678901234567890.123456789012345678901234567890,
		4::oid,1.23,1,1,1,'test',now(),now(),now()::time,now()::timetz,
		'fat & rat'::tsquery,'a fat cat sat on a mat and ate a fat rat'::tsvector,
		txid_current_snapshot(),
		'66073c38-b8df-4bdb-bbca-1c97596b8940'::uuid,
		'{happy,angry}',
		ARRAY[10299301,2579827],
		ARRAY[0.0003, 8902.0092],
		ARRAY['hello','bye'],'happy',
		'key1=>value1, key2=>NULL'::hstore,
		'{2020-01-01, 2020-01-02}'::date[],
		'{"2020-01-01 01:01:01+00", "2020-01-02 01:01:01+00"}'::timestamptz[],
		'{"2020-01-01 01:01:01", "2020-01-02 01:01:01"}'::timestamp[],
		'{true, false}'::boolean[],
		'{1, 2}'::smallint[],
		'{"66073c38-b8df-4bdb-bbca-1c97596b8940","66073c38-b8df-4bdb-bbca-1c97596b8940"}'::uuid[];`,
		srcFullName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_test_types"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	e2e.EnvWaitForCount(env, s, "waiting for initial snapshot count", dstTableName, "id", 1)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "check comparable types 1", srcTableName, dstTableName,
		"id,c1,c4,c7,c8,c11,c12,c13,c15,c23,c28,c29,c30,c31,c32,c33,c34,c35,c36,c40,c41,c42,c43,c44,c45,c52")

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s SELECT 3,2,b'1',b'101',
		true,random_bytes(32),'s','test','1.1.10.2'::cidr,
		CURRENT_DATE,1.23,1.234,'10.0.0.0/32'::inet,1,
		'5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds'::interval,
		'{"sai":-8.02139037433155}'::json,'{"sai":1}'::jsonb,'08:00:2b:01:02:03'::macaddr,
		1.2,123456789012345678901234567890.123456789012345678901234567890,
		4::oid,1.23,1,1,1,'test',now(),now(),now()::time,now()::timetz,
		'fat & rat'::tsquery,'a fat cat sat on a mat and ate a fat rat'::tsvector,
		txid_current_snapshot(),
		'66073c38-b8df-4bdb-bbca-1c97596b8940'::uuid,
		'{sad,happy}',
		ARRAY[10299301,2579827],
		ARRAY[0.0003, 8902.0092],
		ARRAY['hello','bye'],'happy',
		'key1=>value1, key2=>NULL'::hstore,
		'{2020-01-01, 2020-01-02}'::date[],
		'{"2020-01-01 01:01:01+00", "2020-01-02 01:01:01+00"}'::timestamptz[],
		'{"2020-01-01 01:01:01", "2020-01-02 01:01:01"}'::timestamp[],
		'{true, false}'::boolean[],
		'{1, 2}'::smallint[],
		'{"86073c38-b8df-4bdb-bbca-1c97596b8940","66073c38-b8df-4bdb-bbca-1c97596b8940"}'::uuid[];`, srcFullName))
	require.NoError(s.t, err)
	e2e.EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id", 2)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "check comparable types 2", srcTableName, dstTableName,
		"id,c1,c4,c7,c8,c11,c12,c13,c15,c23,c28,c29,c30,c31,c32,c33,c34,c35,c36,c40,c41,c42,c43,c44,c45,c52")

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		UPDATE %[1]s SET c1=3,c32='testery' WHERE id=2;
		UPDATE %[1]s SET c33=now(),c34=now(),c35=now()::TIME,c36=now()::TIMETZ WHERE id=3;
		INSERT INTO %[1]s SELECT 4,2,b'1',b'101',
		true,random_bytes(32),'s','test','1.1.10.2'::cidr,
		CURRENT_DATE,1.23,1.234,'10.0.0.0/32'::inet,1,
		'5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds'::interval,
		'{"sai":-8.02139037433155}'::json,'{"sai":1}'::jsonb,'08:00:2b:01:02:03'::macaddr,
		1.2,123456789012345678901234567890.123456789012345678901234567890,
		4::oid,1.23,1,1,1,'test',now(),now(),now()::time,now()::timetz,
		'fat & rat'::tsquery,'a fat cat sat on a mat and ate a fat rat'::tsvector,
		txid_current_snapshot(),
		'66073c38-b8df-4bdb-bbca-1c97596b8940'::uuid,
		'{angry}',
		ARRAY[10299301,2579827],
		ARRAY[0.0003, 8902.0092],
		ARRAY['hello','bye'],'happy',
		'key1=>value1, key2=>NULL'::hstore,
		'{2020-01-01, 2020-01-02}'::date[],
		'{"2020-01-01 01:01:01+00", "2020-01-02 01:01:01+00"}'::timestamptz[],
		'{"2020-01-01 01:01:01", "2020-01-02 01:01:01"}'::timestamp[],
		'{true, false}'::boolean[],
		'{1, 2}'::smallint[],
		'{"66073c38-b8df-4bdb-bbca-1c97596b8940","66073c38-b8df-4bdb-bbca-1c97596b8940"}'::uuid[];`, srcFullName))

	require.NoError(s.t, err)
	e2e.EnvWaitForCount(env, s, "waiting for CDC count again", dstTableName, "id", 3)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "check comparable types 3", srcTableName, dstTableName,
		"id,c1,c4,c7,c8,c11,c12,c13,c15,c23,c28,c29,c30,c31,c32,c33,c34,c35,c36,c40,c41,c42,c43,c44,c45,c52")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Column_Exclusion() {
	if mySource, ok := s.source.(*e2e.MySqlSource); ok && mySource.Config.Flavor == protos.MySqlFlavor_MYSQL_MARIA {
		s.t.Skip("skip maria, testing minimal row metadata on maria")
	}

	tc := e2e.NewTemporalClient(s.t)

	tableName := "test_exclude_ch"
	srcFullName := s.attachSchemaSuffix(tableName)
	dstTableName := tableName

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			c1 INT,
			c2 INT,
			t TEXT
		);
	`, srcFullName)))

	// insert 5 rows into the source table
	for i := range 5 {
		require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
			`INSERT INTO %[1]s(c1,c2,t) VALUES (%[2]d,%[2]d,'test_value_%[2]d')`,
			srcFullName, i,
		)))
	}

	config := &protos.FlowConnectionConfigs{
		FlowJobName:     s.attachSuffix(tableName),
		DestinationName: s.Peer().Name,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcFullName,
				DestinationTableIdentifier: dstTableName,
				Exclude:                    []string{"c2"},
			},
		},
		SourceName:        s.Source().GeneratePeer(s.t).Name,
		SyncedAtColName:   "_PEERDB_SYNCED_AT",
		MaxBatchSize:      100,
		DoInitialSnapshot: true,
	}

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, config, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, config)

	// insert 5 rows into the source table
	for i := range 5 {
		e2e.EnvNoError(s.t, env, s.source.Exec(s.t.Context(), fmt.Sprintf(
			`INSERT INTO %[1]s(c1,c2,t) VALUES (%[2]d,%[2]d,'test_value_%[2]d')`,
			srcFullName, i,
		)))
	}

	e2e.EnvWaitForEqualTables(env, s, "normalize table", tableName, "id,c1,t")
	e2e.EnvNoError(s.t, env, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=1`, srcFullName)))
	e2e.EnvNoError(s.t, env, s.source.Exec(s.t.Context(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=0`, srcFullName)))
	e2e.EnvWaitForEqualTables(env, s, "normalize update/delete", tableName, "id,c1,t")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)

	rows, err := s.GetRows(tableName, "*")
	require.NoError(s.t, err)

	for _, field := range rows.Schema.Fields {
		require.NotEqual(s.t, "c2", field.Name)
	}
	require.Len(s.t, rows.Schema.Fields, 6)
}

func (s ClickHouseSuite) Test_Nullable_Schema_Change() {
	tc := e2e.NewTemporalClient(s.t)

	tableName := "test_nullable_sc_ch"
	srcFullName := s.attachSchemaSuffix(tableName)
	dstTableName := tableName

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, c1 INT);`, srcFullName)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, tableName),
		TableMappings: e2e.TableMappings(s, tableName, dstTableName),
		Destination:   s.Peer().Name,
	}
	config := connectionGen.GenerateFlowConnectionConfigs(s)
	config.Env = map[string]string{"PEERDB_NULLABLE": "true"}

	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, config, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, config)

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`ALTER TABLE %s ADD COLUMN c2 INT`, srcFullName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (c1,c2) VALUES (1,null)`, srcFullName)))

	e2e.EnvWaitForEqualTables(env, s, "new column", tableName, "id,c1,c2")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Unprivileged_Postgres_Columns() {
	if _, ok := s.source.(*e2e.PostgresSource); !ok {
		s.t.Skip("only applies to postgres")
	}

	// This test is for checking that the flow can handle a table with columns that the user does not have SELECT privileges on
	srcTableName := "test_unprivileged_columns"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_unprivileged_columns_dst"

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL,
			"id number" SERIAL,
			key TEXT NOT NULL,
			"se'cret" TEXT,
			"spacey column" TEXT,
			"#sync_me!" BOOLEAN,
			"2birds1stone" INT,
			"quo'te" TEXT,
			PRIMARY KEY (id, "id number")
		);
	`, srcFullName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	INSERT INTO %s (key, "se'cret", "spacey column", "#sync_me!", "2birds1stone", "quo'te")
	VALUES ('init_initial_load', 'secret', 'neptune', 'true', 509, 'abcd')`, srcFullName))
	require.NoError(s.t, err)

	err = e2e.RevokePermissionForTableColumns(s.t.Context(), s.Conn(), srcFullName,
		[]string{"id", "id number", "key", "spacey column", "#sync_me!", "2birds1stone", "quo'te"})
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("clickhouse_test_unprivileged_columns"),
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      srcFullName,
			DestinationTableIdentifier: dstTableName,
			Exclude:                    []string{"se'cret"},
		}},
		Destination: s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName,
		"id,\"id number\",key,\"spacey column\",\"#sync_me!\",\"2birds1stone\",\"quo'te\"")
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	INSERT INTO %s (key, "se'cret", "spacey column", "#sync_me!", "2birds1stone","quo'te")
	VALUES ('cdc1', 'secret', 'pluto', 'false', 123324, 'lwkfj')`, srcFullName))

	require.NoError(s.t, err)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName,
		"id,\"id number\",key,\"spacey column\",\"#sync_me!\",\"2birds1stone\",\"quo'te\"")
	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_InitialLoadOnly_No_Primary_Key() {
	srcTableName := "test_no_pkey"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_no_pkey_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id INT, "key" TEXT NOT NULL)`, srcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (id,"key") VALUES (1,'init')`, srcFullName)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_no_pkey"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.InitialSnapshotOnly = true

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\"")
	e2e.EnvWaitForFinished(s.t, env, time.Minute)
}

func (s ClickHouseSuite) Test_Geometric_Types() {
	if _, ok := s.source.(*e2e.PostgresSource); !ok {
		s.t.Skip("only applies to postgres")
	}

	srcTableName := "test_geometric_types"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_geometric_types"

	// Create a table with various PostgreSQL geometric types
	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %[1]s(
		id serial PRIMARY KEY,
		point_col POINT,
		line_col LINE,
		lseg_col LSEG,
		box_col BOX,
		path_col PATH,
		polygon_col POLYGON,
		circle_col CIRCLE
	);

	-- Insert test data with various geometric types
	INSERT INTO %[1]s (
		point_col, line_col, lseg_col, box_col, path_col, polygon_col, circle_col
	) VALUES (
		'(1,2)',                                -- POINT
		'{1,2,3}',                              -- LINE
		'((1,2),(3,4))',                        -- LSEG
		'((1,2),(3,4))',                        -- BOX
		'((1,2),(3,4),(5,6))',                  -- PATH
		'((1,2),(3,4),(5,6),(1,2))',            -- POLYGON
		'<(1,2),3>'                             -- CIRCLE
	), (
		'(10,20)',                              -- POINT
		'{10,20,30}',                           -- LINE
		'((10,20),(30,40))',                    -- LSEG
		'((10,20),(30,40))',                    -- BOX
		'((10,20),(30,40),(50,60))',            -- PATH
		'((10,20),(30,40),(50,60),(10,20))',    -- POLYGON
		'<(10,20),30>'                          -- CIRCLE
	);`, srcFullName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_test_geometric_types"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	// Wait for initial snapshot to complete
	e2e.EnvWaitForCount(env, s, "waiting for initial snapshot count", dstTableName, "id", 2)

	// Insert a third row to test CDC
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	INSERT INTO %[1]s (
		point_col, line_col, lseg_col, box_col, path_col, polygon_col, circle_col
	) VALUES (
		'(100,200)',                            -- POINT
		'{100,200,300}',                        -- LINE
		'((100,200),(300,400))',                -- LSEG
		'((100,200),(300,400))',                -- BOX
		'((100,200),(300,400),(500,600))',      -- PATH
		'((100,200),(300,400),(500,600),(100,200))', -- POLYGON
		'<(100,200),300>'                       -- CIRCLE
	);`, srcFullName))
	require.NoError(s.t, err)

	// Wait for CDC to replicate the new row
	e2e.EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id", 3)

	// Verify that the data was correctly replicated
	rows, err := s.GetRows(dstTableName, "id, point_col, line_col, lseg_col, box_col, path_col, polygon_col, circle_col")
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 3, "expected 3 rows")

	// Check that the geometric data is in the expected format in ClickHouse
	// All geometric types should be stored as strings in WKT format
	expectedValues := []struct {
		point   string
		line    string
		lseg    string
		box     string
		path    string
		polygon string
		circle  string
	}{
		{
			point:   "POINT(1.000000 2.000000)",
			line:    "{1 2 3 true}",
			lseg:    "{[{1 2} {3 4}] true}",
			box:     "{[{3 4} {1 2}] true}",
			path:    "{[{1 2} {3 4} {5 6}] true true}",
			polygon: "{[{1 2} {3 4} {5 6} {1 2}] true}",
			circle:  "{{1 2} 3 true}",
		},
		{
			point:   "POINT(10.000000 20.000000)",
			line:    "{10 20 30 true}",
			lseg:    "{[{10 20} {30 40}] true}",
			box:     "{[{30 40} {10 20}] true}",
			path:    "{[{10 20} {30 40} {50 60}] true true}",
			polygon: "{[{10 20} {30 40} {50 60} {10 20}] true}",
			circle:  "{{10 20} 30 true}",
		},
		{
			point:   "POINT(100.000000 200.000000)",
			line:    "{100 200 300 true}",
			lseg:    "{[{100 200} {300 400}] true}",
			box:     "{[{300 400} {100 200}] true}",
			path:    "{[{100 200} {300 400} {500 600}] true true}",
			polygon: "{[{100 200} {300 400} {500 600} {100 200}] true}",
			circle:  "{{100 200} 300 true}",
		},
	}

	require.Len(s.t, rows.Records, 3, "expected 3 rows")
	for i, row := range rows.Records {
		require.Len(s.t, row, 8, "expected 8 columns")

		// Verify point column format
		pointVal := row[1].Value()
		require.Contains(s.t, pointVal, "POINT", "point_col should be in WKT format")
		require.Equal(s.t, expectedValues[i].point, pointVal, "point_col value mismatch")

		// Verify line column format
		lineVal := row[2].Value().(string)
		require.Equal(s.t, expectedValues[i].line, lineVal, "line_col value mismatch")

		// Verify lseg column format
		lsegVal := row[3].Value()
		require.Equal(s.t, expectedValues[i].lseg, lsegVal, "lseg_col value mismatch")

		// Verify box column format
		boxVal := row[4].Value()
		require.Equal(s.t, expectedValues[i].box, boxVal, "box_col value mismatch")

		// Verify path column format
		pathVal := row[5].Value()
		require.Equal(s.t, expectedValues[i].path, pathVal, "path_col value mismatch")

		// Verify polygon column format
		polygonVal := row[6].Value()
		require.Equal(s.t, expectedValues[i].polygon, polygonVal, "polygon_col value mismatch")

		// Verify circle column format
		circleVal := row[7].Value()
		require.Equal(s.t, expectedValues[i].circle, circleVal, "circle_col value mismatch")
	}

	// Clean up
	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_SkipSnapshotExport() {
	srcTableName := "test_skip_snapshot"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_skip_snapshot"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, "key" TEXT NOT NULL)`, srcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (id,"key") VALUES (1,'init')`, srcFullName)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_skip_snapshot"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_SKIP_SNAPSHOT_EXPORT": "true"}

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\"")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (id,"key") VALUES (2,'cdc')`, srcFullName)))
	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,\"key\"")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_SchemaAsColumn() {
	srcTableName := "test_schema_as_column"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_schema_as_column"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, "key" TEXT NOT NULL)`, srcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (id,"key") VALUES (1,'init')`, srcFullName)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_schema_as_column"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN": "true"}

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\"")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (id,"key") VALUES (2,'cdc')`, srcFullName)))
	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,\"key\"")

	rows, err := s.GetRows(dstTableName, "_peerdb_source_schema")
	require.NoError(s.t, err, "error selecting schema column")
	require.Len(s.t, rows.Records, 2, "expected 2 rows")
	for _, row := range rows.Records {
		require.Equal(s.t, "e2e_test_"+s.suffix, row[0].Value(), "schema column incorrectly populated")
	}

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Extra_CH_Columns() {
	srcTableName := "test_extra_ch_cols"
	srcFullName := s.attachSchemaSuffix("test_extra_ch_cols")
	dstTableName := "test_extra_ch_cols"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, "key" TEXT NOT NULL)`, srcFullName)))

	require.NoError(s.t, s.CreateRMTTable(dstTableName, []TestClickHouseColumn{
		{Name: "id", Type: "Int32"},
		{Name: "key", Type: "String"},
		{Name: "updatedAt", Type: "String"},
		{Name: "_peerdb_is_deleted", Type: "Int8"},
		{Name: "_peerdb_synced_at", Type: "DateTime"},
		{Name: "_peerdb_version", Type: "Int64"},
	}, "id"),
	)

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (id,"key") VALUES (1,'init')`, srcFullName)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_extra_ch_cols"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\"")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (id,"key") VALUES (2,'cdc')`, srcFullName)))
	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,\"key\"")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_NullEngine() {
	srcTableName := "test_nullengine"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_nullengine"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, "key" TEXT NOT NULL)`, srcFullName)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("clickhouse_nullengine"),
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      srcFullName,
			DestinationTableIdentifier: dstTableName,
			Engine:                     protos.TableEngine_CH_ENGINE_NULL,
		}},
		Destination: s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	ch, err := connclickhouse.Connect(s.t.Context(), nil, s.Peer().GetClickhouseConfig())
	require.NoError(s.t, err)
	require.NoError(s.t, ch.Exec(s.t.Context(),
		`create table nulltarget (id Int32, "key" String, _peerdb_is_deleted Int8) engine = ReplacingMergeTree() order by id`))
	require.NoError(s.t, ch.Exec(s.t.Context(),
		`create materialized view nullmv to nulltarget as select id, "key", _peerdb_is_deleted from test_nullengine`))
	require.NoError(s.t, ch.Close())

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`insert into %s values (1, 'cdc')`, srcFullName)))
	e2e.EnvWaitForEqualTablesWithNames(env, s, "null insert", srcTableName, "nulltarget", "id,\"key\"")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}
