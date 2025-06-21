package e2e_clickhouse

import (
	"embed"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	connclickhouse "github.com/PeerDB-io/peerdb/flow/connectors/clickhouse"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
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
		"PEERDB_S3_BYTES_PER_AVRO_FILE":                      "1",
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
	CREATE TABLE IF NOT EXISTS %[1]s(
		id INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
		c1 INT,
		c2 INT,
		t TEXT);
	ALTER TABLE %[1]s REPLICA IDENTITY FULL`, srcFullName))
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
			c2 NUMERIC(78,0),
			c3 NUMERIC(76,0)[],
			c4 NUMERIC(78,0)[]
		);
	`, srcFullName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf("INSERT INTO %s(c1,c2,c3,c4) VALUES($1,$2,$3,$4)", srcFullName),
		strings.Repeat("7", 76), strings.Repeat("9", 78), "{"+strings.Repeat("6", 76)+"}", "{"+strings.Repeat("8", 78)+"}")
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

	e2e.EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id,c1,c2,c3,c4", 1)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf("INSERT INTO %s(c1,c2,c3,c4) VALUES($1,$2,$3,$4)", srcFullName),
		strings.Repeat("7", 76), strings.Repeat("9", 78), "{"+strings.Repeat("6", 76)+"}", "{"+strings.Repeat("8", 78)+"}")
	require.NoError(s.t, err)

	e2e.EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id,c1,c2,c3,c4", 2)

	rows, err := s.GetRows(dstTableName, "c1,c2,c3,c4")
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 2, "expected 2 rows")
	for _, row := range rows.Records {
		require.Len(s.t, row, 4, "expected 4 columns")
		require.Equal(s.t, types.QValueKindNumeric, row[0].Kind(), "expected NUMERIC(76,0) to be Decimal")
		require.Equal(s.t, types.QValueKindString, row[1].Kind(), "expected NUMERIC(78,0) to be String")
		require.Equal(s.t, types.QValueKindArrayNumeric, row[2].Kind(), "expected NUMERIC(76,0)[] to be Decimal[]")
		require.Equal(s.t, types.QValueKindArrayString, row[3].Kind(), "expected NUMERIC(78,0)[] to be String[]")
		c1, ok := row[0].Value().(decimal.Decimal)
		require.True(s.t, ok, "expected NUMERIC(76,0) to be Decimal")
		require.Equal(s.t, strings.Repeat("7", 76), c1.String(), "expected NUMERIC(76,0) to be 7s")
		c2, ok := row[1].Value().(string)
		require.True(s.t, ok, "expected NUMERIC(78,0) to be String")
		require.Equal(s.t, strings.Repeat("9", 78), c2, "expected NUMERIC(78,0) to be 9s")
		c3, ok := row[2].Value().([]decimal.Decimal)
		require.True(s.t, ok, "expected NUMERIC(76,0)[] to be Decimal")
		require.Equal(s.t, strings.Repeat("6", 76), c3[0].String(), "expected NUMERIC(76,0)[] to be 6s")
		c4, ok := row[3].Value().([]string)
		require.True(s.t, ok, "expected NUMERIC(78,0)[] to be String[]")
		require.Equal(s.t, strings.Repeat("8", 78), c4[0], "expected NUMERIC(78,0)[] to be 8s")
	}

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Destination_Type_Conversion() {
	srcTableName := "test_destination_type_conversion"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_destination_type_conversion"

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %[1]s(
		id SERIAL PRIMARY KEY,
		c1 NUMERIC NOT NULL,
		c2 NUMERIC
	);`, srcFullName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	INSERT INTO %s(c1, c2) VALUES($1, $2)`, srcFullName), strings.Repeat("9", 77), strings.Repeat("9", 78))
	require.NoError(s.t, err)
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s(c1) VALUES($1)`, srcFullName), strings.Repeat("9", 77))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_test_dest_type_conv"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.TableMappings[0].Columns = []*protos.ColumnSetting{
		{
			SourceName:      "c1",
			DestinationType: "String",
		},
		{
			SourceName:      "c2",
			DestinationType: "String",
		},
	}

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "c1,c2", 2)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	INSERT INTO %s(c1, c2) VALUES($1, $2)`, srcFullName), strings.Repeat("9", 77), strings.Repeat("9", 78))
	require.NoError(s.t, err)
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s(c1) VALUES($1)`, srcFullName), strings.Repeat("9", 77))
	require.NoError(s.t, err)

	e2e.EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "c1,c2", 4)

	rows, err := s.GetRows(dstTableName, "c1,c2")
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 4, "expected 4 rows")
	for i, row := range rows.Records {
		require.Len(s.t, row, 2, "expected 2 columns")
		require.Equal(s.t, types.QValueKindString, row[0].Kind(), "c1 type mismatch")
		require.Equal(s.t, types.QValueKindString, row[1].Kind(), "c2 type mismatch")
		require.Equal(s.t, strings.Repeat("9", 77), row[0].Value(), "c1 value mismatch")
		if i%2 == 0 {
			require.Equal(s.t, strings.Repeat("9", 78), row[1].Value(), "c2 value mismatch")
		} else {
			require.Empty(s.t, row[1].Value(), "c2 value mismatch")
		}
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

func (s ClickHouseSuite) testNumericTruncation(unbNumAsStringFf bool) {
	nines := func(integer, fraction int) string {
		integerStr := strings.Repeat("9", integer)
		if integer == 0 {
			integerStr = "0"
		}
		if fraction > 0 {
			return integerStr + "." + strings.Repeat("9", fraction)
		}
		return integerStr
	}
	tests := []struct {
		SrcType        string
		SrcValue       string
		Expected       string
		ExpectedWithFF string // if empty, same as above
	}{
		{SrcType: "numeric", SrcValue: nines(38, 38), Expected: nines(38, 38)},
		{SrcType: "numeric", SrcValue: nines(39, 0), Expected: "0", ExpectedWithFF: nines(39, 0)},
		{SrcType: "numeric", SrcValue: nines(0, 39), Expected: nines(0, 38), ExpectedWithFF: nines(0, 39)},
		{SrcType: "numeric(96, 48)", SrcValue: nines(48, 48), Expected: nines(48, 48)},
		{SrcType: "numeric(76, 38)", SrcValue: nines(38, 38), Expected: nines(38, 38)},
		{SrcType: "numeric(76, 0)", SrcValue: nines(76, 0), Expected: nines(76, 0)},
		{SrcType: "numeric(76, 76)", SrcValue: nines(0, 76), Expected: nines(0, 76)},
	}

	dstTableName := fmt.Sprintf("numeric_truncation_unas_ff_%v", unbNumAsStringFf)
	srcFullName := s.attachSchemaSuffix(dstTableName)

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(\n", srcFullName))
	sb.WriteString("id INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY")
	for i, tc := range tests {
		sb.WriteString(fmt.Sprintf(",\ncol%d %s", i, tc.SrcType))
		sb.WriteString(fmt.Sprintf(",\ncol%d_neg %s", i, tc.SrcType))
		sb.WriteString(fmt.Sprintf(",\ncol%d_arr %s[]", i, tc.SrcType))
	}
	sb.WriteString(")")

	createQuery := sb.String()
	_, err := s.Conn().Exec(s.t.Context(), createQuery)
	require.NoError(s.t, err)

	sb.Reset()
	sb.WriteString(fmt.Sprintf("INSERT INTO %s(", srcFullName))
	for i := range tests {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("col%d, col%d_neg, col%d_arr", i, i, i))
	}
	sb.WriteString(") VALUES(")
	for i, tc := range tests {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(tc.SrcValue + "::numeric")
		sb.WriteString(", ")
		sb.WriteString(fmt.Sprintf("-%s::numeric", tc.SrcValue))
		sb.WriteString(", ")
		sb.WriteString(fmt.Sprintf("array[%s, -%s]::numeric[]", tc.SrcValue, tc.SrcValue))
	}
	sb.WriteString(")")
	insertQuery := sb.String()

	_, err = s.Conn().Exec(s.t.Context(), insertQuery)
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(fmt.Sprintf("clickhouse_test_num_trunc_ff_%v", unbNumAsStringFf)),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": strconv.FormatBool(unbNumAsStringFf)}
	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id", 1)

	_, err = s.Conn().Exec(s.t.Context(), insertQuery)
	require.NoError(s.t, err)
	e2e.EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id", 2)

	sb.Reset()
	for i := range tests {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("col%d, col%d_neg, col%d_arr", i, i, i))
	}
	selectCols := sb.String()

	ninesRegex := regexp.MustCompile(`^(0?)(9*)\.?(9*)`)
	countNines := func(value string) string {
		if len(value) < 10 {
			return value
		}
		submatches := ninesRegex.FindStringSubmatch(value)
		if submatches[1] == "0" {
			return fmt.Sprintf("nines(0, %d)", len(submatches[3]))
		}
		return fmt.Sprintf("nines(%d, %d)", len(submatches[2]), len(submatches[3]))
	}
	rows, err := s.GetRows(dstTableName, selectCols)
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 2)
	for _, row := range rows.Records {
		require.Len(s.t, row, 3*len(tests))
		for i, tc := range tests {
			testName := fmt.Sprintf("col%d: type=%s value=%s ff=%t", i, tc.SrcType, countNines(tc.SrcValue), unbNumAsStringFf)

			expected := tc.Expected
			if unbNumAsStringFf && tc.ExpectedWithFF != "" {
				expected = tc.ExpectedWithFF
			}
			assert.Equal(s.t, expected, fmt.Sprint(row[3*i].Value()), testName)

			negExpected := "-" + expected
			if negExpected == "-0" {
				negExpected = "0"
			}
			assert.Equal(s.t, negExpected, fmt.Sprint(row[3*i+1].Value()), testName+" negative")

			arr := row[3*i+2].Value()
			rArr := reflect.ValueOf(arr)
			if assert.Equal(s.t, 2, rArr.Len(), testName+" array length") {
				assert.Equal(s.t, expected, fmt.Sprint(rArr.Index(0).Interface()), testName+" in array")
				assert.Equal(s.t, negExpected, fmt.Sprint(rArr.Index(1).Interface()), testName+" negative in array")
			}
		}
	}

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Numeric_Truncation_With_UnbNumAsString_FF() {
	s.testNumericTruncation(true)
}

func (s ClickHouseSuite) Test_Numeric_Truncation_Without_UnbNumAsString_FF() {
	s.testNumericTruncation(false)
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
		c47 DATE[], c48 TIMESTAMPTZ[], c49 TIMESTAMP[], c50 BOOLEAN[], c51 SMALLINT[], c52 UUID[],
		c53 NUMERIC(16,2)[], c54 NUMERIC[], c55 NUMERIC(16,2)[], c56 NUMERIC[], c57 INTERVAL[]);
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
		'{"66073c38-b8df-4bdb-bbca-1c97596b8940","66073c38-b8df-4bdb-bbca-1c97596b8940"}'::uuid[],
		'{1.2, 1.23, null}'::numeric(16,2)[], '{1.2, 1.23, null}'::numeric[], null::numeric(16,2)[], null::numeric[],
		'{1 second, 5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds}'::interval[];`,
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
		"id,c1,c4,c7,c8,c11,c12,c13,c15,c23,c28,c29,c30,c31,c32,c33,c34,c35,c36,c40,c41,c42,c43,c44,c45,c48,c49,c52,c53,c54,c55,c56,c57")

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
		'{"86073c38-b8df-4bdb-bbca-1c97596b8940","66073c38-b8df-4bdb-bbca-1c97596b8940"}'::uuid[],
		'{2.2, 2.23, null}'::numeric(16,2)[], '{2.2, 2.23, null}'::numeric[], null::numeric(16,2)[], null::numeric[],
		'{1 second, 5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds}'::interval[];`, srcFullName))
	require.NoError(s.t, err)
	e2e.EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id", 2)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "check comparable types 2", srcTableName, dstTableName,
		"id,c1,c4,c7,c8,c11,c12,c13,c15,c23,c28,c29,c30,c31,c32,c33,c34,c35,c36,c40,c41,c42,c43,c44,c45,c48,c49,c52,c53,c54,c55,c56,c57")

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
		'{"66073c38-b8df-4bdb-bbca-1c97596b8940","66073c38-b8df-4bdb-bbca-1c97596b8940"}'::uuid[],
		'{1.2, 1.23, null}'::numeric(16,2)[], '{1.2, 1.23, null}'::numeric[], null::numeric(16,2)[], null::numeric[],
		'{1 second, 5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds}'::interval[];`, srcFullName))

	require.NoError(s.t, err)
	e2e.EnvWaitForCount(env, s, "waiting for CDC count again", dstTableName, "id", 3)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "check comparable types 3", srcTableName, dstTableName,
		"id,c1,c4,c7,c8,c11,c12,c13,c15,c23,c28,c29,c30,c31,c32,c33,c34,c35,c36,c40,c41,c42,c43,c44,c45,c48,c49,c52,c53,c54,c55,c56,c57")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_PgVector() {
	if _, ok := s.source.(*e2e.PostgresSource); !ok {
		s.t.Skip("only applies to postgres")
	}

	srcTableName := "test_pgvector"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_pgvector"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, v1 vector, hv halfvec, sv sparsevec)`, srcFullName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`insert into %s (v1,hv,sv) values ('[1.5,2,3]','[1,2.5,3]','{1:1.5,3:3.5}/5')`, srcFullName)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, srcTableName),
		TableMappings: e2e.TableMappings(s, srcTableName, dstTableName),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "check comparable types 1", srcTableName, dstTableName, "id,v1,hv,sv")

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`insert into %s (v1,hv,sv) values ('[1.5,2,3.5]','[1,2,3.5]','{2:2.5,3:3.5}/5')`, srcFullName)))
	e2e.EnvWaitForEqualTablesWithNames(env, s, "check comparable types 2", srcTableName, dstTableName, "id,v1,hv,sv")

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

func (s ClickHouseSuite) Test_Nullable_Schema_Change_Replident_Full() {
	if _, ok := s.source.(*e2e.PostgresSource); !ok {
		s.t.Skip("only applies to postgres")
	}
	tc := e2e.NewTemporalClient(s.t)

	tableName := "test_nullable_sc_ch_replident_full"
	srcFullName := s.attachSchemaSuffix(tableName)
	dstTableName := tableName

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, c1 INT)`, srcFullName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`ALTER TABLE %[1]s REPLICA IDENTITY FULL`, srcFullName)))
	// makes sure queries don't mixup tables
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, c1 INT, "Ac2" INT NOT NULL, "Ac3" INT)`, srcFullName+"fake")))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, tableName),
		TableMappings: e2e.TableMappings(s, tableName, dstTableName),
		Destination:   s.Peer().Name,
	}
	config := connectionGen.GenerateFlowConnectionConfigs(s)
	config.Env = map[string]string{"PEERDB_NULLABLE": "true"}

	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, config, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, config)

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN "Ac2" INT, ADD COLUMN "Ac3" INT NOT NULL,
		 ADD COLUMN c4 INT NOT NULL, DROP CONSTRAINT %s_pkey, ADD PRIMARY KEY (c4);`, srcFullName, tableName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (c1,"Ac2","Ac3",c4) VALUES (1,null,2,3)`, srcFullName)))

	e2e.EnvWaitForEqualTables(env, s, "new column", tableName, `id,c1,"Ac2","Ac3",c4`)

	rows, err := s.GetRows(dstTableName, `id,c1,"Ac2","Ac3",c4`)
	require.NoError(s.t, err)
	require.False(s.t, rows.Schema.Fields[0].Nullable)
	require.True(s.t, rows.Schema.Fields[1].Nullable)
	require.True(s.t, rows.Schema.Fields[2].Nullable)
	require.False(s.t, rows.Schema.Fields[3].Nullable)
	require.False(s.t, rows.Schema.Fields[4].Nullable)

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

// old logic would mark pkey being nullable if replident index was used
func (s ClickHouseSuite) Test_Nullable_Schema_Change_Replident_Index() {
	if _, ok := s.source.(*e2e.PostgresSource); !ok {
		s.t.Skip("only applies to postgres")
	}
	tc := e2e.NewTemporalClient(s.t)

	tableName := "test_nullable_sc_ch_replident_full"
	srcFullName := s.attachSchemaSuffix(tableName)
	dstTableName := tableName

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, "Ac1" INT NOT NULL, c2 INT NOT NULL);`, srcFullName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE UNIQUE INDEX idx_uniqlo_%s ON %s("Ac1",c2);`, tableName, srcFullName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`ALTER TABLE %s REPLICA IDENTITY USING INDEX idx_uniqlo_%s`, srcFullName, tableName)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, tableName),
		TableMappings: e2e.TableMappings(s, tableName, dstTableName),
		Destination:   s.Peer().Name,
	}
	config := connectionGen.GenerateFlowConnectionConfigs(s)
	config.Env = map[string]string{"PEERDB_NULLABLE": "true"}

	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, config, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, config)

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN "Ac3" INT NOT NULL,
		 ADD COLUMN c4 INT NOT NULL, ADD COLUMN "Ac5" TEXT, DROP CONSTRAINT %s_pkey, ADD PRIMARY KEY (c4);`, srcFullName, tableName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO %s ("Ac1",c2,"Ac3",c4,"Ac5") VALUES (1,2,3,4,null)`, srcFullName)))

	e2e.EnvWaitForEqualTables(env, s, "new column", tableName, `id,"Ac1",c2,"Ac3",c4,"Ac5"`)

	rows, err := s.GetRows(dstTableName, `id,"Ac1",c2,"Ac3",c4,"Ac5"`)
	require.NoError(s.t, err)
	require.False(s.t, rows.Schema.Fields[0].Nullable)
	require.False(s.t, rows.Schema.Fields[1].Nullable)
	require.False(s.t, rows.Schema.Fields[2].Nullable)
	require.False(s.t, rows.Schema.Fields[3].Nullable)
	require.False(s.t, rows.Schema.Fields[4].Nullable)
	require.True(s.t, rows.Schema.Fields[5].Nullable)

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

// Test_Normalize_Metadata_With_Retry tests the chunking normalization
// with a push to ClickHouse thrown in via renaming a target table.
func (s ClickHouseSuite) Test_Normalize_Metadata_With_Retry() {
	var pgSource *e2e.PostgresSource
	var ok bool
	if pgSource, ok = s.source.(*e2e.PostgresSource); !ok {
		s.t.Skip("todo: only applies to postgres for now")
	}

	srcTableName1 := "test_normalize_metadata_with_retry_1"
	srcFullName1 := s.attachSchemaSuffix(srcTableName1)
	dstTableName1 := "test_normalize_metadata_with_retry_dst_1"
	srcTableName2 := "test_normalize_metadata_with_retry_2"
	srcFullName2 := s.attachSchemaSuffix(srcTableName2)
	dstTableName2 := "test_normalize_metadata_with_retry_dst_2"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY,
			"key" TEXT NOT NULL
		);
	`, srcFullName1)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id INT PRIMARY KEY,
		"key" TEXT NOT NULL
	);
	`, srcFullName2)))

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO %s (id,"key") VALUES (1,'init'),(2,'two'),(3,'tri'),(4,'cry')`, srcFullName1)))

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO %s (id,"key") VALUES (1,'init'),(2,'two'),(3,'tri'),(4,'cry')`, srcFullName2)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_normalize_metadata_with_retry"),
		TableNameMapping: map[string]string{srcFullName1: dstTableName1, srcFullName2: dstTableName2},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName1, dstTableName1, "id,\"key\"")

	// Rename the table to simulate a push failure to ClickHouse
	ch, err := connclickhouse.Connect(s.t.Context(), nil, s.Peer().GetClickhouseConfig())
	require.NoError(s.t, err)
	fakeDestination2 := "test_normalize_metadata_with_retry_dst_2_fake"
	renameErr := ch.Exec(s.t.Context(), fmt.Sprintf(`RENAME TABLE %s TO %s`, dstTableName2, fakeDestination2))
	require.NoError(s.t, renameErr)
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`UPDATE %s SET "key"='update1'`, srcFullName2)))

	e2e.EnvWaitFor(s.t, env, 5*time.Minute, "waiting for first sync to complete", func() bool {
		rows, err := pgSource.Query(s.t.Context(),
			fmt.Sprintf("SELECT sync_batch_id FROM metadata_last_sync_state WHERE job_name='%s'",
				flowConnConfig.FlowJobName))
		if err != nil {
			return false
		}

		if len(rows.Records) == 0 {
			return false
		}

		return rows.Records[0][0].Value().(int64) == 1
	})

	e2e.EnvWaitFor(s.t, env, 5*time.Minute, "waiting for raw table push to complete", func() bool {
		rows, err := s.GetRows(s.connector.GetRawTableName(connectionGen.FlowJobName), "_peerdb_batch_id")
		if err != nil {
			return false
		}

		if len(rows.Records) != 4 {
			return false
		}

		return rows.Records[0][0].Value().(int64) == 1
	})

	e2e.EnvWaitFor(s.t, env, 5*time.Minute, "waiting for normalize error", func() bool {
		rows, err := pgSource.Query(s.t.Context(), fmt.Sprintf(`
		SELECT COUNT(*) FROM peerdb_stats.flow_errors
		WHERE error_type='error' AND position('%s' in flow_name) > 0
		AND error_message ILIKE '%%error while inserting into target clickhouse table%%'`, flowConnConfig.FlowJobName))
		if err != nil {
			return false
		}

		return rows.Records[0][0].Value().(int64) > 0
	})

	// Rename the table back to simulate a successful push to ClickHouse
	renameErr = ch.Exec(s.t.Context(), fmt.Sprintf(`RENAME TABLE %s TO %s`, fakeDestination2, dstTableName2))
	require.NoError(s.t, renameErr)
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`UPDATE %s SET "key"='update2'`, srcFullName2)))
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`UPDATE %s SET "key"='update2'`, srcFullName1)))

	e2e.EnvWaitFor(s.t, env, 5*time.Minute, "waiting for second sync to complete", func() bool {
		rows, err := pgSource.Query(s.t.Context(),
			fmt.Sprintf("SELECT sync_batch_id FROM metadata_last_sync_state WHERE job_name='%s'",
				flowConnConfig.FlowJobName))
		if err != nil {
			return false
		}

		if len(rows.Records) == 0 {
			return false
		}

		return rows.Records[0][0].Value().(int64) == 2
	})

	e2e.EnvWaitFor(s.t, env, 5*time.Minute, "waiting for second raw table push to complete", func() bool {
		rows, err := s.GetRows(s.connector.GetRawTableName(connectionGen.FlowJobName), "_peerdb_batch_id")
		if err != nil {
			return false
		}

		if len(rows.Records) != 12 {
			return false
		}
		return true
	})

	e2e.EnvWaitFor(s.t, env, 5*time.Minute, "check normalize table metadata after normalize", func() bool {
		rows, err := pgSource.Query(s.t.Context(), fmt.Sprintf(`
		SELECT (table_batch_id_data->>'%s')::bigint, (table_batch_id_data->>'%s')::bigint
		FROM metadata_last_sync_state WHERE job_name='%s'`,
			dstTableName1, dstTableName2, flowConnConfig.FlowJobName))
		if err != nil {
			s.t.Log("error querying metadata_last_sync_state:", err)
			return false
		}

		if len(rows.Records) == 0 {
			s.t.Log("no records found in metadata_last_sync_state")
			return false
		}
		s.t.Log("metadata_last_sync_state:", rows.Records[0][0].Value(), rows.Records[0][1].Value())
		return rows.Records[0][0].Value().(int64) == 2 && rows.Records[0][1].Value().(int64) == 2
	})

	e2e.EnvWaitForEqualTablesWithNames(env, s, "after 2 batches of cdc for table 1", srcTableName1, dstTableName1, "id,\"key\"")
	e2e.EnvWaitForEqualTablesWithNames(env, s, "after 2 batches of cdc for table 2", srcTableName2, dstTableName2, "id,\"key\"")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
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
			line:    "{1,2,3}",
			lseg:    "[(1,2),(3,4)]",
			box:     "(3,4),(1,2)",
			path:    "((1,2),(3,4),(5,6))",
			polygon: "((1,2),(3,4),(5,6),(1,2))",
			circle:  "<(1,2),3>",
		},
		{
			point:   "POINT(10.000000 20.000000)",
			line:    "{10,20,30}",
			lseg:    "[(10,20),(30,40)]",
			box:     "(30,40),(10,20)",
			path:    "((10,20),(30,40),(50,60))",
			polygon: "((10,20),(30,40),(50,60),(10,20))",
			circle:  "<(10,20),30>",
		},
		{
			point:   "POINT(100.000000 200.000000)",
			line:    "{100,200,300}",
			lseg:    "[(100,200),(300,400)]",
			box:     "(300,400),(100,200)",
			path:    "((100,200),(300,400),(500,600))",
			polygon: "((100,200),(300,400),(500,600),(100,200))",
			circle:  "<(100,200),300>",
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
	chPeer := s.Peer().GetClickhouseConfig()
	srcTableName := "test_nullengine"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_nullengine"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, "key" TEXT NOT NULL, val TEXT)`, srcFullName)))

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

	ch, err := connclickhouse.Connect(s.t.Context(), nil, chPeer)
	require.NoError(s.t, err)
	require.NoError(s.t, ch.Exec(s.t.Context(),
		`create table nulltarget (id Int32, "key" String, _peerdb_is_deleted Int8) engine = ReplacingMergeTree() order by id`))
	require.NoError(s.t, ch.Exec(s.t.Context(),
		`create materialized view nullmv to nulltarget as select id, "key", _peerdb_is_deleted from test_nullengine`))
	require.NoError(s.t, ch.Close())

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`insert into %s values (1, 'cdc', 'val')`, srcFullName)))
	e2e.EnvWaitForEqualTablesWithNames(env, s, "null insert", srcTableName, "nulltarget", "id,\"key\"")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
	env = e2e.ExecuteWorkflow(s.t.Context(), tc, shared.PeerFlowTaskQueue, peerflow.DropFlowWorkflow, &protos.DropFlowInput{
		FlowJobName:           flowConnConfig.FlowJobName,
		DropFlowStats:         false,
		FlowConnectionConfigs: flowConnConfig,
	})
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf("ALTER TABLE %s DROP COLUMN val", srcFullName)))

	ch, err = connclickhouse.Connect(s.t.Context(), nil, chPeer)
	require.NoError(s.t, err)
	require.NoError(s.t, ch.Exec(s.t.Context(), "TRUNCATE TABLE nulltarget"))
	require.NoError(s.t, ch.Close())
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Resync = true
	env = e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, "nulltarget", "id,\"key\"")

	var count uint64
	ch, err = connclickhouse.Connect(s.t.Context(), nil, chPeer)
	require.NoError(s.t, err)
	row := ch.QueryRow(s.t.Context(),
		fmt.Sprintf("select count(*) from system.columns where database = '%s' and table = 'test_nullengine'", chPeer.Database))
	require.NoError(s.t, row.Err())
	require.NoError(s.t, row.Scan(&count))
	require.NoError(s.t, ch.Close())
	require.Equal(s.t, uint64(5), count)

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Partition_Key_Integer() {
	srcTableName := "test_partition_key_integer"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_partition_key_integer"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id INT PRIMARY KEY,
		myname TEXT NOT NULL,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)`, srcFullName)))

	for i := 1; i <= 100; i++ {
		if _, ok := s.source.(*e2e.PostgresSource); ok {
			require.NoError(s.t, s.source.Exec(s.t.Context(),
				fmt.Sprintf(`INSERT INTO %s (id,myname,updated_at)
			VALUES (%d,'init_%d',CURRENT_TIMESTAMP + INTERVAL '%d seconds')`,
					srcFullName, i, i, i)))
		} else {
			require.NoError(s.t, s.source.Exec(s.t.Context(),
				fmt.Sprintf(`INSERT INTO %s (id,myname,updated_at)
			VALUES (%d,'init_%d',CURRENT_TIMESTAMP + INTERVAL %d SECOND)`,
					srcFullName, i, i, i)))
		}
	}

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("clickhouse_partition_key_integer"),
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      srcFullName,
			DestinationTableIdentifier: dstTableName,
			PartitionKey:               "id",
		}},
		Destination: s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.SnapshotMaxParallelWorkers = 4
	flowConnConfig.SnapshotNumRowsPerPartition = 10
	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,myname,updated_at")

	countRow := s.Conn().QueryRow(s.t.Context(),
		`SELECT COUNT(*) FROM peerdb_stats.qrep_partitions WHERE parent_mirror_name = $1`,
		flowConnConfig.FlowJobName)

	var partitionCount int
	require.NoError(s.t, countRow.Scan(&partitionCount), "failed to get partition count")
	require.GreaterOrEqual(s.t, partitionCount, 10, "expected at least 10 partitions to be created")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Partition_Key_Timestamp() {
	srcTableName := "test_partition_key_timestamp"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_partition_key_timestamp"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id INT PRIMARY KEY,
		myname TEXT NOT NULL,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)`, srcFullName)))

	for i := 1; i <= 100; i++ {
		if _, ok := s.source.(*e2e.PostgresSource); ok {
			require.NoError(s.t, s.source.Exec(s.t.Context(),
				fmt.Sprintf(`INSERT INTO %s (id,myname,updated_at)
			VALUES (%d,'init_%d',CURRENT_TIMESTAMP + INTERVAL '%d seconds')`,
					srcFullName, i, i, i)))
		} else {
			require.NoError(s.t, s.source.Exec(s.t.Context(),
				fmt.Sprintf(`INSERT INTO %s (id,myname,updated_at)
			VALUES (%d,'init_%d',CURRENT_TIMESTAMP + INTERVAL %d SECOND)`,
					srcFullName, i, i, i)))
		}
	}

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("clickhouse_partition_key_timestamp"),
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      srcFullName,
			DestinationTableIdentifier: dstTableName,
			PartitionKey:               "updated_at",
		}},
		Destination: s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.SnapshotMaxParallelWorkers = 4
	flowConnConfig.SnapshotNumRowsPerPartition = 10
	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,myname,updated_at")

	countRow := s.Conn().QueryRow(s.t.Context(),
		`SELECT COUNT(*) FROM peerdb_stats.qrep_partitions WHERE parent_mirror_name = $1`,
		flowConnConfig.FlowJobName)

	var partitionCount int
	require.NoError(s.t, countRow.Scan(&partitionCount), "failed to get partition count")
	require.GreaterOrEqual(s.t, partitionCount, 10, "expected at least 10 partitions to be created")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}
