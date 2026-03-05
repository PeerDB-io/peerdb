package e2e

import (
	"embed"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	connclickhouse "github.com/PeerDB-io/peerdb/flow/connectors/clickhouse"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/pkg/clickhouse"
	mysql_validation "github.com/PeerDB-io/peerdb/flow/pkg/mysql"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

//go:embed test_data/*
var testData embed.FS

func TestPeerFlowE2ETestSuitePG_CH(t *testing.T) {
	e2eshared.RunSuite(t, SetupClickHouseSuite(t, false, func(t *testing.T) (*PostgresSource, string, error) {
		t.Helper()
		suffix := "pgch_" + strings.ToLower(shared.RandomString(8))
		source, err := SetupPostgres(t, suffix)
		return source, suffix, err
	}))
}

func TestPeerFlowE2ETestSuiteMySQL_CH(t *testing.T) {
	e2eshared.RunSuite(t, SetupClickHouseSuite(t, false, func(t *testing.T) (*MySqlSource, string, error) {
		t.Helper()
		suffix := "mych_" + strings.ToLower(shared.RandomString(8))
		source, err := SetupMySQL(t, suffix)
		return source, suffix, err
	}))
}

func TestPeerFlowE2ETestSuitePG_CH_Cluster(t *testing.T) {
	e2eshared.RunSuite(t, SetupClickHouseSuite(t, true, func(t *testing.T) (*PostgresSource, string, error) {
		t.Helper()
		suffix := "pgchcl_" + strings.ToLower(shared.RandomString(8))
		source, err := SetupPostgres(t, suffix)
		return source, suffix, err
	}))
}

func TestPeerFlowE2ETestSuiteMySQL_CH_Cluster(t *testing.T) {
	e2eshared.RunSuite(t, SetupClickHouseSuite(t, true, func(t *testing.T) (*MySqlSource, string, error) {
		t.Helper()
		suffix := "mychcl_" + strings.ToLower(shared.RandomString(8))
		source, err := SetupMySQL(t, suffix)
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
	tc := NewTemporalClient(s.t)

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

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhousetableremoval"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.MaxBatchSize = 1

	env := ExecutePeerflow(s.t, tc, flowConnConfig)

	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s ("key") VALUES ('test')`, srcTableName)))
	EnvWaitForEqualTablesWithNames(env, s, "first insert", "test_table_add_remove", dstTableName, "id,\"key\"")
	SignalWorkflow(s.t.Context(), env, model.FlowSignal, model.PauseSignal)
	EnvWaitFor(s.t, env, 4*time.Minute, "pausing for add table", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_PAUSED
	})

	if pgconn, ok := s.source.Connector().(*connpostgres.PostgresConnector); ok {
		conn := pgconn.Conn()
		_, err := conn.Exec(s.t.Context(),
			fmt.Sprintf(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
				WHERE query LIKE '%%START_REPLICATION%%' AND query LIKE '%%%s%%' AND backend_type='walsender'`,
				s.attachSuffix("clickhousetableremoval")))
		require.NoError(s.t, err)

		EnvWaitFor(s.t, env, 3*time.Minute, "waiting for replication to stop", func() bool {
			rows, err := conn.Query(s.t.Context(),
				fmt.Sprintf(`SELECT pid FROM pg_stat_activity
					WHERE query LIKE '%%START_REPLICATION%%' AND query LIKE '%%%s%%' AND backend_type='walsender'`,
					s.attachSuffix("clickhousetableremoval")))
			require.NoError(s.t, err)
			defer rows.Close()
			return !rows.Next()
		})
	}

	runID := EnvGetRunID(s.t, env)
	SignalWorkflow(s.t.Context(), env, model.CDCDynamicPropertiesSignal, &protos.CDCFlowConfigUpdate{
		AdditionalTables: []*protos.TableMapping{{
			SourceTableIdentifier:      addedSrcTableName,
			DestinationTableIdentifier: addedDstTableName,
			ShardingKey:                "id",
		}},
	})

	EnvWaitFor(s.t, env, 4*time.Minute, "adding table", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	EnvWaitFor(s.t, env, time.Minute, "ContinueAsNew", func() bool {
		return runID != EnvGetRunID(s.t, env)
	})

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s ("key") VALUES ('test')`, addedSrcTableName)))
	EnvWaitForEqualTablesWithNames(env, s, "first insert to added table", "test_table_add_remove_added", addedDstTableName, "id,\"key\"")
	SignalWorkflow(s.t.Context(), env, model.FlowSignal, model.PauseSignal)
	EnvWaitFor(s.t, env, 3*time.Minute, "pausing again for removing table", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_PAUSED
	})

	if pgconn, ok := s.source.Connector().(*connpostgres.PostgresConnector); ok {
		conn := pgconn.Conn()
		_, err := conn.Exec(s.t.Context(),
			fmt.Sprintf(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
				WHERE query LIKE '%%START_REPLICATION%%' AND query LIKE '%%%s%%' AND backend_type='walsender'`,
				s.attachSuffix("clickhousetableremoval")))
		require.NoError(s.t, err)

		EnvWaitFor(s.t, env, 3*time.Minute, "waiting for replication to stop", func() bool {
			rows, err := conn.Query(s.t.Context(),
				fmt.Sprintf(`SELECT pid FROM pg_stat_activity
					WHERE query LIKE '%%START_REPLICATION%%' AND query LIKE '%%%s%%' AND backend_type='walsender'`,
					s.attachSuffix("clickhousetableremoval")))
			require.NoError(s.t, err)
			defer rows.Close()
			return !rows.Next()
		})
	}

	SignalWorkflow(s.t.Context(), env, model.CDCDynamicPropertiesSignal, &protos.CDCFlowConfigUpdate{
		RemovedTables: []*protos.TableMapping{{
			SourceTableIdentifier:      srcTableName,
			DestinationTableIdentifier: dstTableName,
		}},
	})

	EnvWaitFor(s.t, env, 4*time.Minute, "removing table", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	afterRemoveRunID := EnvGetRunID(s.t, env)
	require.NotEqual(s.t, runID, afterRemoveRunID)

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s ("key") VALUES ('test')`, srcTableName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s ("key") VALUES ('test')`, addedSrcTableName)))

	EnvWaitForEqualTablesWithNames(env, s, "second insert to added table", "test_table_add_remove_added", addedDstTableName, "id,\"key\"")

	rows, err := s.GetRows(dstTableName, "id")
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 1, "expected no new rows in removed table")
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
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

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("ch_nullable_mirror"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_NULLABLE": "true"}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,ky,val,n,t")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (ky) VALUES ('cdc')`, srcFullName)))

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,ky,val,n,t")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
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

	connectionGen := FlowConnectionGenerationConfig{
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

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,ky,val,n,t")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (ky) VALUES ('cdc')`, srcFullName)))

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,ky,val,n,t")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
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

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_pkey_update_disabled"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_CLICKHOUSE_ENABLE_PRIMARY_UPDATE": "false"}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\"")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`UPDATE %s SET id=2, "key"='update' WHERE id=1`, srcFullName)))

	EnvWaitFor(s.t, env, time.Minute, "waiting for duplicate row", func() bool {
		rows, err := s.GetRows(dstTableName, "id")
		require.NoError(s.t, err)
		return len(rows.Records) == 2
	})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
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

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_pkey_update_enabled"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_CLICKHOUSE_ENABLE_PRIMARY_UPDATE": "true"}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\"")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`UPDATE %s SET id=2, "key"='update' WHERE id=1`, srcFullName)))

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,\"key\"")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Chunking_Initial_Load_Parts_Per_Partition() {
	srcTableName := "test_update_pkey_chunking_initial_load_enabled"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_update_pkey_chunking_initial_load_enabled_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY,
			"key" TEXT NOT NULL
		);
	`, srcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO %s (id,"key") VALUES (1,'init'),(2,'two'),(3,'tri'),(4,'cry')`, srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_pkey_update_chunking_enabled"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{
		"PEERDB_CLICKHOUSE_ENABLE_PRIMARY_UPDATE":            "true",
		"PEERDB_CLICKHOUSE_INITIAL_LOAD_PARTS_PER_PARTITION": "2",
		"PEERDB_S3_BYTES_PER_AVRO_FILE":                      "1",
	}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\"")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`UPDATE %s SET id=id+10, "key"='update'`, srcFullName)))

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,\"key\"")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
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

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_test_replident_full_toast"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	content, err := testData.ReadFile("test_data/big_data.json")
	require.NoError(s.t, err)
	contentStr := string(content)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s (c1,c2,t) VALUES ($1,$2,$3)`, srcFullName), 1, 2, contentStr)
	require.NoError(s.t, err)
	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial insert", srcTableName, dstTableName, "id,c1,c2,t")

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(
		`UPDATE %s SET c1=$1 WHERE id=$2`, srcFullName), 3, 1)
	require.NoError(s.t, err)
	EnvWaitForEqualTablesWithNames(env, s, "waiting on update", srcTableName, dstTableName, "id,c1,c2,t")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) WeirdTable(tableName string) {
	if s.cluster {
		s.t.Skip("SetupCDCFlowStatusQuery stuck in snapshot somehow")
	}

	srcTableName := tableName
	srcFullName := s.attachSchemaSuffix(fmt.Sprintf("\"%s\"", tableName))
	dstTableName := tableName

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			"includedColumn?" TEXT,
			"excludedColumn?" TEXT
		);
	`, srcFullName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s (key, \"includedColumn?\", \"excludedColumn?\") VALUES ('init','include','exclude')", srcFullName))
	require.NoError(s.t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("clickhouse_test_weird_table_" + strings.ToLower(qvalue.ConvertToAvroCompatibleName(tableName))),
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      s.attachSchemaSuffix(tableName),
			DestinationTableIdentifier: dstTableName,
			Exclude:                    []string{"excludedColumn?"},
			ShardingKey:                "id",
		}},
		Destination: s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\"")

	_, err = s.Conn().Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s (key, \"includedColumn?\", \"excludedColumn?\") VALUES ('cdc','still','ex')", srcFullName))
	require.NoError(s.t, err)
	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,\"key\"")
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)

	env = ExecuteDropFlow(s.t.Context(), tc, flowConnConfig, 0)
	EnvWaitForFinished(s.t, env, 3*time.Minute)

	// now test weird names with rename based resync
	ch, err := connclickhouse.Connect(s.t.Context(), nil, s.Peer().GetClickhouseConfig())
	require.NoError(s.t, err)
	onCluster := ""
	if s.cluster {
		onCluster = " ON CLUSTER cicluster"
	}
	require.NoError(s.t, ch.Exec(s.t.Context(), "DROP TABLE "+clickhouse.QuoteIdentifier(dstTableName)+onCluster))
	require.NoError(s.t, ch.Close())
	flowConnConfig.Resync = true
	env = ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\"")
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)

	env = ExecuteDropFlow(s.t.Context(), tc, flowConnConfig, 0)
	EnvWaitForFinished(s.t, env, 3*time.Minute)
	// now test weird names with exchange based resync
	ch, err = connclickhouse.Connect(s.t.Context(), nil, s.Peer().GetClickhouseConfig())
	require.NoError(s.t, err)
	require.NoError(s.t, ch.Exec(s.t.Context(), "TRUNCATE TABLE "+clickhouse.QuoteIdentifier(dstTableName)+onCluster))
	require.NoError(s.t, ch.Close())
	env = ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\"")
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_WeirdTable_Keyword() {
	s.WeirdTable("table")
}

func (s ClickHouseSuite) Test_WeirdTable_MixedCase() {
	s.WeirdTable("myMixedCaseTable")
}

func (s ClickHouseSuite) Test_WeirdTable_Question() {
	s.WeirdTable("whatIsTable?")
}

func (s ClickHouseSuite) Test_WeirdTable_Dash() {
	s.WeirdTable("table-group%c%i%t%i%z%e%n")
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

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_test_large_numerics"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id,c1,c2,c3,c4", 1)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf("INSERT INTO %s(c1,c2,c3,c4) VALUES($1,$2,$3,$4)", srcFullName),
		strings.Repeat("7", 76), strings.Repeat("9", 78), "{"+strings.Repeat("6", 76)+"}", "{"+strings.Repeat("8", 78)+"}")
	require.NoError(s.t, err)

	EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id,c1,c2,c3,c4", 2)

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
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Destination_Type_Conversion() {
	srcTableName := "test_destination_type_conversion"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_destination_type_conversion"

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %[1]s(
		id SERIAL PRIMARY KEY,
		c1 NUMERIC NOT NULL,
		c2 NUMERIC,
		c3 NUMERIC,
		s256 NUMERIC,
		u256 NUMERIC
	);`, srcFullName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO %s(c1,c2,c3,s256,u256) VALUES($1,$2,9,9,$1)`, srcFullName), strings.Repeat("9", 77), strings.Repeat("9", 78))
	require.NoError(s.t, err)
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s(c1,s256) VALUES($1,-9)`, srcFullName), strings.Repeat("9", 77))
	require.NoError(s.t, err)

	connectionGen := FlowConnectionGenerationConfig{
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
			NullableEnabled: true,
		},
		{
			SourceName:      "c2",
			DestinationType: "String",
			NullableEnabled: true,
		},
		{
			SourceName:      "c3",
			DestinationType: "String",
			NullableEnabled: false,
		},
		{
			SourceName:      "s256",
			DestinationType: "Int256",
			NullableEnabled: false,
		},
		{
			SourceName:      "u256",
			DestinationType: "UInt256",
			NullableEnabled: false,
		},
	}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id,c1,c2,c3,s256,u256", 2)

	_, err = s.Conn().Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO %s(c1,c2,c3,s256,u256) VALUES($1,$2,9,9,$1)`, srcFullName), strings.Repeat("9", 77), strings.Repeat("9", 78))
	require.NoError(s.t, err)
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s(c1,s256) VALUES($1,-9)`, srcFullName), strings.Repeat("9", 77))
	require.NoError(s.t, err)

	EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id,c1,c2,c3,s256,u256", 4)

	rows, err := s.GetRows(dstTableName, "id,c1,c2,c3,s256,u256")
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 4, "expected 4 rows")
	require.False(s.t, rows.Schema.Fields[1].Nullable)
	require.True(s.t, rows.Schema.Fields[2].Nullable)
	require.False(s.t, rows.Schema.Fields[3].Nullable)
	big977, ok := new(big.Int).SetString(strings.Repeat("9", 77), 10)
	require.True(s.t, ok)
	for i, row := range rows.Records {
		require.Len(s.t, row, 6, "expected 4 columns")
		require.Equal(s.t, types.QValueKindString, row[1].Kind(), "c1 type mismatch")
		require.Equal(s.t, types.QValueKindString, row[2].Kind(), "c2 type mismatch")
		require.Equal(s.t, types.QValueKindString, row[3].Kind(), "c3 type mismatch")
		require.Equal(s.t, types.QValueKindInt256, row[4].Kind(), "s256 type mismatch")
		require.Equal(s.t, types.QValueKindUInt256, row[5].Kind(), "u256 type mismatch")
		require.Equal(s.t, strings.Repeat("9", 77), row[1].Value(), "c1 value mismatch")
		if i%2 == 0 {
			require.Equal(s.t, strings.Repeat("9", 78), row[2].Value(), "c2 value mismatch")
			require.Equal(s.t, "9", row[3].Value(), "c3 value mismatch")
			require.Zero(s.t, big.NewInt(9).Cmp(row[4].Value().(*big.Int)), "s256 value mismatch")
			require.Zero(s.t, big977.Cmp(row[5].Value().(*big.Int)), "u256 value mismatch")
		} else {
			require.Empty(s.t, row[2].Value(), "c2 value mismatch")
			require.Empty(s.t, row[3].Value(), "c3 value mismatch")
			require.Zero(s.t, big.NewInt(-9).Cmp(row[4].Value().(*big.Int)), "s256 value mismatch")
			require.Zero(s.t, new(big.Int).Cmp(row[5].Value().(*big.Int)), "u256 value mismatch")
		}
	}

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
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

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(fmt.Sprintf("clickhouse_test_unbounded_numerics_ff_%v", ffValue)),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": strconv.FormatBool(ffValue)}
	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id,c", 1)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf("INSERT INTO %s(c) VALUES($1)", srcFullName), nines)
	require.NoError(s.t, err)
	EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id,c", 2)

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
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Unbounded_Numeric_With_FF() {
	s.testNumericFF(true)
}

func (s ClickHouseSuite) Test_Unbounded_Numeric_Without_FF() {
	s.testNumericFF(false)
}

func (s ClickHouseSuite) testNumericTruncation(unbNumAsStringFf bool) {
	var pgSource *PostgresSource
	var ok bool
	if pgSource, ok = s.source.(*PostgresSource); !ok {
		s.t.Skip("only applies to postgres")
	}

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
	//nolint:govet // it's a test, no need for fieldalignment
	tests := []struct {
		SrcType          string
		SrcValue         string
		Expected         string
		ExpectedCleared  int
		ExpectedTrunated int
		ExpectedWithFF   string // if empty, same as above
	}{
		{SrcType: "numeric", SrcValue: nines(38, 38), Expected: nines(38, 38)},
		{SrcType: "numeric", SrcValue: nines(39, 0), Expected: "0", ExpectedCleared: 1, ExpectedWithFF: nines(39, 0)},
		{SrcType: "numeric", SrcValue: nines(0, 39), Expected: nines(0, 38), ExpectedTrunated: 1, ExpectedWithFF: nines(0, 39)},
		{SrcType: "numeric(96, 48)", SrcValue: nines(48, 48), Expected: nines(48, 48)},
		{SrcType: "numeric(76, 38)", SrcValue: nines(38, 38), Expected: nines(38, 38)},
		{SrcType: "numeric(76, 0)", SrcValue: nines(76, 0), Expected: nines(76, 0)},
		{SrcType: "numeric(76, 76)", SrcValue: nines(0, 76), Expected: nines(0, 76)},
	}

	totalCleared := 0
	totalTruncated := 0
	if !unbNumAsStringFf {
		for _, tc := range tests {
			totalCleared += tc.ExpectedCleared
			totalTruncated += tc.ExpectedTrunated
		}
	}

	dstTableName := fmt.Sprintf("numeric_truncation_unas_ff_%v", unbNumAsStringFf)
	srcFullName := s.attachSchemaSuffix(dstTableName)

	var sb strings.Builder
	fmt.Fprintf(&sb, "CREATE TABLE IF NOT EXISTS %s(\n", srcFullName)
	sb.WriteString("id INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY")
	for i, tc := range tests {
		fmt.Fprintf(&sb, ",\ncol%d %s", i, tc.SrcType)
		fmt.Fprintf(&sb, ",\ncol%d_neg %s", i, tc.SrcType)
		fmt.Fprintf(&sb, ",\ncol%d_arr %s[]", i, tc.SrcType)
	}
	sb.WriteString(")")

	createQuery := sb.String()
	_, err := s.Conn().Exec(s.t.Context(), createQuery)
	require.NoError(s.t, err)

	sb.Reset()
	fmt.Fprintf(&sb, "INSERT INTO %s(", srcFullName)
	for i := range tests {
		if i > 0 {
			fmt.Fprint(&sb, ", ")
		}
		fmt.Fprintf(&sb, "col%d, col%d_neg, col%d_arr", i, i, i)
	}
	fmt.Fprint(&sb, ") VALUES(")
	for i, tc := range tests {
		if i > 0 {
			fmt.Fprint(&sb, ", ")
		}
		fmt.Fprintf(&sb, "%s::numeric", tc.SrcValue)
		fmt.Fprint(&sb, ", ")
		fmt.Fprintf(&sb, "-%s::numeric", tc.SrcValue)
		fmt.Fprint(&sb, ", ")
		fmt.Fprintf(&sb, "array[%s, -%s]::numeric[]", tc.SrcValue, tc.SrcValue)
	}
	fmt.Fprint(&sb, ")")
	insertQuery := sb.String()

	_, err = s.Conn().Exec(s.t.Context(), insertQuery)
	require.NoError(s.t, err)

	flowJobName := s.attachSuffix(fmt.Sprintf("clickhouse_test_num_trunc_ff_%v", unbNumAsStringFf))
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      flowJobName,
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": strconv.FormatBool(unbNumAsStringFf)}
	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id", 1)
	if totalCleared > 0 {
		EnvWaitFor(s.t, env, 5*time.Minute, "waiting for cleared messages", func() bool {
			count, err := pgSource.GetLogCount(
				s.t.Context(), flowJobName, "warn", "cleared 1 NUMERIC value too big to fit into the destination column",
			)
			return err == nil && count == totalCleared*2 // positive and negative
		})
		EnvWaitFor(s.t, env, 5*time.Minute, "waiting for cleared array messages", func() bool {
			count, err := pgSource.GetLogCount(
				s.t.Context(), flowJobName, "warn", "cleared 2 NUMERIC values too big to fit into the destination column",
			)
			return err == nil && count == totalCleared
		})
	}
	if totalTruncated > 0 {
		EnvWaitFor(s.t, env, 5*time.Minute, "waiting for truncated messages", func() bool {
			count, err := pgSource.GetLogCount(
				s.t.Context(), flowJobName, "warn", "truncated 1 NUMERIC value too precise to fit into the destination column",
			)
			return err == nil && count == totalTruncated*2 // positive and negative
		})
		EnvWaitFor(s.t, env, 5*time.Minute, "waiting for truncated array messages", func() bool {
			count, err := pgSource.GetLogCount(
				s.t.Context(), flowJobName, "warn", "truncated 2 NUMERIC values too precise to fit into the destination column",
			)
			return err == nil && count == totalTruncated
		})
	}

	_, err = s.Conn().Exec(s.t.Context(), insertQuery)
	require.NoError(s.t, err)
	EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id", 2)

	if totalCleared > 0 {
		EnvWaitFor(s.t, env, 5*time.Minute, "waiting for cleared messages", func() bool {
			count, err := pgSource.GetLogCount(
				s.t.Context(), flowJobName, "warn", "cleared 1 NUMERIC value too big to fit into the destination column",
			)
			return err == nil && count == totalCleared*2*2 // positive and negative, snapshot and cdc
		})
		EnvWaitFor(s.t, env, 5*time.Minute, "waiting for cleared array messages", func() bool {
			count, err := pgSource.GetLogCount(
				s.t.Context(), flowJobName, "warn", "cleared 2 NUMERIC values too big to fit into the destination column",
			)
			return err == nil && count == totalCleared*2 // snapshot and cdc
		})
	}
	if totalTruncated > 0 {
		EnvWaitFor(s.t, env, 5*time.Minute, "waiting for truncated messages", func() bool {
			count, err := pgSource.GetLogCount(
				s.t.Context(), flowJobName, "warn", "truncated 1 NUMERIC value too precise to fit into the destination column",
			)
			return err == nil && count == totalTruncated*2*2 // positive and negative, snapshot and cdc
		})
		EnvWaitFor(s.t, env, 5*time.Minute, "waiting for truncated array messages", func() bool {
			count, err := pgSource.GetLogCount(
				s.t.Context(), flowJobName, "warn", "truncated 2 NUMERIC values too precise to fit into the destination column",
			)
			return err == nil && count == totalTruncated*2 // snapshot and cdc
		})
	}

	sb.Reset()
	for i := range tests {
		if i > 0 {
			fmt.Fprint(&sb, ", ")
		}
		fmt.Fprintf(&sb, "col%d, col%d_neg, col%d_arr", i, i, i)
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
	RequireEnvCanceled(s.t, env)
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

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("ch_binary_format_" + format),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_CLICKHOUSE_BINARY_FORMAT": format}
	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id,val", 1)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf("INSERT INTO %s(val) VALUES($1)", srcFullName), []byte(binaryFormatTestcase))
	require.NoError(s.t, err)
	EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id,val", 2)

	rows, err := s.GetRows(dstTableName, "val")
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 2, "expected 2 rows")
	for _, row := range rows.Records {
		require.Len(s.t, row, 1, "expected 1 column")
		require.Equal(s.t, expected, row[0].Value())
	}

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
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
	if _, ok := s.source.(*PostgresSource); !ok {
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

	relaxedNumberStr := "1" + strings.Repeat("0", 1000)
	jsonPayload := fmt.Sprintf(`{"key": 123, "relaxedNumber": %s}`, relaxedNumberStr)

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %[1]s(id serial PRIMARY KEY,c1 BIGINT,c2 BIT,c3 VARBIT,c4 BOOLEAN,
		c6 BYTEA,c7 CHARACTER,c8 varchar,c9 CIDR,c11 DATE,c12 FLOAT,c13 DOUBLE PRECISION,
		c14 INET,c15 INTEGER,c16 INTERVAL,c21 MACADDR,c22 MONEY,
		c23 NUMERIC,c24 OID,c28 REAL,c29 SMALLINT,c30 SMALLSERIAL,c31 SERIAL,c32 TEXT,
		c33 TIMESTAMP,c34 TIMESTAMPTZ,c35 TIME,c36 TIMETZ,c37 TSQUERY,c38 TSVECTOR,
		c39 TXID_SNAPSHOT,c40 UUID, c41 mood[], c42 INT[], c43 FLOAT[], c44 TEXT[], c45 mood, c46 HSTORE,
		c47 DATE[], c48 TIMESTAMPTZ[], c49 TIMESTAMP[], c50 BOOLEAN[], c51 SMALLINT[], c52 UUID[],
		c53 NUMERIC(16,2)[], c54 NUMERIC[], c55 NUMERIC(16,2)[], c56 NUMERIC[], c57 INTERVAL[],
		c58 JSON, c59 JSON, c60 JSONB, c61 JSONB, c62 JSON[], c63 JSON[], c64 JSON[], c65 JSONB[],
		c66 JSONB[], c67 JSONB[]);
		INSERT INTO %[1]s SELECT 2,2,b'1',b'101',
		true,random_bytes(32),'s','test','1.1.10.2'::cidr,
		CURRENT_DATE,1.23,1.234,'10.0.0.0/32'::inet,1,
		'5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds'::interval,
		'08:00:2b:01:02:03'::macaddr,
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
		'{1 second, 5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds}'::interval[],
		'%[2]s'::json, null::json, '%[2]s'::jsonb, null::jsonb,
		ARRAY['%[2]s'::json, null]::json[], ARRAY[]::json[], null::json[],
		ARRAY['%[2]s'::jsonb, null]::jsonb[], ARRAY[]::jsonb[], null::jsonb[];`,
		srcFullName, jsonPayload))
	require.NoError(s.t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_test_types"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitForCount(env, s, "waiting for initial snapshot count", dstTableName, "id", 1)
	EnvWaitForEqualTablesWithNames(env, s, "check comparable types 1", srcTableName, dstTableName,
		"id,c1,c4,c7,c8,c11,c12,c13,c15,c23,c28,c29,c30,c31,c32,c33,c34,c35,c36,c40,c41,c42,c43,c44,c45,"+
			"c48,c49,c52,c53,c54,c55,c56,c57,c59,c61,c64,c67")

	// Verify JSON values
	expectedJSON := fmt.Sprintf(`{"key":123,"relaxedNumber":"%s"}`, relaxedNumberStr)
	expectedJSONArray := fmt.Sprintf(`[{"key":123,"relaxedNumber":"%s"},null]`, relaxedNumberStr)
	rows, err := s.GetRows(dstTableName, "c58,c60,c62,c63,c65,c66")
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 1, "expected 1 row")
	c58Str, ok := rows.Records[0][0].Value().(string)
	require.True(s.t, ok, "c58 should be a string")
	require.JSONEq(s.t, expectedJSON, c58Str, "c58 should have exact JSON with quoted number")
	c60Str, ok := rows.Records[0][1].Value().(string)
	require.True(s.t, ok, "c60 should be a string")
	require.JSONEq(s.t, expectedJSON, c60Str, "c60 should have exact JSON with quoted number")
	c62Str, ok := rows.Records[0][2].Value().(string)
	require.True(s.t, ok, "c62 should be a string")
	require.JSONEq(s.t, expectedJSONArray, c62Str, "c62 should have exact JSON array with quoted number")
	c63Str, ok := rows.Records[0][3].Value().(string)
	require.True(s.t, ok, "c63 should be a string")
	require.JSONEq(s.t, "[]", c63Str, "c63 should be empty JSON array")
	c65Str, ok := rows.Records[0][4].Value().(string)
	require.True(s.t, ok, "c65 should be a string")
	require.JSONEq(s.t, expectedJSONArray, c65Str, "c65 should have exact JSON array with quoted number")
	c66Str, ok := rows.Records[0][5].Value().(string)
	require.True(s.t, ok, "c66 should be a string")
	require.JSONEq(s.t, "[]", c66Str, "c66 should be empty JSON array")

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %[1]s SELECT 3,2,b'1',b'101',
		true,random_bytes(32),'s','test','1.1.10.2'::cidr,
		CURRENT_DATE,1.23,1.234,'10.0.0.0/32'::inet,1,
		'5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds'::interval,
		'08:00:2b:01:02:03'::macaddr,
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
		'{1 second, 5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds}'::interval[],
		'%[2]s'::json, null::json, '%[2]s'::jsonb, null::jsonb,
		ARRAY['%[2]s'::json, null]::json[], ARRAY[]::json[], null::json[],
		ARRAY['%[2]s'::jsonb, null]::jsonb[], ARRAY[]::jsonb[], null::jsonb[];`, srcFullName, jsonPayload))
	require.NoError(s.t, err)
	EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id", 2)
	EnvWaitForEqualTablesWithNames(env, s, "check comparable types 2", srcTableName, dstTableName,
		"id,c1,c4,c7,c8,c11,c12,c13,c15,c23,c28,c29,c30,c31,c32,c33,c34,c35,c36,c40,c41,c42,c43,c44,c45,"+
			"c48,c49,c52,c53,c54,c55,c56,c57,c59,c61,c64,c67")

	// Verify JSON values after CDC
	rows, err = s.GetRows(dstTableName, "c58,c60,c62,c63,c65,c66")
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 2, "expected 2 rows after CDC")
	for i := range 2 {
		c58Str, ok := rows.Records[i][0].Value().(string)
		require.True(s.t, ok, "c58 should be a string in row %d", i)
		require.JSONEqf(s.t, expectedJSON, c58Str, "c58 should have exact JSON with quoted number in row %d", i)

		c60Str, ok := rows.Records[i][1].Value().(string)
		require.True(s.t, ok, "c60 should be a string in row %d", i)
		require.JSONEqf(s.t, expectedJSON, c60Str, "c60 should have exact JSON with quoted number in row %d", i)

		c62Str, ok := rows.Records[i][2].Value().(string)
		require.True(s.t, ok, "c62 should be a string in row %d", i)
		require.JSONEqf(s.t, expectedJSONArray, c62Str, "c62 should have exact JSON array with quoted number in row %d", i)

		c63Str, ok := rows.Records[i][3].Value().(string)
		require.True(s.t, ok, "c63 should be a string in row %d", i)
		require.JSONEqf(s.t, "[]", c63Str, "c63 should be empty JSON array in row %d", i)

		c65Str, ok := rows.Records[i][4].Value().(string)
		require.True(s.t, ok, "c65 should be a string in row %d", i)
		require.JSONEqf(s.t, expectedJSONArray, c65Str, "c65 should have exact JSON array with quoted number in row %d", i)

		c66Str, ok := rows.Records[i][5].Value().(string)
		require.True(s.t, ok, "c66 should be a string in row %d", i)
		require.JSONEqf(s.t, "[]", c66Str, "c66 should be empty JSON array in row %d", i)
	}

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		UPDATE %[1]s SET c1=3,c32='testery' WHERE id=2;
		UPDATE %[1]s SET c33=now(),c34=now(),c35=now()::TIME,c36=now()::TIMETZ WHERE id=3;
		INSERT INTO %[1]s SELECT 4,2,b'1',b'101',
		true,random_bytes(32),'s','test','1.1.10.2'::cidr,
		CURRENT_DATE,1.23,1.234,'10.0.0.0/32'::inet,1,
		'5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds'::interval,
		'08:00:2b:01:02:03'::macaddr,
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
		'{1 second, 5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds}'::interval[],
		'%[2]s'::json, null::json, '%[2]s'::jsonb, null::jsonb,
		ARRAY['%[2]s'::json, null]::json[], ARRAY[]::json[], null::json[],
		ARRAY['%[2]s'::jsonb, null]::jsonb[], ARRAY[]::jsonb[], null::jsonb[];`, srcFullName, jsonPayload))

	require.NoError(s.t, err)
	EnvWaitForCount(env, s, "waiting for CDC count again", dstTableName, "id", 3)
	EnvWaitForEqualTablesWithNames(env, s, "check comparable types 3", srcTableName, dstTableName,
		"id,c1,c4,c7,c8,c11,c12,c13,c15,c23,c28,c29,c30,c31,c32,c33,c34,c35,c36,c40,c41,c42,c43,c44,c45,"+
			"c48,c49,c52,c53,c54,c55,c56,c57,c59,c61,c64,c67")

	// Verify JSON values after final updates
	rows, err = s.GetRows(dstTableName, "c58,c60,c62,c63,c65,c66")
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 3, "expected 3 rows after final updates")
	for i := range 3 {
		c58Str, ok := rows.Records[i][0].Value().(string)
		require.True(s.t, ok, "c58 should be a string in row %d", i)
		require.JSONEqf(s.t, expectedJSON, c58Str, "c58 should have exact JSON with quoted number in row %d", i)

		c60Str, ok := rows.Records[i][1].Value().(string)
		require.True(s.t, ok, "c60 should be a string in row %d", i)
		require.JSONEqf(s.t, expectedJSON, c60Str, "c60 should have exact JSON with quoted number in row %d", i)

		c62Str, ok := rows.Records[i][2].Value().(string)
		require.True(s.t, ok, "c62 should be a string in row %d", i)
		require.JSONEqf(s.t, expectedJSONArray, c62Str, "c62 should have exact JSON array with quoted number in row %d", i)

		c63Str, ok := rows.Records[i][3].Value().(string)
		require.True(s.t, ok, "c63 should be a string in row %d", i)
		require.JSONEqf(s.t, "[]", c63Str, "c63 should be empty JSON array in row %d", i)

		c65Str, ok := rows.Records[i][4].Value().(string)
		require.True(s.t, ok, "c65 should be a string in row %d", i)
		require.JSONEqf(s.t, expectedJSONArray, c65Str, "c65 should have exact JSON array with quoted number in row %d", i)

		c66Str, ok := rows.Records[i][5].Value().(string)
		require.True(s.t, ok, "c66 should be a string in row %d", i)
		require.JSONEqf(s.t, "[]", c66Str, "c66 should be empty JSON array in row %d", i)
	}

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Time64() {
	_, isPostgres := s.source.(*PostgresSource)
	_, isMySQL := s.source.(*MySqlSource)
	if !isPostgres && !isMySQL {
		s.t.Skip("only applies to postgres and mysql")
	}

	flags, err := s.connector.GetFlags(s.t.Context())
	require.NoError(s.t, err)
	supportsTime64 := slices.Contains(flags, shared.Flag_ClickHouseTime64Enabled)

	srcTableName := "test_time"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := srcTableName + "_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			t TIME NOT NULL,
			t_nullable TIME,
			t_nullable_2 TIME
		)
	`, srcFullName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s (t, t_nullable, t_nullable_2) VALUES ('14:21:00', '08:30:00.123456', NULL)`, srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	flowConnConfig.TableMappings[0].Columns = []*protos.ColumnSetting{
		{SourceName: "t_nullable", NullableEnabled: false},
		{SourceName: "t_nullable_2", NullableEnabled: true},
	}
	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,t,t_nullable,t_nullable_2")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s (t, t_nullable, t_nullable_2) VALUES ('00:00:00', '23:59:59.999999', NULL)`, srcFullName)))

	if isMySQL {
		// test mysql-specific range outside 24 hours
		require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
			`INSERT INTO %s (t, t_nullable, t_nullable_2) VALUES ('-123:45:67.899999', '123:45:67.899999', NULL)`, srcFullName)))
	}

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,t,t_nullable,t_nullable_2")

	ch, err := connclickhouse.Connect(s.t.Context(), nil, s.Peer().GetClickhouseConfig())
	require.NoError(s.t, err)
	defer ch.Close()
	var dstTableSuffix string
	if s.cluster {
		dstTableSuffix = "_shard"
	}
	assertColumnType := func(columnName string, expectedColumnType string) {
		var columnType string
		// older version of ClickHouse do not support Time64
		if !supportsTime64 {
			expectedColumnType = strings.Replace(expectedColumnType, "Time64", "DateTime64", 1)
		}
		query := fmt.Sprintf(
			"select type from system.columns where database=%s and table=%s and name=%s",
			clickhouse.QuoteLiteral(s.connector.Config.Database),
			clickhouse.QuoteLiteral(dstTableName+dstTableSuffix),
			clickhouse.QuoteLiteral(columnName),
		)
		row := ch.QueryRow(s.t.Context(), query)
		require.NoError(s.t, row.Err())
		require.NoError(s.t, row.Scan(&columnType))
		require.Equal(s.t, expectedColumnType, columnType, "unexpected type for column %s", columnName)
	}
	assertColumnType("t", "Time64(6)")
	assertColumnType("t_nullable", "Time64(6)")
	assertColumnType("t_nullable_2", "Nullable(Time64(6))")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_InfiniteTimestamp() {
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only applies to postgres")
	}

	srcTableName := "test_infinite_time"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_infinite_time"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY,
			t_null TIMESTAMP NULL,
			t_notnull TIMESTAMP NOT NULL,
			d_null DATE NULL,
			d_notnull DATE NOT NULL,
			n_null NUMERIC NULL,
			n_notnull NUMERIC NOT NULL
		);
	`, srcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (id,t_null,t_notnull,d_null,d_notnull,n_null,n_notnull)
		VALUES (1,'infinity'::timestamp,'infinity'::timestamp,'infinity'::date,'infinity'::date,'infinity'::numeric,'infinity'::numeric)`,
		srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("ch_infinite_time"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_NULLABLE": "true"}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (id,t_null,t_notnull,d_null,d_notnull,n_null,n_notnull)
		VALUES (2,'infinity'::timestamp,'infinity'::timestamp,'infinity'::date,'infinity'::date,'infinity'::numeric,'infinity'::numeric)`,
		srcFullName)))

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id")

	ch, err := connclickhouse.Connect(s.t.Context(), nil, s.Peer().GetClickhouseConfig())
	require.NoError(s.t, err)
	rows, err := ch.Query(s.t.Context(),
		fmt.Sprintf("select id,t_null,t_notnull,d_null,d_notnull,n_null,n_notnull from %s order by id", dstTableName))
	require.NoError(s.t, err)
	defer rows.Close()
	numRows := 0
	for rows.Next() {
		numRows++
		var id int32
		var tNull *time.Time
		var dNull *time.Time
		var nNull *decimal.Decimal
		var tNotNull time.Time
		var dNotNull time.Time
		var nNotNull decimal.Decimal
		require.NoError(s.t, rows.Scan(&id, &tNull, &tNotNull, &dNull, &dNotNull, &nNull, &nNotNull))
		s.t.Log(id, tNull, dNull, nNull, tNotNull, dNotNull, nNotNull)
		require.Nil(s.t, tNull)
		require.Nil(s.t, dNull)
		require.Nil(s.t, nNull)
		require.True(s.t, time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC).Equal(tNotNull), "expected 1970-01-01, not %s", tNotNull)
		if id == 1 {
			require.True(s.t, time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC).Equal(dNotNull), "expected 1970-01-01, not %s", dNotNull)
		} else {
			require.True(s.t, time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC).Equal(dNotNull), "expected 1900-01-01, not %s", dNotNull)
		}
		require.True(s.t, nNotNull.IsZero(), "expected 0, not %s", nNotNull)
	}
	require.NoError(s.t, rows.Err())
	require.Equal(s.t, 2, numRows)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_JSON_Null() {
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only applies to postgres")
	}

	srcTableName := "test_json_null"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_json_null"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY,
			j_null JSON NULL,
			j_notnull JSON NOT NULL,
			j_sqlnull JSON NULL,
			jb_null JSONB NULL,
			jb_notnull JSONB NOT NULL,
			jb_sqlnull JSONB NULL
		);
	`, srcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO %s (id,j_null,j_notnull,j_sqlnull,jb_null,jb_notnull,jb_sqlnull)
		VALUES (1,'null'::json,'null'::json,NULL,'null'::jsonb,'null'::jsonb,NULL)`,
			srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("ch_json_null"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_NULLABLE": "true"}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id")

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO %s (id,j_null,j_notnull,j_sqlnull,jb_null,jb_notnull,jb_sqlnull)
		VALUES (2,'null'::json,'null'::json,NULL,'null'::jsonb,'null'::jsonb,NULL)`,
			srcFullName)))

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id")

	ch, err := connclickhouse.Connect(s.t.Context(), nil, s.Peer().GetClickhouseConfig())
	require.NoError(s.t, err)
	rows, err := ch.Query(s.t.Context(),
		fmt.Sprintf("select id,j_null,j_notnull,j_sqlnull,jb_null,jb_notnull,jb_sqlnull from %s order by id", dstTableName))
	require.NoError(s.t, err)
	defer rows.Close()
	numRows := 0
	for rows.Next() {
		numRows++
		var id int32
		var jNull *string
		var jbNull *string
		var jNotNull string
		var jbNotNull string
		var jSqlNull *string
		var jbSqlNull *string
		require.NoError(s.t, rows.Scan(&id, &jNull, &jNotNull, &jSqlNull, &jbNull, &jbNotNull, &jbSqlNull))
		s.t.Log(id, jNull, jbNull, jNotNull, jbNotNull)
		require.NotNil(s.t, jNull)
		require.NotNil(s.t, jbNull)
		require.Equal(s.t, "null", *jNull)
		require.Equal(s.t, "null", jNotNull)
		require.Equal(s.t, "null", *jbNull)
		require.Equal(s.t, "null", jbNotNull)
		require.Nil(s.t, jSqlNull)
		require.Nil(s.t, jbSqlNull)
	}
	require.NoError(s.t, rows.Err())
	require.Equal(s.t, 2, numRows)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_JSON_CH() {
	mysqlUtf8mb4Support := true
	if mySource, ok := s.source.(*MySqlSource); ok {
		if mySource.Config.Flavor == protos.MySqlFlavor_MYSQL_MARIA {
			s.t.Skip("skip maria, where JSON is not a supported data type")
		} else {
			cmp, err := mySource.CompareServerVersion(s.t.Context(), "8")
			require.NoError(s.t, err)
			if cmp < 0 {
				mysqlUtf8mb4Support = false
			}
		}
	}

	// TODO: fix and enable failing test cases
	jsonTestCases := []struct {
		Value      string
		Desc       string
		SkipReason string
		Skip       bool
	}{
		{
			Desc:  "obj",
			Value: `'{"key": "val", "number": 30, "bool": true}'`,
		},
		{
			Desc:  "nested obj",
			Value: `'{"name": "john", "contact": {"email": "john@example.com", "phone": {"cell": "1234567890"}}}'`,
		},
		{
			Desc:  "obj with array as values",
			Value: `'{"array_key": [1, 2, 3, ["hello", "world"]], "empty_array": []}'`,
		},
		{
			Desc:  "obj with numeric values",
			Value: `'{"int": 42, "float": 3.14159, "scientific": 1.23e-4, "negative": -456}'`,
		},
		{
			Desc:  "obj with special chars as values",
			Value: `'{"path": "/home/user","quote": "check \"quoted\"", "newline": "line1\nline2"}'`,
		},
		{
			Desc:  "obj with emoji",
			Value: `'{"unicode": "🚀"}'`,
			Skip:  !mysqlUtf8mb4Support,
		},
		{
			Desc:  "empty object",
			Value: `'{}'`,
		},
		{
			Desc:       "top-level number",
			Value:      `'123''`,
			Skip:       true,
			SkipReason: "not supported by ClickHouse JSON",
		},
		{
			Desc:       "top-level string",
			Value:      `'"string value"'`,
			Skip:       true,
			SkipReason: "not supported by ClickHouse JSON",
		},
		{
			Desc:       "top-level boolean",
			Value:      `'true'`,
			Skip:       true,
			SkipReason: "not supported by ClickHouse JSON",
		},
		{
			Desc:       "top-level arrays",
			Value:      `'[1, 2, 3, {"key": "val"}, ["hello", "world"]]'`,
			Skip:       true,
			SkipReason: "not supported by ClickHouse JSON",
		},
		{
			Desc:       "json null value",
			Value:      `'null'`,
			Skip:       true,
			SkipReason: "peerdb bug where null value is parsed as empty string due to JSONExtractString",
		},
		{
			Desc:       "postgres null value",
			Value:      `null`,
			Skip:       true,
			SkipReason: "peerdb bug where null value is parsed as empty string due to JSONExtractString",
		},
		{
			Desc:       "null as values",
			Value:      `'{"key1": null, "key2": {"nested_key": null}}'`,
			Skip:       true,
			SkipReason: "insertion succeeds; but comparison fails because CH JSON removes all null fields but postgres does not",
		},
	}

	srcTableName := "test_json"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_json_dst"

	cols := "id,json"
	createStmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, json JSON);`, srcFullName)
	insertStmt := func(v string) string { return fmt.Sprintf(`INSERT INTO %s (json) VALUES (%s)`, srcFullName, v) }
	if _, isPostgres := s.source.(*PostgresSource); isPostgres {
		cols = "id,json,jsonb"
		createStmt = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, json JSON, jsonb JSONB);`, srcFullName)
		insertStmt = func(v string) string {
			return fmt.Sprintf(`INSERT INTO %s (json, jsonb) VALUES (%s, %s)`, srcFullName, v, v)
		}
	}

	require.NoError(s.T(), s.source.Exec(s.t.Context(), createStmt))
	for _, testCase := range jsonTestCases {
		if !testCase.Skip {
			require.NoError(s.t, s.source.Exec(s.t.Context(), insertStmt(testCase.Value)))
		}
	}
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_json"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_CLICKHOUSE_ENABLE_JSON": "true"}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "compare initial load", srcTableName, dstTableName, cols)

	for _, testCase := range jsonTestCases {
		if !testCase.Skip {
			require.NoError(s.t, s.source.Exec(s.t.Context(), insertStmt(testCase.Value)))
		}
	}
	EnvWaitForEqualTablesWithNames(env, s, "compare cdc", srcTableName, dstTableName, cols)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_PgVector() {
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only applies to postgres")
	}

	srcTableName := "test_pgvector"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_pgvector"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, v1 vector, hv halfvec, sv sparsevec)`, srcFullName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`insert into %s (v1,hv,sv) values ('[1.5,2,3]','[1,2.5,3]','{1:1.5,3:3.5}/5')`, srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTableName),
		TableMappings: TableMappings(s, srcTableName, dstTableName),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "check comparable types 1", srcTableName, dstTableName, "id,v1,hv,sv")

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`insert into %s (v1,hv,sv) values ('[1.5,2,3.5]','[1,2,3.5]','{2:2.5,3:3.5}/5')`, srcFullName)))
	EnvWaitForEqualTablesWithNames(env, s, "check comparable types 2", srcTableName, dstTableName, "id,v1,hv,sv")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

// Test_AvroNullableLax tests PEERDB_AVRO_NULLABLE_LAX with multi-level inheritance and attnum gaps
// Need to modify code to trigger logging as the logging was added for the issue we were unable to reproduce
func (s ClickHouseSuite) Test_AvroNullableLax() {
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only applies to postgres")
	}

	srcTableName := "test_avro_nullable_lax"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_avro_nullable_lax"

	// Create grandparent -> parent -> child inheritance with dropped columns to create attnum gaps
	grandparentName := s.attachSchemaSuffix("test_avro_nullable_lax_grandparent")
	parentName := s.attachSchemaSuffix("test_avro_nullable_lax_parent")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (id INTEGER NOT NULL, to_drop TEXT, name TEXT)`, grandparentName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`ALTER TABLE %s DROP COLUMN to_drop`, grandparentName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (to_drop2 TEXT, age INTEGER NOT NULL) INHERITS (%s)`, parentName, grandparentName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`ALTER TABLE %s DROP COLUMN to_drop2`, parentName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (to_drop3 TEXT, email TEXT) INHERITS (%s)`, srcFullName, parentName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`ALTER TABLE %s DROP COLUMN to_drop3`, srcFullName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`ALTER TABLE %s ADD PRIMARY KEY (id)`, srcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO %s (id, name, age, email) VALUES (1, NULL, 25, NULL)`, srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTableName),
		TableMappings: TableMappings(s, srcTableName, dstTableName),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_AVRO_NULLABLE_LAX": "true"}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "nullable lax initial load", srcTableName, dstTableName, "id,name,age,email")

	// Check the logs for "Null values in columns that would be non-nullable under strict mode"
	// and a dump of tables/columns

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_PgVector_Version0() {
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only applies to postgres")
	}

	srcTableName := "test_pgvector"
	srcTextTableName := "test_pgvector_text"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	srcTextFullName := s.attachSchemaSuffix(srcTextTableName)
	dstTableName := "test_pgvector"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, v1 vector, hv halfvec, sv sparsevec)`, srcFullName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, v1 text, hv text, sv text)`, srcTextFullName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`insert into %s (v1,hv,sv) values ('[1.5,2,3]','[1,2.5,3]','{1:1.5,3:3.5}/5')`, srcFullName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`insert into %s (v1,hv,sv) values ('[1.5,2,3]','[1,2.5,3]','{1:1.5,3:3.5}/5')`, srcTextFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTableName),
		TableMappings: TableMappings(s, srcTableName, dstTableName),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_FORCE_INTERNAL_VERSION": strconv.FormatUint(uint64(shared.InternalVersion_First), 10)}
	flowConnConfig.Version = shared.InternalVersion_First

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "check comparable types 1", srcTextTableName, dstTableName, "id,v1,hv,sv")

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`insert into %s (v1,hv,sv) values ('[1.5,2,3.5]','[1,2,3.5]','{2:2.5,3:3.5}/5')`, srcFullName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`insert into %s (v1,hv,sv) values ('[1.5,2,3.5]','[1,2,3.5]','{2:2.5,3:3.5}/5')`, srcTextFullName)))
	EnvWaitForEqualTablesWithNames(env, s, "check comparable types 2", srcTextTableName, dstTableName, "id,v1,hv,sv")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Column_Exclusion() {
	if mySource, isMysql := s.source.(*MySqlSource); isMysql {
		cmp, err := mySource.CompareServerVersion(s.t.Context(), mysql_validation.MySQLMinVersionForBinlogRowMetadata)
		require.NoError(s.t, err)
		if cmp < 0 {
			s.t.Skip("not applicable to mysql versions that don't support binlog_row_metadata")
		}
	}

	tc := NewTemporalClient(s.t)

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
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      srcFullName,
			DestinationTableIdentifier: dstTableName,
			Exclude:                    []string{"c2"},
			ShardingKey:                "id",
		}},
		SourceName:        s.Source().GeneratePeer(s.t).Name,
		SyncedAtColName:   "_PEERDB_SYNCED_AT",
		MaxBatchSize:      100,
		DoInitialSnapshot: true,
		Version:           shared.InternalVersion_Latest,
	}

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	env := ExecutePeerflow(s.t, tc, config)
	SetupCDCFlowStatusQuery(s.t, env, config)

	// insert 5 rows into the source table
	for i := range 5 {
		EnvNoError(s.t, env, s.source.Exec(s.t.Context(), fmt.Sprintf(
			`INSERT INTO %[1]s(c1,c2,t) VALUES (%[2]d,%[2]d,'test_value_%[2]d')`,
			srcFullName, i,
		)))
	}

	EnvWaitForEqualTables(env, s, "normalize table", tableName, "id,c1,t")
	EnvNoError(s.t, env, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=1`, srcFullName)))
	EnvNoError(s.t, env, s.source.Exec(s.t.Context(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=0`, srcFullName)))
	EnvWaitForEqualTables(env, s, "normalize update/delete", tableName, "id,c1,t")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)

	rows, err := s.GetRows(tableName, "*")
	require.NoError(s.t, err)

	for _, field := range rows.Schema.Fields {
		require.NotEqual(s.t, "c2", field.Name)
	}
	require.Len(s.t, rows.Schema.Fields, 6)
}

func (s ClickHouseSuite) Test_Nullable_Schema_Change() {
	tc := NewTemporalClient(s.t)

	tableName := "test_nullable_sc_ch"
	srcFullName := s.attachSchemaSuffix(tableName)
	dstTableName := tableName

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, c1 INT);`, srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, tableName),
		TableMappings: TableMappings(s, tableName, dstTableName),
		Destination:   s.Peer().Name,
	}
	config := connectionGen.GenerateFlowConnectionConfigs(s)
	config.Env = map[string]string{"PEERDB_NULLABLE": "true"}

	env := ExecutePeerflow(s.t, tc, config)
	SetupCDCFlowStatusQuery(s.t, env, config)

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`ALTER TABLE %s ADD COLUMN c2 INT`, srcFullName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (c1,c2) VALUES (1,null)`, srcFullName)))

	EnvWaitForEqualTables(env, s, "new column", tableName, "id,c1,c2")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Nullable_Schema_Change_Replident_Full() {
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only applies to postgres")
	}
	tc := NewTemporalClient(s.t)

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

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, tableName),
		TableMappings: TableMappings(s, tableName, dstTableName),
		Destination:   s.Peer().Name,
	}
	config := connectionGen.GenerateFlowConnectionConfigs(s)
	config.Env = map[string]string{"PEERDB_NULLABLE": "true"}

	env := ExecutePeerflow(s.t, tc, config)
	SetupCDCFlowStatusQuery(s.t, env, config)

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN "Ac2" INT, ADD COLUMN "Ac3" INT NOT NULL,
		 ADD COLUMN c4 INT NOT NULL, DROP CONSTRAINT %s_pkey, ADD PRIMARY KEY (c4);`, srcFullName, tableName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (c1,"Ac2","Ac3",c4) VALUES (1,null,2,3)`, srcFullName)))

	EnvWaitForEqualTables(env, s, "new column", tableName, `id,c1,"Ac2","Ac3",c4`)

	rows, err := s.GetRows(dstTableName, `id,c1,"Ac2","Ac3",c4`)
	require.NoError(s.t, err)
	require.False(s.t, rows.Schema.Fields[0].Nullable)
	require.True(s.t, rows.Schema.Fields[1].Nullable)
	require.True(s.t, rows.Schema.Fields[2].Nullable)
	require.False(s.t, rows.Schema.Fields[3].Nullable)
	require.False(s.t, rows.Schema.Fields[4].Nullable)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

// old logic would mark pkey being nullable if replident index was used
func (s ClickHouseSuite) Test_Nullable_Schema_Change_Replident_Index() {
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only applies to postgres")
	}
	tc := NewTemporalClient(s.t)

	tableName := "test_nullable_sc_ch_replident_full"
	srcFullName := s.attachSchemaSuffix(tableName)
	dstTableName := tableName

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, "Ac1" INT NOT NULL, c2 INT NOT NULL);`, srcFullName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE UNIQUE INDEX idx_uniqlo_%s ON %s("Ac1",c2);`, tableName, srcFullName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`ALTER TABLE %s REPLICA IDENTITY USING INDEX idx_uniqlo_%s`, srcFullName, tableName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, tableName),
		TableMappings: TableMappings(s, tableName, dstTableName),
		Destination:   s.Peer().Name,
	}
	config := connectionGen.GenerateFlowConnectionConfigs(s)
	config.Env = map[string]string{"PEERDB_NULLABLE": "true"}

	env := ExecutePeerflow(s.t, tc, config)
	SetupCDCFlowStatusQuery(s.t, env, config)

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN "Ac3" INT NOT NULL,
		 ADD COLUMN c4 INT NOT NULL, ADD COLUMN "Ac5" TEXT, DROP CONSTRAINT %s_pkey, ADD PRIMARY KEY (c4);`, srcFullName, tableName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO %s ("Ac1",c2,"Ac3",c4,"Ac5") VALUES (1,2,3,4,null)`, srcFullName)))

	EnvWaitForEqualTables(env, s, "new column", tableName, `id,"Ac1",c2,"Ac3",c4,"Ac5"`)

	rows, err := s.GetRows(dstTableName, `id,"Ac1",c2,"Ac3",c4,"Ac5"`)
	require.NoError(s.t, err)
	require.False(s.t, rows.Schema.Fields[0].Nullable)
	require.False(s.t, rows.Schema.Fields[1].Nullable)
	require.False(s.t, rows.Schema.Fields[2].Nullable)
	require.False(s.t, rows.Schema.Fields[3].Nullable)
	require.False(s.t, rows.Schema.Fields[4].Nullable)
	require.True(s.t, rows.Schema.Fields[5].Nullable)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Unprivileged_Postgres_Columns() {
	if _, ok := s.source.(*PostgresSource); !ok {
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
			"Анна Каренина" DOUBLE PRECISION,
			"人間失格" CHAR(10),
			PRIMARY KEY (id, "id number")
		);
	`, srcFullName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	INSERT INTO %s (key, "se'cret", "spacey column", "#sync_me!", "2birds1stone", "quo'te", "Анна Каренина", "人間失格")
	VALUES ('init_initial_load', 'secret', 'neptune', 'true', 509, 'abcd', 3.14, '人間失格');
	`, srcFullName))
	require.NoError(s.t, err)

	err = RevokePermissionForTableColumns(s.t.Context(), s.Conn(), srcFullName,
		[]string{"id", "id number", "key", "spacey column", "#sync_me!", "2birds1stone", "quo'te", "Анна Каренина", "人間失格"})
	require.NoError(s.t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("clickhouse_test_unprivileged_columns"),
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      srcFullName,
			DestinationTableIdentifier: dstTableName,
			Exclude:                    []string{"se'cret"},
			ShardingKey:                "id",
		}},
		Destination: s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName,
		"id,\"id number\",key,\"spacey column\",\"#sync_me!\",\"2birds1stone\",\"quo'te\",\"Анна Каренина\",\"人間失格\"")
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	INSERT INTO %s (key, "se'cret", "spacey column", "#sync_me!", "2birds1stone","quo'te", "Анна Каренина", "人間失格")
	VALUES ('cdc1', 'secret', 'pluto', 'false', 123324, 'lwkfj', 2.718, '人間失格');
	`, srcFullName))

	require.NoError(s.t, err)
	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName,
		"id,\"id number\",key,\"spacey column\",\"#sync_me!\",\"2birds1stone\",\"quo'te\",\"Анна Каренина\",\"人間失格\"")
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_InitialLoadOnly_No_Primary_Key() {
	// No primary key means empty ordering key,  which works in 25.11 but will fail in 25.12 unless
	// `SETTINGS allow_suspicious_primary_key = TRUE`. So we skip this test here.
	chVersion, err := s.connector.GetVersion(s.t.Context())
	require.NoError(s.t, err)
	if chproto.CheckMinVersion(chproto.Version{Major: 25, Minor: 12}, chproto.ParseVersion(chVersion)) {
		s.t.Skip("'ORDER BY tuple()' is not supported in ClickHouse version for ReplacingMergeTree")
	}

	srcTableName := "test_no_pkey"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_no_pkey_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id INT, "key" TEXT NOT NULL)`, srcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (id,"key") VALUES (1,'init')`, srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_no_pkey"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.InitialSnapshotOnly = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\"")
	EnvWaitForFinished(s.t, env, time.Minute)
}

// Test_Normalize_Metadata_With_Retry tests the chunking normalization
// with a push to ClickHouse thrown in via renaming a target table.
func (s ClickHouseSuite) Test_Normalize_Metadata_With_Retry() {
	var pgSource *PostgresSource
	var ok bool
	if pgSource, ok = s.source.(*PostgresSource); !ok {
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

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_normalize_metadata_with_retry"),
		TableNameMapping: map[string]string{srcFullName1: dstTableName1, srcFullName2: dstTableName2},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName1, dstTableName1, "id,\"key\"")

	// Rename the table to simulate a push failure to ClickHouse
	ch, err := connclickhouse.Connect(s.t.Context(), nil, s.Peer().GetClickhouseConfig())
	require.NoError(s.t, err)
	fakeDestination2 := "test_normalize_metadata_with_retry_dst_2_fake"
	onCluster := ""
	if s.cluster {
		onCluster = " ON CLUSTER cicluster"
	}
	renameErr := ch.Exec(s.t.Context(), fmt.Sprintf(`RENAME TABLE %s TO %s%s`, dstTableName2, fakeDestination2, onCluster))
	require.NoError(s.t, renameErr)
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`UPDATE %s SET "key"='update1'`, srcFullName2)))

	EnvWaitFor(s.t, env, 5*time.Minute, "waiting for first sync to complete", func() bool {
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

	EnvWaitFor(s.t, env, 5*time.Minute, "waiting for raw table push to complete", func() bool {
		rows, err := s.GetRows(s.connector.GetRawTableName(connectionGen.FlowJobName), "_peerdb_batch_id")
		if err != nil {
			return false
		}

		if len(rows.Records) != 4 {
			return false
		}

		return rows.Records[0][0].Value().(int64) == 1
	})

	EnvWaitFor(s.t, env, 5*time.Minute, "waiting for normalize error", func() bool {
		errorCount, err := pgSource.GetLogCount(
			s.t.Context(), flowConnConfig.FlowJobName, "error", "error while inserting into target clickhouse table",
		)
		return err == nil && errorCount > 0
	})

	// Rename the table back to simulate a successful push to ClickHouse
	renameErr = ch.Exec(s.t.Context(), fmt.Sprintf(`RENAME TABLE %s TO %s%s`, fakeDestination2, dstTableName2, onCluster))
	require.NoError(s.t, renameErr)
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`UPDATE %s SET "key"='update2'`, srcFullName2)))
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`UPDATE %s SET "key"='update2'`, srcFullName1)))

	EnvWaitFor(s.t, env, 5*time.Minute, "waiting for second sync to complete", func() bool {
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

	EnvWaitFor(s.t, env, 5*time.Minute, "waiting for second raw table push to complete", func() bool {
		rows, err := s.GetRows(s.connector.GetRawTableName(connectionGen.FlowJobName), "_peerdb_batch_id")
		if err != nil {
			return false
		}

		if len(rows.Records) != 12 {
			return false
		}
		return true
	})

	EnvWaitFor(s.t, env, 5*time.Minute, "check normalize table metadata after normalize", func() bool {
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
		s.t.Log("metadata_last_sync_state", rows.Records[0][0].Value(), rows.Records[0][1].Value())
		return rows.Records[0][0].Value().(int64) == 2 && rows.Records[0][1].Value().(int64) == 2
	})

	EnvWaitForEqualTablesWithNames(env, s, "after 2 batches of cdc for table 1", srcTableName1, dstTableName1, "id,\"key\"")
	EnvWaitForEqualTablesWithNames(env, s, "after 2 batches of cdc for table 2", srcTableName2, dstTableName2, "id,\"key\"")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Geometric_Types() {
	if _, ok := s.source.(*PostgresSource); !ok {
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

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_test_geometric_types"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	// Wait for initial snapshot to complete
	EnvWaitForCount(env, s, "waiting for initial snapshot count", dstTableName, "id", 2)

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
	EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id", 3)

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
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_SkipSnapshotExport() {
	srcTableName := "test_skip_snapshot"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_skip_snapshot"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, "key" TEXT NOT NULL)`, srcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (id,"key") VALUES (1,'init')`, srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_skip_snapshot"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_SKIP_SNAPSHOT_EXPORT": "true"}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\"")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (id,"key") VALUES (2,'cdc')`, srcFullName)))
	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,\"key\"")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_SchemaAsColumn() {
	srcTableName := "test_schema_as_column"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_schema_as_column"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, "key" TEXT NOT NULL)`, srcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (id,"key") VALUES (1,'init')`, srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_schema_as_column"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN": "true"}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\"")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (id,"key") VALUES (2,'cdc')`, srcFullName)))
	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,\"key\"")

	rows, err := s.GetRows(dstTableName, "_peerdb_source_schema")
	require.NoError(s.t, err, "error selecting schema column")
	require.Len(s.t, rows.Records, 2, "expected 2 rows")
	for _, row := range rows.Records {
		require.Equal(s.t, "e2e_test_"+s.suffix, row[0].Value(), "schema column incorrectly populated")
	}

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
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

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_extra_ch_cols"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\"")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (id,"key") VALUES (2,'cdc')`, srcFullName)))
	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,\"key\"")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_NullEngine() {
	chPeer := s.Peer().GetClickhouseConfig()
	srcTableName := "test_nullengine"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_nullengine"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, "key" TEXT NOT NULL, val TEXT)`, srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("clickhouse_nullengine"),
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      srcFullName,
			DestinationTableIdentifier: dstTableName,
			Engine:                     protos.TableEngine_CH_ENGINE_NULL,
			ShardingKey:                "id",
		}},
		Destination: s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	ch, err := connclickhouse.Connect(s.t.Context(), nil, chPeer)
	require.NoError(s.t, err)
	require.NoError(s.t, ch.Exec(s.t.Context(),
		`create table nulltarget (id Int32, "key" String, _peerdb_is_deleted Int8) engine = ReplacingMergeTree() order by id`))
	require.NoError(s.t, ch.Exec(s.t.Context(),
		`create materialized view nullmv to nulltarget as select id, "key", _peerdb_is_deleted from test_nullengine`))
	require.NoError(s.t, ch.Close())

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`insert into %s values (1,'cdc','val')`, srcFullName)))
	EnvWaitForEqualTablesWithNames(env, s, "null insert", srcTableName, "nulltarget", "id,\"key\"")

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN added INT`, srcFullName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`insert into %s values (2,'no','add',0)`, srcFullName)))
	EnvWaitForEqualTablesWithNames(env, s, "null insert after column added", srcTableName, "nulltarget", "id,\"key\"")

	var count uint64
	ch, err = connclickhouse.Connect(s.t.Context(), nil, chPeer)
	require.NoError(s.t, err)
	row := ch.QueryRow(s.t.Context(),
		fmt.Sprintf("select count(*) from system.columns where database = '%s' and table = 'test_nullengine'", chPeer.Database))
	require.NoError(s.t, row.Err())
	require.NoError(s.t, row.Scan(&count))
	require.NoError(s.t, ch.Close())
	require.Equal(s.t, uint64(7), count)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
	env = ExecuteDropFlow(s.t.Context(), tc, flowConnConfig, 0)
	EnvWaitForFinished(s.t, env, 3*time.Minute)

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf("ALTER TABLE %s DROP COLUMN val", srcFullName)))

	ch, err = connclickhouse.Connect(s.t.Context(), nil, chPeer)
	require.NoError(s.t, err)
	require.NoError(s.t, ch.Exec(s.t.Context(), "TRUNCATE TABLE nulltarget"))
	require.NoError(s.t, ch.Close())
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Resync = true
	env = ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, "nulltarget", "id,\"key\"")

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`insert into %s values (3,'cdcresync',1)`, srcFullName)))
	EnvWaitForEqualTablesWithNames(env, s, "waiting on insert after resync", srcTableName, "nulltarget", "id,\"key\"")

	ch, err = connclickhouse.Connect(s.t.Context(), nil, chPeer)
	require.NoError(s.t, err)
	row = ch.QueryRow(s.t.Context(),
		fmt.Sprintf("select count(*) from system.columns where database = '%s' and table = 'test_nullengine'", chPeer.Database))
	require.NoError(s.t, row.Err())
	require.NoError(s.t, row.Scan(&count))
	require.NoError(s.t, ch.Close())
	require.Equal(s.t, uint64(6), count)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_CoalescingEngine() {
	// temporarily skip this test to unblock CI until underlying issue with CoalescingMergeTree engine is resolved:
	// DB::Exception: Too large size (18446464455627382046) passed to allocator. It indicates an error.
	s.t.Skip("remove me when CoalescingMergeTree issue is fixed")

	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("relies on random_string UDF")
	}

	srcTableName := "test_coalescing"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_coalescing"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, num INT, val TEXT)`, srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("clickhouse_nullengine"),
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      srcFullName,
			DestinationTableIdentifier: dstTableName,
			Engine:                     protos.TableEngine_CH_ENGINE_COALESCING_MERGE_TREE,
			ShardingKey:                "id",
		}},
		Destination: s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.Env = map[string]string{"PEERDB_NULLABLE": "true"}
	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	// test toast
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO %s (id,num,val) VALUES (0,0,random_string(9000))`, srcFullName)))
	EnvWaitForEqualTablesWithNames(env, s, "insert", srcTableName, dstTableName, "id,num,val")
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`UPDATE %s SET num = 1`, srcFullName)))
	EnvWaitForEqualTablesWithNames(env, s, "update", srcTableName, dstTableName, "id,num,val")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
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
		if _, ok := s.source.(*PostgresSource); ok {
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

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("clickhouse_partition_key_integer"),
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      srcFullName,
			DestinationTableIdentifier: dstTableName,
			PartitionKey:               "id",
			ShardingKey:                "id",
		}},
		Destination: s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.SnapshotMaxParallelWorkers = 4
	flowConnConfig.SnapshotNumRowsPerPartition = 10
	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,myname,updated_at")

	countRow := s.Conn().QueryRow(s.t.Context(),
		`SELECT COUNT(*) FROM peerdb_stats.qrep_partitions WHERE parent_mirror_name = $1`,
		flowConnConfig.FlowJobName)

	var partitionCount int
	require.NoError(s.t, countRow.Scan(&partitionCount), "failed to get partition count")
	require.GreaterOrEqual(s.t, partitionCount, 10, "expected at least 10 partitions to be created")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
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
		if _, ok := s.source.(*PostgresSource); ok {
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

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("clickhouse_partition_key_timestamp"),
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      srcFullName,
			DestinationTableIdentifier: dstTableName,
			PartitionKey:               "updated_at",
			ShardingKey:                "id",
		}},
		Destination: s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.SnapshotMaxParallelWorkers = 4
	flowConnConfig.SnapshotNumRowsPerPartition = 10
	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,myname,updated_at")

	countRow := s.Conn().QueryRow(s.t.Context(),
		`SELECT COUNT(*) FROM peerdb_stats.qrep_partitions WHERE parent_mirror_name = $1`,
		flowConnConfig.FlowJobName)

	var partitionCount int
	require.NoError(s.t, countRow.Scan(&partitionCount), "failed to get partition count")
	require.GreaterOrEqual(s.t, partitionCount, 10, "expected at least 10 partitions to be created")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

// tests where panic happened when using custom partition key on empty table
func (s ClickHouseSuite) Test_Partition_Key_Empty() {
	srcTableName := "test_partition_key_empty"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_partition_key_empty"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id INT PRIMARY KEY,
		myname TEXT NOT NULL,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)`, srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("clickhouse_partition_key_empty"),
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      srcFullName,
			DestinationTableIdentifier: dstTableName,
			PartitionKey:               "id",
			ShardingKey:                "id",
		}},
		Destination: s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.SnapshotMaxParallelWorkers = 4
	flowConnConfig.SnapshotNumRowsPerPartition = 10
	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,myname,updated_at")

	countRow := s.Conn().QueryRow(s.t.Context(),
		`SELECT COUNT(*) FROM peerdb_stats.qrep_partitions WHERE parent_mirror_name = $1`,
		flowConnConfig.FlowJobName)

	var partitionCount int
	require.NoError(s.t, countRow.Scan(&partitionCount), "failed to get partition count")
	require.Zero(s.t, partitionCount, "expected no partitions to be created")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

// edge case: min/max will be null. initial load should skip these for now, & not panic
func (s ClickHouseSuite) Test_Partition_Key_Null() {
	srcTableName := "test_partition_key_null"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_partition_key_null"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id INT PRIMARY KEY,
		myname TEXT NOT NULL,
		updated_at TIMESTAMP)`, srcFullName)))

	for i := 1; i <= 100; i++ {
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf(`INSERT INTO %s (id,myname) VALUES (%d,'init_%d')`, srcFullName, i, i)))
	}

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("clickhouse_partition_key_null"),
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      srcFullName,
			DestinationTableIdentifier: dstTableName,
			PartitionKey:               "updated_at",
			ShardingKey:                "id",
		}},
		Destination: s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.SnapshotMaxParallelWorkers = 4
	flowConnConfig.SnapshotNumRowsPerPartition = 10
	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	countRow := s.Conn().QueryRow(s.t.Context(),
		`SELECT COUNT(*) FROM peerdb_stats.qrep_partitions WHERE parent_mirror_name = $1`,
		flowConnConfig.FlowJobName)

	var partitionCount int
	require.NoError(s.t, countRow.Scan(&partitionCount), "failed to get partition count")
	require.Zero(s.t, partitionCount, "expected no partitions to be created")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_PartitionBy() {
	srcTableName := "test_partition_by"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_partition_by"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, num INT, val TEXT NOT NULL)`, srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("clickhouse_partition_by"),
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      srcFullName,
			DestinationTableIdentifier: dstTableName,
			Columns: []*protos.ColumnSetting{
				{SourceName: "id", NullableEnabled: true},
				{SourceName: "num", NullableEnabled: true, Partitioning: 1},
				{SourceName: "val", NullableEnabled: true, Ordering: 1},
			},
		}},
		Destination: s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "table setup", srcTableName, dstTableName, "id")

	var partitionKey, sortingKey string
	ch, err := connclickhouse.Connect(s.t.Context(), nil, s.Peer().GetClickhouseConfig())
	require.NoError(s.t, err)
	var dstTableSuffix string
	if s.cluster {
		dstTableSuffix = "_shard"
	}
	require.NoError(s.t,
		ch.QueryRow(s.t.Context(), fmt.Sprintf(
			"select partition_key,sorting_key from system.tables where database=%s and name=%s",
			clickhouse.QuoteLiteral(s.connector.Config.Database),
			clickhouse.QuoteLiteral(dstTableName+dstTableSuffix),
		)).Scan(&partitionKey, &sortingKey),
	)
	require.NoError(s.t, ch.Close())
	require.Equal(s.t, "num", partitionKey)
	require.Equal(s.t, "val", sortingKey)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_PartitionByExpr() {
	srcTableName := "test_partition_by_expr"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_partition_by_expr"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, num INT, val TEXT NOT NULL)`, srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("clickhouse_partition_by"),
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      srcFullName,
			DestinationTableIdentifier: dstTableName,
			PartitionByExpr:            "num%2,val",
			Columns: []*protos.ColumnSetting{
				{SourceName: "id", NullableEnabled: true},
				{SourceName: "num", NullableEnabled: true},
				{SourceName: "val", NullableEnabled: true},
			},
		}},
		Destination: s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "table setup", srcTableName, dstTableName, "id")

	var partitionKey string
	ch, err := connclickhouse.Connect(s.t.Context(), nil, s.Peer().GetClickhouseConfig())
	require.NoError(s.t, err)
	var dstTableSuffix string
	if s.cluster {
		dstTableSuffix = "_shard"
	}
	require.NoError(s.t,
		ch.QueryRow(s.t.Context(), fmt.Sprintf(
			"select partition_key from system.tables where database=%s and name=%s",
			clickhouse.QuoteLiteral(s.connector.Config.Database),
			clickhouse.QuoteLiteral(dstTableName+dstTableSuffix),
		)).Scan(&partitionKey),
	)
	require.NoError(s.t, ch.Close())
	require.Equal(s.t, "(num % 2, val)", partitionKey)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Partition_By_CTID_With_Num_Partitions_Override() {
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only applies to postgres")
	}

	srcTableName := "test_ctid_block_partitions"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_ctid_block_partitions_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			name TEXT,
			age INT,
			email TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`, srcFullName)))
	numRows := 1000
	deletedRows := 10
	for i := 1; i <= numRows; i++ {
		require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
			INSERT INTO %s (name, age, email) VALUES ('user_%d', %d, 'user_%d@example.com')
		`, srcFullName, i, 20+(i%50), i)))
	}
	for i := 1; i <= numRows; i++ {
		require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
			UPDATE %s SET age = %d WHERE id = %d
		`, srcFullName, 30+(i%50), i)))
	}
	for i := 1; i <= deletedRows; i++ {
		require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
			DELETE FROM %s WHERE id = %d
		`, srcFullName, i)))
	}

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_partition_by_ctid"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.SnapshotNumPartitionsOverride = 3

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)

	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitForCount(env, s, "wait on initial", dstTableName, "id", numRows-deletedRows)

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
			INSERT INTO %s (name, age, email) VALUES ('user_%d', %d, 'user_%d@example.com')
		`, srcFullName, numRows+1, 25, numRows+1)))
	EnvWaitForCount(env, s, "wait on cdc", dstTableName, "id", numRows-deletedRows+1)

	rows, err := s.Conn().Query(s.t.Context(),
		`SELECT partition_start, partition_end FROM peerdb_stats.qrep_partitions WHERE parent_mirror_name = $1
		ORDER BY
			CAST(split_part(trim(both '()' from partition_start), ',', 1) AS bigint),
			CAST(split_part(trim(both '()' from partition_start), ',', 2) AS bigint)`,
		flowConnConfig.FlowJobName)
	require.NoError(s.t, err, "failed to query partition ranges")
	defer rows.Close()

	var partitionRanges []struct{ start, end string }
	for rows.Next() {
		var start, end string
		require.NoError(s.t, rows.Scan(&start, &end), "failed to scan partition range")
		partitionRanges = append(partitionRanges, struct{ start, end string }{start, end})
	}
	require.NoError(s.t, rows.Err())
	// Verify partition count matches override
	require.Len(s.t, partitionRanges, 3, "expected exactly 3 partitions to be created with SnapshotNumPartitionsOverride=3")

	// Verify partitions ranges are contiguous (intentionally ignoring `TID.Valid` field for tests)
	tidParse := func(tidStr string) pgtype.TID {
		blockStr, offsetStr, found := strings.Cut(tidStr[1:len(tidStr)-1], ",")
		require.True(s.t, found, "failed to parse block number")
		block, err := strconv.ParseUint(blockStr, 10, 32)
		require.NoError(s.t, err, "failed to parse block number")
		offset, err := strconv.ParseUint(offsetStr, 10, 16)
		require.NoError(s.t, err, "failed to parse offset number")
		return pgtype.TID{BlockNumber: uint32(block), OffsetNumber: uint16(offset)}
	}
	tidInc := func(t pgtype.TID) pgtype.TID {
		if t.OffsetNumber < math.MaxUint16 {
			return pgtype.TID{BlockNumber: t.BlockNumber, OffsetNumber: t.OffsetNumber + 1}
		}
		return pgtype.TID{BlockNumber: t.BlockNumber + 1, OffsetNumber: 0}
	}
	tidEq := func(t1, t2 pgtype.TID) bool {
		return t1.BlockNumber == t2.BlockNumber && t1.OffsetNumber == t2.OffsetNumber
	}
	for i, pr := range partitionRanges {
		startTID := tidParse(pr.start)
		if i > 0 {
			prevEndTID := tidParse(partitionRanges[i-1].end)
			require.True(s.t, tidEq(tidInc(prevEndTID), startTID),
				"partitions not contiguous; partition ranges are %v", partitionRanges)
		} else {
			require.True(s.t, tidEq(pgtype.TID{}, startTID))
		}
	}

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Composite_PKey() {
	srcTableName := "test_composite_pkey_ordering"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_composite_pkey_ordering"

	orderedPk := "b, a, c"
	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
		    id int,
			a INT NOT NULL,
			b INT NOT NULL,
			c INT NOT NULL,
			PRIMARY KEY (%s)
		)
	`, srcFullName, orderedPk))
	require.NoError(s.t, err)
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (id,a,b,c) VALUES (0,1,2,3)`, srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("ch_composite_pkey_order"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, orderedPk)

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (id,a,b,c) VALUES (4,5,6,7)`, srcFullName)))
	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, orderedPk)

	var sortingKey string
	ch, err := connclickhouse.Connect(s.t.Context(), nil, s.Peer().GetClickhouseConfig())
	require.NoError(s.t, err)
	var dstTableSuffix string
	if s.cluster {
		dstTableSuffix = "_shard"
	}
	rows := ch.QueryRow(s.t.Context(),
		fmt.Sprintf("SELECT sorting_key FROM system.tables WHERE database=%s AND name=%s",
			clickhouse.QuoteLiteral(s.connector.Config.Database),
			clickhouse.QuoteLiteral(dstTableName+dstTableSuffix)))
	require.NoError(s.t, rows.Err())
	require.NoError(s.t, rows.Scan(&sortingKey))
	require.NoError(s.t, ch.Close())

	require.Equal(s.t, orderedPk, sortingKey, "sort key should preserve source primary key column order")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}
