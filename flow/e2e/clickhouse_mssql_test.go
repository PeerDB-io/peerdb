package e2e

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func TestPeerFlowE2ETestSuiteMSSQL_CH(t *testing.T) {
	if os.Getenv("CI_MSSQL_HOST") == "" {
		t.Skip("CI_MSSQL_HOST not set")
	}
	e2eshared.RunSuite(t, SetupClickHouseSuite(t, false, func(t *testing.T) (*MsSqlSource, string, error) {
		t.Helper()
		suffix := "mssch_" + strings.ToLower(shared.RandomString(8))
		source, err := SetupMsSql(t, suffix)
		return source, suffix, err
	}))
}

// mssqlTable returns "dbo.<table>" for use as source table identifier.
// MSSQL connector targets the database, tables live in dbo schema.
func mssqlTable(table string) string {
	return "dbo." + table
}

func (s ClickHouseSuite) Test_Simple_MsSQL() {
	if _, ok := s.source.(*MsSqlSource); !ok {
		s.t.Skip("only applies to mssql")
	}
	mssqlSource := s.source.(*MsSqlSource)

	srcTableName := "test_simple"
	srcFullName := mssqlTable(srcTableName)
	dstTableName := "test_simple"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`CREATE TABLE %s (
		id int identity primary key,
		val nvarchar(255),
		num int
	)`, srcTableName)))

	require.NoError(s.t, mssqlSource.EnableTableCdc(s.t.Context(), "dbo", srcTableName))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		"INSERT INTO %s (val, num) VALUES ('hello', 42)", srcTableName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		"INSERT INTO %s (val, num) VALUES ('world', 99)", srcTableName)))

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial snapshot",
		srcTableName, dstTableName, "id,val,num")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		"INSERT INTO %s (val, num) VALUES ('cdc_insert', 123)", srcTableName)))

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc insert",
		srcTableName, dstTableName, "id,val,num")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		"UPDATE %s SET val='updated', num=999 WHERE num=42", srcTableName)))

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc update",
		srcTableName, dstTableName, "id,val,num")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		"DELETE FROM %s WHERE num=99", srcTableName)))

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc delete",
		srcTableName, dstTableName, "id,val,num")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Types_MsSQL() {
	if _, ok := s.source.(*MsSqlSource); !ok {
		s.t.Skip("only applies to mssql")
	}
	mssqlSource := s.source.(*MsSqlSource)

	srcTableName := "test_types"
	srcFullName := mssqlTable(srcTableName)
	dstTableName := "test_types"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`CREATE TABLE %s (
		id int identity primary key,
		c_bit bit,
		c_tinyint tinyint,
		c_smallint smallint,
		c_int int,
		c_bigint bigint,
		c_real real,
		c_float float,
		c_decimal decimal(10,4),
		c_numeric numeric(18,6),
		c_money money,
		c_smallmoney smallmoney,
		c_char char(10),
		c_varchar varchar(100),
		c_text text,
		c_nchar nchar(10),
		c_nvarchar nvarchar(100),
		c_ntext ntext,
		c_binary binary(8),
		c_varbinary varbinary(100),
		c_date date,
		c_time time,
		c_datetime datetime,
		c_datetime2 datetime2,
		c_smalldatetime smalldatetime,
		c_datetimeoffset datetimeoffset,
		c_uniqueid uniqueidentifier,
		c_xml xml,
		c_sql_variant sql_variant
	)`, srcTableName)))

	require.NoError(s.t, mssqlSource.EnableTableCdc(s.t.Context(), "dbo", srcTableName))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s
		(c_bit, c_tinyint, c_smallint, c_int, c_bigint,
		 c_real, c_float, c_decimal, c_numeric, c_money, c_smallmoney,
		 c_char, c_varchar, c_text, c_nchar, c_nvarchar, c_ntext,
		 c_binary, c_varbinary,
		 c_date, c_time, c_datetime, c_datetime2, c_smalldatetime, c_datetimeoffset,
		 c_uniqueid, c_xml, c_sql_variant)
		VALUES (
			1, 255, -32768, 2147483647, 9223372036854775807,
			3.14, 2.718281828, 123456.7890, 123456.789012, 922337203685477.5807, 214748.3647,
			'fixed', 'variable', 'long text', N'unicode', N'nvarchar val', N'ntext val',
			0xDEADBEEF00000000, 0xCAFEBABE,
			'2024-01-15', '10:30:45.123456', '2024-01-15T10:30:00',
			'2024-01-15T10:30:00.123456', '2024-01-15T10:30:00', '2024-01-15T10:30:00+05:30',
			'550e8400-e29b-41d4-a716-446655440000', '<root><item>test</item></root>',
			CAST('hello variant' AS sql_variant)
		)`, srcTableName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	allCols := "id,c_bit,c_tinyint,c_smallint,c_int,c_bigint," +
		"c_real,c_float,c_decimal,c_numeric,c_money,c_smallmoney," +
		"c_char,c_varchar,c_text,c_nchar,c_nvarchar,c_ntext," +
		"c_binary,c_varbinary," +
		"c_date,c_time,c_datetime,c_datetime2,c_smalldatetime,c_datetimeoffset," +
		"c_uniqueid,c_xml,c_sql_variant"

	EnvWaitForEqualTablesWithNames(env, s, "waiting on snapshot",
		srcTableName, dstTableName, allCols)

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s
		(c_bit, c_tinyint, c_smallint, c_int, c_bigint,
		 c_real, c_float, c_decimal, c_numeric, c_money, c_smallmoney,
		 c_char, c_varchar, c_text, c_nchar, c_nvarchar, c_ntext,
		 c_binary, c_varbinary,
		 c_date, c_time, c_datetime, c_datetime2, c_smalldatetime, c_datetimeoffset,
		 c_uniqueid, c_xml, c_sql_variant)
		VALUES (
			0, 0, 32767, -1, -1,
			0.0, 0.0, 0.0000, 0.000000, 0.0000, 0.0000,
			'', '', '', N'', N'', N'',
			0x0000000000000000, 0x00,
			'1970-01-01', '00:00:00', '1970-01-01T00:00:00',
			'1970-01-01T00:00:00', '1900-01-01T00:00:00', '1970-01-01T00:00:00+00:00',
			'00000000-0000-0000-0000-000000000000', '<empty/>',
			CAST(0 AS sql_variant)
		)`, srcTableName)))

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc insert",
		srcTableName, dstTableName, allCols)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Nullable_MsSQL() {
	if _, ok := s.source.(*MsSqlSource); !ok {
		s.t.Skip("only applies to mssql")
	}
	mssqlSource := s.source.(*MsSqlSource)

	srcTableName := "test_nullable"
	srcFullName := mssqlTable(srcTableName)
	dstTableName := "test_nullable"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`CREATE TABLE %s (
		id int identity primary key,
		c_int int NULL,
		c_varchar nvarchar(100) NULL,
		c_decimal decimal(10,2) NULL,
		c_date date NULL,
		c_uniqueid uniqueidentifier NULL,
		c_varbinary varbinary(100) NULL
	)`, srcTableName)))

	require.NoError(s.t, mssqlSource.EnableTableCdc(s.t.Context(), "dbo", srcTableName))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		"INSERT INTO %s (c_int, c_varchar, c_decimal, c_date, c_uniqueid, c_varbinary)"+
			" VALUES (NULL, NULL, NULL, NULL, NULL, NULL)", srcTableName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		"INSERT INTO %s (c_int, c_varchar, c_decimal, c_date, c_uniqueid, c_varbinary)"+
			" VALUES (42, 'hello', 3.14, '2024-06-15', 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', 0xDEAD)",
		srcTableName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_NULLABLE": "true"}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	allCols := "id,c_int,c_varchar,c_decimal,c_date,c_uniqueid,c_varbinary"
	EnvWaitForEqualTablesWithNames(env, s, "waiting on snapshot",
		srcTableName, dstTableName, allCols)

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		"UPDATE %s SET c_int=NULL, c_varchar=NULL WHERE c_int=42", srcTableName)))

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc update",
		srcTableName, dstTableName, allCols)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}
