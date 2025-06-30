package e2e_clickhouse

import (
	"fmt"
	"math"
	"strings"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
	peerflow "github.com/PeerDB-io/peerdb/flow/workflows"
)

func (s ClickHouseSuite) Test_UnsignedMySQL() {
	if _, ok := s.source.(*e2e.MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	}

	srcTableName := "test_unsigned"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_unsigned"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`CREATE TABLE %s (
		id serial primary key,
		i8 tinyint, u8 tinyint unsigned,
		i16 smallint, u16 smallint unsigned,
		i24 mediumint, u24 mediumint unsigned,
		i32 int, u32 int unsigned,
		i64 bigint, u64 bigint unsigned,
		d decimal(7, 6), b boolean
	)`, srcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`insert into %s
		(i8,u8,i16,u16,i24,u24,i32,u32,i64,u64,d,b)
		values (-1, 200, -2, 40000, -3, 10000000, -4, 3000000000, %d, %d, 3.141592,true)
	`, srcFullName, int64(math.MinInt64), uint64(math.MaxUint64))))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      srcFullName,
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,i8,u8,i16,u16,i24,u24,i32,u32,i64,u64,d,b")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`insert into %s
		(i8,u8,i16,u16,i24,u24,i32,u32,i64,u64,d,b)
		values (-1, 200, -2, 40000, -3, 10000000, -4, 3000000000, %d, %d, 3.141592,false)
	`, srcFullName, int64(math.MinInt64), uint64(math.MaxUint64))))

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,i8,u8,i16,u16,i24,u24,i32,u32,i64,u64,d,b")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Time() {
	if _, ok := s.source.(*e2e.MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	}

	srcTableName := "test_datetime"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	quotedSrcFullName := "\"" + strings.ReplaceAll(srcFullName, ".", "\".\"") + "\""
	dstTableName := "test_datetime_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			"key" TEXT NOT NULL,
			d DATE NOT NULL,
			dt DATETIME NOT NULL,
			tm TIMESTAMP(6) NOT NULL,
			t TIME NOT NULL
		)
	`, quotedSrcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s ("key",d,dt,tm,t) VALUES
		('init','1935-01-01','1953-02-02 12:01:02','1973-02-02 13:01:02.123','14:21.654321'),
		('init','0000-00-00','0000-00-00 00:00:00','0000-00-00 00:00:00.000','00:00'),
		('init','0000-00-00','0000-00-00 00:00:00','0000-00-00 00:00:00.000','-800:0:1')`,
		quotedSrcFullName)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\",d,dt,tm,t")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s ("key",d,dt,tm,t) VALUES
		('cdc','1935-01-01','1953-02-02 12:01:02','1973-02-02 13:01:02.123','14:21.654321'),
		('cdc','0000-00-00','0000-00-00 00:00:00','0000-00-00 00:00:00.000','00:00'),
		('cdc','0000-00-00','0000-00-00 00:00:00','0000-00-00 00:00:00.000','-800:0:1')`,
		quotedSrcFullName)))

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,\"key\",d,dt,tm,t")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Bit() {
	if _, ok := s.source.(*e2e.MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	}

	srcTableName := "test_bit"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	quotedSrcFullName := "\"" + strings.ReplaceAll(srcFullName, ".", "\".\"") + "\""
	dstTableName := "test_bit_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			"key" TEXT NOT NULL,
			b1 bit(1) NOT NULL,
			b20 bit(20) NOT NULL
		)
	`, quotedSrcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s ("key",b1,b20) VALUES
		('init',b'1',b'11100011100011100011'), ('init',b'0',b'00011100011100011100')`, quotedSrcFullName)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\",b1,b20")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s ("key",b1,b20) VALUES
		('cdc','1','11100011100011100011'), ('cdc','0','00011100011100011100')`, quotedSrcFullName)))

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,\"key\",b1,b20")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Blobs() {
	if _, ok := s.source.(*e2e.MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	}

	srcTableName := "test_blobs"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	quotedSrcFullName := "\"" + strings.ReplaceAll(srcFullName, ".", "\".\"") + "\""
	dstTableName := "test_blobs_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			k TEXT NOT NULL,
			tb tinyblob NOT NULL,
			mb mediumblob NOT NULL,
			lb longblob NOT NULL,
			bi binary(6) NOT NULL,
			vb varbinary(100) NOT NULL,
			tt tinytext NOT NULL,
			mt mediumtext NOT NULL,
			lt longtext NOT NULL,
			ch char(4) NOT NULL,
			vc varchar(100) NOT NULL,
			js json NOT NULL
		)
	`, quotedSrcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (k,tb,mb,lb,bi,vb,tt,mt,lt,ch,vc,js) VALUES
		('init','tinyblob','mediumblob','longblob','binary','varbinary',
		'tinytext','mediumtext','longtext','char','varchar','{"a":2}')`, quotedSrcFullName)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,k,tb,mb,lb,vb,bi,tt,mt,lt,ch,vc,js")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (k,tb,mb,lb,bi,vb,tt,mt,lt,ch,vc,js) VALUES
		('cdc','tinyblob','mediumblob','longblob','binary','varbinary',
		'tinytext','mediumtext','longtext','char','varchar','{"a":2}')`, quotedSrcFullName)))

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,k,tb,mb,lb,bi,vb,tt,mt,lt,ch,vc,js")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Enum() {
	if _, ok := s.source.(*e2e.MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	}

	srcTableName := "test_my_enum"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	quotedSrcFullName := "\"" + strings.ReplaceAll(srcFullName, ".", "\".\"") + "\""
	dstTableName := "test_my_enum_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			"key" TEXT NOT NULL,
			e enum('a','b''s', 'c') NOT NULL,
			s set('a','b','c') NOT NULL
		)
	`, quotedSrcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s ("key",e,s) VALUES
		('init','b''s','a,b'),('init','','')`, quotedSrcFullName)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\",e,s")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s ("key",e,s) VALUES
		('cdc','b''s','a,b'),('cdc','','')`, quotedSrcFullName)))

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,\"key\",e,s")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Vector() {
	if mysource, ok := s.source.(*e2e.MySqlSource); !ok || mysource.Config.Flavor != protos.MySqlFlavor_MYSQL_MYSQL {
		s.t.Skip("only applies to mysql")
	}

	srcTableName := "test_vector"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	quotedSrcFullName := "\"" + strings.ReplaceAll(srcFullName, ".", "\".\"") + "\""
	dstTableName := "test_vector_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, val VECTOR)
	`, quotedSrcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO %s (val) VALUES (to_vector('[1.1,1.0]'))`, quotedSrcFullName)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,val")

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO %s (val) VALUES (to_vector('[2.718, 1.414]'))`, quotedSrcFullName)))

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,val")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Numbers() {
	if mysource, ok := s.source.(*e2e.MySqlSource); !ok || mysource.Config.Flavor != protos.MySqlFlavor_MYSQL_MYSQL {
		s.t.Skip("only applies to mysql")
	}

	srcTableName := "test_float"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	quotedSrcFullName := "\"" + strings.ReplaceAll(srcFullName, ".", "\".\"") + "\""
	dstTableName := "test_float_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, num numeric, num603 numeric(60, 3), f32 float, f64 double precision, r real)
	`, quotedSrcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO %s(num,num603,f32,f64,r)VALUES(1.23,780780780.780,1.41421,2.718281828459045,6.28319)`,
			quotedSrcFullName)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,num,num603,f32,f64,r")

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO%s(num,num603,f32,f64,r)VALUES(1.23,780780780.780,1.41421,2.718281828459045,6.28319)`,
			quotedSrcFullName)))

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,num,num603,f32,f64,r")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Geometric_Types() {
	if _, ok := s.source.(*e2e.MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	}

	srcTableName := "test_mysql_geometric_types"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_mysql_geometric_types"

	// Create a table with a geometry column that can store any geometric type
	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %[1]s(
		id serial PRIMARY KEY,
		geometry_col GEOMETRY
	);

	-- Insert test data with various geometric types
	INSERT INTO %[1]s (geometry_col) VALUES
		(ST_GeomFromText('POINT(1 2)')),
		(ST_GeomFromText('LINESTRING(1 2, 3 4)')),
		(ST_GeomFromText('POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))')),
		(ST_GeomFromText('MULTIPOINT((1 2), (3 4))')),
		(ST_GeomFromText('MULTILINESTRING((1 2, 3 4), (5 6, 7 8))')),
		(ST_GeomFromText('MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)), ((4 4, 6 4, 6 6, 4 6, 4 4)))')),
		(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2), LINESTRING(1 2, 3 4))'));`, srcFullName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_test_mysql_geometric_types"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	// Wait for initial snapshot to complete
	e2e.EnvWaitForCount(env, s, "waiting for initial snapshot count", dstTableName, "id", 7)

	// Insert additional rows to test CDC
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	INSERT INTO %[1]s (geometry_col) VALUES
		(ST_GeomFromText('POINT(10 20)')),
		(ST_GeomFromText('LINESTRING(10 20, 30 40)')),
		(ST_GeomFromText('POLYGON((10 10, 30 10, 30 30, 10 30, 10 10))'));`, srcFullName))
	require.NoError(s.t, err)

	// Wait for CDC to replicate the new rows
	e2e.EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id", 10)

	// Verify that the data was correctly replicated
	rows, err := s.GetRows(dstTableName, "id, geometry_col")
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 10, "expected 10 rows")

	// Expected WKT format values for each geometric type
	expectedValues := []string{
		"POINT(1 2)",
		"LINESTRING(1 2, 3 4)",
		"POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
		"MULTIPOINT((1 2), (3 4))",
		"MULTILINESTRING((1 2, 3 4), (5 6, 7 8))",
		"MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)), ((4 4, 6 4, 6 6, 4 6, 4 4)))",
		"GEOMETRYCOLLECTION(POINT(1 2), LINESTRING(1 2, 3 4))",
		"POINT(10 20)",
		"LINESTRING(10 20, 30 40)",
		"POLYGON((10 10, 30 10, 30 30, 10 30, 10 10))",
	}

	for i, row := range rows.Records {
		require.Len(s.t, row, 2, "expected 2 columns")
		geometryVal := row[1].Value()
		require.Equal(s.t, expectedValues[i], geometryVal, "geometry_col value mismatch at row %d", i+1)
	}

	// Clean up
	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Specific_Geometric_Types() {
	if _, ok := s.source.(*e2e.MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	}

	srcTableName := "test_mysql_s_geometric_types"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_mysql_s_geometric_types"

	// Create a table with a geometry column that can store any geometric type
	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %[1]s(
		id serial PRIMARY KEY,
		point_col POINT,
		linestring_col LINESTRING,
		polygon_col POLYGON,
		multipoint_col MULTIPOINT,
		multilinestring_col MULTILINESTRING,
		multipolygon_col MULTIPOLYGON,
		geometrycollection_col GEOMETRYCOLLECTION
	);

	-- Insert test data with various geometric types
	INSERT INTO %[1]s (
		point_col,
		linestring_col,
		polygon_col,
		multipoint_col,
		multilinestring_col,
		multipolygon_col,
		geometrycollection_col
	) VALUES (
		ST_GeomFromText('POINT(1 2)'),
		ST_GeomFromText('LINESTRING(1 2, 3 4)'),
		ST_GeomFromText('POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))'),
		ST_GeomFromText('MULTIPOINT((1 2), (3 4))'),
		ST_GeomFromText('MULTILINESTRING((1 2, 3 4), (5 6, 7 8))'),
		ST_GeomFromText('MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)), ((4 4, 6 4, 6 6, 4 6, 4 4)))'),
		ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2), LINESTRING(1 2, 3 4))')
 	);`, srcFullName))

	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_test_mysql_geometric_types"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	// Wait for initial snapshot to complete
	e2e.EnvWaitForCount(env, s, "waiting for initial snapshot count", dstTableName, "id", 1)

	// Insert additional rows to test CDC
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
	INSERT INTO %[1]s (
		point_col,
		linestring_col,
		polygon_col,
		multipoint_col,
		multilinestring_col,
		multipolygon_col,
		geometrycollection_col
	) VALUES (
		ST_PointFromText('POINT(10 20)'),
		ST_LineFromText('LINESTRING(10 20, 30 40)'),
		ST_PolygonFromText('POLYGON((10 10, 30 10, 30 30, 10 30, 10 10))'),
		ST_MPointFromText('MULTIPOINT((10 20), (30 40))'),
		ST_MLineFromText('MULTILINESTRING((10 20, 30 40), (50 60, 70 80))'),
		ST_MPolyFromText('MULTIPOLYGON(((10 10, 30 10, 30 30, 10 30, 10 10)), ((40 40, 60 40, 60 60, 40 60, 40 40)))'),
		ST_GeomCollFromText('GEOMETRYCOLLECTION(POINT(10 20), LINESTRING(10 20, 30 40))')
	);`, srcFullName)))

	// Wait for CDC to replicate the new rows
	e2e.EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id", 2)

	// Verify that the data was correctly replicated
	rows, err := s.GetRows(dstTableName, `id, point_col, linestring_col, polygon_col, multipoint_col,
		multilinestring_col, multipolygon_col, geometrycollection_col`)
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 2, "expected 2 rows")

	// Expected WKT format values for each geometric type
	expectedValues := [][]string{
		{
			"POINT(1 2)",
			"LINESTRING(1 2, 3 4)",
			"POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
			"MULTIPOINT((1 2), (3 4))",
			"MULTILINESTRING((1 2, 3 4), (5 6, 7 8))",
			"MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)), ((4 4, 6 4, 6 6, 4 6, 4 4)))",
			"GEOMETRYCOLLECTION(POINT(1 2), LINESTRING(1 2, 3 4))",
		},
		{
			"POINT(10 20)",
			"LINESTRING(10 20, 30 40)",
			"POLYGON((10 10, 30 10, 30 30, 10 30, 10 10))",
			"MULTIPOINT((10 20), (30 40))",
			"MULTILINESTRING((10 20, 30 40), (50 60, 70 80))",
			"MULTIPOLYGON(((10 10, 30 10, 30 30, 10 30, 10 10)), ((40 40, 60 40, 60 60, 40 60, 40 40)))",
			"GEOMETRYCOLLECTION(POINT(10 20), LINESTRING(10 20, 30 40))",
		},
	}

	for i, row := range rows.Records {
		require.Len(s.t, row, 8, "expected 8 columns")
		for j := 1; j < 8; j++ {
			geometryVal := row[j].Value()
			require.Equal(s.t, expectedValues[i][j-1], geometryVal, "geometry value mismatch at row %d column %d", i+1, j)
		}
	}

	// Clean up
	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Schema_Changes() {
	if _, ok := s.source.(*e2e.MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	}

	t := s.T()
	destinationSchemaConnector, ok := s.DestinationConnector().(connectors.GetTableSchemaConnector)
	if !ok {
		t.Skip("skipping test because destination connector does not implement GetTableSchemaConnector")
	}

	srcTable := "test_mysql_schema_changes"
	dstTable := "test_mysql_schema_changes_dst"
	srcTableName := e2e.AttachSchema(s, srcTable)
	dstTableName := s.DestinationTable(dstTable)
	secondSrcTable := "test_mysql_schema_changes_second"
	secondDstTable := "test_mysql_schema_changes_second_dst"
	secondSrcTableName := e2e.AttachSchema(s, secondSrcTable)

	require.NoError(t, s.Source().Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			c1 BIGINT
		);
	`, srcTableName)))
	require.NoError(t, s.Source().Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY
		);
	`, secondSrcTableName)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, srcTable),
		TableMappings: e2e.TableMappings(s, srcTable, dstTable, secondSrcTable, secondDstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert and mutate schema repeatedly.
	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	e2e.EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s(c1) VALUES(1)`, srcTableName)))

	e2e.EnvWaitForEqualTablesWithNames(env, s, "normalize reinsert", srcTable, dstTable, "id,c1")

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier: e2e.ExpectedDestinationTableName(s, dstTable),
		Columns: []*protos.FieldDescription{
			{
				Name:         e2e.ExpectedDestinationIdentifier(s, "id"),
				Type:         string(types.QValueKindNumeric),
				TypeModifier: -1,
			},
			{
				Name:         e2e.ExpectedDestinationIdentifier(s, "c1"),
				Type:         string(types.QValueKindNumeric),
				TypeModifier: -1,
			},
			{
				Name:         "_PEERDB_IS_DELETED",
				Type:         string(types.QValueKindBoolean),
				TypeModifier: -1,
			},
			{
				Name:         "_PEERDB_SYNCED_AT",
				Type:         string(types.QValueKindTimestamp),
				TypeModifier: -1,
			},
		},
	}
	output, err := destinationSchemaConnector.GetTableSchema(t.Context(), nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: dstTableName}})
	e2e.EnvNoError(t, env, err)
	e2e.EnvTrue(t, env, e2e.CompareTableSchemas(expectedTableSchema, output[dstTableName]))

	// alter source table, add column addedColumn and insert another row.
	e2e.EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf("ALTER TABLE %s ADD COLUMN `addedColumn` BIGINT", srcTableName)))
	// so that the batch finishes, insert a row into the second source table.
	e2e.EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s DEFAULT VALUES`, secondSrcTable)))
	e2e.EnvWaitForEqualTablesWithNames(env, s, "normalize altered row", srcTable, dstTable, "id,c1,coalesce(`addedColumn`,0) `addedColumn`")
	expectedTableSchema = &protos.TableSchema{
		TableIdentifier: e2e.ExpectedDestinationTableName(s, dstTable),
		Columns: []*protos.FieldDescription{
			{
				Name:         e2e.ExpectedDestinationIdentifier(s, "id"),
				Type:         string(types.QValueKindNumeric),
				TypeModifier: -1,
			},
			{
				Name:         e2e.ExpectedDestinationIdentifier(s, "c1"),
				Type:         string(types.QValueKindNumeric),
				TypeModifier: -1,
			},
			{
				Name:         "_PEERDB_SYNCED_AT",
				Type:         string(types.QValueKindTimestamp),
				TypeModifier: -1,
			},
			{
				Name:         e2e.ExpectedDestinationIdentifier(s, "addedColumn"),
				Type:         string(types.QValueKindNumeric),
				TypeModifier: -1,
			},
		},
	}
	output, err = destinationSchemaConnector.GetTableSchema(t.Context(), nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: dstTableName}})
	e2e.EnvNoError(t, env, err)
	e2e.EnvTrue(t, env, e2e.CompareTableSchemas(expectedTableSchema, output[dstTableName]))
	e2e.EnvEqualTablesWithNames(env, s, srcTable, dstTable, "id,c1,coalesce(`addedColumn`,0) `addedColumn`")

	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}
