package e2e_snowflake

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

func TestPeerFlowE2ETestSuiteSF(t *testing.T) {
	e2eshared.RunSuite(t, SetupSuite)
}

func (s PeerFlowE2ETestSuiteSF) attachSchemaSuffix(tableName string) string {
	return e2e.AttachSchema(s, tableName)
}

func (s PeerFlowE2ETestSuiteSF) attachSuffix(input string) string {
	return e2e.AddSuffix(s, input)
}

func (s PeerFlowE2ETestSuiteSF) Test_Flow_ReplicaIdentity_Index_No_Pkey() {
	tc := e2e.NewTemporalClient(s.t)

	tableName := "test_replica_identity_no_pkey"
	srcTableName := s.attachSchemaSuffix(tableName)
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_replica_identity_no_pkey")

	// Create a table without a primary key and create a named unique index
	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
		CREATE UNIQUE INDEX unique_idx_on_id_key ON %s (id, key);
		ALTER TABLE %s REPLICA IDENTITY USING INDEX unique_idx_on_id_key;
	`, srcTableName, srcTableName, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(tableName),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 100

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert 20 rows into the source table
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	// insert 20 rows into the source table
	for i := range 20 {
		testKey := fmt.Sprintf("test_key_%d", i)
		testValue := fmt.Sprintf("test_value_%d", i)
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s (id, key, value) VALUES ($1, $2, $3)
	`, srcTableName), i, testKey, testValue)
		e2e.EnvNoError(s.t, env, err)
	}
	s.t.Log("Inserted 20 rows into the source table")

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize insert", func() bool {
		count, err := s.sfHelper.CountRows("test_replica_identity_no_pkey")
		return err == nil && count == 20
	})
	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteSF) Test_Invalid_Numeric() {
	tableName := "test_invalid_numeric"
	srcTableName := s.attachSchemaSuffix(tableName)
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tableName)

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY,
			num1 NUMERIC(100, 50) NOT NULL,
			num2 NUMERIC(100, 50) NOT NULL
		);
	`, srcTableName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(context.Background(),
		fmt.Sprintf("INSERT INTO %s (id, num1, num2) VALUES (1,$1,$2)", srcTableName),
		"999999999999999999999999999999999999999",
		"9999999999999999")
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(tableName),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize shapes", func() bool {
		records, err := s.sfHelper.ExecuteAndProcessQuery("select num1, num2 from " + dstTableName + " where id = 1")
		if err != nil || len(records.Records) == 0 {
			return false
		}
		return records.Records[0][0].Value() == nil && records.Records[0][1].Value() != nil
	})

	// Fewer 9s this time are still invalid, with precision 2 `1` is 3 digits: `1.00`
	_, err = s.Conn().Exec(context.Background(),
		fmt.Sprintf("INSERT INTO %s (id, num1, num2) VALUES (2,$1,$2)", srcTableName),
		"9999999999999999999999999999999999",
		"9999999999999999")
	e2e.EnvNoError(s.t, env, err)

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize shapes", func() bool {
		records, err := s.sfHelper.ExecuteAndProcessQuery("select num1, num2 from " + dstTableName + " where id = 2")
		if err != nil || len(records.Records) == 0 {
			return false
		}
		return records.Records[0][0].Value() == nil && records.Records[0][1].Value() != nil
	})

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteSF) Test_Invalid_Geo_SF_Avro_CDC() {
	tableName := "test_invalid_geo_sf_avro_cdc"
	srcTableName := s.attachSchemaSuffix(tableName)
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tableName)

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			line GEOMETRY(LINESTRING) NOT NULL,
			poly GEOGRAPHY(POLYGON) NOT NULL
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(tableName),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert 10 rows into the source table
	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	// insert 4 invalid shapes and 6 valid shapes into the source table
	for range 4 {
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s (line,poly) VALUES ($1,$2)
		`, srcTableName), "010200000001000000000000000000F03F0000000000000040",
			"0103000020e6100000010000000c0000001a8361d35dc64140afdb8d2b1bc3c9bf1b8ed4685fc641405ba64c"+
				"579dc2c9bf6a6ad95a5fc64140cd82767449c2c9bf9570fbf85ec641408a07944db9c2c9bf729a18a55ec6414021b8b748c7c2c9bfba46de4c"+
				"5fc64140f2567052abc2c9bf2df9c5925fc641409394e16573c2c9bf2df9c5925fc6414049eceda9afc1c9bfdd1cc1a05fc64140fe43faedebc0"+
				"c9bf4694f6065fc64140fe43faedebc0c9bfffe7305f5ec641406693d6f2ddc0c9bf1a8361d35dc64140afdb8d2b1bc3c9bf",
		)
		e2e.EnvNoError(s.t, env, err)
	}
	s.t.Log("Inserted 4 invalid geography rows into the source table")
	for range 6 {
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s (line,poly) VALUES ($1,$2)
		`, srcTableName), "SRID=5678;010200000002000000000000000000F03F000000000000004000000000000008400000000000001040",
			"010300000001000000050000000000000000000000000000000000000000000000"+
				"00000000000000000000f03f000000000000f03f000000000000f03f0000000000"+
				"00f03f000000000000000000000000000000000000000000000000")
		e2e.EnvNoError(s.t, env, err)
	}
	s.t.Log("Inserted 6 valid geography rows and 10 total rows into source")

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize shapes", func() bool {
		// We inserted 4 invalid shapes in each,
		// which should be filtered out as null on destination.
		lineCount, err := s.sfHelper.CountNonNullRows(tableName, "line")
		if err != nil {
			return false
		}

		// Make sure SRIDs are set
		sridCount, err := s.sfHelper.CountSRIDs(tableName, "line")
		if err != nil {
			s.t.Log(err)
			return false
		}

		polyCount, err := s.sfHelper.CountNonNullRows(tableName, "poly")
		if err != nil {
			return false
		}

		if lineCount != 6 || polyCount != 6 {
			s.t.Logf("wrong counts, expect 6 lines 6 polies, not %d lines %d polies", lineCount, polyCount)
			return false
		}

		if sridCount != 6 {
			s.t.Logf("there are some srids that are 0, expected 6 non-zero srids, got %d non-zero srids", sridCount)
			return false
		}

		return true
	})

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteSF) Test_Toast_SF() {
	tc := e2e.NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_toast_sf_1")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_toast_sf_1")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			t1 text,
			t2 text,
			k int
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_sf_1"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 100

	// wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	/*
		Executing a transaction which
		1. changes both toast column
		2. changes no toast column
		2. changes 1 toast column
	*/
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		BEGIN;
		INSERT INTO %s (t1,t2,k) SELECT random_string(9000),random_string(9000),
		1 FROM generate_series(1,2);
		UPDATE %s SET k=102 WHERE id=1;
		UPDATE %s SET t1='dummy' WHERE id=2;
		END;
	`, srcTableName, srcTableName, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Executed a transaction touching toast columns")
	e2e.EnvWaitForEqualTables(env, s, "normalizing tx", "test_toast_sf_1", `id,t1,t2,k`)
	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteSF) Test_Toast_Advance_1_SF() {
	tc := e2e.NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_toast_sf_3")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_toast_sf_3")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			t1 text,
			t2 text,
			k int
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_sf_3"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 100

	// wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	// complex transaction with random DMLs on a table with toast columns
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		BEGIN;
		INSERT INTO %s (t1,t2,k) SELECT random_string(9000),random_string(9000),
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
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Executed a transaction touching toast columns")
	e2e.EnvWaitForEqualTables(env, s, "normalizing tx", "test_toast_sf_3", `id,t1,t2,k`)
	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteSF) Test_Toast_Advance_2_SF() {
	tc := e2e.NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_toast_sf_4")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_toast_sf_4")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			t1 text,
			k int
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_sf_4"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 100

	// wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	// complex transaction with random DMLs on a table with toast columns
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		BEGIN;
		INSERT INTO %s (t1,k) SELECT random_string(9000),
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
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Executed a transaction touching toast columns")
	e2e.EnvWaitForEqualTables(env, s, "normalizing tx", "test_toast_sf_4", `id,t1,k`)
	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteSF) Test_Toast_Advance_3_SF() {
	tc := e2e.NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_toast_sf_5")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_toast_sf_5")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			t1 text,
			t2 text,
			k int
	);
`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_toast_sf_5"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 100

	// wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	/*
		transaction updating a single row
		multiple times with changed/unchanged toast columns
	*/
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		BEGIN;
		INSERT INTO %s (t1,t2,k) SELECT random_string(9000),random_string(9000),
		1 FROM generate_series(1,1);
		UPDATE %s SET k=102 WHERE id=1;
		UPDATE %s SET t1='dummy' WHERE id=1;
		UPDATE %s SET t2='dummy' WHERE id=1;
		END;
	`, srcTableName, srcTableName, srcTableName, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Executed a transaction touching toast columns")

	e2e.EnvWaitForEqualTables(env, s, "normalizing tx", "test_toast_sf_5", `id,t1,t2,k`)
	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteSF) Test_Types_SF() {
	tc := e2e.NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_types_sf")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_types_sf")
	createMoodEnum := "CREATE TYPE mood AS ENUM ('happy', 'sad', 'angry');"
	_, enumErr := s.Conn().Exec(context.Background(), createMoodEnum)
	if enumErr != nil &&
		!shared.IsSQLStateError(enumErr, pgerrcode.DuplicateObject, pgerrcode.UniqueViolation) {
		require.NoError(s.t, enumErr)
	}
	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (id serial PRIMARY KEY,c1 BIGINT,c2 BIT,c3 VARBIT,c4 BOOLEAN,
		c6 BYTEA,c7 CHARACTER,c8 varchar,c9 CIDR,c11 DATE,c12 FLOAT,c13 DOUBLE PRECISION,
		c14 INET,c15 INTEGER,c16 INTERVAL,c17 JSON,c18 JSONB,c21 MACADDR,c22 MONEY,
		c23 NUMERIC(16,5),c24 OID,c28 REAL,c29 SMALLINT,c30 SMALLSERIAL,c31 SERIAL,c32 TEXT,
		c33 TIMESTAMP,c34 TIMESTAMPTZ,c35 TIME, c36 TIMETZ,c37 TSQUERY,c38 TSVECTOR,
		c39 TXID_SNAPSHOT,c40 UUID,c41 XML, c42 GEOMETRY(POINT), c43 GEOGRAPHY(POINT),
		c44 GEOGRAPHY(POLYGON), c45 GEOGRAPHY(LINESTRING), c46 GEOMETRY(LINESTRING), c47 GEOMETRY(POLYGON),
		c48 mood, c49 HSTORE, c50 DATE[], c51 TIMESTAMPTZ[], c52 TIMESTAMP[], c53 BOOLEAN[],c54 SMALLINT[]);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_types_sf"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 100

	// wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	/* test inserting various types*/
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
	INSERT INTO %s SELECT 2,2,b'1',b'101',
	true,random_bytea(32),'s','test','1.1.10.2'::cidr,
	CURRENT_DATE,1.23,1.234,'192.168.1.5'::inet,1,
	'5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds'::interval,
	'{"sai":1}'::json,'{"sai":1}'::jsonb,'08:00:2b:01:02:03'::macaddr,
	1.2,100.24553,4::oid,1.23,1,1,1,'test',now(),now(),now()::time,now()::timetz,
	'fat & rat'::tsquery,'a fat cat sat on a mat and ate a fat rat'::tsvector,
	txid_current_snapshot(),
	'66073c38-b8df-4bdb-bbca-1c97596b8940'::uuid,xmlcomment('hello'),
	'POINT(1 2)','POINT(40.7128 -74.0060)','POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))',
	'LINESTRING(-74.0060 40.7128, -73.9352 40.7306, -73.9123 40.7831)','LINESTRING(0 0, 1 1, 2 2)',
	'POLYGON((-74.0060 40.7128, -73.9352 40.7306, -73.9123 40.7831, -74.0060 40.7128))', 'happy','"a"=>"a\"quote\"", "b"=>NULL',
	'{2020-01-01, 2020-01-02}'::date[],
	'{"2020-01-01 01:01:01+00", "2020-01-02 01:01:01+00"}'::timestamptz[],
	'{"2020-01-01 01:01:01", "2020-01-02 01:01:01"}'::timestamp[],
	'{true, false}'::boolean[],
	'{1,2}'::smallint[];
	`, srcTableName))
	e2e.EnvNoError(s.t, env, err)

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize types", func() bool {
		noNulls, err := s.sfHelper.CheckNull("test_types_sf", []string{
			"c41", "c1", "c2", "c3", "c4", "c6", "c39", "c40", "id", "c9", "c11", "c12", "c13",
			"c14", "c15", "c16", "c17", "c18", "c21", "c22", "c23", "c24", "c28", "c29", "c30",
			"c31", "c33", "c34", "c35", "c36", "c37", "c38", "c7", "c8", "c32", "c42", "c43",
			"c44", "c45", "c46", "c47", "c48", "c49", "c50", "c51", "c52", "c53", "c54",
		})
		if err != nil {
			return false
		}

		// interval checks
		if err := s.checkJSONValue(dstTableName, "c16", "years", "5"); err != nil {
			return false
		}

		if err := s.checkJSONValue(dstTableName, "c16", "months", "2"); err != nil {
			return false
		}

		if err := s.checkJSONValue(dstTableName, "c16", "days", "29"); err != nil {
			return false
		}

		// check if JSON on snowflake side is a good JSON
		if err := s.checkJSONValue(dstTableName, "c17", "sai", "1"); err != nil {
			return false
		}

		// check if HSTORE on snowflake is a good JSON
		if err := s.checkJSONValue(dstTableName, "c49", "a", `"a\"quote\""`); err != nil {
			return false
		}

		if err := s.checkJSONValue(dstTableName, "c49", "b", "null"); err != nil {
			return false
		}

		return noNulls
	})

	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteSF) Test_Multi_Table_SF() {
	tc := e2e.NewTemporalClient(s.t)

	srcTable1Name := s.attachSchemaSuffix("test1_sf")
	srcTable2Name := s.attachSchemaSuffix("test2_sf")
	dstTable1Name := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test1_sf")
	dstTable2Name := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test2_sf")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (id serial primary key, c1 int, c2 text);
	CREATE TABLE IF NOT EXISTS %s (id serial primary key, c1 int, c2 text);
	`, srcTable1Name, srcTable2Name))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_multi_table"),
		TableNameMapping: map[string]string{srcTable1Name: dstTable1Name, srcTable2Name: dstTable2Name},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 100

	// wait for PeerFlowStatusQuery to finish setup
	// and execute a transaction touching toast columns
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	/* inserting across multiple tables*/
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
	INSERT INTO %s (c1,c2) VALUES (1,'dummy_1');
	INSERT INTO %s (c1,c2) VALUES (-1,'dummy_-1');
	`, srcTable1Name, srcTable2Name))
	e2e.EnvNoError(s.t, env, err)

	e2e.EnvWaitFor(s.t, env, 2*time.Minute, "normalize both tables", func() bool {
		count1, err := s.sfHelper.CountRows("test1_sf")
		if err != nil {
			return false
		}
		count2, err := s.sfHelper.CountRows("test2_sf")
		if err != nil {
			return false
		}

		return count1 == 1 && count2 == 1
	})

	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteSF) Test_Composite_PKey_SF() {
	tc := e2e.NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_simple_cpkey")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_simple_cpkey")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
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
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 100

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	// insert 10 rows into the source table
	for i := range 10 {
		testValue := fmt.Sprintf("test_value_%d", i)
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c2,t) VALUES ($1,$2)
		`, srcTableName), i, testValue)
		e2e.EnvNoError(s.t, env, err)
	}
	s.t.Log("Inserted 10 rows into the source table")

	e2e.EnvWaitForEqualTables(env, s, "normalize table", "test_simple_cpkey", "id,c1,c2,t")

	_, err = s.Conn().Exec(context.Background(),
		fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=$1`, srcTableName), 1)
	e2e.EnvNoError(s.t, env, err)
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=$1`, srcTableName), 0)
	e2e.EnvNoError(s.t, env, err)
	e2e.EnvWaitForEqualTables(env, s, "normalize update/delete", "test_simple_cpkey", "id,c1,c2,t")

	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteSF) Test_Composite_PKey_Toast_1_SF() {
	tc := e2e.NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_cpkey_toast1")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_cpkey_toast1")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
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
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 100
	flowConnConfig.SoftDeleteColName = ""
	flowConnConfig.SyncedAtColName = ""

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	rowsTx, err := s.Conn().Begin(context.Background())
	e2e.EnvNoError(s.t, env, err)

	// insert 10 rows into the source table
	for i := range 10 {
		testValue := fmt.Sprintf("test_value_%d", i)
		_, err = rowsTx.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c2,t,t2) VALUES ($1,$2,random_string(9000))
		`, srcTableName), i, testValue)
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

	e2e.EnvWaitForEqualTables(env, s, "normalizing tx", "test_cpkey_toast1", "id,c1,c2,t,t2")
	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteSF) Test_Composite_PKey_Toast_2_SF() {
	tc := e2e.NewTemporalClient(s.t)

	tableName := "test_cpkey_toast2"
	srcTableName := s.attachSchemaSuffix(tableName)
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tableName)

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
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
		FlowJobName:      s.attachSuffix(tableName),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 100

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	// insert 10 rows into the source table
	for i := range 10 {
		testValue := fmt.Sprintf("test_value_%d", i)
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c2,t,t2) VALUES ($1,$2,random_string(9000))
		`, srcTableName), i, testValue)
		e2e.EnvNoError(s.t, env, err)
	}
	s.t.Log("Inserted 10 rows into the source table")

	e2e.EnvWaitForEqualTables(env, s, "normalize table", tableName, "id,c2,t,t2")
	_, err = s.Conn().Exec(context.Background(),
		fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=$1`, srcTableName), 1)
	e2e.EnvNoError(s.t, env, err)
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=$1`, srcTableName), 0)
	e2e.EnvNoError(s.t, env, err)
	e2e.EnvWaitForEqualTables(env, s, "normalize update/delete", tableName, "id,c2,t,t2")

	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteSF) Test_Column_Exclusion() {
	tc := e2e.NewTemporalClient(s.t)

	tableName := "test_exclude_sf"
	srcTableName := s.attachSchemaSuffix(tableName)
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tableName)

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
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

	config := &protos.FlowConnectionConfigs{
		FlowJobName:     s.attachSuffix(tableName),
		DestinationName: s.Peer().Name,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: dstTableName,
				Exclude:                    []string{"c2"},
			},
		},
		SourceName:      e2e.GeneratePostgresPeer(s.t).Name,
		SyncedAtColName: "_PEERDB_SYNCED_AT",
		MaxBatchSize:    100,
	}

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, config, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, config)

	// insert 10 rows into the source table
	for i := range 10 {
		testValue := fmt.Sprintf("test_value_%d", i)
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c2,t,t2) VALUES ($1,$2,random_string(100))
		`, srcTableName), i, testValue)
		e2e.EnvNoError(s.t, env, err)
	}
	s.t.Log("Inserted 10 rows into the source table")

	e2e.EnvWaitForEqualTables(env, s, "normalize table", tableName, "id,c1,t,t2")
	_, err = s.Conn().Exec(context.Background(),
		fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=1`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=0`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	e2e.EnvWaitForEqualTables(env, s, "normalize update/delete", tableName, "id,c1,t,t2")

	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)

	sfRows, err := s.GetRows(tableName, "*")
	require.NoError(s.t, err)

	for _, field := range sfRows.Schema.Fields {
		require.NotEqual(s.t, "c2", field.Name)
	}
	require.Len(s.t, sfRows.Schema.Fields, 5)
}

func (s PeerFlowE2ETestSuiteSF) Test_Soft_Delete_Basic() {
	tc := e2e.NewTemporalClient(s.t)

	tableName := "test_softdel_src"
	dstName := "test_softdel"
	srcTableName := s.attachSchemaSuffix(tableName)
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, dstName)

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			c1 INT,
			c2 INT,
			t TEXT
		);
	`, srcTableName))
	require.NoError(s.t, err)

	config := &protos.FlowConnectionConfigs{
		FlowJobName:     s.attachSuffix(dstName),
		DestinationName: s.Peer().Name,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: dstTableName,
			},
		},
		SourceName:        e2e.GeneratePostgresPeer(s.t).Name,
		SoftDeleteColName: "_PEERDB_IS_DELETED",
		SyncedAtColName:   "_PEERDB_SYNCED_AT",
		MaxBatchSize:      100,
	}

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, config, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, config)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c1,c2,t) VALUES (1,2,random_string(9000))`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "normalize row", tableName, dstName, "id,c1,c2,t")
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			UPDATE %s SET c1=c1+4 WHERE id=1`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "normalize update", tableName, dstName, "id,c1,c2,t")
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	e2e.EnvWaitForEqualTablesWithNames(
		env,
		s,
		"normalize delete",
		tableName,
		dstName+" WHERE NOT _PEERDB_IS_DELETED",
		"id,c1,c2,t",
	)

	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)

	newerSyncedAtQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s WHERE _PEERDB_IS_DELETED`, dstTableName)
	numNewRows, err := s.sfHelper.RunIntQuery(newerSyncedAtQuery)
	require.NoError(s.t, err)
	require.Equal(s.t, 1, numNewRows)
}

func (s PeerFlowE2ETestSuiteSF) Test_Soft_Delete_IUD_Same_Batch() {
	tc := e2e.NewTemporalClient(s.t)

	cmpTableName := s.attachSchemaSuffix("test_softdel_iud")
	srcTableName := cmpTableName + "_src"
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "test_softdel_iud")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			c1 INT,
			c2 INT,
			t TEXT
		);
	`, srcTableName))
	require.NoError(s.t, err)

	config := &protos.FlowConnectionConfigs{
		FlowJobName:     s.attachSuffix("test_softdel_iud"),
		DestinationName: s.Peer().Name,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: dstTableName,
			},
		},
		SourceName:        e2e.GeneratePostgresPeer(s.t).Name,
		SoftDeleteColName: "_PEERDB_IS_DELETED",
		SyncedAtColName:   "_PEERDB_SYNCED_AT",
		MaxBatchSize:      100,
	}

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, config, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, config)

	insertTx, err := s.Conn().Begin(context.Background())
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

	e2e.EnvWaitForEqualTables(env, s, "normalizing tx", "test_softdel_iud", "id,c1,c2,t")
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "checking soft delete", func() bool {
		newerSyncedAtQuery := fmt.Sprintf(`
				SELECT COUNT(*) FROM %s WHERE _PEERDB_IS_DELETED`, dstTableName)
		numNewRows, err := s.sfHelper.RunIntQuery(newerSyncedAtQuery)
		e2e.EnvNoError(s.t, env, err)
		return numNewRows == 1
	})

	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteSF) Test_Soft_Delete_UD_Same_Batch() {
	tc := e2e.NewTemporalClient(s.t)

	tableName := "test_softdel_ud_src"
	dstName := "test_softdel_ud"
	srcTableName := s.attachSchemaSuffix(tableName)
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, dstName)

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			c1 INT,
			c2 INT,
			t TEXT
		);
	`, srcTableName))
	require.NoError(s.t, err)

	config := &protos.FlowConnectionConfigs{
		FlowJobName:     s.attachSuffix(dstName),
		DestinationName: s.Peer().Name,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: dstTableName,
			},
		},
		SourceName:        e2e.GeneratePostgresPeer(s.t).Name,
		SoftDeleteColName: "_PEERDB_IS_DELETED",
		SyncedAtColName:   "_PEERDB_SYNCED_AT",
		MaxBatchSize:      100,
	}

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, config, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, config)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c1,c2,t) VALUES (1,2,random_string(9000))`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "normalize insert", tableName, dstName, "id,c1,c2,t")

	insertTx, err := s.Conn().Begin(context.Background())
	e2e.EnvNoError(s.t, env, err)
	_, err = insertTx.Exec(context.Background(), fmt.Sprintf(`
			UPDATE %s SET t=random_string(10000) WHERE id=1`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	_, err = insertTx.Exec(context.Background(), fmt.Sprintf(`
			UPDATE %s SET c1=c1+4 WHERE id=1`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	_, err = insertTx.Exec(context.Background(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1`, srcTableName))
	e2e.EnvNoError(s.t, env, err)

	e2e.EnvNoError(s.t, env, insertTx.Commit(context.Background()))
	e2e.EnvWaitForEqualTablesWithNames(
		env,
		s,
		"normalize transaction",
		tableName,
		dstName+" WHERE NOT _PEERDB_IS_DELETED",
		"id,c1,c2,t",
	)
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "checking soft delete", func() bool {
		newerSyncedAtQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE _PEERDB_IS_DELETED", dstTableName)
		numNewRows, err := s.sfHelper.RunIntQuery(newerSyncedAtQuery)
		e2e.EnvNoError(s.t, env, err)
		return numNewRows == 1
	})

	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteSF) Test_Soft_Delete_Insert_After_Delete() {
	tc := e2e.NewTemporalClient(s.t)

	tableName := "test_softdel_iad"
	srcTableName := s.attachSchemaSuffix(tableName)
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tableName)

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
			c1 INT,
			c2 INT,
			t TEXT
		);
	`, srcTableName))
	require.NoError(s.t, err)

	config := &protos.FlowConnectionConfigs{
		FlowJobName:     s.attachSuffix(tableName),
		DestinationName: s.Peer().Name,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: dstTableName,
			},
		},
		SourceName:        e2e.GeneratePostgresPeer(s.t).Name,
		SoftDeleteColName: "_PEERDB_IS_DELETED",
		SyncedAtColName:   "_PEERDB_SYNCED_AT",
		MaxBatchSize:      100,
	}

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert and delete rows in the table.
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, config, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, config)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c1,c2,t) VALUES (1,2,random_string(9000))`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	e2e.EnvWaitForEqualTables(env, s, "normalize row", tableName, "id,c1,c2,t")
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	e2e.EnvWaitForEqualTablesWithNames(
		env,
		s,
		"normalize delete",
		tableName,
		tableName+" WHERE NOT _PEERDB_IS_DELETED",
		"id,c1,c2,t",
	)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(id,c1,c2,t) VALUES (1,3,4,random_string(10000))`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	e2e.EnvWaitForEqualTables(env, s, "normalize reinsert", tableName, "id,c1,c2,t")

	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)

	newerSyncedAtQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s WHERE _PEERDB_IS_DELETED`, dstTableName)
	numNewRows, err := s.sfHelper.RunIntQuery(newerSyncedAtQuery)
	require.NoError(s.t, err)
	require.Equal(s.t, 0, numNewRows)
}

func (s PeerFlowE2ETestSuiteSF) Test_Supported_Mixed_Case_Table_SF() {
	tc := e2e.NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("testMixedCase")
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, "testMixedCase")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS e2e_test_%s."%s" (
			"pulseArmor" SERIAL PRIMARY KEY,
			"highGold" TEXT NOT NULL,
			"eVe" TEXT NOT NULL,
			id SERIAL
		);
	`, s.pgSuffix, "testMixedCase"))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_mixed_case"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 100

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert 20 rows into the source table
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	// insert 20 rows into the source table
	for i := range 20 {
		testKey := fmt.Sprintf("test_key_%d", i)
		testValue := fmt.Sprintf("test_value_%d", i)
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO e2e_test_%s."%s"("highGold","eVe") VALUES ($1, $2)
		`, s.pgSuffix, "testMixedCase"), testKey, testValue)
		e2e.EnvNoError(s.t, env, err)
	}
	s.t.Log("Inserted 20 rows into the source table")
	e2e.EnvWaitForEqualTablesWithNames(
		env,
		s,
		"normalize mixed case",
		"testMixedCase",
		"\"testMixedCase\"",
		"id,\"pulseArmor\",\"highGold\",\"eVe\"",
	)

	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuiteSF) Test_Column_Exclusion_With_Schema_Changes() {
	tc := e2e.NewTemporalClient(s.t)

	tableName := "test_exclude_schema_changes_sf"
	srcTableName := s.attachSchemaSuffix(tableName)
	dstTableName := fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, tableName)

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT GENERATED ALWAYS AS IDENTITY,
			c1 INT GENERATED BY DEFAULT AS IDENTITY,
			c2 INT,
			t TEXT,
			PRIMARY KEY(id,t)
		);
	`, srcTableName))
	require.NoError(s.t, err)

	config := &protos.FlowConnectionConfigs{
		FlowJobName:     s.attachSuffix(tableName),
		DestinationName: s.Peer().Name,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: dstTableName,
				Exclude:                    []string{"c2"},
			},
		},
		SourceName:   e2e.GeneratePostgresPeer(s.t).Name,
		MaxBatchSize: 100,
	}

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, config, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, config)

	// insert 10 rows into the source table
	for i := range 10 {
		testValue := fmt.Sprintf("test_value_%d", i)
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c2,t) VALUES ($1,$2)
		`, srcTableName), i, testValue)
		e2e.EnvNoError(s.t, env, err)
	}
	s.t.Log("Inserted 10 rows into the source table")

	e2e.EnvWaitForEqualTables(env, s, "normalize table", tableName, "id,c1,t")
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf("ALTER TABLE %s ADD COLUMN t2 TEXT", srcTableName))
	e2e.EnvNoError(s.t, env, err)
	// insert 10 more rows into the source table
	for i := range 10 {
		testValue := fmt.Sprintf("test_value_%d", i)
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
				INSERT INTO %s(c2,t,t2) VALUES ($1,$2,random_string(100))
			`, srcTableName), i, testValue)
		e2e.EnvNoError(s.t, env, err)
	}
	_, err = s.Conn().Exec(context.Background(),
		fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=1`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=0`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	e2e.EnvWaitForEqualTables(env, s, "normalize update/delete", tableName, "id,c1,t,t2")

	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)

	sfRows, err := s.GetRows(tableName, "*")
	require.NoError(s.t, err)

	for _, field := range sfRows.Schema.Fields {
		require.NotEqual(s.t, "c2", field.Name)
	}
	require.Len(s.t, sfRows.Schema.Fields, 4)
}
