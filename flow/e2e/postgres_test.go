package e2e

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
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
	err := s.Conn().QueryRow(s.t.Context(), query).Scan(&isDeleted, &syncedAt)
	if err != nil {
		return fmt.Errorf("failed to query row: %w", err)
	}

	if !isDeleted.Bool {
		return errors.New("isDeleted is not true")
	}

	if !syncedAt.Valid {
		return errors.New("syncedAt is not valid")
	}

	return nil
}

func (s PeerFlowE2ETestSuitePG) Test_Geospatial_PG() {
	srcTableName := s.attachSchemaSuffix("test_geospatial_pg")
	dstTableName := s.attachSchemaSuffix("test_geospatial_pg_dst")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			gg geography NOT NULL,
			gm geometry NOT NULL
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_geo_flow_pg"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.MaxBatchSize = 100

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)

	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	// insert 1 row into the source table
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s(gg, gm) VALUES ('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))','LINESTRING(0 0, 1 1, 2 2)')
		`, srcTableName))
	EnvNoError(s.t, env, err)

	s.t.Log("Inserted 1 row into the source table")
	EnvWaitFor(s.t, env, 3*time.Minute, "normalize shapes", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,gg,gm") == nil
	})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_Types() {
	tc := NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_types_pg")
	dstTableName := s.attachSchemaSuffix("test_types_pg_dst")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (id serial PRIMARY KEY,c1 BIGINT,c2 BYTEA,c4 BOOLEAN,
		c7 CHARACTER,c8 varchar,c9 CIDR,c11 DATE,c12 FLOAT,c13 DOUBLE PRECISION,
		c14 INET,c15 INTEGER,c21 MACADDR,
		c29 SMALLINT,c32 TEXT,
		c33 TIMESTAMP,c34 TIMESTAMPTZ,c35 TIME, c36 TIMETZ, c37 TIMETZ,
		c40 UUID, c42 INT[], c43 FLOAT[], c44 TEXT[], c45 UUID[],
		c46 DATE[], c47 TIMESTAMPTZ[], c48 TIMESTAMP[], c49 BOOLEAN[], c50 SMALLINT[]);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_types_pg"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.MaxBatchSize = 100
	flowConnConfig.SoftDeleteColName = ""
	flowConnConfig.SyncedAtColName = ""

	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
			INSERT INTO %s SELECT 2,2,'\xdeadbeef',
			true,'s','test','1.1.10.2'::cidr,
			CURRENT_DATE,1.23,1.234,'192.168.1.5'::inet,1,
			'08:00:2b:01:02:03'::macaddr,
			1,'test',now(),now(),now()::time,'09:25:00+03'::timetz,'21:00:00 +00:00'::timetz,
			'66073c38-b8df-4bdb-bbca-1c97596b8940'::uuid,
			ARRAY[10299301,2579827],
			ARRAY[0.0003, 8902.0092],
			ARRAY['hello','bye'],
			ARRAY['66073c38-b8df-4bdb-bbca-1c97596b8940','cd76be3e-d20a-451b-8e60-015872d7f607']::uuid[],
			'{2020-01-01, 2020-01-02}'::date[],
			'{"2020-01-01 01:01:01+00", "2020-01-02 01:01:01+00"}'::timestamptz[],
			'{"2020-01-01 01:01:01", "2020-01-02 01:01:01"}'::timestamp[],
			'{true, false}'::boolean[],
			'{1,2}'::smallint[];
			`, srcTableName))
	EnvNoError(s.t, env, err)

	s.t.Log("Inserted 1 row into the source table")
	allCols := strings.Join([]string{
		"c1", "c2", "c4",
		"c40", "id", "c9", "c11", "c12", "c13", "c14", "c15",
		"c21", "c29", "c33", "c34", "c35", "c37",
		"c7", "c8", "c32", "c42", "c43", "c44", "c45", "c46", "c47", "c48", "c49", "c50",
	}, ",")
	EnvWaitFor(s.t, env, 3*time.Minute, "normalize types", func() bool {
		err := s.comparePGTables(srcTableName, dstTableName, allCols)
		if err != nil {
			s.t.Log("mismatch", err)
		}
		return err == nil
	})
	// c36 converted to UTC, losing tz info, so does not compare equal
	var c36 string
	require.NoError(s.t, s.Conn().QueryRow(s.t.Context(), "select c36 from "+dstTableName).Scan(&c36))
	require.Equal(s.t, "06:25:00+00", c36)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_PgVector() {
	srcTableName := "pg_pgvector"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "pg_pgvector_dst"

	require.NoError(s.t, s.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, v1 vector, hv halfvec, sv sparsevec)`, srcFullName)))
	require.NoError(s.t, s.Exec(s.t.Context(),
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

	require.NoError(s.t, s.Exec(s.t.Context(),
		fmt.Sprintf(`insert into %s (v1,hv,sv) values ('[1.5,2,3.5]','[1,2,3.5]','{2:2.5,3:3.5}/5')`, srcFullName)))
	EnvWaitForEqualTablesWithNames(env, s, "check comparable types 2", srcTableName, dstTableName, "id,v1,hv,sv")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_Enums() {
	tc := NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_enum_flow")
	dstTableName := s.attachSchemaSuffix("test_enum_flow_dst")
	createMoodEnum := "CREATE TYPE mood AS ENUM ('happy', 'sad', 'angry');"
	_, enumErr := s.Conn().Exec(s.t.Context(), createMoodEnum)
	if enumErr != nil &&
		!shared.IsSQLStateError(enumErr, pgerrcode.DuplicateObject, pgerrcode.UniqueViolation) {
		require.NoError(s.t, enumErr)
	}
	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			my_mood mood,
			my_null_mood mood,
			moods mood[]
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_enum_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.MaxBatchSize = 100

	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	_, err = s.Conn().Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO %s(my_mood, my_null_mood, moods) VALUES ('happy',null,'{happy,angry}')`, srcTableName))
	EnvNoError(s.t, env, err)
	s.t.Log("Inserted enums into the source table")
	EnvWaitFor(s.t, env, 3*time.Minute, "normalize enum", func() bool {
		return s.checkEnums(srcTableName, dstTableName) == nil
	})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_Composite_PKey_PG() {
	tc := NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_simple_cpkey")
	dstTableName := s.attachSchemaSuffix("test_simple_cpkey_dst")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT GENERATED ALWAYS AS IDENTITY,
			c1 INT GENERATED BY DEFAULT AS IDENTITY,
			c2 INT,
			t TEXT,
			PRIMARY KEY(id,t)
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_cpkey_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.MaxBatchSize = 100

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	// insert 10 rows into the source table
	for i := range 10 {
		testValue := fmt.Sprintf("test_value_%d", i)
		_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
			INSERT INTO %s(c2,t) VALUES ($1,$2)
		`, srcTableName), i, testValue)
		EnvNoError(s.t, env, err)
	}
	s.t.Log("Inserted 10 rows into the source table")

	EnvWaitFor(s.t, env, 3*time.Minute, "normalize 10 rows", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t") == nil
	})

	_, err = s.Conn().Exec(s.t.Context(),
		fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=$1`, srcTableName), 1)
	EnvNoError(s.t, env, err)
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=$1`, srcTableName), 0)
	EnvNoError(s.t, env, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "normalize modifications", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t") == nil
	})
	env.Cancel(s.t.Context())

	RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_Composite_PKey_Toast_1_PG() {
	tc := NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_cpkey_toast1")
	dstTableName := s.attachSchemaSuffix("test_cpkey_toast1_dst")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT GENERATED ALWAYS AS IDENTITY,
			c1 INT GENERATED BY DEFAULT AS IDENTITY,
			c2 INT,
			t TEXT,
			t2 TEXT,
			PRIMARY KEY(id,t)
		);`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_cpkey_toast1_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.MaxBatchSize = 100

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	rowsTx, err := s.Conn().Begin(s.t.Context())
	EnvNoError(s.t, env, err)

	// insert 10 rows into the source table
	for i := range 10 {
		testValue := fmt.Sprintf("test_value_%d", i)
		_, err = rowsTx.Exec(s.t.Context(), fmt.Sprintf(
			`INSERT INTO %s(c2,t,t2) VALUES ($1,$2,random_string(9000))`, srcTableName), i, testValue)
		EnvNoError(s.t, env, err)
	}
	s.t.Log("Inserted 10 rows into the source table")

	_, err = rowsTx.Exec(s.t.Context(),
		fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=$1`, srcTableName), 1)
	EnvNoError(s.t, env, err)
	_, err = rowsTx.Exec(s.t.Context(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=$1`, srcTableName), 0)
	EnvNoError(s.t, env, err)

	EnvNoError(s.t, env, rowsTx.Commit(s.t.Context()))

	EnvWaitFor(s.t, env, 3*time.Minute, "normalize tx", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t,t2") == nil
	})
	env.Cancel(s.t.Context())

	RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_Composite_PKey_Toast_2_PG() {
	tc := NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_cpkey_toast2")
	dstTableName := s.attachSchemaSuffix("test_cpkey_toast2_dst")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT GENERATED ALWAYS AS IDENTITY,
			c1 INT GENERATED BY DEFAULT AS IDENTITY,
			c2 INT,
			t TEXT,
			t2 TEXT,
			PRIMARY KEY(id,t)
		);`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_cpkey_toast2_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.MaxBatchSize = 100

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	// insert 10 rows into the source table
	for i := range 10 {
		testValue := fmt.Sprintf("test_value_%d", i)
		_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(
			`INSERT INTO %s(c2,t,t2) VALUES ($1,$2,random_string(9000))`, srcTableName), i, testValue)
		EnvNoError(s.t, env, err)
	}
	s.t.Log("Inserted 10 rows into the source table")

	EnvWaitFor(s.t, env, 3*time.Minute, "normalize 10 rows", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t,t2") == nil
	})
	_, err = s.Conn().Exec(s.t.Context(),
		fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=$1`, srcTableName), 1)
	EnvNoError(s.t, env, err)
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=$1`, srcTableName), 0)
	EnvNoError(s.t, env, err)

	EnvWaitFor(s.t, env, 3*time.Minute, "normalize update", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t,t2") == nil
	})

	env.Cancel(s.t.Context())

	RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_PeerDB_Columns() {
	tc := NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_peerdb_cols")
	dstTableName := s.attachSchemaSuffix("test_peerdb_cols_dst")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_peerdb_cols_mirror"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
		SoftDelete:       true,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.MaxBatchSize = 100

	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	// insert 1 row into the source table
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(
		"INSERT INTO %s(key, value) VALUES ('test_key', 'test_value')", srcTableName))
	EnvNoError(s.t, env, err)

	// delete that row
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(
		"DELETE FROM %s WHERE id=1", srcTableName))
	EnvNoError(s.t, env, err)
	s.t.Log("Inserted and deleted a row for peerdb column check")

	EnvWaitFor(s.t, env, 3*time.Minute, "normalize insert/delete", func() bool {
		return s.checkPeerdbColumns(dstTableName, 1) == nil
	})
	env.Cancel(s.t.Context())

	RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_Soft_Delete_Basic() {
	tc := NewTemporalClient(s.t)

	cmpTableName := s.attachSchemaSuffix("test_softdel")
	srcTableName := cmpTableName + "_src"
	dstTableName := s.attachSchemaSuffix("test_softdel_dst")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			c1 INT,
			c2 INT,
			t TEXT
		);
	`, srcTableName))
	require.NoError(s.t, err)

	config := &protos.FlowConnectionConfigs{
		FlowJobName:     s.attachSuffix("test_softdel"),
		DestinationName: s.Peer().Name,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: dstTableName,
			},
		},
		SourceName:        GeneratePostgresPeer(s.t).Name,
		SoftDeleteColName: "_PEERDB_IS_DELETED",
		SyncedAtColName:   "_PEERDB_SYNCED_AT",
		MaxBatchSize:      100,
	}

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	env := ExecutePeerflow(s.t, tc, config)
	SetupCDCFlowStatusQuery(s.t, env, config)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
			INSERT INTO %s(c1,c2,t) VALUES (1,2,random_string(9000))`, srcTableName))
	EnvNoError(s.t, env, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "normalize row", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t") == nil
	})
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
			UPDATE %s SET c1=c1+4 WHERE id=1`, srcTableName))
	EnvNoError(s.t, env, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "normalize update", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t") == nil
	})
	// since we delete stuff, create another table to compare with
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
			CREATE TABLE %s AS SELECT * FROM %s`, cmpTableName, srcTableName))
	EnvNoError(s.t, env, err)
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1`, srcTableName))
	EnvNoError(s.t, env, err)

	EnvWaitFor(s.t, env, 3*time.Minute, "normalize delete", func() bool {
		return s.comparePGTables(srcTableName, dstTableName+` WHERE NOT "_PEERDB_IS_DELETED"`, "id,c1,c2,t") == nil
	})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)

	// verify our updates and delete happened
	err = s.comparePGTables(cmpTableName, dstTableName, "id,c1,c2,t")
	require.NoError(s.t, err)

	softDeleteQuery := fmt.Sprintf(
		`SELECT COUNT(*) FROM %s WHERE "_PEERDB_IS_DELETED"`,
		dstTableName,
	)
	numRows, err := s.RunInt64Query(softDeleteQuery)
	require.NoError(s.t, err)
	require.Equal(s.t, int64(1), numRows)
}

func (s PeerFlowE2ETestSuitePG) Test_Soft_Delete_IUD_Same_Batch() {
	tc := NewTemporalClient(s.t)

	cmpTableName := s.attachSchemaSuffix("test_softdel_iud")
	srcTableName := cmpTableName + "_src"
	dstTableName := s.attachSchemaSuffix("test_softdel_iud_dst")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
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
		SourceName:        GeneratePostgresPeer(s.t).Name,
		SoftDeleteColName: "_PEERDB_IS_DELETED",
		SyncedAtColName:   "_PEERDB_SYNCED_AT",
		MaxBatchSize:      100,
	}

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	env := ExecutePeerflow(s.t, tc, config)
	SetupCDCFlowStatusQuery(s.t, env, config)

	insertTx, err := s.Conn().Begin(s.t.Context())
	EnvNoError(s.t, env, err)

	_, err = insertTx.Exec(s.t.Context(), fmt.Sprintf(`
			INSERT INTO %s(c1,c2,t) VALUES (1,2,random_string(9000))`, srcTableName))
	EnvNoError(s.t, env, err)
	_, err = insertTx.Exec(s.t.Context(), fmt.Sprintf(`
			UPDATE %s SET c1=c1+4 WHERE id=1`, srcTableName))
	EnvNoError(s.t, env, err)
	// since we delete stuff, create another table to compare with
	_, err = insertTx.Exec(s.t.Context(), fmt.Sprintf(`
			CREATE TABLE %s AS SELECT * FROM %s`, cmpTableName, srcTableName))
	EnvNoError(s.t, env, err)
	_, err = insertTx.Exec(s.t.Context(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1`, srcTableName))
	EnvNoError(s.t, env, err)

	EnvNoError(s.t, env, insertTx.Commit(s.t.Context()))

	EnvWaitFor(s.t, env, 3*time.Minute, "normalize tx", func() bool {
		return s.comparePGTables(cmpTableName, dstTableName, "id,c1,c2,t") == nil
	})

	softDeleteQuery := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE "_PEERDB_IS_DELETED"`, dstTableName)
	EnvWaitFor(s.t, env, time.Minute, "normalize soft delete", func() bool {
		numRows, err := s.RunInt64Query(softDeleteQuery)
		return err == nil && numRows == 1
	})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_Soft_Delete_UD_Same_Batch() {
	cmpTableName := s.attachSchemaSuffix("test_softdel_ud")
	srcTableName := cmpTableName + "_src"
	dstTableName := s.attachSchemaSuffix("test_softdel_ud_dst")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			c1 INT,
			c2 INT,
			t TEXT
		);
	`, srcTableName))
	require.NoError(s.t, err)

	config := &protos.FlowConnectionConfigs{
		FlowJobName:     s.attachSuffix("test_softdel_ud"),
		DestinationName: s.Peer().Name,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: dstTableName,
			},
		},
		SourceName:        GeneratePostgresPeer(s.t).Name,
		SoftDeleteColName: "_PEERDB_IS_DELETED",
		SyncedAtColName:   "_PEERDB_SYNCED_AT",
		MaxBatchSize:      100,
	}

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, config)
	SetupCDCFlowStatusQuery(s.t, env, config)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
			INSERT INTO %s(c1,c2,t) VALUES (1,2,random_string(9000))`, srcTableName))
	EnvNoError(s.t, env, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "normalize row", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t") == nil
	})

	insertTx, err := s.Conn().Begin(s.t.Context())
	EnvNoError(s.t, env, err)
	_, err = insertTx.Exec(s.t.Context(), fmt.Sprintf(`
			UPDATE %s SET t=random_string(10000) WHERE id=1`, srcTableName))
	EnvNoError(s.t, env, err)
	_, err = insertTx.Exec(s.t.Context(), fmt.Sprintf(`
			UPDATE %s SET c1=c1+4 WHERE id=1`, srcTableName))
	EnvNoError(s.t, env, err)
	_, err = insertTx.Exec(s.t.Context(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1`, srcTableName))
	EnvNoError(s.t, env, err)
	EnvNoError(s.t, env, insertTx.Commit(s.t.Context()))

	EnvWaitFor(s.t, env, 3*time.Minute, "normalize transaction", func() bool {
		return s.comparePGTables(srcTableName,
			dstTableName+` WHERE NOT "_PEERDB_IS_DELETED"`, "id,c1,c2,t") == nil
	})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)

	// verify our updates and delete happened
	require.NoError(s.t, err)

	softDeleteQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s WHERE "_PEERDB_IS_DELETED"`,
		dstTableName)
	numRows, err := s.RunInt64Query(softDeleteQuery)
	require.NoError(s.t, err)
	require.Equal(s.t, int64(1), numRows)
}

func (s PeerFlowE2ETestSuitePG) Test_Soft_Delete_Insert_After_Delete() {
	tc := NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_softdel_iad")
	dstTableName := s.attachSchemaSuffix("test_softdel_iad_dst")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
			c1 INT,
			c2 INT,
			t TEXT
		);
	`, srcTableName))
	require.NoError(s.t, err)

	config := &protos.FlowConnectionConfigs{
		FlowJobName:     s.attachSuffix("test_softdel_iad"),
		DestinationName: s.Peer().Name,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: dstTableName,
			},
		},
		SourceName:        GeneratePostgresPeer(s.t).Name,
		SoftDeleteColName: "_PEERDB_IS_DELETED",
		SyncedAtColName:   "_PEERDB_SYNCED_AT",
		MaxBatchSize:      100,
	}

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert and delete rows in the table.
	env := ExecutePeerflow(s.t, tc, config)
	SetupCDCFlowStatusQuery(s.t, env, config)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
			INSERT INTO %s(c1,c2,t) VALUES (1,2,random_string(9000))`, srcTableName))
	EnvNoError(s.t, env, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "normalize row", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t") == nil
	})
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1`, srcTableName))
	EnvNoError(s.t, env, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "normalize delete", func() bool {
		return s.comparePGTables(srcTableName, dstTableName+` WHERE NOT "_PEERDB_IS_DELETED"`, "id,c1,c2,t") == nil
	})
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
			INSERT INTO %s(id,c1,c2,t) VALUES (1,3,4,random_string(10000))`, srcTableName))
	EnvNoError(s.t, env, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "normalize reinsert", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t") == nil
	})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)

	softDeleteQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s WHERE "_PEERDB_IS_DELETED"`,
		dstTableName)
	numRows, err := s.RunInt64Query(softDeleteQuery)
	require.NoError(s.t, err)
	require.Equal(s.t, int64(0), numRows)
}

func (s PeerFlowE2ETestSuitePG) Test_Supported_Mixed_Case_Table() {
	tc := NewTemporalClient(s.t)

	stmtSrcTableName := fmt.Sprintf(`e2e_test_%s."%s"`, s.suffix, "testMixedCase")
	srcTableName := s.attachSchemaSuffix("testMixedCase")
	stmtDstTableName := fmt.Sprintf(`e2e_test_%s."%s"`, s.suffix, "testMixedCaseDst")
	dstTableName := s.attachSchemaSuffix("testMixedCaseDst")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			"pulseArmor" SERIAL PRIMARY KEY,
			"highGold" TEXT NOT NULL,
			"eVe" TEXT NOT NULL,
			id SERIAL
		);
	`, stmtSrcTableName))
	require.NoError(s.t, err)

	config := &protos.FlowConnectionConfigs{
		FlowJobName:     s.attachSuffix("test_mixed_case"),
		DestinationName: s.Peer().Name,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: dstTableName,
			},
		},
		SourceName:   GeneratePostgresPeer(s.t).Name,
		MaxBatchSize: 100,
	}

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert and delete rows in the table.
	env := ExecutePeerflow(s.t, tc, config)
	SetupCDCFlowStatusQuery(s.t, env, config)
	// insert 20 rows into the source table
	for i := range 10 {
		testKey := fmt.Sprintf("test_key_%d", i)
		testValue := fmt.Sprintf("test_value_%d", i)
		_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
			INSERT INTO %s ("highGold","eVe") VALUES ($1, $2)
		`, stmtSrcTableName), testKey, testValue)
		EnvNoError(s.t, env, err)
	}
	s.t.Log("Inserted 20 rows into the source table")

	EnvWaitFor(s.t, env, 1*time.Minute, "normalize mixed case", func() bool {
		return s.comparePGTables(stmtSrcTableName, stmtDstTableName,
			"id,\"pulseArmor\",\"highGold\",\"eVe\"") == nil
	})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_Multiple_Parallel_Initial() {
	tableMapping := make([]*protos.TableMapping, 5)
	for i := range tableMapping {
		srcTable := fmt.Sprintf("test_multi_init_%d", i)
		dstTable := srcTable + "_dst"
		s.setupSourceTable(srcTable, (i+1)*101)
		tableMapping[i] = &protos.TableMapping{
			SourceTableIdentifier:      s.attachSchemaSuffix(srcTable),
			DestinationTableIdentifier: s.attachSchemaSuffix(dstTable),
		}
	}

	config := &protos.FlowConnectionConfigs{
		DoInitialSnapshot:           true,
		InitialSnapshotOnly:         true,
		FlowJobName:                 s.attachSuffix("test_multi_init"),
		DestinationName:             s.Peer().Name,
		TableMappings:               tableMapping,
		SourceName:                  GeneratePostgresPeer(s.t).Name,
		CdcStagingPath:              "",
		SnapshotMaxParallelWorkers:  4,
		SnapshotNumTablesInParallel: 3,
	}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, config)
	SetupCDCFlowStatusQuery(s.t, env, config)
	EnvWaitForFinished(s.t, env, 3*time.Minute)
	for _, tm := range config.TableMappings {
		require.NoError(s.t, s.comparePGTables(tm.SourceTableIdentifier, tm.DestinationTableIdentifier, "id,address,asset_id"))
	}
}

func (s PeerFlowE2ETestSuitePG) Test_ContinueAsNew() {
	srcTableName := s.attachSchemaSuffix("test_continueasnew")
	dstTableName := s.attachSchemaSuffix("test_continueasnew_dst")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_continueasnew_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.MaxBatchSize = 2
	flowConnConfig.IdleTimeoutSeconds = 10

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)

	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	for i := range 144 {
		testKey := fmt.Sprintf("test_key_%d", i)
		testValue := fmt.Sprintf("test_value_%d", i)
		_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s(key, value) VALUES ($1, $2)
		`, srcTableName), testKey, testValue)
		EnvNoError(s.t, env, err)
	}
	s.t.Log("Inserted 144 rows into the source table")

	EnvWaitFor(s.t, env, 4*time.Minute, "normalize 72 syncs", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,key,value") == nil
	})
	env.Cancel(s.t.Context())

	RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_Dynamic_Mirror_Config_Via_Signals() {
	srcTable1Name := s.attachSchemaSuffix("test_dynconfig_1")
	srcTable2Name := s.attachSchemaSuffix("test_dynconfig_2")
	dstTable1Name := s.attachSchemaSuffix("test_dynconfig_1_dst")
	dstTable2Name := s.attachSchemaSuffix("test_dynconfig_2_dst")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
			t TEXT DEFAULT md5(random()::text));
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
			t TEXT DEFAULT md5(random()::text));
	`, srcTable1Name, srcTable2Name))
	require.NoError(s.t, err)

	config := &protos.FlowConnectionConfigs{
		FlowJobName:     s.attachSuffix("test_dynconfig"),
		DestinationName: s.Peer().Name,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTable1Name,
				DestinationTableIdentifier: dstTable1Name,
			},
		},
		SourceName:                  s.Peer().Name,
		MaxBatchSize:                6,
		IdleTimeoutSeconds:          7,
		DoInitialSnapshot:           true,
		SnapshotNumRowsPerPartition: 1000,
		SnapshotMaxParallelWorkers:  1,
		SnapshotNumTablesInParallel: 1,
	}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, config)

	addRows := func(numRows int) {
		for range numRows {
			_, err = s.Conn().Exec(s.t.Context(),
				fmt.Sprintf(`INSERT INTO %s DEFAULT VALUES`, srcTable1Name))
			EnvNoError(s.t, env, err)
			_, err = s.Conn().Exec(s.t.Context(),
				fmt.Sprintf(`INSERT INTO %s DEFAULT VALUES`, srcTable2Name))
			EnvNoError(s.t, env, err)
		}
		s.t.Logf("Inserted %d rows into the source table", numRows)
	}

	// add before to test initial load too.
	addRows(18)
	SetupCDCFlowStatusQuery(s.t, env, config)
	// insert 18 rows into the source tables, exactly 3 batches
	addRows(18)

	EnvWaitFor(s.t, env, 1*time.Minute, "normalize 18 records - first table", func() bool {
		return s.comparePGTables(srcTable1Name, dstTable1Name, "id,t") == nil
	})

	workflowState := EnvGetWorkflowState(s.t, env)
	assert.EqualValues(s.t, 7, workflowState.SyncFlowOptions.IdleTimeoutSeconds)
	assert.EqualValues(s.t, 6, workflowState.SyncFlowOptions.BatchSize)
	assert.Len(s.t, workflowState.SyncFlowOptions.TableMappings, 1)
	assert.Len(s.t, workflowState.SyncFlowOptions.SrcTableIdNameMapping, 1)

	if !s.t.Failed() {
		SignalWorkflow(s.t.Context(), env, model.FlowSignal, model.PauseSignal)
		EnvWaitFor(s.t, env, 1*time.Minute, "paused workflow", func() bool {
			return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_PAUSED
		})

		_, err = s.Conn().Exec(s.t.Context(),
			`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
			 WHERE query LIKE '%START_REPLICATION%' AND query LIKE '%dynconfig%' AND backend_type='walsender'`)
		require.NoError(s.t, err)
		time.Sleep(5 * time.Second)

		// add rows to both tables before resuming - should handle
		addRows(18)

		SignalWorkflow(s.t.Context(), env, model.CDCDynamicPropertiesSignal, &protos.CDCFlowConfigUpdate{
			IdleTimeout: 14,
			BatchSize:   12,
			AdditionalTables: []*protos.TableMapping{
				{
					SourceTableIdentifier:      srcTable2Name,
					DestinationTableIdentifier: dstTable2Name,
				},
			},
		})

		EnvWaitFor(s.t, env, 1*time.Minute, "resumed workflow", func() bool {
			return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
		})
		EnvWaitFor(s.t, env, 2*time.Minute, "normalize 18 records - first table", func() bool {
			return s.comparePGTables(srcTable1Name, dstTable1Name, "id,t") == nil
		})
		EnvWaitFor(s.t, env, 2*time.Minute, "initial load + normalize 18 records - second table", func() bool {
			return s.comparePGTables(srcTable2Name, dstTable2Name, "id,t") == nil
		})

		workflowState = EnvGetWorkflowState(s.t, env)
		assert.EqualValues(s.t, 14, workflowState.SyncFlowOptions.IdleTimeoutSeconds)
		assert.EqualValues(s.t, 12, workflowState.SyncFlowOptions.BatchSize)
		assert.Len(s.t, workflowState.SyncFlowOptions.TableMappings, 2)
		assert.Len(s.t, workflowState.SyncFlowOptions.SrcTableIdNameMapping, 2)
	}

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_TypeSystem_PG() {
	srcTableName := s.attachSchemaSuffix("test_typesystem_pg")
	dstTableName := s.attachSchemaSuffix("test_typesystem_pg_dst")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		create table %[1]s (
			id uuid not null primary key default gen_random_uuid(),
			created_at timestamptz not null default now(),
			updated_at timestamp,
			j json,
			jb jsonb,
			aa32 integer[][],
			currency char(3)
		)`, srcTableName))
	require.NoError(s.t, err)

	for range 3 {
		_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		insert into %s (updated_at, j, jb, aa32, currency) values (
			NOW(),'{"b" : 123}','{"b" : 123}','{{3,2,1},{6,5,4},{9,8,7}}','ISK'
		)`, srcTableName))
		require.NoError(s.t, err)
	}

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_typesystem_pg"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.System = protos.TypeSystem_PG
	flowConnConfig.SoftDeleteColName = ""
	flowConnConfig.SyncedAtColName = ""

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)

	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		insert into %s (updated_at, j, jb, aa32, currency) values (
			NOW(),'{"b" : 123}','{"b" : 123}','{{3,2,1},{6,5,4},{9,8,7}}','ISK'
		)`, srcTableName))
	EnvNoError(s.t, env, err)

	EnvWaitFor(s.t, env, 3*time.Minute, "normalize rows", func() bool {
		err := s.comparePGTables(srcTableName, dstTableName, "id,created_at,updated_at,j::text,jb,aa32,currency")
		if err != nil {
			s.t.Log(err.Error())
		}
		return err == nil
	})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_TransformRecordScript() {
	srcTableName := s.attachSchemaSuffix("test_transrecord_pg")
	dstTableName := s.attachSchemaSuffix("test_transrecord_pg_dst")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		create table %[1]s (
			id uuid not null primary key default gen_random_uuid(),
			val int
		)`, srcTableName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(s.t.Context(), `insert into public.scripts (name, lang, source) values
		('cdc_transform_record', 'lua', 'function transformRecord(r) if r.row then r.row.val = 1729 end end') on conflict do nothing`)
	require.NoError(s.t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_transrecord_pg"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.Script = "cdc_transform_record"

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)

	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf("insert into %s (val) values (1)", srcTableName))
	EnvNoError(s.t, env, err)

	EnvWaitFor(s.t, env, 3*time.Minute, "normalize rows", func() bool {
		err := s.compareCounts(dstTableName, 1)
		if err != nil {
			s.t.Log(err.Error())
		}
		return err == nil
	})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)

	var exists bool
	err = s.Conn().QueryRow(s.t.Context(),
		fmt.Sprintf("select exists(select * from %s where val <> 1729)", dstTableName)).Scan(&exists)
	require.NoError(s.t, err)
	require.False(s.t, exists)
}

func (s PeerFlowE2ETestSuitePG) Test_TransformRowScript() {
	srcTableName := s.attachSchemaSuffix("test_transrow_pg")
	dstTableName := s.attachSchemaSuffix("test_transrow_pg_dst")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		create table %[1]s (
			id uuid not null primary key default gen_random_uuid(),
			val int
		)`, srcTableName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(s.t.Context(), `insert into public.scripts (name, lang, source) values
	('cdc_transform_row', 'lua', 'function transformRow(r) r.val = 1729 end') on conflict do nothing`)
	require.NoError(s.t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_transrow_pg"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.Script = "cdc_transform_row"

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)

	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf("insert into %s (val) values (1)", srcTableName))
	EnvNoError(s.t, env, err)

	EnvWaitFor(s.t, env, 3*time.Minute, "normalize rows", func() bool {
		err := s.compareCounts(dstTableName, 1)
		if err != nil {
			s.t.Log(err.Error())
		}
		return err == nil
	})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)

	var exists bool
	err = s.Conn().QueryRow(s.t.Context(),
		fmt.Sprintf("select exists(select * from %s where val <> 1729)", dstTableName)).Scan(&exists)
	require.NoError(s.t, err)
	require.False(s.t, exists)
}

func (s PeerFlowE2ETestSuitePG) Test_Mixed_Case_Schema_Changes_PG() {
	tc := NewTemporalClient(s.t)

	srcTableName := "test_mixed_case_schema_changes_PG"
	dstTableName := srcTableName + "_dst"
	quotedSourceTableName := s.attachSchemaSuffix(`"` + srcTableName + `"`)
	quotedDestTableName := s.attachSchemaSuffix(`"` + dstTableName + `"`)
	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			c1 BIGINT
		);
	`, quotedSourceTableName))
	require.NoError(s.t, err)

	flowConnConfig := &protos.FlowConnectionConfigs{
		FlowJobName:     s.attachSuffix("test_mixed_case_schema_changes_pg"),
		DestinationName: s.Peer().Name,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      s.attachSchemaSuffix(srcTableName),
				DestinationTableIdentifier: s.attachSchemaSuffix(dstTableName),
			},
		},
		SourceName:   GeneratePostgresPeer(s.t).Name,
		MaxBatchSize: 100,
	}

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert and mutate schema repeatedly.
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	// insert first row.
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s(c1) VALUES (1)`, quotedSourceTableName))
	EnvNoError(s.t, env, err)
	s.t.Log("Inserted initial row in the source table")
	EnvWaitFor(s.t, env, 1*time.Minute, "normalize mixed case", func() bool {
		return s.comparePGTables(quotedSourceTableName, quotedDestTableName,
			"id,c1") == nil
	})
	// alter source table, add column c2 and insert another row.
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		ALTER TABLE %s ADD COLUMN "myC2" BIGINT`, quotedSourceTableName))
	EnvNoError(s.t, env, err)
	s.t.Log("Altered source table, added column myC2")
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s(c1,"myC2") VALUES (2,2)`, quotedSourceTableName))
	EnvNoError(s.t, env, err)
	s.t.Log("Inserted row with added myC2 in the source table")

	// verify we got our two rows, if schema did not match up it will error.
	EnvWaitFor(s.t, env, 1*time.Minute, "normalize mixed case schema change", func() bool {
		return s.comparePGTables(quotedSourceTableName, quotedDestTableName,
			"id,c1,\"myC2\"") == nil
	})

	// alter source table, add column c3, drop column c2 and insert another row.
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		ALTER TABLE %s DROP COLUMN "myC2", ADD COLUMN c3 FLOAT`, quotedSourceTableName))
	EnvNoError(s.t, env, err)
	s.t.Log("Altered source table, dropped column myC2 and added column c3")
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s(c1,c3) VALUES (3,3.5)`, quotedSourceTableName))
	EnvNoError(s.t, env, err)
	s.t.Log("Inserted row with added c3 in the source table")

	// verify we got our two rows, if schema did not match up it will error.
	EnvWaitFor(s.t, env, 1*time.Minute, "normalize mixed case schema change", func() bool {
		return s.comparePGTables(quotedSourceTableName, quotedDestTableName,
			"id,c1,c3") == nil
	})
	// alter source table, drop column c3 and insert another row.
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		ALTER TABLE %s DROP COLUMN c3`, quotedSourceTableName))
	EnvNoError(s.t, env, err)
	s.t.Log("Altered source table, dropped column c3")
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s(c1) VALUES (4)`, quotedSourceTableName))
	EnvNoError(s.t, env, err)
	s.t.Log("Inserted row after dropping all columns in the source table")

	// verify we got our two rows, if schema did not match up it will error.
	EnvWaitFor(s.t, env, 1*time.Minute, "normalize mixed case schema change", func() bool {
		return s.comparePGTables(quotedSourceTableName, quotedDestTableName,
			"id,c1") == nil
	})
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_SS_Types_PG() {
	tc := NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_ss_types_pg")
	dstTableName := s.attachSchemaSuffix("test_ss_types_pg_dst")

	// Create enum type (handle DuplicateObject gracefully like Test_Enums)
	createEnumSQL := "CREATE TYPE test_platform AS ENUM ('instagram', 'tiktok', 'youtube', 'twitter', 'facebook');"
	_, enumErr := s.Conn().Exec(s.t.Context(), createEnumSQL)
	if enumErr != nil &&
		!shared.IsSQLStateError(enumErr, pgerrcode.DuplicateObject, pgerrcode.UniqueViolation) {
		require.NoError(s.t, enumErr)
	}

	// Create source table with all types
	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
			col_text text,
			col_varchar_2 character varying(2),
			col_varchar_16 character varying(16),
			col_varchar_20 character varying(20),
			col_varchar_30 character varying(30),
			col_varchar_32 character varying(32),
			col_varchar_50 character varying(50),
			col_varchar_64 character varying(64),
			col_varchar_100 character varying(100),
			col_varchar_255 character varying(255),
			col_varchar_256 character varying(256),
			col_smallint smallint,
			col_integer integer,
			col_bigint bigint,
			col_numeric numeric,
			col_double_precision double precision,
			col_boolean boolean,
			col_date date,
			col_timestamp timestamp without time zone,
			col_timestamptz timestamp with time zone,
			col_interval interval,
			col_uuid uuid,
			col_json json,
			col_jsonb jsonb,
			col_text_array text[],
			col_integer_array integer[],
			col_platform test_platform,
			created_at timestamp with time zone DEFAULT now() NOT NULL,
			updated_at timestamp with time zone DEFAULT now() NOT NULL
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_ss_types_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.System = protos.TypeSystem_PG
	flowConnConfig.MaxBatchSize = 100
	flowConnConfig.SoftDeleteColName = ""
	flowConnConfig.SyncedAtColName = ""

	// Column list for comparison (cast json/enum to text for comparison)
	allCols := strings.Join([]string{
		"id",
		"col_text", "col_varchar_2", "col_varchar_16", "col_varchar_20",
		"col_varchar_30", "col_varchar_32", "col_varchar_50", "col_varchar_64",
		"col_varchar_100", "col_varchar_255", "col_varchar_256",
		"col_smallint", "col_integer", "col_bigint", "col_numeric", "col_double_precision",
		"col_boolean",
		"col_date", "col_timestamp", "col_timestamptz", "col_interval",
		"col_uuid",
		"col_json::text", "col_jsonb",
		"col_text_array", "col_integer_array",
		"col_platform::text",
		"created_at", "updated_at",
	}, ",")

	// =====================================================
	// INITIAL SNAPSHOT: Insert 5 edge case rows before starting the flow
	// =====================================================

	// Row 1: All NULL nullable columns
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s (id, created_at, updated_at) VALUES (
			'11111111-1111-1111-1111-111111111111',
			'2024-01-01 00:00:00+00',
			'2024-01-01 00:00:00+00'
		)`, srcTableName))
	require.NoError(s.t, err)

	// Row 2: Empty arrays, empty jsonb, empty strings
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s (
			id, col_text, col_varchar_2, col_varchar_16, col_varchar_20, col_varchar_30,
			col_varchar_32, col_varchar_50, col_varchar_64, col_varchar_100, col_varchar_255, col_varchar_256,
			col_text_array, col_integer_array, col_json, col_jsonb,
			created_at, updated_at
		) VALUES (
			'22222222-2222-2222-2222-222222222222',
			'', '', '', '', '', '', '', '', '', '', '',
			'{}', '{}', '{}', '{}',
			'2024-01-01 00:00:00+00', '2024-01-01 00:00:00+00'
		)`, srcTableName))
	require.NoError(s.t, err)

	// Row 3: Minimum numeric values
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s (
			id, col_smallint, col_integer, col_bigint, col_numeric, col_double_precision,
			created_at, updated_at
		) VALUES (
			'33333333-3333-3333-3333-333333333333',
			-32768, -2147483648, -9223372036854775808,
			-99999999999999999999.999999999999,
			-1.7976931348623157e+308,
			'2024-01-01 00:00:00+00', '2024-01-01 00:00:00+00'
		)`, srcTableName))
	require.NoError(s.t, err)

	// Row 3b: NaN and Infinity values for numeric/double precision
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s (
			id, col_numeric, col_double_precision,
			created_at, updated_at
		) VALUES
			('33333333-3333-3333-3333-33333333333a', 'NaN', 'NaN', '2024-01-01 00:00:00+00', '2024-01-01 00:00:00+00'),
			('33333333-3333-3333-3333-33333333333b', NULL, '+Infinity', '2024-01-01 00:00:00+00', '2024-01-01 00:00:00+00'),
			('33333333-3333-3333-3333-33333333333c', NULL, '-Infinity', '2024-01-01 00:00:00+00', '2024-01-01 00:00:00+00')
		`, srcTableName))
	require.NoError(s.t, err)

	// Row 4: Unicode text (Chinese, Arabic, Russian, Emoji), special chars
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s (
			id, col_text, col_varchar_50, col_varchar_100,
			col_text_array, col_jsonb,
			created_at, updated_at
		) VALUES (
			'44444444-4444-4444-4444-444444444444',
			'中文 العربية Русский 🎉🚀💡',
			'<>&"''\ special chars',
			'Mixed: 你好 مرحبا Привет 😀',
			ARRAY['中文', 'العربية', 'Русский', '🎉'],
			'{"unicode": "中文 🎉", "special": "<>&"}',
			'2024-01-01 00:00:00+00', '2024-01-01 00:00:00+00'
		)`, srcTableName))
	require.NoError(s.t, err)

	// Row 5: Boundary timestamps
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s (
			id, col_date, col_timestamp, col_timestamptz, col_interval,
			created_at, updated_at
		) VALUES (
			'55555555-5555-5555-5555-555555555555',
			'0001-01-01',
			'1970-01-01 00:00:00',
			'2038-01-19 03:14:07+00',
			'178000000 years',
			'2024-01-01 00:00:00+00', '2024-01-01 00:00:00+00'
		)`, srcTableName))
	require.NoError(s.t, err)

	s.t.Log("Inserted 8 edge case rows for initial snapshot")

	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	// Wait for initial snapshot to complete
	EnvWaitFor(s.t, env, 3*time.Minute, "normalize initial snapshot", func() bool {
		err := s.comparePGTables(srcTableName, dstTableName, allCols)
		if err != nil {
			s.t.Log("snapshot comparison mismatch:", err)
		}
		return err == nil
	})

	// =====================================================
	// CDC: Insert 5 more edge case rows after the flow starts
	// =====================================================

	// Row 6: Maximum numeric values, arrays with NULL elements, JSONB array
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s (
			id, col_smallint, col_integer, col_bigint, col_numeric, col_double_precision,
			col_text_array, col_integer_array, col_jsonb,
			created_at, updated_at
		) VALUES (
			'66666666-6666-6666-6666-666666666666',
			32767, 2147483647, 9223372036854775807,
			99999999999999999999.999999999999,
			1.7976931348623157e+308,
			ARRAY['value', NULL, 'another'],
			ARRAY[1, NULL, 3],
			'[1, "two", null, true, {"nested": "value"}]',
			'2024-01-01 00:00:00+00', '2024-01-01 00:00:00+00'
		)`, srcTableName))
	EnvNoError(s.t, env, err)

	// Row 7: Deeply nested JSON objects
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s (
			id, col_json, col_jsonb,
			created_at, updated_at
		) VALUES (
			'77777777-7777-7777-7777-777777777777',
			'{"level1": {"level2": {"level3": {"level4": {"value": "deep"}}}}}',
			'{"string": "text", "number": 123, "float": 1.5, "bool": true, "null": null, "array": [1,2,3], "object": {"key": "val"}}',
			'2024-01-01 00:00:00+00', '2024-01-01 00:00:00+00'
		)`, srcTableName))
	EnvNoError(s.t, env, err)

	// Row 8: Complex interval, far future date
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s (
			id, col_date, col_timestamp, col_timestamptz, col_interval,
			created_at, updated_at
		) VALUES (
			'88888888-8888-8888-8888-888888888888',
			'9999-12-31',
			'9999-12-31 23:59:59',
			'9999-12-31 23:59:59+00',
			'1 year 2 months 3 days 4 hours 5 minutes 6 seconds',
			'2024-01-01 00:00:00+00', '2024-01-01 00:00:00+00'
		)`, srcTableName))
	EnvNoError(s.t, env, err)

	// Row 9: Zero values for all numerics, NULL boolean, zero interval
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s (
			id, col_smallint, col_integer, col_bigint, col_numeric, col_double_precision,
			col_boolean, col_interval,
			created_at, updated_at
		) VALUES (
			'99999999-9999-9999-9999-999999999999',
			0, 0, 0, 0, 0,
			NULL, '0 seconds',
			'2024-01-01 00:00:00+00', '2024-01-01 00:00:00+00'
		)`, srcTableName))
	EnvNoError(s.t, env, err)

	// Row 10: All enum values, test facebook enum, full data
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s (
			id, col_text, col_varchar_16, col_boolean, col_uuid, col_platform,
			col_text_array,
			created_at, updated_at
		) VALUES (
			'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
			'instagram tiktok youtube twitter facebook',
			'enum_test',
			true,
			'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb',
			'facebook',
			ARRAY['instagram', 'tiktok', 'youtube', 'twitter', 'facebook'],
			'2024-01-01 00:00:00+00', '2024-01-01 00:00:00+00'
		)`, srcTableName))
	EnvNoError(s.t, env, err)

	s.t.Log("Inserted 5 more edge case rows via CDC")

	// Wait for CDC inserts to replicate
	EnvWaitFor(s.t, env, 3*time.Minute, "normalize CDC inserts", func() bool {
		err := s.comparePGTables(srcTableName, dstTableName, allCols)
		if err != nil {
			s.t.Log("CDC insert comparison mismatch:", err)
		}
		return err == nil
	})

	// =====================================================
	// CDC: Test UPDATE operations
	// =====================================================

	// Update row 1: change from all NULLs to having values
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		UPDATE %s SET
			col_text = 'updated from null',
			col_smallint = 100,
			col_boolean = true,
			col_jsonb = '{"updated": true}',
			col_platform = 'youtube',
			updated_at = '2024-06-01 00:00:00+00'
		WHERE id = '11111111-1111-1111-1111-111111111111'`, srcTableName))
	EnvNoError(s.t, env, err)

	// Update row 4: change unicode text
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		UPDATE %s SET
			col_text = 'Updated: 更新 تحديث Обновление 🔄',
			col_text_array = ARRAY['updated', '更新', '🔄'],
			updated_at = '2024-06-01 00:00:00+00'
		WHERE id = '44444444-4444-4444-4444-444444444444'`, srcTableName))
	EnvNoError(s.t, env, err)

	// Update row 7: change JSON
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		UPDATE %s SET
			col_json = '{"updated": {"nested": "new_value"}}',
			col_jsonb = '{"completely": "different", "structure": [1,2,3]}',
			updated_at = '2024-06-01 00:00:00+00'
		WHERE id = '77777777-7777-7777-7777-777777777777'`, srcTableName))
	EnvNoError(s.t, env, err)

	s.t.Log("Executed 3 UPDATE operations via CDC")

	// Wait for CDC updates to replicate
	EnvWaitFor(s.t, env, 3*time.Minute, "normalize CDC updates", func() bool {
		err := s.comparePGTables(srcTableName, dstTableName, allCols)
		if err != nil {
			s.t.Log("CDC update comparison mismatch:", err)
		}
		return err == nil
	})

	// =====================================================
	// CDC: Test DELETE operations
	// =====================================================

	// Delete row 9 (zero values row)
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		DELETE FROM %s WHERE id = '99999999-9999-9999-9999-999999999999'`, srcTableName))
	EnvNoError(s.t, env, err)

	s.t.Log("Executed DELETE operation via CDC")

	// Wait for CDC delete to replicate
	EnvWaitFor(s.t, env, 3*time.Minute, "normalize CDC delete", func() bool {
		err := s.comparePGTables(srcTableName, dstTableName, allCols)
		if err != nil {
			s.t.Log("CDC delete comparison mismatch:", err)
		}
		return err == nil
	})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) TestResync(tableName string) {
	srcTableName := "pgresync"
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

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix(srcTableName),
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
	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\"")

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf("INSERT INTO %s (key, \"excludedColumn\") VALUES ('cdc','excluded')", srcFullName))
	require.NoError(s.t, err)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,\"key\"")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)

	env = ExecuteDropFlow(s.t.Context(), tc, flowConnConfig, 0)
	EnvWaitForFinished(s.t, env, 3*time.Minute)

	flowConnConfig.Resync = true
	env = ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\"")
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_Other_Schema_Enums() {
	tc := NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_enum_flow")
	dstTableName := s.attachSchemaSuffix("test_enum_flow_dst")

	// Create a mixed case schema for the enum
	enumSchema := fmt.Sprintf("\"EnumSchema_%s\"", s.suffix)
	_, err := s.Conn().Exec(s.t.Context(), "CREATE SCHEMA IF NOT EXISTS "+enumSchema)
	require.NoError(s.t, err)

	// Create a mixed case enum in the custom schema
	createMoodEnum := fmt.Sprintf("CREATE TYPE %s.\"MoodType\" AS ENUM ('happy', 'sad', 'angry');", enumSchema)
	_, enumErr := s.Conn().Exec(s.t.Context(), createMoodEnum)
	if enumErr != nil &&
		!shared.IsSQLStateError(enumErr, pgerrcode.DuplicateObject, pgerrcode.UniqueViolation) {
		require.NoError(s.t, enumErr)
	}
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			my_mood %s."MoodType",
			my_null_mood %s."MoodType",
			moods %s."MoodType"[]
		);
	`, srcTableName, enumSchema, enumSchema, enumSchema))
	require.NoError(s.t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_enum_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.MaxBatchSize = 100
	flowConnConfig.System = protos.TypeSystem_PG

	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	_, err = s.Conn().Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO %s(my_mood, my_null_mood, moods) VALUES ('happy',null,'{happy,angry}')`, srcTableName))
	EnvNoError(s.t, env, err)
	s.t.Log("Inserted enums into the source table")
	EnvWaitFor(s.t, env, 3*time.Minute, "normalize enum", func() bool {
		return s.checkEnums(srcTableName, dstTableName) == nil
	})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_Table_With_Excluded_PK_And_ReplicaIdentityFull() {
	tc := NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_excluded_pk_replfull")
	dstTableName := s.attachSchemaSuffix("test_excluded_pk_replfull_dst")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			message TEXT NOT NULL UNIQUE,
			updated_at TIMESTAMPTZ
		);
		CREATE UNIQUE INDEX test_excluded_pk_replfull_message_idx ON %s(message);
		ALTER TABLE %s REPLICA IDENTITY USING INDEX test_excluded_pk_replfull_message_idx;
	`, srcTableName, srcTableName, srcTableName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			message TEXT NOT NULL UNIQUE,
			updated_at TIMESTAMPTZ
		);
	`, dstTableName))
	require.NoError(s.t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("test_excluded_pk_replfull"),
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: dstTableName,
				Exclude:                    []string{"id"},
			},
		},
		Destination: s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.System = protos.TypeSystem_PG
	flowConnConfig.SyncedAtColName = ""
	flowConnConfig.SoftDeleteColName = ""
	flowConnConfig.MaxBatchSize = 100

	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s(message, updated_at) VALUES ('initial message', NOW())`, srcTableName))
	EnvNoError(s.t, env, err)
	s.t.Log("Inserted initial row into the source table")

	EnvWaitFor(s.t, env, 1*time.Minute, "normalize insert", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "message,updated_at") == nil
	})

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		UPDATE %s SET updated_at = NOW() WHERE message = 'initial message'`, srcTableName))
	EnvNoError(s.t, env, err)
	s.t.Log("Updated row in the source table")

	EnvWaitFor(s.t, env, 1*time.Minute, "normalize update", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "message") == nil
	})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}
