package e2e_postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
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
	err := s.Conn().QueryRow(context.Background(), query).Scan(&isDeleted, &syncedAt)
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

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			gg geography NOT NULL,
			gm geometry NOT NULL
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_geo_flow_pg"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 100

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)

	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	// insert 1 row into the source table
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(gg, gm) VALUES ('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))','LINESTRING(0 0, 1 1, 2 2)')
		`, srcTableName))
	e2e.EnvNoError(s.t, env, err)

	s.t.Log("Inserted 1 row into the source table")
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize shapes", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,gg,gm") == nil
	})

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_Types_PG() {
	tc := e2e.NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_types_pg")
	dstTableName := s.attachSchemaSuffix("test_types_pg_dst")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (id serial PRIMARY KEY,c1 BIGINT,c2 BYTEA,c4 BOOLEAN,
		c7 CHARACTER,c8 varchar,c9 CIDR,c11 DATE,c12 FLOAT,c13 DOUBLE PRECISION,
		c14 INET,c15 INTEGER,c21 MACADDR,
		c29 SMALLINT,c32 TEXT,
		c33 TIMESTAMP,c34 TIMESTAMPTZ,c35 TIME, c36 TIMETZ,
		c40 UUID, c42 INT[], c43 FLOAT[], c44 TEXT[], c45 UUID[],
		c46 DATE[], c47 TIMESTAMPTZ[], c48 TIMESTAMP[], c49 BOOLEAN[], c50 SMALLINT[]);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_types_pg"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 100
	flowConnConfig.SoftDeleteColName = ""
	flowConnConfig.SyncedAtColName = ""

	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s SELECT 2,2,'\xdeadbeef',
			true,'s','test','1.1.10.2'::cidr,
			CURRENT_DATE,1.23,1.234,'192.168.1.5'::inet,1,
			'08:00:2b:01:02:03'::macaddr,
			1,'test',now(),now(),now()::time,now()::timetz,
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
	e2e.EnvNoError(s.t, env, err)

	s.t.Log("Inserted 1 row into the source table")
	allCols := strings.Join([]string{
		"c1", "c2", "c4",
		"c40", "id", "c9", "c11", "c12", "c13", "c14", "c15",
		"c21", "c29", "c33", "c34", "c35", "c36",
		"c7", "c8", "c32", "c42", "c43", "c44", "c45", "c46", "c47", "c48", "c49", "c50",
	}, ",")
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize types", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, allCols) == nil
	})
	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_Enums_PG() {
	tc := e2e.NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_enum_flow")
	dstTableName := s.attachSchemaSuffix("test_enum_flow_dst")
	createMoodEnum := "CREATE TYPE mood AS ENUM ('happy', 'sad', 'angry');"
	_, enumErr := s.Conn().Exec(context.Background(), createMoodEnum)
	if enumErr != nil &&
		!shared.IsSQLStateError(enumErr, pgerrcode.DuplicateObject, pgerrcode.UniqueViolation) {
		require.NoError(s.t, enumErr)
	}
	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			my_mood mood,
			my_null_mood mood
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_enum_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 100

	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(my_mood, my_null_mood) VALUES ('happy',null)
			`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Inserted enums into the source table")
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize enum", func() bool {
		return s.checkEnums(srcTableName, dstTableName) == nil
	})

	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_Composite_PKey_PG() {
	tc := e2e.NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_simple_cpkey")
	dstTableName := s.attachSchemaSuffix("test_simple_cpkey_dst")

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

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize 10 rows", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t") == nil
	})

	_, err = s.Conn().Exec(context.Background(),
		fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=$1`, srcTableName), 1)
	e2e.EnvNoError(s.t, env, err)
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=$1`, srcTableName), 0)
	e2e.EnvNoError(s.t, env, err)
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize modifications", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t") == nil
	})
	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_Composite_PKey_Toast_1_PG() {
	tc := e2e.NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_cpkey_toast1")
	randomString := s.attachSchemaSuffix("random_string")
	dstTableName := s.attachSchemaSuffix("test_cpkey_toast1_dst")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT GENERATED ALWAYS AS IDENTITY,
			c1 INT GENERATED BY DEFAULT AS IDENTITY,
			c2 INT,
			t TEXT,
			t2 TEXT,
			PRIMARY KEY(id,t)
		);CREATE OR REPLACE FUNCTION %s( int ) RETURNS TEXT as $$
		SELECT string_agg(substring('0123456789bcdfghjkmnpqrstvwxyz',
		round(random() * 30)::integer, 1), '') FROM generate_series(1, $1);
		$$ language sql;
	`, srcTableName, randomString))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_cpkey_toast1_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 100

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
			INSERT INTO %s(c2,t,t2) VALUES ($1,$2,%s(9000))
		`, srcTableName, randomString), i, testValue)
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

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize tx", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t,t2") == nil
	})
	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_Composite_PKey_Toast_2_PG() {
	tc := e2e.NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_cpkey_toast2")
	randomString := s.attachSchemaSuffix("random_string")
	dstTableName := s.attachSchemaSuffix("test_cpkey_toast2_dst")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT GENERATED ALWAYS AS IDENTITY,
			c1 INT GENERATED BY DEFAULT AS IDENTITY,
			c2 INT,
			t TEXT,
			t2 TEXT,
			PRIMARY KEY(id,t)
		);CREATE OR REPLACE FUNCTION %s( int ) RETURNS TEXT as $$
		SELECT string_agg(substring('0123456789bcdfghjkmnpqrstvwxyz',
		round(random() * 30)::integer, 1), '') FROM generate_series(1, $1);
		$$ language sql;
	`, srcTableName, randomString))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_cpkey_toast2_flow"),
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
			INSERT INTO %s(c2,t,t2) VALUES ($1,$2,%s(9000))
		`, srcTableName, randomString), i, testValue)
		e2e.EnvNoError(s.t, env, err)
	}
	s.t.Log("Inserted 10 rows into the source table")

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize 10 rows", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t,t2") == nil
	})
	_, err = s.Conn().Exec(context.Background(),
		fmt.Sprintf(`UPDATE %s SET c1=c1+1 WHERE MOD(c2,2)=$1`, srcTableName), 1)
	e2e.EnvNoError(s.t, env, err)
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s WHERE MOD(c2,2)=$1`, srcTableName), 0)
	e2e.EnvNoError(s.t, env, err)

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize update", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t,t2") == nil
	})

	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_PeerDB_Columns() {
	tc := e2e.NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_peerdb_cols")
	dstTableName := s.attachSchemaSuffix("test_peerdb_cols_dst")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_peerdb_cols_mirror"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
		SoftDelete:       true,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 100

	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	// insert 1 row into the source table
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(
		"INSERT INTO %s(key, value) VALUES ('test_key', 'test_value')", srcTableName))
	e2e.EnvNoError(s.t, env, err)

	// delete that row
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(
		"DELETE FROM %s WHERE id=1", srcTableName))
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Inserted and deleted a row for peerdb column check")

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize insert/delete", func() bool {
		return s.checkPeerdbColumns(dstTableName, 1) == nil
	})
	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_Soft_Delete_Basic() {
	tc := e2e.NewTemporalClient(s.t)

	cmpTableName := s.attachSchemaSuffix("test_softdel")
	srcTableName := cmpTableName + "_src"
	dstTableName := s.attachSchemaSuffix("test_softdel_dst")

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
		FlowJobName:     s.attachSuffix("test_softdel"),
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
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize row", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t") == nil
	})
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			UPDATE %s SET c1=c1+4 WHERE id=1`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize update", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t") == nil
	})
	// since we delete stuff, create another table to compare with
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			CREATE TABLE %s AS SELECT * FROM %s`, cmpTableName, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1`, srcTableName))
	e2e.EnvNoError(s.t, env, err)

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize delete", func() bool {
		return s.comparePGTables(srcTableName, dstTableName+` WHERE NOT "_PEERDB_IS_DELETED"`, "id,c1,c2,t") == nil
	})

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)

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
	tc := e2e.NewTemporalClient(s.t)

	cmpTableName := s.attachSchemaSuffix("test_softdel_iud")
	srcTableName := cmpTableName + "_src"
	dstTableName := s.attachSchemaSuffix("test_softdel_iud_dst")

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

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize tx", func() bool {
		return s.comparePGTables(cmpTableName, dstTableName, "id,c1,c2,t") == nil
	})

	softDeleteQuery := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE "_PEERDB_IS_DELETED"`, dstTableName)
	e2e.EnvWaitFor(s.t, env, time.Minute, "normalize soft delete", func() bool {
		numRows, err := s.RunInt64Query(softDeleteQuery)
		return err == nil && numRows == 1
	})

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_Soft_Delete_UD_Same_Batch() {
	cmpTableName := s.attachSchemaSuffix("test_softdel_ud")
	srcTableName := cmpTableName + "_src"
	dstTableName := s.attachSchemaSuffix("test_softdel_ud_dst")

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
		FlowJobName:     s.attachSuffix("test_softdel_ud"),
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
	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, config, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, config)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(c1,c2,t) VALUES (1,2,random_string(9000))`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize row", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t") == nil
	})

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

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize transaction", func() bool {
		return s.comparePGTables(srcTableName,
			dstTableName+` WHERE NOT "_PEERDB_IS_DELETED"`, "id,c1,c2,t") == nil
	})

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)

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
	tc := e2e.NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_softdel_iad")
	dstTableName := s.attachSchemaSuffix("test_softdel_iad_dst")

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
		FlowJobName:     s.attachSuffix("test_softdel_iad"),
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
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize row", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t") == nil
	})
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize delete", func() bool {
		return s.comparePGTables(srcTableName, dstTableName+` WHERE NOT "_PEERDB_IS_DELETED"`, "id,c1,c2,t") == nil
	})
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(id,c1,c2,t) VALUES (1,3,4,random_string(10000))`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize reinsert", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,c1,c2,t") == nil
	})

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)

	softDeleteQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s WHERE "_PEERDB_IS_DELETED"`,
		dstTableName)
	numRows, err := s.RunInt64Query(softDeleteQuery)
	require.NoError(s.t, err)
	require.Equal(s.t, int64(0), numRows)
}

func (s PeerFlowE2ETestSuitePG) Test_Supported_Mixed_Case_Table() {
	tc := e2e.NewTemporalClient(s.t)

	stmtSrcTableName := fmt.Sprintf(`e2e_test_%s."%s"`, s.suffix, "testMixedCase")
	srcTableName := s.attachSchemaSuffix("testMixedCase")
	stmtDstTableName := fmt.Sprintf(`e2e_test_%s."%s"`, s.suffix, "testMixedCaseDst")
	dstTableName := s.attachSchemaSuffix("testMixedCaseDst")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
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
		SourceName:   e2e.GeneratePostgresPeer(s.t).Name,
		MaxBatchSize: 100,
	}

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert and delete rows in the table.
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, config, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, config)
	// insert 20 rows into the source table
	for i := range 10 {
		testKey := fmt.Sprintf("test_key_%d", i)
		testValue := fmt.Sprintf("test_value_%d", i)
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s ("highGold","eVe") VALUES ($1, $2)
		`, stmtSrcTableName), testKey, testValue)
		e2e.EnvNoError(s.t, env, err)
	}
	s.t.Log("Inserted 20 rows into the source table")

	e2e.EnvWaitFor(s.t, env, 1*time.Minute, "normalize mixed case", func() bool {
		return s.comparePGTables(stmtSrcTableName, stmtDstTableName,
			"id,\"pulseArmor\",\"highGold\",\"eVe\"") == nil
	})

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
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
		SourceName:                  e2e.GeneratePostgresPeer(s.t).Name,
		CdcStagingPath:              "",
		SnapshotMaxParallelWorkers:  4,
		SnapshotNumTablesInParallel: 3,
	}

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, config, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, config)
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)
	for _, tm := range config.TableMappings {
		require.NoError(s.t, s.comparePGTables(tm.SourceTableIdentifier, tm.DestinationTableIdentifier, "id,address,asset_id"))
	}
}

func (s PeerFlowE2ETestSuitePG) Test_ContinueAsNew() {
	srcTableName := s.attachSchemaSuffix("test_continueasnew")
	dstTableName := s.attachSchemaSuffix("test_continueasnew_dst")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_continueasnew_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 2
	flowConnConfig.IdleTimeoutSeconds = 10

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)

	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	for i := range 144 {
		testKey := fmt.Sprintf("test_key_%d", i)
		testValue := fmt.Sprintf("test_value_%d", i)
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(key, value) VALUES ($1, $2)
		`, srcTableName), testKey, testValue)
		e2e.EnvNoError(s.t, env, err)
	}
	s.t.Log("Inserted 144 rows into the source table")

	e2e.EnvWaitFor(s.t, env, 4*time.Minute, "normalize 72 syncs", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,key,value") == nil
	})
	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_Dynamic_Mirror_Config_Via_Signals() {
	srcTable1Name := s.attachSchemaSuffix("test_dynconfig_1")
	srcTable2Name := s.attachSchemaSuffix("test_dynconfig_2")
	dstTable1Name := s.attachSchemaSuffix("test_dynconfig_1_dst")
	dstTable2Name := s.attachSchemaSuffix("test_dynconfig_2_dst")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
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

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, config, nil)

	addRows := func(numRows int) {
		for range numRows {
			_, err = s.Conn().Exec(context.Background(),
				fmt.Sprintf(`INSERT INTO %s DEFAULT VALUES`, srcTable1Name))
			e2e.EnvNoError(s.t, env, err)
			_, err = s.Conn().Exec(context.Background(),
				fmt.Sprintf(`INSERT INTO %s DEFAULT VALUES`, srcTable2Name))
			e2e.EnvNoError(s.t, env, err)
		}
		s.t.Logf("Inserted %d rows into the source table", numRows)
	}

	// add before to test initial load too.
	addRows(18)
	e2e.SetupCDCFlowStatusQuery(s.t, env, config)
	// insert 18 rows into the source tables, exactly 3 batches
	addRows(18)

	e2e.EnvWaitFor(s.t, env, 1*time.Minute, "normalize 18 records - first table", func() bool {
		return s.comparePGTables(srcTable1Name, dstTable1Name, "id,t") == nil
	})

	workflowState := e2e.EnvGetWorkflowState(s.t, env)
	assert.EqualValues(s.t, 7, workflowState.SyncFlowOptions.IdleTimeoutSeconds)
	assert.EqualValues(s.t, 6, workflowState.SyncFlowOptions.BatchSize)
	assert.Len(s.t, workflowState.SyncFlowOptions.TableMappings, 1)
	assert.Len(s.t, workflowState.SyncFlowOptions.SrcTableIdNameMapping, 1)

	if !s.t.Failed() {
		e2e.SignalWorkflow(env, model.FlowSignal, model.PauseSignal)
		e2e.EnvWaitFor(s.t, env, 1*time.Minute, "paused workflow", func() bool {
			return e2e.EnvGetFlowStatus(s.t, env) == protos.FlowStatus_STATUS_PAUSED
		})

		_, err = s.Conn().Exec(context.Background(),
			`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
			 WHERE query LIKE '%START_REPLICATION%' AND query LIKE '%dynconfig%' AND backend_type='walsender'`)
		require.NoError(s.t, err)
		time.Sleep(5 * time.Second)

		// add rows to both tables before resuming - should handle
		addRows(18)

		e2e.SignalWorkflow(env, model.CDCDynamicPropertiesSignal, &protos.CDCFlowConfigUpdate{
			IdleTimeout: 14,
			BatchSize:   12,
			AdditionalTables: []*protos.TableMapping{
				{
					SourceTableIdentifier:      srcTable2Name,
					DestinationTableIdentifier: dstTable2Name,
				},
			},
		})

		e2e.EnvWaitFor(s.t, env, 1*time.Minute, "resumed workflow", func() bool {
			return e2e.EnvGetFlowStatus(s.t, env) == protos.FlowStatus_STATUS_RUNNING
		})
		e2e.EnvWaitFor(s.t, env, 2*time.Minute, "normalize 18 records - first table", func() bool {
			return s.comparePGTables(srcTable1Name, dstTable1Name, "id,t") == nil
		})
		e2e.EnvWaitFor(s.t, env, 2*time.Minute, "initial load + normalize 18 records - second table", func() bool {
			return s.comparePGTables(srcTable2Name, dstTable2Name, "id,t") == nil
		})

		workflowState = e2e.EnvGetWorkflowState(s.t, env)
		assert.EqualValues(s.t, 14, workflowState.SyncFlowOptions.IdleTimeoutSeconds)
		assert.EqualValues(s.t, 12, workflowState.SyncFlowOptions.BatchSize)
		assert.Len(s.t, workflowState.SyncFlowOptions.TableMappings, 2)
		assert.Len(s.t, workflowState.SyncFlowOptions.SrcTableIdNameMapping, 2)
	}

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_CustomSync() {
	srcTableName := s.attachSchemaSuffix("test_customsync")
	dstTableName := s.attachSchemaSuffix("test_customsync_dst")

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_customsync_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`, srcTableName))

	require.NoError(s.t, err)
	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.SignalWorkflow(env, model.FlowSignal, model.PauseSignal)
	e2e.EnvWaitFor(s.t, env, 1*time.Minute, "paused workflow", func() bool {
		return e2e.EnvGetFlowStatus(s.t, env) == protos.FlowStatus_STATUS_PAUSED
	})

	e2e.SignalWorkflow(env, model.CDCDynamicPropertiesSignal, &protos.CDCFlowConfigUpdate{
		NumberOfSyncs: 1,
	})
	e2e.EnvWaitFor(s.t, env, 1*time.Minute, "resumed workflow", func() bool {
		return e2e.EnvGetFlowStatus(s.t, env) == protos.FlowStatus_STATUS_RUNNING
	})

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(
		"INSERT INTO %s(key, value) VALUES ('test_key', 'test_value')", srcTableName))
	e2e.EnvNoError(s.t, env, err)
	e2e.EnvWaitFor(s.t, env, 1*time.Minute, "paused workflow", func() bool {
		return e2e.EnvGetFlowStatus(s.t, env) == protos.FlowStatus_STATUS_PAUSED
	})

	require.NoError(s.t, s.comparePGTables(srcTableName, dstTableName, "id,key,value"))
	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_TypeSystem_PG() {
	srcTableName := s.attachSchemaSuffix("test_typesystem_pg")
	dstTableName := s.attachSchemaSuffix("test_typesystem_pg_dst")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
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
		_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		insert into %s (updated_at, j, jb, aa32, currency) values (
			NOW(),'{"b" : 123}','{"b" : 123}','{{3,2,1},{6,5,4},{9,8,7}}','ISK'
		)`, srcTableName))
		require.NoError(s.t, err)
	}

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_typesystem_pg"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.System = protos.TypeSystem_PG
	flowConnConfig.SoftDeleteColName = ""
	flowConnConfig.SyncedAtColName = ""

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)

	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		insert into %s (updated_at, j, jb, aa32, currency) values (
			NOW(),'{"b" : 123}','{"b" : 123}','{{3,2,1},{6,5,4},{9,8,7}}','ISK'
		)`, srcTableName))
	e2e.EnvNoError(s.t, env, err)

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize rows", func() bool {
		err := s.comparePGTables(srcTableName, dstTableName, "id,created_at,updated_at,j::text,jb,aa32,currency")
		if err != nil {
			s.t.Log(err.Error())
		}
		return err == nil
	})

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) Test_TransformRecordScript() {
	srcTableName := s.attachSchemaSuffix("test_transrecord_pg")
	dstTableName := s.attachSchemaSuffix("test_transrecord_pg_dst")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		create table %[1]s (
			id uuid not null primary key default gen_random_uuid(),
			val int
		)`, srcTableName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(context.Background(), `insert into public.scripts (name, lang, source) values
		('cdc_transform_record', 'lua', 'function transformRecord(r) if r.row then r.row.val = 1729 end end') on conflict do nothing`)
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_transrecord_pg"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.Script = "cdc_transform_record"

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)

	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf("insert into %s (val) values (1)", srcTableName))
	e2e.EnvNoError(s.t, env, err)

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize rows", func() bool {
		err := s.compareCounts(dstTableName, 1)
		if err != nil {
			s.t.Log(err.Error())
		}
		return err == nil
	})

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)

	var exists bool
	err = s.Conn().QueryRow(context.Background(),
		fmt.Sprintf("select exists(select * from %s where val <> 1729)", dstTableName)).Scan(&exists)
	require.NoError(s.t, err)
	require.False(s.t, exists)
}

func (s PeerFlowE2ETestSuitePG) Test_TransformRowScript() {
	srcTableName := s.attachSchemaSuffix("test_transrow_pg")
	dstTableName := s.attachSchemaSuffix("test_transrow_pg_dst")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		create table %[1]s (
			id uuid not null primary key default gen_random_uuid(),
			val int
		)`, srcTableName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(context.Background(), `insert into public.scripts (name, lang, source) values
	('cdc_transform_row', 'lua', 'function transformRow(r) r.val = 1729 end') on conflict do nothing`)
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_transrow_pg"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.Script = "cdc_transform_row"

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)

	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf("insert into %s (val) values (1)", srcTableName))
	e2e.EnvNoError(s.t, env, err)

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize rows", func() bool {
		err := s.compareCounts(dstTableName, 1)
		if err != nil {
			s.t.Log(err.Error())
		}
		return err == nil
	})

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)

	var exists bool
	err = s.Conn().QueryRow(context.Background(),
		fmt.Sprintf("select exists(select * from %s where val <> 1729)", dstTableName)).Scan(&exists)
	require.NoError(s.t, err)
	require.False(s.t, exists)
}

func (s PeerFlowE2ETestSuitePG) Test_Mixed_Case_Schema_Changes_PG() {
	tc := e2e.NewTemporalClient(s.t)

	srcTableName := "test_mixed_case_schema_changes_PG"
	dstTableName := srcTableName + "_dst"
	quotedSourceTableName := s.attachSchemaSuffix(`"` + srcTableName + `"`)
	quotedDestTableName := s.attachSchemaSuffix(`"` + dstTableName + `"`)
	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
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
		SourceName:   e2e.GeneratePostgresPeer(s.t).Name,
		MaxBatchSize: 100,
	}

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert and mutate schema repeatedly.
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	// insert first row.
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1) VALUES (1)`, quotedSourceTableName))
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Inserted initial row in the source table")
	e2e.EnvWaitFor(s.t, env, 1*time.Minute, "normalize mixed case", func() bool {
		return s.comparePGTables(quotedSourceTableName, quotedDestTableName,
			"id,c1") == nil
	})
	// alter source table, add column c2 and insert another row.
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s ADD COLUMN "myC2" BIGINT`, quotedSourceTableName))
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Altered source table, added column myC2")
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,"myC2") VALUES (2,2)`, quotedSourceTableName))
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Inserted row with added myC2 in the source table")

	// verify we got our two rows, if schema did not match up it will error.
	e2e.EnvWaitFor(s.t, env, 1*time.Minute, "normalize mixed case schema change", func() bool {
		return s.comparePGTables(quotedSourceTableName, quotedDestTableName,
			"id,c1,\"myC2\"") == nil
	})

	// alter source table, add column c3, drop column c2 and insert another row.
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s DROP COLUMN "myC2", ADD COLUMN c3 FLOAT`, quotedSourceTableName))
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Altered source table, dropped column myC2 and added column c3")
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,c3) VALUES (3,3.5)`, quotedSourceTableName))
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Inserted row with added c3 in the source table")

	// verify we got our two rows, if schema did not match up it will error.
	e2e.EnvWaitFor(s.t, env, 1*time.Minute, "normalize mixed case schema change", func() bool {
		return s.comparePGTables(quotedSourceTableName, quotedDestTableName,
			"id,c1,c3") == nil
	})
	// alter source table, drop column c3 and insert another row.
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s DROP COLUMN c3`, quotedSourceTableName))
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Altered source table, dropped column c3")
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1) VALUES (4)`, quotedSourceTableName))
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Inserted row after dropping all columns in the source table")

	// verify we got our two rows, if schema did not match up it will error.
	e2e.EnvWaitFor(s.t, env, 1*time.Minute, "normalize mixed case schema change", func() bool {
		return s.comparePGTables(quotedSourceTableName, quotedDestTableName,
			"id,c1") == nil
	})
	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}
