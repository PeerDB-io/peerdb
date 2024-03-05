package e2e_postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
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

func (s PeerFlowE2ETestSuitePG) WaitForSchema(
	env e2e.WorkflowRun,
	reason string,
	srcTableName string,
	dstTableName string,
	cols string,
	expectedSchema *protos.TableSchema,
) {
	s.t.Helper()
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, reason, func() bool {
		s.t.Helper()
		output, err := s.conn.GetTableSchema(context.Background(), &protos.GetTableSchemaBatchInput{
			TableIdentifiers: []string{dstTableName},
		})
		if err != nil {
			return false
		}
		tableSchema := output.TableNameSchemaMapping[dstTableName]
		if !e2e.CompareTableSchemas(expectedSchema, tableSchema) {
			s.t.Log("schemas unequal", expectedSchema, tableSchema)
			return false
		}
		return s.comparePGTables(srcTableName, dstTableName, cols) == nil
	})
}

func (s PeerFlowE2ETestSuitePG) Test_Simple_Flow_PG() {
	srcTableName := s.attachSchemaSuffix("test_simple_flow")
	dstTableName := s.attachSchemaSuffix("test_simple_flow_dst")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL,
			myh HSTORE NOT NULL
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_simple_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.peer,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)

	e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
	// insert 10 rows into the source table
	for i := range 10 {
		testKey := fmt.Sprintf("test_key_%d", i)
		testValue := fmt.Sprintf("test_value_%d", i)
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(key, value, myh) VALUES ($1, $2, '"a"=>"b"')
		`, srcTableName), testKey, testValue)
		e2e.EnvNoError(s.t, env, err)
	}
	s.t.Log("Inserted 10 rows into the source table")

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize 10 rows", func() bool {
		return s.comparePGTables(srcTableName, dstTableName, "id,key,value") == nil
	})
	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)
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
		Destination:      s.peer,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)

	e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
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
	CREATE TABLE IF NOT EXISTS %s (id serial PRIMARY KEY,c1 BIGINT,c2 BIT,c4 BOOLEAN,
		c7 CHARACTER,c8 varchar,c9 CIDR,c11 DATE,c12 FLOAT,c13 DOUBLE PRECISION,
		c14 INET,c15 INTEGER,c21 MACADDR,
		c29 SMALLINT,c32 TEXT,
		c33 TIMESTAMP,c34 TIMESTAMPTZ,c35 TIME, c36 TIMETZ,
		c40 UUID, c42 INT[], c43 FLOAT[], c44 TEXT[],
		c46 DATE[], c47 TIMESTAMPTZ[], c48 TIMESTAMP[], c49 BOOLEAN[], c50 SMALLINT[]);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_types_pg"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.peer,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s SELECT 2,2,b'1',
			true,'s','test','1.1.10.2'::cidr,
			CURRENT_DATE,1.23,1.234,'192.168.1.5'::inet,1,
			'08:00:2b:01:02:03'::macaddr,
			1,'test',now(),now(),now()::time,now()::timetz,
			'66073c38-b8df-4bdb-bbca-1c97596b8940'::uuid,
			ARRAY[10299301,2579827],
			ARRAY[0.0003, 8902.0092],
			ARRAY['hello','bye'],
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
		"c7", "c8", "c32", "c42", "c43", "c44", "c46", "c47", "c48", "c49", "c50",
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
	var pgErr *pgconn.PgError
	_, enumErr := s.Conn().Exec(context.Background(), createMoodEnum)
	if errors.As(enumErr, &pgErr) && pgErr.Code != pgerrcode.DuplicateObject && !utils.IsUniqueError(enumErr) {
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
		Destination:      s.peer,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
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

func (s PeerFlowE2ETestSuitePG) Test_Simple_Schema_Changes_PG() {
	tc := e2e.NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_simple_schema_changes")
	dstTableName := s.attachSchemaSuffix("test_simple_schema_changes_dst")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			c1 BIGINT
		);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_simple_schema_changes"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.peer,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 1

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert and mutate schema repeatedly.
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)

	// insert first row.
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1) VALUES ($1)`, srcTableName), 1)
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Inserted initial row in the source table")

	s.WaitForSchema(env, "normalizing first row", srcTableName, dstTableName, "id,c1", &protos.TableSchema{
		TableIdentifier:   dstTableName,
		PrimaryKeyColumns: []string{"id"},
		Columns: []*protos.FieldDescription{
			{
				Name:         "id",
				Type:         string(qvalue.QValueKindInt64),
				TypeModifier: -1,
			},
			{
				Name:         "c1",
				Type:         string(qvalue.QValueKindInt64),
				TypeModifier: -1,
			},
			{
				Name:         "_PEERDB_SYNCED_AT",
				Type:         string(qvalue.QValueKindTimestamp),
				TypeModifier: -1,
			},
		},
	})

	// alter source table, add column c2 and insert another row.
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s ADD COLUMN c2 BIGINT`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Altered source table, added column c2")
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,c2) VALUES ($1,$2)`, srcTableName), 2, 2)
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Inserted row with added c2 in the source table")

	s.WaitForSchema(env, "normalizing altered row", srcTableName, dstTableName, "id,c1,c2", &protos.TableSchema{
		TableIdentifier:   dstTableName,
		PrimaryKeyColumns: []string{"id"},
		Columns: []*protos.FieldDescription{
			{
				Name:         "id",
				Type:         string(qvalue.QValueKindInt64),
				TypeModifier: -1,
			},
			{
				Name:         "c1",
				Type:         string(qvalue.QValueKindInt64),
				TypeModifier: -1,
			},
			{
				Name:         "_PEERDB_SYNCED_AT",
				Type:         string(qvalue.QValueKindTimestamp),
				TypeModifier: -1,
			},
			{
				Name:         "c2",
				Type:         string(qvalue.QValueKindInt64),
				TypeModifier: -1,
			},
		},
	})

	// alter source table, add column c3, drop column c2 and insert another row.
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s DROP COLUMN c2, ADD COLUMN c3 BIGINT`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Altered source table, dropped column c2 and added column c3")
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,c3) VALUES ($1,$2)`, srcTableName), 3, 3)
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Inserted row with added c3 in the source table")

	s.WaitForSchema(env, "normalizing dropped column row", srcTableName, dstTableName, "id,c1,c3", &protos.TableSchema{
		TableIdentifier:   dstTableName,
		PrimaryKeyColumns: []string{"id"},
		Columns: []*protos.FieldDescription{
			{
				Name:         "id",
				Type:         string(qvalue.QValueKindInt64),
				TypeModifier: -1,
			},
			{
				Name:         "c1",
				Type:         string(qvalue.QValueKindInt64),
				TypeModifier: -1,
			},
			{
				Name:         "c2",
				Type:         string(qvalue.QValueKindInt64),
				TypeModifier: -1,
			},
			{
				Name:         "_PEERDB_SYNCED_AT",
				Type:         string(qvalue.QValueKindTimestamp),
				TypeModifier: -1,
			},
			{
				Name:         "c3",
				Type:         string(qvalue.QValueKindInt64),
				TypeModifier: -1,
			},
		},
	})

	// alter source table, drop column c3 and insert another row.
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s DROP COLUMN c3`, srcTableName))
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Altered source table, dropped column c3")
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1) VALUES ($1)`, srcTableName), 4)
	e2e.EnvNoError(s.t, env, err)
	s.t.Log("Inserted row after dropping all columns in the source table")

	s.WaitForSchema(env, "normalizing 2nd dropped column row", srcTableName, dstTableName, "id,c1", &protos.TableSchema{
		TableIdentifier:   dstTableName,
		PrimaryKeyColumns: []string{"id"},
		Columns: []*protos.FieldDescription{
			{
				Name:         "id",
				Type:         string(qvalue.QValueKindInt64),
				TypeModifier: -1,
			},
			{
				Name:         "c1",
				Type:         string(qvalue.QValueKindInt64),
				TypeModifier: -1,
			},
			{
				Name:         "_PEERDB_SYNCED_AT",
				Type:         string(qvalue.QValueKindTimestamp),
				TypeModifier: -1,
			},
			{
				Name:         "c2",
				Type:         string(qvalue.QValueKindInt64),
				TypeModifier: -1,
			},
			{
				Name:         "c3",
				Type:         string(qvalue.QValueKindInt64),
				TypeModifier: -1,
			},
		},
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
		Destination:      s.peer,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
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
		Destination:      s.peer,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
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
		Destination:      s.peer,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)

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
		Destination:      s.peer,
		SoftDelete:       true,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 100

	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
	// insert 1 row into the source table
	testKey := fmt.Sprintf("test_key_%d", 1)
	testValue := fmt.Sprintf("test_value_%d", 1)
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s(key, value) VALUES ($1, $2)
		`, srcTableName), testKey, testValue)
	e2e.EnvNoError(s.t, env, err)

	// delete that row
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
			DELETE FROM %s WHERE id=1
		`, srcTableName))
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

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("test_softdel"),
	}

	config := &protos.FlowConnectionConfigs{
		FlowJobName: connectionGen.FlowJobName,
		Destination: s.peer,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: dstTableName,
			},
		},
		Source:            e2e.GeneratePostgresPeer(),
		CdcStagingPath:    connectionGen.CdcStagingPath,
		SoftDelete:        true,
		SoftDeleteColName: "_PEERDB_IS_DELETED",
		SyncedAtColName:   "_PEERDB_SYNCED_AT",
		MaxBatchSize:      100,
	}

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, config, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)

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

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("test_softdel_iud"),
	}

	config := &protos.FlowConnectionConfigs{
		FlowJobName: connectionGen.FlowJobName,
		Destination: s.peer,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: dstTableName,
			},
		},
		Source:            e2e.GeneratePostgresPeer(),
		CdcStagingPath:    connectionGen.CdcStagingPath,
		SoftDelete:        true,
		SoftDeleteColName: "_PEERDB_IS_DELETED",
		SyncedAtColName:   "_PEERDB_SYNCED_AT",
		MaxBatchSize:      100,
	}

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, config, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)

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

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("test_softdel_ud"),
	}

	config := &protos.FlowConnectionConfigs{
		FlowJobName: connectionGen.FlowJobName,
		Destination: s.peer,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: dstTableName,
			},
		},
		Source:            e2e.GeneratePostgresPeer(),
		CdcStagingPath:    connectionGen.CdcStagingPath,
		SoftDelete:        true,
		SoftDeleteColName: "_PEERDB_IS_DELETED",
		SyncedAtColName:   "_PEERDB_SYNCED_AT",
		MaxBatchSize:      100,
	}

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert, update and delete rows in the table.
	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, config, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)

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

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("test_softdel_iad"),
	}

	config := &protos.FlowConnectionConfigs{
		FlowJobName: connectionGen.FlowJobName,
		Destination: s.peer,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: dstTableName,
			},
		},
		Source:            e2e.GeneratePostgresPeer(),
		CdcStagingPath:    connectionGen.CdcStagingPath,
		SoftDelete:        true,
		SoftDeleteColName: "_PEERDB_IS_DELETED",
		SyncedAtColName:   "_PEERDB_SYNCED_AT",
		MaxBatchSize:      100,
	}

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert and delete rows in the table.
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, config, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)

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

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("test_mixed_case"),
	}

	config := &protos.FlowConnectionConfigs{
		FlowJobName: connectionGen.FlowJobName,
		Destination: s.peer,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: dstTableName,
			},
		},
		Source:         e2e.GeneratePostgresPeer(),
		CdcStagingPath: connectionGen.CdcStagingPath,
		MaxBatchSize:   100,
	}

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert and delete rows in the table.
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, config, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
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
		FlowJobName:      s.attachSuffix("test_simple_flow"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.peer,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 2

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)

	e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
	for i := range 180 {
		testKey := fmt.Sprintf("test_key_%d", i)
		testValue := fmt.Sprintf("test_value_%d", i)
		_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(key, value) VALUES ($1, $2)
		`, srcTableName), testKey, testValue)
		e2e.EnvNoError(s.t, env, err)
	}
	s.t.Log("Inserted 180 rows into the source table")

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize 90 syncs", func() bool {
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

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("test_dynconfig"),
	}

	config := &protos.FlowConnectionConfigs{
		FlowJobName: connectionGen.FlowJobName,
		Destination: s.peer,
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTable1Name,
				DestinationTableIdentifier: dstTable1Name,
			},
		},
		Source:                      e2e.GeneratePostgresPeer(),
		CdcStagingPath:              connectionGen.CdcStagingPath,
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

	getWorkflowState := func() peerflow.CDCFlowWorkflowState {
		var state peerflow.CDCFlowWorkflowState
		val, err := env.Query(shared.CDCFlowStateQuery)
		e2e.EnvNoError(s.t, env, err)
		err = val.Get(&state)
		e2e.EnvNoError(s.t, env, err)

		return state
	}

	getFlowStatus := func() protos.FlowStatus {
		var flowStatus protos.FlowStatus
		val, err := env.Query(shared.FlowStatusQuery)
		e2e.EnvNoError(s.t, env, err)
		err = val.Get(&flowStatus)
		e2e.EnvNoError(s.t, env, err)

		return flowStatus
	}

	// add before to test initial load too.
	addRows(18)
	e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
	// insert 18 rows into the source tables, exactly 3 batches
	addRows(18)

	e2e.EnvWaitFor(s.t, env, 1*time.Minute, "normalize 18 records - first table", func() bool {
		return s.comparePGTables(srcTable1Name, dstTable1Name, "id,t") == nil
	})

	workflowState := getWorkflowState()
	assert.EqualValues(s.t, 7, workflowState.SyncFlowOptions.IdleTimeoutSeconds)
	assert.EqualValues(s.t, 6, workflowState.SyncFlowOptions.BatchSize)
	assert.Len(s.t, workflowState.SyncFlowOptions.TableMappings, 1)
	assert.Len(s.t, workflowState.SyncFlowOptions.SrcTableIdNameMapping, 1)
	assert.Len(s.t, workflowState.SyncFlowOptions.TableNameSchemaMapping, 1)

	if !s.t.Failed() {
		addRows(1)
		e2e.SignalWorkflow(env, model.FlowSignal, model.PauseSignal)
		addRows(1)
		e2e.EnvWaitFor(s.t, env, 1*time.Minute, "paused workflow", func() bool {
			// keep adding 1 more row - finishing another sync
			addRows(1)

			flowStatus := getFlowStatus()
			return flowStatus == protos.FlowStatus_STATUS_PAUSED
		})

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

		// add rows to both tables before resuming - should handle
		addRows(18)

		e2e.SignalWorkflow(env, model.FlowSignal, model.NoopSignal)

		e2e.EnvWaitFor(s.t, env, 1*time.Minute, "resumed workflow", func() bool {
			return getFlowStatus() == protos.FlowStatus_STATUS_RUNNING
		})
		e2e.EnvWaitFor(s.t, env, 1*time.Minute, "normalize 18 records - first table", func() bool {
			return s.comparePGTables(srcTable1Name, dstTable1Name, "id,t") == nil
		})
		e2e.EnvWaitFor(s.t, env, 2*time.Minute, "initial load + normalize 18 records - second table", func() bool {
			return s.comparePGTables(srcTable2Name, dstTable2Name, "id,t") == nil
		})

		workflowState = getWorkflowState()
		assert.EqualValues(s.t, 14, workflowState.SyncFlowOptions.IdleTimeoutSeconds)
		assert.EqualValues(s.t, 12, workflowState.SyncFlowOptions.BatchSize)
		assert.Len(s.t, workflowState.SyncFlowOptions.TableMappings, 2)
		assert.Len(s.t, workflowState.SyncFlowOptions.SrcTableIdNameMapping, 2)
		assert.Len(s.t, workflowState.SyncFlowOptions.TableNameSchemaMapping, 2)
	}

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}
