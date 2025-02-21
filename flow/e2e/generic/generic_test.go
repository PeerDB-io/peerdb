package e2e_generic

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connmysql "github.com/PeerDB-io/peerdb/flow/connectors/mysql"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/e2e"
	e2e_bigquery "github.com/PeerDB-io/peerdb/flow/e2e/bigquery"
	e2e_clickhouse "github.com/PeerDB-io/peerdb/flow/e2e/clickhouse"
	e2e_postgres "github.com/PeerDB-io/peerdb/flow/e2e/postgres"
	e2e_snowflake "github.com/PeerDB-io/peerdb/flow/e2e/snowflake"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	peerflow "github.com/PeerDB-io/peerdb/flow/workflows"
)

func TestGenericPG(t *testing.T) {
	e2eshared.RunSuite(t, SetupGenericSuite(e2e_postgres.SetupSuite))
}

func TestGenericSF(t *testing.T) {
	e2eshared.RunSuite(t, SetupGenericSuite(e2e_snowflake.SetupSuite))
}

func TestGenericBQ(t *testing.T) {
	e2eshared.RunSuite(t, SetupGenericSuite(e2e_bigquery.SetupSuite))
}

func TestGenericCH_PG(t *testing.T) {
	e2eshared.RunSuite(t, SetupGenericSuite(e2e_clickhouse.SetupSuite(t, e2e.SetupPostgres)))
}

func TestGenericCH_MySQL(t *testing.T) {
	e2eshared.RunSuite(t, SetupGenericSuite(e2e_clickhouse.SetupSuite(t, e2e.SetupMySQL)))
}

func TestGenericCH_MySQL_FilePos(t *testing.T) {
	e2eshared.RunSuite(t, SetupGenericSuite(e2e_clickhouse.SetupSuite(t, func(t *testing.T, suffix string) (*e2e.MySqlSource, error) {
		t.Helper()
		return e2e.SetupMyCore(t, suffix, false, protos.MySqlReplicationMechanism_MYSQL_FILEPOS)
	})))
}

func TestGenericCH_MariaDB(t *testing.T) {
	e2eshared.RunSuite(t, SetupGenericSuite(e2e_clickhouse.SetupSuite(t, e2e.SetupMariaDB)))
}

type Generic struct {
	e2e.GenericSuite
}

func SetupGenericSuite[T e2e.GenericSuite](f func(t *testing.T) T) func(t *testing.T) Generic {
	return func(t *testing.T) Generic {
		t.Helper()
		return Generic{f(t)}
	}
}

func (s Generic) Test_Simple_Flow() {
	t := s.T()
	srcTable := "test_simple"
	dstTable := "test_simple_dst"
	srcSchemaTable := e2e.AttachSchema(s, srcTable)
	hstoreType := "TEXT"
	if _, isPg := s.Source().Connector().(*connpostgres.PostgresConnector); isPg {
		hstoreType = "HSTORE"
	}

	require.NoError(t, s.Source().Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			ky TEXT NOT NULL,
			value TEXT NOT NULL,
			j JSON NOT NULL,
			myh %s NOT NULL
		);
	`, srcSchemaTable, hstoreType)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, "test_simple"),
		TableMappings: e2e.TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	// insert 10 rows into the source table
	for i := range 10 {
		testKey := fmt.Sprintf("init_key_%d", i)
		testValue := fmt.Sprintf("init_value_%d", i)
		require.NoError(t, s.Source().Exec(
			t.Context(),
			fmt.Sprintf(`INSERT INTO %s(ky, value, j, myh) VALUES ('%s', '%s', '{"a":[1,2,3]}', '"a"=>"b"')`,
				srcSchemaTable, testKey, testValue),
		))
	}

	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)

	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	// insert 10 rows into the source table
	for i := range 10 {
		testKey := fmt.Sprintf("test_key_%d", i)
		testValue := fmt.Sprintf("test_value_%d", i)
		e2e.EnvNoError(t, env, s.Source().Exec(
			t.Context(),
			fmt.Sprintf(`INSERT INTO %s(ky, value, j, myh) VALUES ('%s', '%s', '{"a":[1,2,3]}', '"a"=>"b"')`,
				srcSchemaTable, testKey, testValue),
		))
	}
	t.Log("Inserted 10 rows into the source table")

	e2e.EnvWaitForEqualTablesWithNames(env, s, "normalizing 10 rows", srcTable, dstTable, `id,ky,value,myh`)
	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}

func (s Generic) Test_Simple_Schema_Changes() {
	t := s.T()

	if _, ok := s.Source().(*e2e.MySqlSource); ok {
		t.Skip("mysql connector does not support schema changes yet")
	}

	destinationSchemaConnector, ok := s.DestinationConnector().(connectors.GetTableSchemaConnector)
	if !ok {
		t.Skip("skipping test because destination connector does not implement GetTableSchemaConnector")
	}

	t.Log("Testing simple schema changes")
	srcTable := "test_simple_schema_changes"
	dstTable := "test_simple_schema_changes_dst"
	srcTableName := e2e.AttachSchema(s, srcTable)
	dstTableName := s.DestinationTable(dstTable)
	t.Log("Source table name: ", srcTableName)
	t.Log("Destination table name: ", dstTableName)

	require.NoError(t, s.Source().Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			c1 BIGINT
		);
	`, srcTableName)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, srcTable),
		TableMappings: e2e.TableMappings(s, srcTable, dstTable),
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
				Type:         string(qvalue.QValueKindNumeric),
				TypeModifier: -1,
			},
			{
				Name:         e2e.ExpectedDestinationIdentifier(s, "c1"),
				Type:         string(qvalue.QValueKindNumeric),
				TypeModifier: -1,
			},
			{
				Name:         "_PEERDB_IS_DELETED",
				Type:         string(qvalue.QValueKindBoolean),
				TypeModifier: -1,
			},
			{
				Name:         "_PEERDB_SYNCED_AT",
				Type:         string(qvalue.QValueKindTimestamp),
				TypeModifier: -1,
			},
		},
	}
	output, err := destinationSchemaConnector.GetTableSchema(t.Context(), nil, protos.TypeSystem_Q, []string{dstTableName})
	e2e.EnvNoError(t, env, err)
	e2e.EnvTrue(t, env, e2e.CompareTableSchemas(expectedTableSchema, output[dstTableName]))

	// alter source table, add column c2 and insert another row.
	e2e.EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(`ALTER TABLE %s ADD COLUMN c2 BIGINT`, srcTableName)))
	e2e.EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s(c1,c2) VALUES (2,2)`, srcTableName)))

	// verify we got our two rows, if schema did not match up it will error.
	e2e.EnvWaitForEqualTablesWithNames(env, s, "normalize altered row", srcTable, dstTable, "id,c1,coalesce(c2,0) c2")
	expectedTableSchema = &protos.TableSchema{
		TableIdentifier: e2e.ExpectedDestinationTableName(s, dstTable),
		Columns: []*protos.FieldDescription{
			{
				Name:         e2e.ExpectedDestinationIdentifier(s, "id"),
				Type:         string(qvalue.QValueKindNumeric),
				TypeModifier: -1,
			},
			{
				Name:         e2e.ExpectedDestinationIdentifier(s, "c1"),
				Type:         string(qvalue.QValueKindNumeric),
				TypeModifier: -1,
			},
			{
				Name:         "_PEERDB_SYNCED_AT",
				Type:         string(qvalue.QValueKindTimestamp),
				TypeModifier: -1,
			},
			{
				Name:         e2e.ExpectedDestinationIdentifier(s, "c2"),
				Type:         string(qvalue.QValueKindNumeric),
				TypeModifier: -1,
			},
		},
	}
	output, err = destinationSchemaConnector.GetTableSchema(t.Context(), nil, protos.TypeSystem_Q, []string{dstTableName})
	e2e.EnvNoError(t, env, err)
	e2e.EnvTrue(t, env, e2e.CompareTableSchemas(expectedTableSchema, output[dstTableName]))
	e2e.EnvEqualTablesWithNames(env, s, srcTable, dstTable, "id,c1,coalesce(c2,0) c2")

	// alter source table, add column c3, drop column c2 and insert another row.
	e2e.EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(`ALTER TABLE %s DROP COLUMN c2, ADD COLUMN c3 BIGINT`, srcTableName)))
	e2e.EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s(c1,c3) VALUES (3,3)`, srcTableName)))

	// verify we got our two rows, if schema did not match up it will error.
	e2e.EnvWaitForEqualTablesWithNames(env, s, "normalize dropped c2 column", srcTable, dstTable, "id,c1,coalesce(c3,0) c3")
	expectedTableSchema = &protos.TableSchema{
		TableIdentifier: e2e.ExpectedDestinationTableName(s, dstTable),
		Columns: []*protos.FieldDescription{
			{
				Name:         e2e.ExpectedDestinationIdentifier(s, "id"),
				Type:         string(qvalue.QValueKindNumeric),
				TypeModifier: -1,
			},
			{
				Name:         e2e.ExpectedDestinationIdentifier(s, "c1"),
				Type:         string(qvalue.QValueKindNumeric),
				TypeModifier: -1,
			},
			{
				Name:         "_PEERDB_SYNCED_AT",
				Type:         string(qvalue.QValueKindTimestamp),
				TypeModifier: -1,
			},
			{
				Name:         e2e.ExpectedDestinationIdentifier(s, "c2"),
				Type:         string(qvalue.QValueKindNumeric),
				TypeModifier: -1,
			},
			{
				Name:         e2e.ExpectedDestinationIdentifier(s, "c3"),
				Type:         string(qvalue.QValueKindNumeric),
				TypeModifier: -1,
			},
		},
	}
	output, err = destinationSchemaConnector.GetTableSchema(t.Context(), nil, protos.TypeSystem_Q, []string{dstTableName})
	e2e.EnvNoError(t, env, err)
	e2e.EnvTrue(t, env, e2e.CompareTableSchemas(expectedTableSchema, output[dstTableName]))
	e2e.EnvEqualTablesWithNames(env, s, srcTable, dstTable, "id,c1,coalesce(c3,0) c3")

	// alter source table, drop column c3 and insert another row.
	e2e.EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(`ALTER TABLE %s DROP COLUMN c3`, srcTableName)))
	e2e.EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s(c1) VALUES (4)`, srcTableName)))

	// verify we got our two rows, if schema did not match up it will error.
	e2e.EnvWaitForEqualTablesWithNames(env, s, "normalize dropped c3 column", srcTable, dstTable, "id,c1")
	expectedTableSchema = &protos.TableSchema{
		TableIdentifier: e2e.ExpectedDestinationTableName(s, dstTable),
		Columns: []*protos.FieldDescription{
			{
				Name:         e2e.ExpectedDestinationIdentifier(s, "id"),
				Type:         string(qvalue.QValueKindNumeric),
				TypeModifier: -1,
			},
			{
				Name:         e2e.ExpectedDestinationIdentifier(s, "c1"),
				Type:         string(qvalue.QValueKindNumeric),
				TypeModifier: -1,
			},
			{
				Name:         "_PEERDB_SYNCED_AT",
				Type:         string(qvalue.QValueKindTimestamp),
				TypeModifier: -1,
			},
			{
				Name:         e2e.ExpectedDestinationIdentifier(s, "c2"),
				Type:         string(qvalue.QValueKindNumeric),
				TypeModifier: -1,
			},
			{
				Name:         e2e.ExpectedDestinationIdentifier(s, "c3"),
				Type:         string(qvalue.QValueKindNumeric),
				TypeModifier: -1,
			},
		},
	}
	output, err = destinationSchemaConnector.GetTableSchema(t.Context(), nil, protos.TypeSystem_Q, []string{dstTableName})
	e2e.EnvNoError(t, env, err)
	e2e.EnvTrue(t, env, e2e.CompareTableSchemas(expectedTableSchema, output[dstTableName]))
	e2e.EnvEqualTablesWithNames(env, s, srcTable, dstTable, "id,c1")

	env.Cancel(t.Context())

	e2e.RequireEnvCanceled(t, env)
}

func (s Generic) Test_Partitioned_Table() {
	t := s.T()

	pgSource, ok := s.Source().(*e2e.PostgresSource)
	if !ok {
		t.Skip("test only applies to postgres")
	}
	conn := pgSource.PostgresConnector

	srcTable := "test_partition"
	dstTable := "test_partition_dst"
	srcSchemaTable := e2e.AttachSchema(s, srcTable)

	_, err := conn.Conn().Exec(t.Context(), fmt.Sprintf(`
			CREATE TABLE %[1]s(
				id SERIAL NOT NULL,
				name TEXT,
				created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
				PRIMARY KEY (created_at, id)
			) PARTITION BY RANGE(created_at);
			CREATE TABLE %[1]s_2024q1
				PARTITION OF %[1]s
				FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');
			CREATE TABLE %[1]s_2024q2
				PARTITION OF %[1]s
				FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');
			CREATE TABLE %[1]s_2024q3
				PARTITION OF %[1]s
				FOR VALUES FROM ('2024-07-01') TO ('2024-10-01');
	`, srcSchemaTable))
	require.NoError(t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, "test_partition"),
		TableMappings: e2e.TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)

	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)

	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	// insert 10 rows into the source table
	for i := range 10 {
		testName := fmt.Sprintf("test_name_%d", i)
		_, err := conn.Conn().Exec(t.Context(),
			fmt.Sprintf(`INSERT INTO %s(name, created_at) VALUES ($1, '2024-%d-01')`,
				srcSchemaTable, max(1, i)), testName)
		e2e.EnvNoError(t, env, err)
	}
	t.Log("Inserted 10 rows into the source table")

	e2e.EnvWaitForEqualTablesWithNames(env, s, "normalizing 10 rows", srcTable, dstTable, `id,name,created_at`)
	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}
