package e2e_generic

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/e2e"
	e2e_bigquery "github.com/PeerDB-io/peerdb/flow/e2e/bigquery"
	e2e_clickhouse "github.com/PeerDB-io/peerdb/flow/e2e/clickhouse"
	e2e_postgres "github.com/PeerDB-io/peerdb/flow/e2e/postgres"
	e2e_snowflake "github.com/PeerDB-io/peerdb/flow/e2e/snowflake"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/shared"
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
	e2eshared.RunSuite(t, SetupGenericSuite(e2e_clickhouse.SetupSuite(t, func(t *testing.T) (*e2e.PostgresSource, string, error) {
		t.Helper()
		suffix := "pgchg_" + strings.ToLower(shared.RandomString(8))
		source, err := e2e.SetupPostgres(t, suffix)
		return source, suffix, err
	})))
}

func TestGenericCH_MySQL(t *testing.T) {
	e2eshared.RunSuite(t, SetupGenericSuite(e2e_clickhouse.SetupSuite(t, func(t *testing.T) (*e2e.MySqlSource, string, error) {
		t.Helper()
		suffix := "mychg_" + strings.ToLower(shared.RandomString(8))
		source, err := e2e.SetupMySQL(t, suffix)
		return source, suffix, err
	})))
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

	destinationSchemaConnector, ok := s.DestinationConnector().(connectors.GetTableSchemaConnector)
	if !ok {
		t.Skip("skipping test because destination connector does not implement GetTableSchemaConnector")
	}

	srcTable := "test_simple_schema_changes"
	dstTable := "test_simple_schema_changes_dst"
	srcTableName := e2e.AttachSchema(s, srcTable)
	dstTableName := s.DestinationTable(dstTable)

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
	output, err := destinationSchemaConnector.GetTableSchema(t.Context(), nil, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: dstTableName}})
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
	output, err = destinationSchemaConnector.GetTableSchema(t.Context(), nil, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: dstTableName}})
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
	output, err = destinationSchemaConnector.GetTableSchema(t.Context(), nil, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: dstTableName}})
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
	output, err = destinationSchemaConnector.GetTableSchema(t.Context(), nil, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: dstTableName}})
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

func (s Generic) Test_Schema_Changes_Cutoff_Bug() {
	t := s.T()

	destinationSchemaConnector, ok := s.DestinationConnector().(connectors.GetTableSchemaConnector)
	if !ok {
		t.Skip("skipping test because destination connector does not implement GetTableSchemaConnector")
	}

	srcTable1 := "test_schema_changes_cutoff_bug1"
	dstTable1 := "test_schema_changes_cutoff_bug_dst1"
	srcTable2 := "test_schema_changes_cutoff_bug2"
	dstTable2 := "test_schema_changes_cutoff_bug_dst2"
	srcTableName1 := e2e.AttachSchema(s, srcTable1)
	dstTableName1 := s.DestinationTable(dstTable1)
	srcTableName2 := e2e.AttachSchema(s, srcTable2)
	dstTableName2 := s.DestinationTable(dstTable2)

	require.NoError(t, s.Source().Exec(t.Context(),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, c1 BIGINT)", srcTableName1)))
	require.NoError(t, s.Source().Exec(t.Context(),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, c1 BIGINT)", srcTableName2)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, srcTable1),
		TableMappings: e2e.TableMappings(s, srcTable1, dstTable1, srcTable2, dstTable2),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.MaxBatchSize = 2

	// wait for PeerFlowStatusQuery to finish setup
	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	e2e.EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s(c1) VALUES(2)`, srcTableName2)))
	e2e.EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(`ALTER TABLE %s ADD COLUMN c2 BIGINT`, srcTableName1)))
	e2e.EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(`ALTER TABLE %s ADD COLUMN c2 BIGINT`, srcTableName2)))
	e2e.EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s(c1,c2) VALUES(2,2)`, srcTableName1)))

	e2e.EnvWaitForEqualTablesWithNames(env, s, "table1 added column", srcTable1, dstTable1, "id,c1,coalesce(c2,0) c2")
	e2e.EnvWaitForEqualTablesWithNames(env, s, "table2 not added column", srcTable2, dstTable2, "id,c1")

	expectedTableSchema1 := &protos.TableSchema{
		TableIdentifier: e2e.ExpectedDestinationTableName(s, dstTable1),
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
				Name:         e2e.ExpectedDestinationIdentifier(s, "c2"),
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
	expectedTableSchema2 := &protos.TableSchema{
		TableIdentifier: e2e.ExpectedDestinationTableName(s, dstTable1),
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
	output, err := destinationSchemaConnector.GetTableSchema(t.Context(), nil, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: dstTableName1}, {SourceTableIdentifier: dstTableName2}})
	e2e.EnvNoError(t, env, err)
	e2e.EnvTrue(t, env, e2e.CompareTableSchemas(expectedTableSchema1, output[dstTableName1]))
	e2e.EnvTrue(t, env, e2e.CompareTableSchemas(expectedTableSchema2, output[dstTableName2]))

	e2e.EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s(c1,c2) VALUES (3, 3)`, srcTableName1)))
	e2e.EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s(c1,c2) VALUES (2, 2)`, srcTableName2)))

	// verify we got our two rows, if schema did not match up it will error.
	e2e.EnvWaitForEqualTablesWithNames(env, s, "table1 added column", srcTable1, dstTable1, "id,c1,coalesce(c2,0) c2")
	e2e.EnvWaitForEqualTablesWithNames(env, s, "table2 added column", srcTable2, dstTable2, "id,c1,coalesce(c2,0) c2")
	output, err = destinationSchemaConnector.GetTableSchema(t.Context(), nil, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: dstTableName1}, {SourceTableIdentifier: dstTableName2}})
	e2e.EnvNoError(t, env, err)
	e2e.EnvTrue(t, env, e2e.CompareTableSchemas(expectedTableSchema1, output[dstTableName1]))
	e2e.EnvTrue(t, env, e2e.CompareTableSchemas(expectedTableSchema1, output[dstTableName2]))

	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}

func (s Generic) Test_Partitioned_Table_Without_Publish_Via_Partition_Root() {
	t := s.T()

	pgSource, ok := s.Source().(*e2e.PostgresSource)
	if !ok {
		t.Skip("test only applies to postgres")
	}
	conn := pgSource.PostgresConnector

	srcTable := "test_partition_noroot"
	dstTable := "test_partition_noroot_dst"
	srcSchemaTable := e2e.AttachSchema(s, srcTable)
	srcPublicationName := fmt.Sprintf("%s_%s_pub", srcTable, s.Suffix())

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
			CREATE PUBLICATION %[2]s FOR TABLES IN SCHEMA %[3]s;
	`, srcSchemaTable, srcPublicationName, e2e.Schema(s)))
	require.NoError(t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, "test_partition_noroot"),
		TableMappings: e2e.TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.PublicationName = srcPublicationName
	flowConnConfig.IdleTimeoutSeconds = 60

	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)

	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	// add a partition to the source table after CDC is running to test if
	// the partition is picked up by the flow.
	go func() {
		time.Sleep(15 * time.Second)
		_, err := conn.Conn().Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE %[1]s_2024q4
			PARTITION OF %[1]s
			FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');`, srcSchemaTable))
		e2e.EnvNoError(t, env, err)
		_, err = conn.Conn().Exec(t.Context(), fmt.Sprintf(`
		INSERT INTO %[1]s(name, created_at) VALUES ('test_name', '2024-10-01');
		INSERT INTO %[1]s(name, created_at) VALUES ('test_name', '2024-11-01');
		INSERT INTO %[1]s(name, created_at) VALUES ('test_name', '2024-12-01');`,
			srcSchemaTable))
		e2e.EnvNoError(t, env, err)
	}()
	// insert 10 rows into the source table
	for i := range 10 {
		testName := fmt.Sprintf("test_name_%d", i)
		_, err := conn.Conn().Exec(t.Context(),
			fmt.Sprintf(`INSERT INTO %s(name, created_at) VALUES ($1, '2024-%d-01')`,
				srcSchemaTable, max(1, i)), testName)
		e2e.EnvNoError(t, env, err)
	}
	t.Log("Inserted 13 rows into the source table")

	e2e.EnvWaitForEqualTablesWithNames(env, s, "normalizing 13 rows", srcTable, dstTable, `id,name,created_at`)
	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}

func (s Generic) Test_Inheritance_Table_Without_Dynamic_Setting() {
	t := s.T()

	pgSource, ok := s.Source().(*e2e.PostgresSource)
	if !ok {
		t.Skip("test only applies to postgres")
	}
	conn := pgSource.PostgresConnector

	srcTable := "test_inheritance"
	dstTable := "test_inheritance_dst"
	srcSchemaTable := e2e.AttachSchema(s, srcTable)
	srcPublicationName := fmt.Sprintf("%s_%s_pub", srcTable, s.Suffix())

	_, err := conn.Conn().Exec(t.Context(), fmt.Sprintf(`
			CREATE TABLE %[1]s(
				id SERIAL NOT NULL,
				name TEXT,
				created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
				PRIMARY KEY (created_at, id)
			);
			CREATE TABLE %[1]s_child1() INHERITS (%[1]s);
			CREATE TABLE %[1]s_child2() INHERITS (%[1]s);
			CREATE PUBLICATION %[2]s FOR TABLES IN SCHEMA %[3]s;
	`, srcSchemaTable, srcPublicationName, e2e.Schema(s)))
	require.NoError(t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, "test_inheritance"),
		TableMappings: e2e.TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.PublicationName = srcPublicationName
	flowConnConfig.Env = map[string]string{"PEERDB_POSTGRES_CDC_HANDLE_INHERITANCE_FOR_NON_PARTITIONED_TABLES": "false"}

	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)

	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	_, err = conn.Conn().Exec(t.Context(), fmt.Sprintf(`
	INSERT INTO %[1]s(name, created_at) VALUES ('test_name', '2025-01-01');
	INSERT INTO %[1]s_child1(name, created_at) VALUES ('test_name', '2025-02-01');
	INSERT INTO %[1]s_child2(name, created_at) VALUES ('test_name', '2025-03-01');`,
		srcSchemaTable))
	e2e.EnvNoError(t, env, err)
	t.Log("Inserted 3 rows into the source table during CDC")

	e2e.EnvWaitForEqualTablesWithNames_Only(env, s, "only 1 row should be present", srcTable, dstTable, `id,name,created_at`)
	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}

func (s Generic) Test_Inheritance_Table_With_Dynamic_Setting() {
	t := s.T()

	pgSource, ok := s.Source().(*e2e.PostgresSource)
	if !ok {
		t.Skip("test only applies to postgres")
	}
	conn := pgSource.PostgresConnector

	srcTable := "test_inheritance_dynconf"
	dstTable := "test_inheritance_dynconf_dst"
	srcSchemaTable := e2e.AttachSchema(s, srcTable)
	srcPublicationName := fmt.Sprintf("%s_%s_pub", srcTable, s.Suffix())

	_, err := conn.Conn().Exec(t.Context(), fmt.Sprintf(`
			CREATE TABLE %[1]s(
				id SERIAL NOT NULL,
				name TEXT,
				created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
				PRIMARY KEY (created_at, id)
			);
			CREATE TABLE %[1]s_child1() INHERITS (%[1]s);
			CREATE TABLE %[1]s_child2() INHERITS (%[1]s);
			CREATE PUBLICATION %[2]s FOR TABLES IN SCHEMA %[3]s;
	`, srcSchemaTable, srcPublicationName, e2e.Schema(s)))
	require.NoError(t, err)
	_, err = conn.Conn().Exec(t.Context(), fmt.Sprintf(`
	INSERT INTO %[1]s(name, created_at) VALUES ('test_name', '2024-01-01');
	INSERT INTO %[1]s_child1(name, created_at) VALUES ('test_name', '2024-02-01');
	INSERT INTO %[1]s_child2(name, created_at) VALUES ('test_name', '2024-03-01');`,
		srcSchemaTable))
	require.NoError(t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, "test_inheritance_dynconf"),
		TableMappings: e2e.TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.PublicationName = srcPublicationName
	flowConnConfig.IdleTimeoutSeconds = 60
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)

	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	// add a child table after CDC is running to test if is picked up by the flow.
	go func() {
		time.Sleep(15 * time.Second)
		_, err := conn.Conn().Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %[1]s_child3() INHERITS (%[1]s);`, srcSchemaTable))
		e2e.EnvNoError(t, env, err)
		_, err = conn.Conn().Exec(t.Context(), fmt.Sprintf(`
		INSERT INTO %[1]s_child3(name, created_at) VALUES ('test_name', '2025-04-01');
		INSERT INTO %[1]s_child3(name, created_at) VALUES ('test_name', '2025-05-01');`,
			srcSchemaTable))
		e2e.EnvNoError(t, env, err)
		t.Log("Inserted 2 rows into child table created during CDC")
	}()
	_, err = conn.Conn().Exec(t.Context(), fmt.Sprintf(`
	INSERT INTO %[1]s(name, created_at) VALUES ('test_name', '2025-01-01');
	INSERT INTO %[1]s_child1(name, created_at) VALUES ('test_name', '2025-02-01');
	INSERT INTO %[1]s_child2(name, created_at) VALUES ('test_name', '2025-03-01');`,
		srcSchemaTable))
	e2e.EnvNoError(t, env, err)
	t.Log("Inserted 3 rows into the source table during CDC")

	e2e.EnvWaitForEqualTablesWithNames(env, s,
		"rows from parent and 3 child tables should be present", srcTable, dstTable, `id,name,created_at`)
	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}
