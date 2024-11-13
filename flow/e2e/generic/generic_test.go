package e2e_generic

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peer-flow/connectors"
	"github.com/PeerDB-io/peer-flow/e2e"
	e2e_bigquery "github.com/PeerDB-io/peer-flow/e2e/bigquery"
	e2e_clickhouse "github.com/PeerDB-io/peer-flow/e2e/clickhouse"
	e2e_postgres "github.com/PeerDB-io/peer-flow/e2e/postgres"
	e2e_snowflake "github.com/PeerDB-io/peer-flow/e2e/snowflake"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
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

func TestGenericCH(t *testing.T) {
	e2eshared.RunSuite(t, SetupGenericSuite(e2e_clickhouse.SetupSuite))
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

	_, err := s.Connector().Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL,
			myh HSTORE NOT NULL
		);
	`, srcSchemaTable))
	require.NoError(t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, "test_simple"),
		TableMappings: e2e.TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(t)

	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)

	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	// insert 10 rows into the source table
	for i := range 10 {
		testKey := fmt.Sprintf("test_key_%d", i)
		testValue := fmt.Sprintf("test_value_%d", i)
		_, err = s.Connector().Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(key, value, myh) VALUES ($1, $2, '"a"=>"b"')
		`, srcSchemaTable), testKey, testValue)
		e2e.EnvNoError(t, env, err)
	}
	t.Log("Inserted 10 rows into the source table")

	e2e.EnvWaitForEqualTablesWithNames(env, s, "normalizing 10 rows", srcTable, dstTable, `id,key,value,myh`)
	env.Cancel()
	e2e.RequireEnvCanceled(t, env)
}

func (s Generic) Test_Simple_Schema_Changes() {
	t := s.T()

	destinationSchemaConnector, ok := s.DestinationConnector().(connectors.GetTableSchemaConnector)
	if !ok {
		t.SkipNow()
	}

	srcTable := "test_simple_schema_changes"
	dstTable := "test_simple_schema_changes_dst"
	srcTableName := e2e.AttachSchema(s, srcTable)
	dstTableName := s.DestinationTable(dstTable)

	_, err := s.Connector().Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			c1 BIGINT
		);
	`, srcTableName))
	require.NoError(t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, srcTable),
		TableMappings: e2e.TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(t)

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert and mutate schema repeatedly.
	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	_, err = s.Connector().Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1) VALUES ($1)`, srcTableName), 1)
	e2e.EnvNoError(t, env, err)
	t.Log("Inserted initial row in the source table")

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
	output, err := destinationSchemaConnector.GetTableSchema(context.Background(), nil, protos.TypeSystem_Q, []string{dstTableName})
	e2e.EnvNoError(t, env, err)
	e2e.EnvTrue(t, env, e2e.CompareTableSchemas(expectedTableSchema, output[dstTableName]))

	// alter source table, add column c2 and insert another row.
	_, err = s.Connector().Conn().Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s ADD COLUMN c2 BIGINT`, srcTableName))
	e2e.EnvNoError(t, env, err)
	t.Log("Altered source table, added column c2")
	_, err = s.Connector().Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,c2) VALUES ($1,$2)`, srcTableName), 2, 2)
	e2e.EnvNoError(t, env, err)
	t.Log("Inserted row with added c2 in the source table")

	// verify we got our two rows, if schema did not match up it will error.
	e2e.EnvWaitForEqualTablesWithNames(env, s, "normalize altered row", srcTable, dstTable, "id,c1,c2")
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
	output, err = destinationSchemaConnector.GetTableSchema(context.Background(), nil, protos.TypeSystem_Q, []string{dstTableName})
	e2e.EnvNoError(t, env, err)
	e2e.EnvTrue(t, env, e2e.CompareTableSchemas(expectedTableSchema, output[dstTableName]))
	e2e.EnvEqualTablesWithNames(env, s, srcTable, dstTable, "id,c1,c2")

	// alter source table, add column c3, drop column c2 and insert another row.
	_, err = s.Connector().Conn().Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s DROP COLUMN c2, ADD COLUMN c3 BIGINT`, srcTableName))
	e2e.EnvNoError(t, env, err)
	t.Log("Altered source table, dropped column c2 and added column c3")
	_, err = s.Connector().Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,c3) VALUES ($1,$2)`, srcTableName), 3, 3)
	e2e.EnvNoError(t, env, err)
	t.Log("Inserted row with added c3 in the source table")

	// verify we got our two rows, if schema did not match up it will error.
	e2e.EnvWaitForEqualTablesWithNames(env, s, "normalize dropped c2 column", srcTable, dstTable, "id,c1,c3")
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
	output, err = destinationSchemaConnector.GetTableSchema(context.Background(), nil, protos.TypeSystem_Q, []string{dstTableName})
	e2e.EnvNoError(t, env, err)
	e2e.EnvTrue(t, env, e2e.CompareTableSchemas(expectedTableSchema, output[dstTableName]))
	e2e.EnvEqualTablesWithNames(env, s, srcTable, dstTable, "id,c1,c3")

	// alter source table, drop column c3 and insert another row.
	_, err = s.Connector().Conn().Exec(context.Background(), fmt.Sprintf(`
		ALTER TABLE %s DROP COLUMN c3`, srcTableName))
	e2e.EnvNoError(t, env, err)
	t.Log("Altered source table, dropped column c3")
	_, err = s.Connector().Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1) VALUES ($1)`, srcTableName), 4)
	e2e.EnvNoError(t, env, err)
	t.Log("Inserted row after dropping all columns in the source table")

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
	output, err = destinationSchemaConnector.GetTableSchema(context.Background(), nil, protos.TypeSystem_Q, []string{dstTableName})
	e2e.EnvNoError(t, env, err)
	e2e.EnvTrue(t, env, e2e.CompareTableSchemas(expectedTableSchema, output[dstTableName]))
	e2e.EnvEqualTablesWithNames(env, s, srcTable, dstTable, "id,c1")

	env.Cancel()

	e2e.RequireEnvCanceled(t, env)
}
