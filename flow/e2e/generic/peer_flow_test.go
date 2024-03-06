package e2e_generic

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/e2e/bigquery"
	"github.com/PeerDB-io/peer-flow/e2e/postgres"
	"github.com/PeerDB-io/peer-flow/e2e/snowflake"
	"github.com/PeerDB-io/peer-flow/e2eshared"
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
		Destination:   s.Peer(),
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()

	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)

	e2e.SetupCDCFlowStatusQuery(t, env, connectionGen)
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
