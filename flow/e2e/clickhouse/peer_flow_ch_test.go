package e2e_clickhouse

import (
	"context"
	"fmt"
	"testing"

	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
	"github.com/stretchr/testify/require"
)

func TestPeerFlowE2ETestSuiteCH(t *testing.T) {
	e2eshared.RunSuite(t, SetupSuite)
}

func (s PeerFlowE2ETestSuiteCH) attachSchemaSuffix(tableName string) string {
	return fmt.Sprintf("e2e_test_%s.%s", s.suffix, tableName)
}

func (s PeerFlowE2ETestSuiteCH) attachSuffix(input string) string {
	return fmt.Sprintf("%s_%s", input, s.suffix)
}

func (s PeerFlowE2ETestSuiteCH) Test_Simple_CDC_CH() {
	testName := "test_cdc_ch_simple"
	srcTableName := s.attachSchemaSuffix(testName)
	dstTableName := testName

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id SERIAL NOT NULL,
		key INTEGER NOT NULL,
		value TEXT NOT NULL
	);
	`, srcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(testName),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 100
	flowConnConfig.SoftDeleteColName = ""
	flowConnConfig.SyncedAtColName = ""

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)

	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	// insert 10 rows into the source table
	for i := range 10 {
		testKey := i
		testValue := fmt.Sprintf("test_value_%d", i)
		_, err = s.Conn().Exec(context.Background(),
			fmt.Sprintf("INSERT INTO %s(key, value) VALUES ($1, $2, %s)", srcTableName),
			testKey, testValue)
		e2e.EnvNoError(s.t, env, err)
	}
	s.t.Log("Inserted 10 rows into the source table")

	e2e.EnvWaitForEqualTables(env, s, "normalize inserts", dstTableName, "id,key,value,j")

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}
