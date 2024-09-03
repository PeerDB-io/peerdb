package e2e_clickhouse

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

func TestPeerFlowE2ETestSuiteCH(t *testing.T) {
	e2eshared.RunSuite(t, SetupSuite)
}

func (s ClickHouseSuite) attachSchemaSuffix(tableName string) string {
	return fmt.Sprintf("e2e_test_%s.%s", s.suffix, tableName)
}

func (s ClickHouseSuite) attachSuffix(input string) string {
	return fmt.Sprintf("%s_%s", input, s.suffix)
}

func (s ClickHouseSuite) Test_Addition_Removal() {
	tc := e2e.NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_table_add_remove")
	addedSrcTableName := s.attachSchemaSuffix("test_table_add_remove_added")
	dstTableName := "test_table_add_remove_target"
	addedDstTableName := "test_table_add_remove_target_added"

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL
		);
	`, srcTableName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id SERIAL PRIMARY KEY,
		key TEXT NOT NULL
	);
	`, addedSrcTableName))
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("test_table_add_remove"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 1

	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s (key) VALUES ('test');
	`, srcTableName))
	require.NoError(s.t, err)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "first insert", "test_table_add_remove", dstTableName, "id,key")
	e2e.SignalWorkflow(env, model.FlowSignal, model.PauseSignal)
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "pausing", func() bool {
		response, err := env.Query(shared.FlowStatusQuery)
		if err != nil {
			s.t.Log(err)
			return false
		}
		var state *protos.FlowStatus
		err = response.Get(&state)
		if err != nil {
			s.t.Fatal("decode failed", err)
		}
		return state == nil || *state != protos.FlowStatus_STATUS_PAUSED
	})

	e2e.SignalWorkflow(env, model.CDCDynamicPropertiesSignal, &protos.CDCFlowConfigUpdate{
		AdditionalTables: []*protos.TableMapping{
			{
				SourceTableIdentifier:      addedSrcTableName,
				DestinationTableIdentifier: addedDstTableName,
			},
		},
	})

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "adding table", func() bool {
		response, err := env.Query(shared.FlowStatusQuery)
		if err != nil {
			s.t.Log(err)
			return false
		}
		var state *protos.FlowStatus
		err = response.Get(&state)
		if err != nil {
			s.t.Fatal("decode failed", err)
		}
		return state == nil || *state != protos.FlowStatus_STATUS_RUNNING
	})

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s (key) VALUES ('test');
	`, addedSrcTableName))
	require.NoError(s.t, err)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "first insert to added table", "test_table_add_remove_added", addedDstTableName, "id,key")
	e2e.SignalWorkflow(env, model.CDCDynamicPropertiesSignal, &protos.CDCFlowConfigUpdate{
		RemovedTables: []*protos.TableMapping{
			{
				SourceTableIdentifier:      srcTableName,
				DestinationTableIdentifier: dstTableName,
			},
		},
	})

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "removing table", func() bool {
		response, err := env.Query(shared.FlowStatusQuery)
		if err != nil {
			s.t.Log(err)
			return false
		}
		var state *protos.FlowStatus
		err = response.Get(&state)
		if err != nil {
			s.t.Fatal("decode failed", err)
		}
		return state == nil || *state != protos.FlowStatus_STATUS_RUNNING
	})

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
	INSERT INTO %s (key) VALUES ('test');
	`, srcTableName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
	INSERT INTO %s (key) VALUES ('test');
	`, addedSrcTableName))
	require.NoError(s.t, err)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "second insert to added table", "test_table_add_remove_added", addedDstTableName, "id,key")

	rows, err := s.GetRows(dstTableName, "id")
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 1, "expected no new rows in removed table")
	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}
