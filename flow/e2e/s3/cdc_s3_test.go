package e2e_s3

import (
	"context"
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peer-flow/e2e"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

func (s PeerFlowE2ETestSuiteS3) attachSchemaSuffix(tableName string) string {
	return fmt.Sprintf("e2e_test_%s.%s", s.suffix, tableName)
}

func (s PeerFlowE2ETestSuiteS3) attachSuffix(input string) string {
	return fmt.Sprintf("%s_%s", input, s.suffix)
}

func (s PeerFlowE2ETestSuiteS3) Test_Complete_Simple_Flow_S3() {
	tc := e2e.NewTemporalClient(s.t)

	srcTableName := s.attachSchemaSuffix("test_simple_flow_s3")
	dstTableName := fmt.Sprintf("%s.%s", "peerdb_test_s3", "test_simple_flow_s3")
	flowJobName := s.attachSuffix("test_simple_flow_s3")
	_, err := s.conn.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`, srcTableName))
	require.NoError(s.t, err)
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      flowJobName,
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.s3Helper.GetPeer(),
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.MaxBatchSize = 5

	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)
	// insert 20 rows
	for i := 1; i <= 20; i++ {
		testKey := fmt.Sprintf("test_key_%d", i)
		testValue := fmt.Sprintf("test_value_%d", i)
		_, err = s.conn.Conn().Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s (key, value) VALUES ($1, $2)
		`, srcTableName), testKey, testValue)
		e2e.EnvNoError(s.t, env, err)
	}
	e2e.EnvNoError(s.t, env, err)

	e2e.EnvWaitFor(s.t, env, 2*time.Minute, "waiting for blobs", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		files, err := s.s3Helper.ListAllFiles(ctx, flowJobName)
		s.t.Logf("Files in Test_Complete_Simple_Flow_S3 %s: %d", flowJobName, len(files))
		e2e.EnvNoError(s.t, env, err)
		return len(files) == 4
	})

	env.Cancel()

	e2e.RequireEnvCanceled(s.t, env)
}
