package e2e_elasticsearch

import (
	"context"
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peer-flow/e2e"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

func (s elasticsearchSuite) Test_Simple_PKey_CDC_Mirror() {
	srcTableName := e2e.AttachSchema(s, "es_simple_pkey_cdc")

	_, err := s.conn.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			c1 INT,
			val TEXT,
			updated_at TIMESTAMP DEFAULT now()
		);
	`, srcTableName))
	require.NoError(s.t, err, "failed creating table")

	tc := e2e.NewTemporalClient(s.t)
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      e2e.AddSuffix(s, "es_simple_pkey_cdc"),
		TableNameMapping: map[string]string{srcTableName: srcTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 100
	flowConnConfig.DoInitialSnapshot = true

	rowCount := 10
	for i := range rowCount {
		_, err := s.conn.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,val) VALUES(%d,'val%d')
	`, srcTableName, i, i))
		require.NoError(s.t, err, "failed to insert row")
	}

	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	for i := range rowCount {
		_, err := s.conn.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,val) VALUES(%d,'val%d')
	`, srcTableName, i, i))
		require.NoError(s.t, err, "failed to insert row")
	}
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "wait for initial snapshot + inserted rows", func() bool {
		return s.countDocumentsInIndex(srcTableName) == int64(2*rowCount)
	})

	_, err = s.conn.Conn().Exec(context.Background(), fmt.Sprintf(`
	UPDATE %s SET c1=c1+2,updated_at=now() WHERE id%%2=0;`, srcTableName))
	require.NoError(s.t, err, "failed to update rows on source")
	for i := range rowCount {
		_, err := s.conn.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,val) VALUES(%d,'val%d')
	`, srcTableName, i, i))
		require.NoError(s.t, err, "failed to insert row")
	}
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "wait for updates + new inserts", func() bool {
		return s.countDocumentsInIndex(srcTableName) == int64(3*rowCount)
	})

	_, err = s.conn.Conn().Exec(context.Background(), fmt.Sprintf(`
	DELETE FROM %s WHERE id%%2=1;`, srcTableName))
	require.NoError(s.t, err, "failed to delete rows on source")
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "wait for deletes", func() bool {
		return s.countDocumentsInIndex(srcTableName) == int64(3*rowCount/2)
	})

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}

func (s elasticsearchSuite) Test_Composite_PKey_CDC_Mirror() {
	srcTableName := e2e.AttachSchema(s, "es_composite_pkey_cdc")

	_, err := s.conn.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT GENERATED ALWAYS AS IDENTITY,
			c1 INT,
			val TEXT,
			updated_at TIMESTAMP DEFAULT now(),
			PRIMARY KEY(id,val)
		);
	`, srcTableName))
	require.NoError(s.t, err, "failed creating table")

	tc := e2e.NewTemporalClient(s.t)
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      e2e.AddSuffix(s, "es_composite_pkey_cdc"),
		TableNameMapping: map[string]string{srcTableName: srcTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.MaxBatchSize = 100
	flowConnConfig.DoInitialSnapshot = true

	rowCount := 10
	for i := range rowCount {
		_, err := s.conn.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,val) VALUES(%d,'val%d')
	`, srcTableName, i, i))
		require.NoError(s.t, err, "failed to insert row")
	}

	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	for i := range rowCount {
		_, err := s.conn.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,val) VALUES(%d,'val%d')
	`, srcTableName, i, i))
		require.NoError(s.t, err, "failed to insert row")
	}
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "wait for initial snapshot + inserted rows", func() bool {
		return s.countDocumentsInIndex(srcTableName) == int64(2*rowCount)
	})

	_, err = s.conn.Conn().Exec(context.Background(), fmt.Sprintf(`
	UPDATE %s SET c1=c1+2,updated_at=now() WHERE id%%2=0;`, srcTableName))
	require.NoError(s.t, err, "failed to update rows on source")
	for i := range rowCount {
		_, err := s.conn.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(c1,val) VALUES(%d,'val%d')
	`, srcTableName, i, i))
		require.NoError(s.t, err, "failed to insert row")
	}
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "wait for updates + new inserts", func() bool {
		return s.countDocumentsInIndex(srcTableName) == int64(3*rowCount)
	})

	_, err = s.conn.Conn().Exec(context.Background(), fmt.Sprintf(`
	DELETE FROM %s WHERE id%%2=1;`, srcTableName))
	require.NoError(s.t, err, "failed to delete rows on source")
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "wait for deletes", func() bool {
		return s.countDocumentsInIndex(srcTableName) == int64(3*rowCount/2)
	})

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}
