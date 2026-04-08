package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

func (s elasticsearchSuite) Test_Simple_PKey_CDC_Mirror() {
	srcTableName := AttachSchema(s, "es_simple_pkey_cdc")

	_, err := s.conn.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			c1 INT,
			val TEXT,
			updated_at TIMESTAMP DEFAULT now()
		);
	`, srcTableName))
	require.NoError(s.t, err, "failed creating table")

	tc := NewTemporalClient(s.t)
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      AddSuffix(s, "es_simple_pkey_cdc"),
		TableNameMapping: map[string]string{srcTableName: srcTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.MaxBatchSize = 100
	flowConnConfig.DoInitialSnapshot = true

	rowCount := 10
	for i := range rowCount {
		_, err := s.conn.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s(c1,val) VALUES(%d,'val%d')
	`, srcTableName, i, i))
		require.NoError(s.t, err, "failed to insert row")
	}

	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	for i := range rowCount {
		_, err := s.conn.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s(c1,val) VALUES(%d,'val%d')
	`, srcTableName, i, i))
		require.NoError(s.t, err, "failed to insert row")
	}
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for initial snapshot + inserted rows", func() bool {
		return s.countDocumentsInIndex(srcTableName) == int64(2*rowCount)
	})

	_, err = s.conn.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	UPDATE %s SET c1=c1+2,updated_at=now() WHERE id%%2=0;`, srcTableName))
	require.NoError(s.t, err, "failed to update rows on source")
	for i := range rowCount {
		_, err := s.conn.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s(c1,val) VALUES(%d,'val%d')
	`, srcTableName, i, i))
		require.NoError(s.t, err, "failed to insert row")
	}
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for updates + new inserts", func() bool {
		return s.countDocumentsInIndex(srcTableName) == int64(3*rowCount)
	})

	_, err = s.conn.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	DELETE FROM %s WHERE id%%2=1;`, srcTableName))
	require.NoError(s.t, err, "failed to delete rows on source")
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for deletes", func() bool {
		return s.countDocumentsInIndex(srcTableName) == int64(3*rowCount/2)
	})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s elasticsearchSuite) Test_Composite_PKey_CDC_Mirror() {
	srcTableName := AttachSchema(s, "es_composite_pkey_cdc")

	_, err := s.conn.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT GENERATED ALWAYS AS IDENTITY,
			c1 INT,
			val TEXT,
			updated_at TIMESTAMP DEFAULT now(),
			PRIMARY KEY(id,val)
		);
	`, srcTableName))
	require.NoError(s.t, err, "failed creating table")

	tc := NewTemporalClient(s.t)
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      AddSuffix(s, "es_composite_pkey_cdc"),
		TableNameMapping: map[string]string{srcTableName: srcTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.MaxBatchSize = 100
	flowConnConfig.DoInitialSnapshot = true

	rowCount := 10
	for i := range rowCount {
		_, err := s.conn.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s(c1,val) VALUES(%d,'val%d')
	`, srcTableName, i, i))
		require.NoError(s.t, err, "failed to insert row")
	}

	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	for i := range rowCount {
		_, err := s.conn.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s(c1,val) VALUES(%d,'val%d')
	`, srcTableName, i, i))
		require.NoError(s.t, err, "failed to insert row")
	}
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for initial snapshot + inserted rows", func() bool {
		return s.countDocumentsInIndex(srcTableName) == int64(2*rowCount)
	})

	_, err = s.conn.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	UPDATE %s SET c1=c1+2,updated_at=now() WHERE id%%2=0;`, srcTableName))
	require.NoError(s.t, err, "failed to update rows on source")
	for i := range rowCount {
		_, err := s.conn.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s(c1,val) VALUES(%d,'val%d')
	`, srcTableName, i, i))
		require.NoError(s.t, err, "failed to insert row")
	}
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for updates + new inserts", func() bool {
		return s.countDocumentsInIndex(srcTableName) == int64(3*rowCount)
	})

	_, err = s.conn.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	DELETE FROM %s WHERE id%%2=1;`, srcTableName))
	require.NoError(s.t, err, "failed to delete rows on source")
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for deletes", func() bool {
		return s.countDocumentsInIndex(srcTableName) == int64(3*rowCount/2)
	})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}
