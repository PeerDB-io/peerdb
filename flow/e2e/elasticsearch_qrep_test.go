package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func Test_Elasticsearch(t *testing.T) {
	e2eshared.RunSuite(t, SetupElasticSuite)
}

func (s elasticsearchSuite) Test_Simple_QRep_Append() {
	jobName := AddSuffix(s, "test_es_simple_append")
	srcTableName := AttachSchema(s, "test_es_simple_append")

	_, err := s.conn.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			c1 INT,
			val TEXT,
			updated_at TIMESTAMP DEFAULT now()
		);
	`, srcTableName))
	require.NoError(s.t, err, "failed creating table")

	rowCount := 10
	for i := range rowCount {
		_, err := s.conn.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s(c1,val) VALUES(%d,'val%d')
	`, srcTableName, i, i))
		require.NoError(s.t, err, "failed to insert row")
	}

	tc := NewTemporalClient(s.t)

	query := fmt.Sprintf("SELECT * FROM %s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		srcTableName)

	qrepConfig := CreateQRepWorkflowConfig(s.t,
		jobName,
		srcTableName,
		srcTableName,
		query,
		s.Peer().Name,
		"",
		false,
		"",
		"",
	)
	qrepConfig.InitialCopyOnly = false

	env := RunQRepFlowWorkflow(s.t, tc, qrepConfig)

	EnvWaitFor(s.t, env, 10*time.Second, "waiting for ES to catch up", func() bool {
		return s.countDocumentsInIndex(srcTableName) == int64(rowCount)
	})
	_, err = s.conn.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	UPDATE %s SET c1=c1+2,updated_at=now() WHERE id%%2=0;`, srcTableName))
	require.NoError(s.t, err, "failed to update rows on source")
	EnvWaitFor(s.t, env, 20*time.Second, "waiting for ES to catch up", func() bool {
		return s.countDocumentsInIndex(srcTableName) == int64(3*rowCount/2)
	})

	require.NoError(s.t, env.Error(s.t.Context()))
}

func (s elasticsearchSuite) Test_Simple_QRep_Upsert() {
	jobName := AddSuffix(s, "test_es_simple_upsert")
	srcTableName := AttachSchema(s, "test_es_simple_upsert")

	_, err := s.conn.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			c1 INT,
			val TEXT,
			updated_at TIMESTAMP DEFAULT now()
		);
	`, srcTableName))
	require.NoError(s.t, err, "failed creating table")

	rowCount := 10
	for i := range rowCount {
		_, err := s.conn.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s(c1,val) VALUES(%d,'val%d')
	`, srcTableName, i, i))
		require.NoError(s.t, err, "failed to insert row")
	}

	tc := NewTemporalClient(s.t)

	query := fmt.Sprintf("SELECT * FROM %s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		srcTableName)

	qrepConfig := CreateQRepWorkflowConfig(s.t,
		jobName,
		srcTableName,
		srcTableName,
		query,
		s.Peer().Name,
		"",
		false,
		"",
		"",
	)
	qrepConfig.WriteMode = &protos.QRepWriteMode{
		WriteType:        protos.QRepWriteType_QREP_WRITE_MODE_UPSERT,
		UpsertKeyColumns: []string{"id"},
	}
	qrepConfig.InitialCopyOnly = false

	env := RunQRepFlowWorkflow(s.t, tc, qrepConfig)

	EnvWaitFor(s.t, env, 10*time.Second, "waiting for ES to catch up", func() bool {
		return s.countDocumentsInIndex(srcTableName) == int64(rowCount)
	})

	require.NoError(s.t, env.Error(s.t.Context()))
}
