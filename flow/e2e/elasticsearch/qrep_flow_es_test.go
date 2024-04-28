package e2e_elasticsearch

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
)

func Test_Elasticsearch(t *testing.T) {
	e2eshared.RunSuite(t, SetupSuite)
}

func (s elasticsearchSuite) Test_Simple_QRep_Append() {
	srcTableName := e2e.AttachSchema(s, "es_simple_append")

	_, err := s.conn.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			val TEXT,
			updated_at TIMESTAMP DEFAULT now()
		);
	`, srcTableName))
	require.NoError(s.t, err, "failed creating table")

	rowCount := 10
	for i := range rowCount {
		_, err := s.conn.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(val) VALUES('val%d')
	`, srcTableName, i))
		require.NoError(s.t, err, "failed to insert row")
	}

	tc := e2e.NewTemporalClient(s.t)

	query := fmt.Sprintf("SELECT * FROM %s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		srcTableName)

	qrepConfig, err := e2e.CreateQRepWorkflowConfig("test_es_simple_qrep",
		srcTableName,
		srcTableName,
		query,
		s.peer,
		"",
		false,
		"",
		"",
	)
	qrepConfig.InitialCopyOnly = false

	require.NoError(s.t, err)
	env := e2e.RunQRepFlowWorkflow(tc, qrepConfig)

	e2e.EnvWaitFor(s.t, env, 10*time.Second, "waiting for ES to catch up", func() bool {
		return s.CountDocumentsInIndex(srcTableName) == int64(rowCount)
	})
	_, err = s.conn.Conn().Exec(context.Background(), fmt.Sprintf(`
	UPDATE %s SET c1=c1+2,updated_at=now() WHERE id%%2=0;`, srcTableName))
	require.NoError(s.t, err, "failed to update rows on source")
	e2e.EnvWaitFor(s.t, env, 20*time.Second, "waiting for ES to catch up", func() bool {
		return s.CountDocumentsInIndex(srcTableName) == int64(3*rowCount/2)
	})

	require.NoError(s.t, env.Error())
}

func (s elasticsearchSuite) Test_Simple_QRep_Upsert() {
	srcTableName := e2e.AttachSchema(s, "es_simple_upsert")

	_, err := s.conn.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			val TEXT,
			updated_at TIMESTAMP DEFAULT now()
		);
	`, srcTableName))
	require.NoError(s.t, err, "failed creating table")

	rowCount := 10
	for i := range rowCount {
		_, err := s.conn.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s(val) VALUES('val%d')
	`, srcTableName, i))
		require.NoError(s.t, err, "failed to insert row")
	}

	tc := e2e.NewTemporalClient(s.t)

	query := fmt.Sprintf("SELECT * FROM %s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		srcTableName)

	qrepConfig, err := e2e.CreateQRepWorkflowConfig("test_es_simple_qrep",
		srcTableName,
		srcTableName,
		query,
		s.peer,
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

	require.NoError(s.t, err)
	env := e2e.RunQRepFlowWorkflow(tc, qrepConfig)

	e2e.EnvWaitFor(s.t, env, 10*time.Second, "waiting for ES to catch up", func() bool {
		return s.CountDocumentsInIndex(srcTableName) == int64(rowCount)
	})
	_, err = s.conn.Conn().Exec(context.Background(), fmt.Sprintf(`
	UPDATE %s SET c1=c1+2,updated_at=now() WHERE id%%2=0;`, srcTableName))
	require.NoError(s.t, err, "failed to update rows on source")
	e2e.EnvWaitFor(s.t, env, 20*time.Second, "waiting for ES to catch up", func() bool {
		return s.CountDocumentsInIndex(srcTableName) == int64(rowCount)
	})

	require.NoError(s.t, env.Error())
}
