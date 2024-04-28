package e2e_elasticsearch

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/e2eshared"
)

func Test_Elasticsearch(t *testing.T) {
	e2eshared.RunSuite(t, SetupSuite)
}

func (s elasticsearchSuite) Test_Simple_Qrep() {
	srcTableName := e2e.AttachSchema(s, "es_simple")

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
	require.NoError(s.t, err)
	env := e2e.RunQRepFlowWorkflow(tc, qrepConfig)
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)
	require.NoError(s.t, env.Error())
	time.Sleep(2 * time.Second)

	require.EqualValues(s.t, rowCount, s.CountDocumentsInIndex(srcTableName))
}
