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
	srcTableQualified := AttachSchema(s, "test_es_simple_append")
	srcTableName := srcTableQualified.String()
	srcTableIdent := srcTableQualified.Deparse()

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
		srcTableIdent,
		srcTableIdent,
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
		return s.countDocumentsInIndex(srcTableIdent) == int64(rowCount)
	})
	_, err = s.conn.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	UPDATE %s SET c1=c1+2,updated_at=now() WHERE id%%2=0;`, srcTableName))
	require.NoError(s.t, err, "failed to update rows on source")
	EnvWaitFor(s.t, env, 20*time.Second, "waiting for ES to catch up", func() bool {
		return s.countDocumentsInIndex(srcTableIdent) == int64(3*rowCount/2)
	})

	require.NoError(s.t, env.Error(s.t.Context()))
}

func (s elasticsearchSuite) Test_Simple_QRep_Upsert() {
	jobName := AddSuffix(s, "test_es_simple_upsert")
	srcTableQualified := AttachSchema(s, "test_es_simple_upsert")
	srcTableName := srcTableQualified.String()
	srcTableIdent := srcTableQualified.Deparse()

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
		srcTableIdent,
		srcTableIdent,
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
		return s.countDocumentsInIndex(srcTableIdent) == int64(rowCount)
	})

	require.NoError(s.t, env.Error(s.t.Context()))
}

func (s elasticsearchSuite) Test_QRep_Upsert_Aliased_Key() {
	jobName := AddSuffix(s, "test_es_upsert_alias")
	srcTableQualified := AttachSchema(s, "test_es_upsert_alias")
	srcTableName := SourceSQL(s, srcTableQualified)
	srcTableIdent := srcTableQualified.Deparse()

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

	query := fmt.Sprintf(
		"SELECT id AS aliased_id, c1, val, updated_at FROM %s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		srcTableName)

	qrepConfig := CreateQRepWorkflowConfig(s.t,
		jobName,
		srcTableIdent,
		srcTableIdent,
		query,
		s.Peer().Name,
		"",
		false,
		"",
		"",
	)
	qrepConfig.WriteMode = &protos.QRepWriteMode{
		WriteType:        protos.QRepWriteType_QREP_WRITE_MODE_UPSERT,
		UpsertKeyColumns: []string{"aliased_id"},
	}
	qrepConfig.InitialCopyOnly = false

	env := RunQRepFlowWorkflow(s.t, tc, qrepConfig)

	EnvWaitFor(s.t, env, 10*time.Second, "waiting for initial load", func() bool {
		return s.countDocumentsInIndex(srcTableIdent) == int64(rowCount)
	})

	_, err = s.conn.Conn().Exec(s.t.Context(), fmt.Sprintf(`
	UPDATE %s SET c1=c1+2,updated_at=now() WHERE id%%2=0;`, srcTableName))
	require.NoError(s.t, err, "failed to update rows on source")

	// with correct aliased upsert key, updates should not create duplicates
	EnvWaitFor(s.t, env, 20*time.Second, "waiting for upserts", func() bool {
		return s.countDocumentsInIndex(srcTableIdent) == int64(rowCount)
	})

	require.NoError(s.t, env.Error(s.t.Context()))
}
