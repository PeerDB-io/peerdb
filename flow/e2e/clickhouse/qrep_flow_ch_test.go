package e2e_clickhouse

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/stretchr/testify/require"
)

func (s PeerFlowE2ETestSuiteCH) setupSimpleTable(tableName string) {
	tblFields := []string{
		"id SERIAL PRIMARY KEY",
		"key INTEGER NOT NULL",
		"value TEXT NOT NULL",
		"updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
	}

	tblFieldStr := strings.Join(tblFields, ",")
	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
			CREATE TABLE e2e_test_%s.%s (
				%s
			);`, s.suffix, tableName, tblFieldStr))

	require.NoError(s.t, err)

	var rows []string
	row := `(1, 1, 'test_value_1', CURRENT_TIMESTAMP)`
	rows = append(rows, row)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(
		`INSERT INTO e2e_test_%s.%s (id, key, value, updated_at) VALUES %s;`,
		s.suffix, tableName, strings.Join(rows, ",")))
	require.NoError(s.t, err)
}

func (s PeerFlowE2ETestSuiteCH) Test_Append_QRep_CH() {
	tc := e2e.NewTemporalClient(s.t)

	tblName := "test_append_ch_qrep"
	s.setupSimpleTable(tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.suffix, tblName)

	qrepConfig := e2e.CreateQRepWorkflowConfig(s.t, "test_append_ch_qrep",
		fmt.Sprintf("e2e_test_%s.%s", s.suffix, tblName),
		tblName,
		query,
		s.Peer().Name,
		"",
		true,
		"_PEERDB_SYNCED_AT",
		"")
	env := e2e.RunQRepFlowWorkflow(tc, qrepConfig)
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)
	require.NoError(s.t, env.Error())

	e2e.RequireEqualTables(s, tblName, "*")
}
