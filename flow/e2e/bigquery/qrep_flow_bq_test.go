package e2e_bigquery

import (
	"fmt"
	"strings"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/e2e"
)

func (s PeerFlowE2ETestSuiteBQ) setupSourceTable(tableName string, rowCount int) {
	require.NoError(s.t, e2e.CreateTableForQRep(s.t.Context(), s.Conn(), s.bqSuffix, tableName))
	require.NoError(s.t, e2e.PopulateSourceTable(s.t.Context(), s.Conn(), s.bqSuffix, tableName, rowCount))
}

func (s PeerFlowE2ETestSuiteBQ) setupTimeTable(tableName string) {
	tblFields := []string{
		"watermark_ts timestamp",
		"mytimestamp timestamp",
		"mytztimestamp timestamptz",
		"medieval timestamptz",
		"mybaddate date",
		"mydate date",
	}
	tblFieldStr := strings.Join(tblFields, ",")
	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
			CREATE TABLE e2e_test_%s.%s (
				%s
			);`, s.bqSuffix, tableName, tblFieldStr))

	require.NoError(s.t, err)

	var rows []string
	row := `(CURRENT_TIMESTAMP,
			'10001-03-14 23:05:52',
			'50001-03-14 23:05:52.216809+00',
			'1534-03-14 23:05:52.216809+00',
			'10000-03-14',
			CURRENT_TIMESTAMP)`
	rows = append(rows, row)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO e2e_test_%s.%s (
			watermark_ts, mytimestamp, mytztimestamp, medieval, mybaddate, mydate
		) VALUES %s;
	`, s.bqSuffix, tableName, strings.Join(rows, ",")))
	require.NoError(s.t, err)
}

func (s PeerFlowE2ETestSuiteBQ) Test_Complete_QRep_Flow_Avro() {
	tc := e2e.NewTemporalClient(s.t)

	numRows := 10

	tblName := "test_qrep_flow_avro_bq"
	s.setupSourceTable(tblName, numRows)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.bqSuffix, tblName)

	qrepConfig := e2e.CreateQRepWorkflowConfig(s.t, "test_qrep_flow_avro",
		fmt.Sprintf("e2e_test_%s.%s", s.bqSuffix, tblName),
		tblName,
		query,
		s.Peer().Name,
		"",
		true,
		"",
		"")
	env := e2e.RunQRepFlowWorkflow(s.t.Context(), tc, qrepConfig)
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)
	require.NoError(s.t, env.Error(s.t.Context()))

	e2e.RequireEqualTables(s, tblName, "*")
}

func (s PeerFlowE2ETestSuiteBQ) Test_Invalid_Timestamps_And_Date_QRep() {
	tc := e2e.NewTemporalClient(s.t)
	tblName := "test_invalid_time_bq"
	s.setupTimeTable(tblName)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE watermark_ts BETWEEN {{.start}} AND {{.end}}",
		s.bqSuffix, tblName)

	qrepConfig := e2e.CreateQRepWorkflowConfig(s.t, "test_invalid_time_bq",
		fmt.Sprintf("e2e_test_%s.%s", s.bqSuffix, tblName),
		tblName,
		query,
		s.Peer().Name,
		"",
		true,
		"",
		"")
	qrepConfig.WatermarkColumn = "watermark_ts"
	env := e2e.RunQRepFlowWorkflow(s.t.Context(), tc, qrepConfig)
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)
	require.NoError(s.t, env.Error(s.t.Context()))

	goodValues := []string{"watermark_ts", "mydate", "medieval"}
	badValues := []string{"mytimestamp", "mytztimestamp", "mybaddate"}

	for _, col := range goodValues {
		ok, err := s.bqHelper.CheckNull(s.t.Context(), tblName, []string{col})
		require.NoError(s.t, err)
		require.True(s.t, ok)
	}

	for _, col := range badValues {
		ok, err := s.bqHelper.CheckNull(s.t.Context(), tblName, []string{col})
		require.NoError(s.t, err)
		require.False(s.t, ok)
	}
}

func (s PeerFlowE2ETestSuiteBQ) Test_PeerDB_Columns_QRep_BQ() {
	tc := e2e.NewTemporalClient(s.t)

	numRows := 10

	tblName := "test_columns_bq_qrep"
	s.setupSourceTable(tblName, numRows)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.bqSuffix, tblName)

	qrepConfig := e2e.CreateQRepWorkflowConfig(s.t, "test_qrep_flow_avro",
		fmt.Sprintf("e2e_test_%s.%s", s.bqSuffix, tblName),
		tblName,
		query,
		s.Peer().Name,
		"",
		true,
		"_PEERDB_SYNCED_AT",
		"")
	env := e2e.RunQRepFlowWorkflow(s.t.Context(), tc, qrepConfig)
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)
	require.NoError(s.t, env.Error(s.t.Context()))

	require.NoError(s.t, s.checkPeerdbColumns(tblName, false))
}
