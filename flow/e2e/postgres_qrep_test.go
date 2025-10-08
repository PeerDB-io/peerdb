package e2e

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func TestPeerFlowE2ETestSuitePG(t *testing.T) {
	e2eshared.RunSuite(t, SetupPostgresSuite)
}

func (s PeerFlowE2ETestSuitePG) setupSourceTable(tableName string, rowCount int) {
	require.NoError(s.t, CreateTableForQRep(s.t.Context(), s.Conn(), s.suffix, tableName))
	s.populateSourceTable(tableName, rowCount)
}

func (s PeerFlowE2ETestSuitePG) populateSourceTable(tableName string, rowCount int) {
	if rowCount > 0 {
		require.NoError(s.t, PopulateSourceTable(s.t.Context(), s.Conn(), s.suffix, tableName, rowCount))
	}
}

func (s PeerFlowE2ETestSuitePG) comparePGTables(srcSchemaQualified, dstSchemaQualified, selector string) error {
	// Execute the two EXCEPT queries
	return errors.Join(
		s.compareQuery(srcSchemaQualified, dstSchemaQualified, selector),
		s.compareQuery(dstSchemaQualified, srcSchemaQualified, selector),
	)
}

func (s PeerFlowE2ETestSuitePG) checkEnums(srcSchemaQualified, dstSchemaQualified string) error {
	var exists pgtype.Bool
	query := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s src "+
		"WHERE NOT EXISTS (SELECT 1 FROM %s dst "+
		"WHERE src.my_mood::text = dst.my_mood::text)) LIMIT 1;", srcSchemaQualified,
		dstSchemaQualified)
	if err := s.Conn().QueryRow(s.t.Context(), query).Scan(&exists); err != nil {
		return err
	}

	if exists.Bool {
		return errors.New("enum comparison failed: rows are not equal")
	}
	return nil
}

func (s PeerFlowE2ETestSuitePG) compareQuery(srcSchemaQualified, dstSchemaQualified, selector string) error {
	query := fmt.Sprintf("SELECT %s FROM %s EXCEPT SELECT %s FROM %s", selector, srcSchemaQualified,
		selector, dstSchemaQualified)
	rows, err := s.Conn().Query(s.t.Context(), query, pgx.QueryExecModeExec)
	if err != nil {
		return err
	}
	defer rows.Close()

	errors := make([]string, 0)
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return err
		}

		columns := rows.FieldDescriptions()

		errmsg := make([]string, 0, len(values))
		for i, value := range values {
			errmsg = append(errmsg, fmt.Sprintf("%s: %v", columns[i].Name, value))
		}
		errors = append(errors, strings.Join(errmsg, "\n"))
	}

	if rows.Err() != nil {
		return rows.Err()
	}
	if len(errors) > 0 {
		return fmt.Errorf("comparison failed: rows are not equal\n%s", strings.Join(errors, "\n---\n"))
	}
	return nil
}

func (s PeerFlowE2ETestSuitePG) compareCounts(dstSchemaQualified string, expectedCount int64) error {
	query := "SELECT COUNT(*) FROM " + dstSchemaQualified
	count, err := s.RunInt64Query(query)
	if err != nil {
		return err
	}

	if count != expectedCount {
		return fmt.Errorf("expected %d rows, got %d", expectedCount, count)
	}

	return nil
}

func (s PeerFlowE2ETestSuitePG) checkSyncedAt(dstSchemaQualified string) error {
	query := `SELECT "_PEERDB_SYNCED_AT" FROM ` + dstSchemaQualified

	rows, _ := s.Conn().Query(s.t.Context(), query)

	defer rows.Close()
	for rows.Next() {
		var syncedAt pgtype.Timestamp
		err := rows.Scan(&syncedAt)
		if err != nil {
			return err
		}

		if !syncedAt.Valid {
			return errors.New("synced_at is not valid")
		}
	}

	return rows.Err()
}

func (s PeerFlowE2ETestSuitePG) RunInt64Query(query string) (int64, error) {
	var count pgtype.Int8
	err := s.Conn().QueryRow(s.t.Context(), query).Scan(&count)
	return count.Int64, err
}

func (s PeerFlowE2ETestSuitePG) TestSimpleSlotCreation() {
	setupTx, err := s.Conn().Begin(s.t.Context())
	require.NoError(s.t, err)
	// setup 3 tables in pgpeer_repl_test schema
	// test_1, test_2, test_3, all have 5 columns all text, c1, c2, c3, c4, c5
	tables := []string{"test_1", "test_2", "test_3"}
	for _, table := range tables {
		_, err = setupTx.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE e2e_test_%s.%s (c1 text, c2 text, c3 text, c4 text, c5 text)", s.suffix, table))
		require.NoError(s.t, err)
	}

	require.NoError(s.t, setupTx.Commit(s.t.Context()))

	flowJobName := "test_simple_slot_creation"
	setupReplicationInput := &protos.SetupReplicationInput{
		FlowJobName: flowJobName,
		TableNameMapping: map[string]string{
			fmt.Sprintf("e2e_test_%s.test_1", s.suffix): "test_1_dst",
		},
	}

	s.t.Log("waiting for slot creation to complete: " + flowJobName)
	slotInfo, err := s.conn.SetupReplication(s.t.Context(), setupReplicationInput)
	require.NoError(s.t, err)

	s.t.Logf("slot creation complete: %v", slotInfo)
	if slotInfo.Conn != nil {
		require.NoError(s.t, slotInfo.Conn.Close(s.t.Context()))
	}
}

func (s PeerFlowE2ETestSuitePG) Test_Complete_QRep_Flow_Multi_Insert_PG() {
	numRows := 10

	baseName := "test_qrep_flow_avro_pg"
	srcTable := baseName + "_1"
	s.setupSourceTable(srcTable, numRows)

	dstTable := baseName + "_2"

	err := CreateTableForQRep(s.t.Context(), s.Conn(), s.suffix, dstTable)
	require.NoError(s.t, err)

	srcSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, srcTable)
	dstSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, dstTable)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.suffix, srcTable)

	jobName := AddSuffix(s, baseName)
	qrepConfig := CreateQRepWorkflowConfig(
		s.t,
		jobName,
		srcSchemaQualified,
		dstSchemaQualified,
		query,
		GeneratePostgresPeer(s.t).Name,
		"",
		true,
		"",
		"",
	)

	tc := NewTemporalClient(s.t)
	env := RunQRepFlowWorkflow(s.t, tc, qrepConfig)
	EnvWaitForFinished(s.t, env, 3*time.Minute)
	require.NoError(s.t, env.Error(s.t.Context()))

	require.NoError(s.t, s.comparePGTables(srcSchemaQualified, dstSchemaQualified, "*"))
}

func (s PeerFlowE2ETestSuitePG) Test_PG_TypeSystemQRep() {
	numRows := 10

	baseName := "test_qrep_flow_pgpg"
	srcTable := baseName + "_1"
	s.setupSourceTable(srcTable, numRows)

	dstTable := baseName + "_2"

	err := CreateTableForQRep(s.t.Context(), s.Conn(), s.suffix, dstTable)
	require.NoError(s.t, err)

	srcSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, srcTable)
	dstSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, dstTable)

	query := fmt.Sprintf("SELECT * FROM %s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		srcSchemaQualified)

	jobName := AddSuffix(s, baseName)
	qrepConfig := CreateQRepWorkflowConfig(
		s.t,
		jobName,
		srcSchemaQualified,
		dstSchemaQualified,
		query,
		GeneratePostgresPeer(s.t).Name,
		"",
		true,
		"",
		"",
	)
	qrepConfig.System = protos.TypeSystem_PG

	tc := NewTemporalClient(s.t)
	env := RunQRepFlowWorkflow(s.t, tc, qrepConfig)
	EnvWaitForFinished(s.t, env, 3*time.Minute)
	require.NoError(s.t, env.Error(s.t.Context()))

	require.NoError(s.t, s.comparePGTables(srcSchemaQualified, dstSchemaQualified, "*"))
}

func (s PeerFlowE2ETestSuitePG) Test_PeerDB_Columns_QRep_PG() {
	numRows := 10

	baseName := "test_qrep_columns_pg"
	srcTable := baseName + "_1"
	s.setupSourceTable(srcTable, numRows)

	dstTable := baseName + "_2"

	srcSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, srcTable)
	dstSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, dstTable)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.suffix, srcTable)

	jobName := AddSuffix(s, baseName)
	qrepConfig := CreateQRepWorkflowConfig(
		s.t,
		jobName,
		srcSchemaQualified,
		dstSchemaQualified,
		query,
		GeneratePostgresPeer(s.t).Name,
		"",
		true,
		"_PEERDB_SYNCED_AT",
		"",
	)

	tc := NewTemporalClient(s.t)
	env := RunQRepFlowWorkflow(s.t, tc, qrepConfig)
	EnvWaitForFinished(s.t, env, 3*time.Minute)
	require.NoError(s.t, env.Error(s.t.Context()))

	require.NoError(s.t, s.checkSyncedAt(dstSchemaQualified))
}

func (s PeerFlowE2ETestSuitePG) Test_Overwrite_PG() {
	numRows := 10

	baseName := "test_overwrite_pg"
	srcTable := baseName + "_1"
	s.setupSourceTable(srcTable, numRows)

	dstTable := baseName + "_2"

	srcSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, srcTable)
	dstSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, dstTable)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.suffix, srcTable)

	jobName := AddSuffix(s, baseName)
	qrepConfig := CreateQRepWorkflowConfig(
		s.t,
		jobName,
		srcSchemaQualified,
		dstSchemaQualified,
		query,
		GeneratePostgresPeer(s.t).Name,
		"",
		true,
		"_PEERDB_SYNCED_AT",
		"",
	)
	qrepConfig.WriteMode = &protos.QRepWriteMode{
		WriteType: protos.QRepWriteType_QREP_WRITE_MODE_OVERWRITE,
	}
	qrepConfig.InitialCopyOnly = false

	tc := NewTemporalClient(s.t)
	env := RunQRepFlowWorkflow(s.t, tc, qrepConfig)
	EnvWaitFor(s.t, env, 3*time.Minute, "waiting for first sync to complete", func() bool {
		err := s.compareCounts(dstSchemaQualified, int64(numRows))
		return err == nil
	})

	newRowCount := 5
	s.populateSourceTable(srcTable, newRowCount)

	EnvWaitFor(s.t, env, 2*time.Minute, "waiting for overwrite sync to complete", func() bool {
		err := s.compareCounts(dstSchemaQualified, int64(newRowCount))
		return err == nil
	})

	require.NoError(s.t, env.Error(s.t.Context()))
}

func (s PeerFlowE2ETestSuitePG) Test_No_Rows_QRep_PG() {
	numRows := 0

	baseName := "test_no_rows_qrep_pg"
	srcTable := baseName + "_1"
	s.setupSourceTable(srcTable, numRows)

	dstTable := baseName + "_2"

	srcSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, srcTable)
	dstSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, dstTable)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.suffix, srcTable)

	jobName := AddSuffix(s, baseName)
	qrepConfig := CreateQRepWorkflowConfig(
		s.t,
		jobName,
		srcSchemaQualified,
		dstSchemaQualified,
		query,
		GeneratePostgresPeer(s.t).Name,
		"",
		true,
		"_PEERDB_SYNCED_AT",
		"",
	)

	tc := NewTemporalClient(s.t)
	env := RunQRepFlowWorkflow(s.t, tc, qrepConfig)
	EnvWaitForFinished(s.t, env, 3*time.Minute)
	require.NoError(s.t, env.Error(s.t.Context()))
}

func (s PeerFlowE2ETestSuitePG) TestQRepPause() {
	numRows := 10

	srcTable := "test_qrep_pause"
	s.setupSourceTable(srcTable, numRows)

	dstTable := "test_qrep_pause_dst"

	srcSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, srcTable)
	dstSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, dstTable)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.suffix, srcTable)

	jobName := AddSuffix(s, srcTable)
	config := CreateQRepWorkflowConfig(
		s.t,
		jobName,
		srcSchemaQualified,
		dstSchemaQualified,
		query,
		GeneratePostgresPeer(s.t).Name,
		"",
		true,
		"_PEERDB_SYNCED_AT",
		"",
	)
	config.InitialCopyOnly = false

	tc := NewTemporalClient(s.t)
	env := RunQRepFlowWorkflow(s.t, tc, config)
	SignalWorkflow(s.t.Context(), env, model.FlowSignal, model.PauseSignal)

	EnvWaitFor(s.t, env, 3*time.Minute, "pausing", func() bool {
		response, err := env.Query(s.t.Context(), shared.QRepFlowStateQuery)
		if err != nil {
			s.t.Log(err)
			return false
		}
		var state *protos.QRepFlowState
		err = response.Get(&state)
		if err != nil {
			s.t.Fatal("decode failed", err)
		}
		return state.CurrentFlowStatus == protos.FlowStatus_STATUS_PAUSED
	})
	SignalWorkflow(s.t.Context(), env, model.FlowSignal, model.NoopSignal)
	EnvWaitFor(s.t, env, time.Minute, "unpausing", func() bool {
		response, err := env.Query(s.t.Context(), shared.QRepFlowStateQuery)
		if err != nil {
			s.t.Fatal(err)
		}
		var state *protos.QRepFlowState
		if err := response.Get(&state); err != nil {
			s.t.Fatal("decode failed", err)
		}
		return state.CurrentFlowStatus == protos.FlowStatus_STATUS_RUNNING
	})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) TestXminPause() {
	numRows := 10

	baseName := "test_xmin_pause_pg"
	srcTable := "xmin_pause"
	s.setupSourceTable(srcTable, numRows)

	dstTable := "xmin_pause_dst"

	srcSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, srcTable)
	dstSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, dstTable)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s", s.suffix, srcTable)

	jobName := AddSuffix(s, baseName)
	config := CreateQRepWorkflowConfig(
		s.t,
		jobName,
		srcSchemaQualified,
		dstSchemaQualified,
		query,
		GeneratePostgresPeer(s.t).Name,
		"",
		true,
		"_PEERDB_SYNCED_AT",
		"",
	)
	config.WatermarkColumn = "xmin"
	config.InitialCopyOnly = false

	tc := NewTemporalClient(s.t)
	env := RunQRepFlowWorkflow(s.t, tc, config)
	SignalWorkflow(s.t.Context(), env, model.FlowSignal, model.PauseSignal)

	EnvWaitFor(s.t, env, 3*time.Minute, "pausing", func() bool {
		response, err := env.Query(s.t.Context(), shared.QRepFlowStateQuery)
		if err != nil {
			s.t.Log(err)
			return false
		}
		var state *protos.QRepFlowState
		err = response.Get(&state)
		if err != nil {
			s.t.Fatal("decode failed", err)
		}
		return state.CurrentFlowStatus == protos.FlowStatus_STATUS_PAUSED
	})
	SignalWorkflow(s.t.Context(), env, model.FlowSignal, model.NoopSignal)
	EnvWaitFor(s.t, env, time.Minute, "unpausing", func() bool {
		response, err := env.Query(s.t.Context(), shared.QRepFlowStateQuery)
		if err != nil {
			s.t.Fatal(err)
		}
		var state *protos.QRepFlowState
		if err := response.Get(&state); err != nil {
			s.t.Fatal("decode failed", err)
		}
		return state.CurrentFlowStatus == protos.FlowStatus_STATUS_RUNNING
	})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) TestTransform() {
	numRows := 10

	srcTable := "test_transform"
	s.setupSourceTable(srcTable, numRows)

	dstTable := "test_transformdst"

	srcSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, srcTable)
	dstSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, dstTable)

	query := fmt.Sprintf("SELECT * FROM %s WHERE updated_at BETWEEN {{.start}} AND {{.end}}", srcSchemaQualified)

	_, err := s.Conn().Exec(s.t.Context(), `insert into public.scripts (name, lang, source) values
	('pgtransform', 'lua', 'function transformRow(row) row.myreal = 1729 end') on conflict do nothing`)
	require.NoError(s.t, err)

	jobName := AddSuffix(s, srcTable)
	qrepConfig := CreateQRepWorkflowConfig(
		s.t,
		jobName,
		srcSchemaQualified,
		dstSchemaQualified,
		query,
		GeneratePostgresPeer(s.t).Name,
		"",
		true,
		"_PEERDB_SYNCED_AT",
		"",
	)
	qrepConfig.WriteMode = &protos.QRepWriteMode{
		WriteType: protos.QRepWriteType_QREP_WRITE_MODE_OVERWRITE,
	}
	qrepConfig.InitialCopyOnly = false
	qrepConfig.Script = "pgtransform"

	tc := NewTemporalClient(s.t)
	env := RunQRepFlowWorkflow(s.t, tc, qrepConfig)
	EnvWaitFor(s.t, env, 3*time.Minute, "waiting for first sync to complete", func() bool {
		return s.compareCounts(dstSchemaQualified, int64(numRows)) == nil
	})
	require.NoError(s.t, env.Error(s.t.Context()))

	var exists bool
	err = s.Conn().QueryRow(s.t.Context(),
		fmt.Sprintf("select exists(select * from %s where myreal <> 1729)", dstSchemaQualified)).Scan(&exists)
	require.NoError(s.t, err)
	require.False(s.t, exists)
}
