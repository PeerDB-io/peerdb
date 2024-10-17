package e2e_postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
)

func TestPeerFlowE2ETestSuitePG(t *testing.T) {
	e2eshared.RunSuite(t, SetupSuite)
}

func (s PeerFlowE2ETestSuitePG) setupSourceTable(tableName string, rowCount int) {
	require.NoError(s.t, e2e.CreateTableForQRep(s.Conn(), s.suffix, tableName))
	s.populateSourceTable(tableName, rowCount)
}

func (s PeerFlowE2ETestSuitePG) populateSourceTable(tableName string, rowCount int) {
	if rowCount > 0 {
		require.NoError(s.t, e2e.PopulateSourceTable(s.Conn(), s.suffix, tableName, rowCount))
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
		"WHERE NOT EXISTS ("+
		"SELECT 1 FROM %s dst "+
		"WHERE src.my_mood::text = dst.my_mood::text)) LIMIT 1;", srcSchemaQualified,
		dstSchemaQualified)
	err := s.Conn().QueryRow(context.Background(), query).Scan(&exists)
	if err != nil {
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
	rows, err := s.Conn().Query(context.Background(), query, pgx.QueryExecModeExec)
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

	rows, _ := s.Conn().Query(context.Background(), query)

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
	err := s.Conn().QueryRow(context.Background(), query).Scan(&count)
	return count.Int64, err
}

func (s PeerFlowE2ETestSuitePG) TestSimpleSlotCreation() {
	setupTx, err := s.Conn().Begin(context.Background())
	require.NoError(s.t, err)
	// setup 3 tables in pgpeer_repl_test schema
	// test_1, test_2, test_3, all have 5 columns all text, c1, c2, c3, c4, c5
	tables := []string{"test_1", "test_2", "test_3"}
	for _, table := range tables {
		_, err = setupTx.Exec(context.Background(),
			fmt.Sprintf("CREATE TABLE e2e_test_%s.%s (c1 text, c2 text, c3 text, c4 text, c5 text)", s.suffix, table))
		require.NoError(s.t, err)
	}

	err = setupTx.Commit(context.Background())
	require.NoError(s.t, err)

	flowJobName := "test_simple_slot_creation"
	setupReplicationInput := &protos.SetupReplicationInput{
		FlowJobName: flowJobName,
		TableNameMapping: map[string]string{
			fmt.Sprintf("e2e_test_%s.test_1", s.suffix): "test_1_dst",
		},
	}

	signal := connpostgres.NewSlotSignal()
	go s.conn.SetupReplication(context.Background(), signal, setupReplicationInput)

	s.t.Log("waiting for slot creation to complete: " + flowJobName)
	slotInfo := <-signal.SlotCreated
	s.t.Logf("slot creation complete: %v. Signaling clone complete in 2 seconds", slotInfo)
	time.Sleep(2 * time.Second)
	close(signal.CloneComplete)

	require.NoError(s.t, slotInfo.Err)
	s.t.Logf("successfully setup replication: %s", flowJobName)
}

func (s PeerFlowE2ETestSuitePG) Test_Complete_QRep_Flow_Multi_Insert_PG() {
	numRows := 10

	srcTable := "test_qrep_flow_avro_pg_1"
	s.setupSourceTable(srcTable, numRows)

	dstTable := "test_qrep_flow_avro_pg_2"

	err := e2e.CreateTableForQRep(s.Conn(), s.suffix, dstTable)
	require.NoError(s.t, err)

	srcSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, srcTable)
	dstSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, dstTable)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.suffix, srcTable)

	qrepConfig := e2e.CreateQRepWorkflowConfig(
		s.t,
		"test_qrep_flow_avro_pg",
		srcSchemaQualified,
		dstSchemaQualified,
		query,
		e2e.GeneratePostgresPeer(s.t).Name,
		"",
		true,
		"",
		"",
	)

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.RunQRepFlowWorkflow(tc, qrepConfig)
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)
	require.NoError(s.t, env.Error())

	err = s.comparePGTables(srcSchemaQualified, dstSchemaQualified, "*")
	require.NoError(s.t, err)
}

func (s PeerFlowE2ETestSuitePG) Test_PG_TypeSystemQRep() {
	numRows := 10

	srcTable := "test_qrep_flow_pgpg_1"
	s.setupSourceTable(srcTable, numRows)

	dstTable := "test_qrep_flow_pgpg_2"

	err := e2e.CreateTableForQRep(s.Conn(), s.suffix, dstTable)
	require.NoError(s.t, err)

	srcSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, srcTable)
	dstSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, dstTable)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.suffix, srcTable)

	qrepConfig := e2e.CreateQRepWorkflowConfig(
		s.t,
		"test_qrep_flow_pgpg",
		srcSchemaQualified,
		dstSchemaQualified,
		query,
		e2e.GeneratePostgresPeer(s.t).Name,
		"",
		true,
		"",
		"",
	)
	qrepConfig.System = protos.TypeSystem_PG

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.RunQRepFlowWorkflow(tc, qrepConfig)
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)
	require.NoError(s.t, env.Error())

	err = s.comparePGTables(srcSchemaQualified, dstSchemaQualified, "*")
	require.NoError(s.t, err)
}

func (s PeerFlowE2ETestSuitePG) Test_PeerDB_Columns_QRep_PG() {
	numRows := 10

	srcTable := "test_qrep_columns_pg_1"
	s.setupSourceTable(srcTable, numRows)

	dstTable := "test_qrep_columns_pg_2"

	srcSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, srcTable)
	dstSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, dstTable)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.suffix, srcTable)

	qrepConfig := e2e.CreateQRepWorkflowConfig(
		s.t,
		"test_qrep_columns_pg",
		srcSchemaQualified,
		dstSchemaQualified,
		query,
		e2e.GeneratePostgresPeer(s.t).Name,
		"",
		true,
		"_PEERDB_SYNCED_AT",
		"",
	)

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.RunQRepFlowWorkflow(tc, qrepConfig)
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)
	require.NoError(s.t, env.Error())

	require.NoError(s.t, s.checkSyncedAt(dstSchemaQualified))
}

func (s PeerFlowE2ETestSuitePG) Test_Overwrite_PG() {
	numRows := 10

	srcTable := "test_overwrite_pg_1"
	s.setupSourceTable(srcTable, numRows)

	dstTable := "test_overwrite_pg_2"

	srcSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, srcTable)
	dstSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, dstTable)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.suffix, srcTable)

	qrepConfig := e2e.CreateQRepWorkflowConfig(
		s.t,
		"test_overwrite_pg",
		srcSchemaQualified,
		dstSchemaQualified,
		query,
		e2e.GeneratePostgresPeer(s.t).Name,
		"",
		true,
		"_PEERDB_SYNCED_AT",
		"",
	)
	qrepConfig.WriteMode = &protos.QRepWriteMode{
		WriteType: protos.QRepWriteType_QREP_WRITE_MODE_OVERWRITE,
	}
	qrepConfig.InitialCopyOnly = false

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.RunQRepFlowWorkflow(tc, qrepConfig)
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "waiting for first sync to complete", func() bool {
		err := s.compareCounts(dstSchemaQualified, int64(numRows))
		return err == nil
	})

	newRowCount := 5
	s.populateSourceTable(srcTable, newRowCount)

	e2e.EnvWaitFor(s.t, env, 2*time.Minute, "waiting for overwrite sync to complete", func() bool {
		err := s.compareCounts(dstSchemaQualified, int64(newRowCount))
		return err == nil
	})

	require.NoError(s.t, env.Error())
}

func (s PeerFlowE2ETestSuitePG) Test_No_Rows_QRep_PG() {
	numRows := 0

	srcTable := "test_no_rows_qrep_pg_1"
	s.setupSourceTable(srcTable, numRows)

	dstTable := "test_no_rows_qrep_pg_2"

	srcSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, srcTable)
	dstSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, dstTable)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.suffix, srcTable)

	qrepConfig := e2e.CreateQRepWorkflowConfig(
		s.t,
		"test_no_rows_qrep_pg",
		srcSchemaQualified,
		dstSchemaQualified,
		query,
		e2e.GeneratePostgresPeer(s.t).Name,
		"",
		true,
		"_PEERDB_SYNCED_AT",
		"",
	)

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.RunQRepFlowWorkflow(tc, qrepConfig)
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)
	require.NoError(s.t, env.Error())
}

func (s PeerFlowE2ETestSuitePG) Test_Pause() {
	numRows := 10

	srcTable := "qrep_pause"
	s.setupSourceTable(srcTable, numRows)

	dstTable := "qrep_pause_dst"

	srcSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, srcTable)
	dstSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, dstTable)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.suffix, srcTable)

	config := e2e.CreateQRepWorkflowConfig(
		s.t,
		"test_qrep_pause_pg",
		srcSchemaQualified,
		dstSchemaQualified,
		query,
		e2e.GeneratePostgresPeer(s.t).Name,
		"",
		true,
		"_PEERDB_SYNCED_AT",
		"",
	)
	config.InitialCopyOnly = false

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.RunQRepFlowWorkflow(tc, config)
	e2e.SignalWorkflow(env, model.FlowSignal, model.PauseSignal)

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "pausing", func() bool {
		response, err := env.Query(shared.QRepFlowStateQuery)
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
	e2e.SignalWorkflow(env, model.FlowSignal, model.NoopSignal)
	e2e.EnvWaitFor(s.t, env, time.Minute, "unpausing", func() bool {
		response, err := env.Query(shared.QRepFlowStateQuery)
		if err != nil {
			s.t.Fatal(err)
		}
		var state *protos.QRepFlowState
		err = response.Get(&state)
		if err != nil {
			s.t.Fatal("decode failed", err)
		}
		return state.CurrentFlowStatus == protos.FlowStatus_STATUS_RUNNING
	})

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PeerFlowE2ETestSuitePG) TestTransform() {
	numRows := 10

	srcTable := "test_transform"
	s.setupSourceTable(srcTable, numRows)

	dstTable := "test_transformdst"

	srcSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, srcTable)
	dstSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, dstTable)

	query := fmt.Sprintf("SELECT * FROM %s WHERE updated_at BETWEEN {{.start}} AND {{.end}}", srcSchemaQualified)

	_, err := s.Conn().Exec(context.Background(), `insert into public.scripts (name, lang, source) values
	('pgtransform', 'lua', 'function transformRow(row) row.myreal = 1729 end') on conflict do nothing`)
	require.NoError(s.t, err)

	qrepConfig := e2e.CreateQRepWorkflowConfig(
		s.t,
		"test_transform",
		srcSchemaQualified,
		dstSchemaQualified,
		query,
		e2e.GeneratePostgresPeer(s.t).Name,
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

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.RunQRepFlowWorkflow(tc, qrepConfig)
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "waiting for first sync to complete", func() bool {
		err := s.compareCounts(dstSchemaQualified, int64(numRows))
		return err == nil
	})
	require.NoError(s.t, env.Error())

	var exists bool
	err = s.Conn().QueryRow(context.Background(),
		fmt.Sprintf("select exists(select * from %s where myreal <> 1729)", dstSchemaQualified)).Scan(&exists)
	require.NoError(s.t, err)
	require.False(s.t, exists)
}
