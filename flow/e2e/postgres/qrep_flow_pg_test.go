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
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/require"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
)

type PeerFlowE2ETestSuitePG struct {
	t *testing.T

	conn   *connpostgres.PostgresConnector
	peer   *protos.Peer
	suffix string
}

func (s PeerFlowE2ETestSuitePG) T() *testing.T {
	return s.t
}

func (s PeerFlowE2ETestSuitePG) Conn() *pgx.Conn {
	return s.conn.Conn()
}

func (s PeerFlowE2ETestSuitePG) Connector() *connpostgres.PostgresConnector {
	return s.conn
}

func (s PeerFlowE2ETestSuitePG) Suffix() string {
	return s.suffix
}

func TestPeerFlowE2ETestSuitePG(t *testing.T) {
	e2eshared.RunSuite(t, SetupSuite, func(s PeerFlowE2ETestSuitePG) {
		e2e.TearDownPostgres(s)
	})
}

func SetupSuite(t *testing.T) PeerFlowE2ETestSuitePG {
	t.Helper()

	err := godotenv.Load()
	if err != nil {
		// it's okay if the .env file is not present
		// we will use the default values
		t.Log("Unable to load .env file, using default values from env")
	}

	suffix := "pg_" + strings.ToLower(shared.RandomString(8))
	conn, err := e2e.SetupPostgres(t, suffix)
	require.NoError(t, err, "failed to setup postgres")

	return PeerFlowE2ETestSuitePG{
		t:      t,
		conn:   conn,
		peer:   e2e.GeneratePostgresPeer(),
		suffix: suffix,
	}
}

func (s PeerFlowE2ETestSuitePG) setupSourceTable(tableName string, rowCount int) {
	err := e2e.CreateTableForQRep(s.Conn(), s.suffix, tableName)
	require.NoError(s.t, err)

	if rowCount > 0 {
		err = e2e.PopulateSourceTable(s.Conn(), s.suffix, tableName, rowCount)
		require.NoError(s.t, err)
	}
}

func (s PeerFlowE2ETestSuitePG) comparePGTables(srcSchemaQualified, dstSchemaQualified, selector string) error {
	// Execute the two EXCEPT queries
	if err := s.compareQuery(srcSchemaQualified, dstSchemaQualified, selector); err != nil {
		return err
	}
	if err := s.compareQuery(dstSchemaQualified, srcSchemaQualified, selector); err != nil {
		return err
	}

	// If no error is returned, then the contents of the two tables are the same
	return nil
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

	setupError := make(chan error)
	go func() {
		setupError <- s.conn.SetupReplication(context.Background(), signal, setupReplicationInput)
	}()

	s.t.Log("waiting for slot creation to complete: ", flowJobName)
	slotInfo := <-signal.SlotCreated
	s.t.Logf("slot creation complete: %v. Signaling clone complete in 2 seconds", slotInfo)
	time.Sleep(2 * time.Second)
	close(signal.CloneComplete)

	require.NoError(s.t, <-setupError)
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

	postgresPeer := e2e.GeneratePostgresPeer()

	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
		"test_qrep_flow_avro_pg",
		srcSchemaQualified,
		dstSchemaQualified,
		query,
		postgresPeer,
		"",
		true,
		"",
	)
	require.NoError(s.t, err)

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.RunQrepFlowWorkflow(tc, qrepConfig)
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

	postgresPeer := e2e.GeneratePostgresPeer()

	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
		"test_qrep_columns_pg",
		srcSchemaQualified,
		dstSchemaQualified,
		query,
		postgresPeer,
		"",
		true,
		"_PEERDB_SYNCED_AT",
	)
	require.NoError(s.t, err)

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.RunQrepFlowWorkflow(tc, qrepConfig)
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)
	require.NoError(s.t, env.Error())

	err = s.checkSyncedAt(dstSchemaQualified)
	require.NoError(s.t, err)
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

	postgresPeer := e2e.GeneratePostgresPeer()

	qrepConfig, err := e2e.CreateQRepWorkflowConfig(
		"test_no_rows_qrep_pg",
		srcSchemaQualified,
		dstSchemaQualified,
		query,
		postgresPeer,
		"",
		true,
		"_PEERDB_SYNCED_AT",
	)
	require.NoError(s.t, err)

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.RunQrepFlowWorkflow(tc, qrepConfig)
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

	config, err := e2e.CreateQRepWorkflowConfig(
		"test_qrep_pause_pg",
		srcSchemaQualified,
		dstSchemaQualified,
		query,
		e2e.GeneratePostgresPeer(),
		"",
		true,
		"_PEERDB_SYNCED_AT",
	)
	require.NoError(s.t, err)

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.RunQrepFlowWorkflow(tc, config)
	e2e.SignalWorkflow(env, model.FlowSignal, model.PauseSignal)

	e2e.EnvWaitFor(s.t, env, time.Minute, "pausing", func() bool {
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
