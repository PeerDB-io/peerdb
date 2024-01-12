package e2e_postgres

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/require"
)

type PeerFlowE2ETestSuitePG struct {
	t *testing.T

	pool      *pgxpool.Pool
	peer      *protos.Peer
	connector *connpostgres.PostgresConnector
	suffix    string
}

func (s PeerFlowE2ETestSuitePG) T() *testing.T {
	return s.t
}

func (s PeerFlowE2ETestSuitePG) Pool() *pgxpool.Pool {
	return s.pool
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
		slog.Info("Unable to load .env file, using default values from env")
	}

	suffix := "pg_" + strings.ToLower(shared.RandomString(8))
	pool, err := e2e.SetupPostgres(suffix)
	if err != nil {
		require.Fail(t, "failed to setup postgres", err)
	}

	var connector *connpostgres.PostgresConnector
	connector, err = connpostgres.NewPostgresConnector(context.Background(),
		&protos.PostgresConfig{
			Host:     "localhost",
			Port:     7132,
			User:     "postgres",
			Password: "postgres",
			Database: "postgres",
		})
	require.NoError(t, err)

	return PeerFlowE2ETestSuitePG{
		t:         t,
		pool:      pool,
		peer:      generatePGPeer(e2e.GetTestPostgresConf()),
		connector: connector,
		suffix:    suffix,
	}
}

func (s PeerFlowE2ETestSuitePG) setupSourceTable(tableName string, rowCount int) {
	err := e2e.CreateTableForQRep(s.pool, s.suffix, tableName)
	require.NoError(s.t, err)
	err = e2e.PopulateSourceTable(s.pool, s.suffix, tableName, rowCount)
	require.NoError(s.t, err)
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
	err := s.pool.QueryRow(context.Background(), query).Scan(&exists)
	if err != nil {
		return err
	}

	if exists.Bool {
		return fmt.Errorf("enum comparison failed: rows are not equal\n")
	}
	return nil
}

func (s PeerFlowE2ETestSuitePG) compareQuery(srcSchemaQualified, dstSchemaQualified, selector string) error {
	query := fmt.Sprintf("SELECT %s FROM %s EXCEPT SELECT %s FROM %s", selector, srcSchemaQualified,
		selector, dstSchemaQualified)
	rows, err := s.pool.Query(context.Background(), query, pgx.QueryExecModeExec)
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
	query := fmt.Sprintf(`SELECT "_PEERDB_SYNCED_AT" FROM %s`, dstSchemaQualified)

	rows, _ := s.pool.Query(context.Background(), query)

	defer rows.Close()
	for rows.Next() {
		var syncedAt pgtype.Timestamp
		err := rows.Scan(&syncedAt)
		if err != nil {
			return err
		}

		if !syncedAt.Valid {
			return fmt.Errorf("synced_at is not valid")
		}
	}

	return rows.Err()
}

func (s PeerFlowE2ETestSuitePG) RunInt64Query(query string) (int64, error) {
	var count pgtype.Int8
	err := s.pool.QueryRow(context.Background(), query).Scan(&count)
	return count.Int64, err
}

func (s PeerFlowE2ETestSuitePG) TestSimpleSlotCreation() {
	setupTx, err := s.pool.Begin(context.Background())
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
	flowLog := slog.String(string(shared.FlowNameKey), flowJobName)
	setupReplicationInput := &protos.SetupReplicationInput{
		FlowJobName: flowJobName,
		TableNameMapping: map[string]string{
			fmt.Sprintf("e2e_test_%s.test_1", s.suffix): "test_1_dst",
		},
	}

	signal := connpostgres.NewSlotSignal()

	setupError := make(chan error)
	go func() {
		setupError <- s.connector.SetupReplication(signal, setupReplicationInput)
	}()

	slog.Info("waiting for slot creation to complete", flowLog)
	slotInfo := <-signal.SlotCreated
	slog.Info(fmt.Sprintf("slot creation complete: %v", slotInfo), flowLog)

	slog.Info("signaling clone complete after waiting for 2 seconds", flowLog)
	time.Sleep(2 * time.Second)
	signal.CloneComplete <- struct{}{}

	require.NoError(s.t, <-setupError)
	slog.Info("successfully setup replication", flowLog)
}

func (s PeerFlowE2ETestSuitePG) Test_Complete_QRep_Flow_Multi_Insert_PG() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	numRows := 10

	srcTable := "test_qrep_flow_avro_pg_1"
	s.setupSourceTable(srcTable, numRows)

	dstTable := "test_qrep_flow_avro_pg_2"

	err := e2e.CreateTableForQRep(s.pool, s.suffix, dstTable)
	require.NoError(s.t, err)

	srcSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, srcTable)
	dstSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, dstTable)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.suffix, srcTable)

	postgresPeer := e2e.GeneratePostgresPeer(e2e.PostgresPort)

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

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())

	err = env.GetWorkflowError()
	require.NoError(s.t, err)

	err = s.comparePGTables(srcSchemaQualified, dstSchemaQualified, "*")
	require.NoError(s.t, err)
}

func (s PeerFlowE2ETestSuitePG) Test_Setup_Destination_And_PeerDB_Columns_QRep_PG() {
	env := e2e.NewTemporalTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.t, env)

	numRows := 10

	srcTable := "test_qrep_columns_pg_1"
	s.setupSourceTable(srcTable, numRows)

	dstTable := "test_qrep_columns_pg_2"

	srcSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, srcTable)
	dstSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", s.suffix, dstTable)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		s.suffix, srcTable)

	postgresPeer := e2e.GeneratePostgresPeer(e2e.PostgresPort)

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

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	require.True(s.t, env.IsWorkflowCompleted())

	err = env.GetWorkflowError()
	require.NoError(s.t, err)

	err = s.checkSyncedAt(dstSchemaQualified)
	require.NoError(s.t, err)
}
