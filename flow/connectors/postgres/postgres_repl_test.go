package connpostgres

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type PostgresReplicationSnapshotTestSuite struct {
	suite.Suite
	connector *PostgresConnector
}

func (suite *PostgresReplicationSnapshotTestSuite) SetupSuite() {
	var err error
	suite.connector, err = NewPostgresConnector(context.Background(), &protos.PostgresConfig{
		Host:     "localhost",
		Port:     7132,
		User:     "postgres",
		Password: "postgres",
		Database: "postgres",
	})
	require.NoError(suite.T(), err)

	setupTx, err := suite.connector.pool.Begin(context.Background())
	require.NoError(suite.T(), err)
	defer func() {
		err := setupTx.Rollback(context.Background())
		if err != pgx.ErrTxClosed {
			require.NoError(suite.T(), err)
		}
	}()

	_, err = setupTx.Exec(context.Background(), "DROP SCHEMA IF EXISTS pgpeer_repl_test CASCADE")
	require.NoError(suite.T(), err)

	_, err = setupTx.Exec(context.Background(), "CREATE SCHEMA pgpeer_repl_test")
	require.NoError(suite.T(), err)

	// setup 3 tables in pgpeer_repl_test schema
	// test_1, test_2, test_3, all have 5 columns all text, c1, c2, c3, c4, c5
	tables := []string{"test_1", "test_2", "test_3"}
	for _, table := range tables {
		_, err = setupTx.Exec(context.Background(),
			fmt.Sprintf("CREATE TABLE pgpeer_repl_test.%s (c1 text, c2 text, c3 text, c4 text, c5 text)", table))
		require.NoError(suite.T(), err)
	}

	err = setupTx.Commit(context.Background())
	require.NoError(suite.T(), err)
}

func (suite *PostgresReplicationSnapshotTestSuite) TearDownSuite() {
	teardownTx, err := suite.connector.pool.Begin(context.Background())
	require.NoError(suite.T(), err)
	defer func() {
		err := teardownTx.Rollback(context.Background())
		if err != pgx.ErrTxClosed {
			require.NoError(suite.T(), err)
		}
	}()

	_, err = teardownTx.Exec(context.Background(), "DROP SCHEMA IF EXISTS pgpeer_test CASCADE")
	require.NoError(suite.T(), err)

	// Fetch all the publications
	rows, err := teardownTx.Query(context.Background(),
		"SELECT pubname FROM pg_publication WHERE pubname LIKE $1", fmt.Sprintf("%%%s", "test_simple_slot_creation"))
	require.NoError(suite.T(), err)

	// Iterate over the publications and drop them
	for rows.Next() {
		var pubname pgtype.Text
		err := rows.Scan(&pubname)
		require.NoError(suite.T(), err)

		// Drop the publication in a new transaction
		_, err = suite.connector.pool.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION %s", pubname.String))
		require.NoError(suite.T(), err)
	}

	_, err = teardownTx.Exec(context.Background(),
		"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE $1",
		fmt.Sprintf("%%%s", "test_simple_slot_creation"))
	require.NoError(suite.T(), err)

	err = teardownTx.Commit(context.Background())
	require.NoError(suite.T(), err)

	suite.True(suite.connector.ConnectionActive() == nil)

	err = suite.connector.Close()
	require.NoError(suite.T(), err)

	suite.False(suite.connector.ConnectionActive() == nil)
}

func (suite *PostgresReplicationSnapshotTestSuite) TestSimpleSlotCreation() {

	tables := map[string]string{
		"pgpeer_repl_test.test_1": "test_1_dst",
	}

	flowJobName := "test_simple_slot_creation"
	flowLog := slog.String(string(shared.FlowNameKey), flowJobName)
	setupReplicationInput := &protos.SetupReplicationInput{
		FlowJobName:      flowJobName,
		TableNameMapping: tables,
	}

	signal := NewSlotSignal()

	// Moved to a go routine
	go func() {
		err := suite.connector.SetupReplication(signal, setupReplicationInput)
		require.NoError(suite.T(), err)
	}()

	slog.Info("waiting for slot creation to complete", flowLog)
	slotInfo := <-signal.SlotCreated
	slog.Info(fmt.Sprintf("slot creation complete: %v", slotInfo), flowLog)

	slog.Info("signaling clone complete after waiting for 2 seconds", flowLog)
	time.Sleep(2 * time.Second)
	signal.CloneComplete <- struct{}{}

	slog.Info("successfully setup replication", flowLog)
}

func TestPostgresReplTestSuite(t *testing.T) {
	suite.Run(t, new(PostgresReplicationSnapshotTestSuite))
}
