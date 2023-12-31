package connpostgres

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	"github.com/ysmood/got"
)

type PostgresReplicationSnapshotTestSuite struct {
	got.G
	t         *testing.T
	connector *PostgresConnector
	schema    string
}

func SetupSuite(t *testing.T, g got.G) PostgresReplicationSnapshotTestSuite {
	t.Helper()

	connector, err := NewPostgresConnector(context.Background(), &protos.PostgresConfig{
		Host:     "localhost",
		Port:     7132,
		User:     "postgres",
		Password: "postgres",
		Database: "postgres",
	})
	require.NoError(t, err)

	setupTx, err := connector.pool.Begin(context.Background())
	require.NoError(t, err)
	defer func() {
		err := setupTx.Rollback(context.Background())
		if err != pgx.ErrTxClosed {
			require.NoError(t, err)
		}
	}()

	schema := "pgrepl_" + strings.ToLower(shared.RandomString(8))
	_, err = setupTx.Exec(context.Background(),
		fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
	require.NoError(t, err)

	_, err = setupTx.Exec(context.Background(),
		fmt.Sprintf("CREATE SCHEMA %s", schema))
	require.NoError(t, err)

	// make 3 test tables with 5 text columns
	tables := []string{"test_1", "test_2", "test_3"}
	for _, table := range tables {
		_, err = setupTx.Exec(context.Background(),
			fmt.Sprintf("CREATE TABLE %s.%s (c1 text, c2 text, c3 text, c4 text, c5 text)", schema, table))
		require.NoError(t, err)
	}

	require.NoError(t, setupTx.Commit(context.Background()))
	return PostgresReplicationSnapshotTestSuite{
		G:         g,
		t:         t,
		connector: connector,
		schema:    schema,
	}
}

func (s PostgresReplicationSnapshotTestSuite) TestSimpleSlotCreation() {
	tables := map[string]string{
		fmt.Sprintf("%s.test_1", s.schema): "test_1_dst",
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
		err := s.connector.SetupReplication(signal, setupReplicationInput)
		require.NoError(s.t, err)
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
	e2eshared.GotSuite(t, SetupSuite, func(s PostgresReplicationSnapshotTestSuite) {
		teardownTx, err := s.connector.pool.Begin(context.Background())
		require.NoError(s.t, err)
		defer func() {
			err := teardownTx.Rollback(context.Background())
			if err != pgx.ErrTxClosed {
				require.NoError(s.t, err)
			}
		}()

		_, err = teardownTx.Exec(context.Background(), "DROP SCHEMA IF EXISTS pgpeer_test CASCADE")
		require.NoError(s.t, err)

		// Fetch all the publications
		rows, err := teardownTx.Query(context.Background(),
			"SELECT pubname FROM pg_publication WHERE pubname LIKE $1", fmt.Sprintf("%%%s", "test_simple_slot_creation"))
		require.NoError(s.t, err)

		// Iterate over the publications and drop them
		for rows.Next() {
			var pubname pgtype.Text
			err := rows.Scan(&pubname)
			require.NoError(s.t, err)

			// Drop the publication in a new transaction
			_, err = s.connector.pool.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION %s", pubname.String))
			require.NoError(s.t, err)
		}

		_, err = teardownTx.Exec(context.Background(),
			"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE $1",
			fmt.Sprintf("%%%s", "test_simple_slot_creation"))
		require.NoError(s.t, err)

		err = teardownTx.Commit(context.Background())
		require.NoError(s.t, err)

		s.True(s.connector.ConnectionActive() == nil)

		err = s.connector.Close()
		require.NoError(s.t, err)

		s.False(s.connector.ConnectionActive() == nil)
	})
}
