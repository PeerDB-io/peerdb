package connpostgres

import (
	"context"
	"fmt"
	"log/slog"
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
	t *testing.T

	connector *PostgresConnector
	schema    string
}

func setupSuite(t *testing.T, g got.G) PostgresReplicationSnapshotTestSuite {
	connector, err := NewPostgresConnector(context.Background(), &protos.PostgresConfig{
		Host:     "localhost",
		Port:     7132,
		User:     "postgres",
		Password: "postgres",
		Database: "postgres",
	}, true)
	require.NoError(t, err)

	setupTx, err := connector.pool.Begin(context.Background())
	require.NoError(t, err)
	defer func() {
		err := setupTx.Rollback(context.Background())
		if err != pgx.ErrTxClosed {
			require.NoError(t, err)
		}
	}()

	schema := "repltest_" + shared.RandomString(8)

	_, err = setupTx.Exec(context.Background(),
		fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
	require.NoError(t, err)

	_, err = setupTx.Exec(context.Background(),
		fmt.Sprintf("CREATE SCHEMA %s", schema))
	require.NoError(t, err)

	// setup 3 tables test_1, test_2, test_3
	// all have 5 text columns c1, c2, c3, c4, c5
	tables := []string{"test_1", "test_2", "test_3"}
	for _, table := range tables {
		_, err = setupTx.Exec(context.Background(),
			fmt.Sprintf("CREATE TABLE %s.%s (c1 text, c2 text, c3 text, c4 text, c5 text)", schema, table))
		require.NoError(t, err)
	}

	err = setupTx.Commit(context.Background())
	require.NoError(t, err)

	return PostgresReplicationSnapshotTestSuite{
		G:         g,
		t:         t,
		connector: connector,
		schema:    schema,
	}
}

func (suite PostgresReplicationSnapshotTestSuite) TearDownSuite() {
	teardownTx, err := suite.connector.pool.Begin(context.Background())
	require.NoError(suite.t, err)
	defer func() {
		err := teardownTx.Rollback(context.Background())
		if err != pgx.ErrTxClosed {
			require.NoError(suite.t, err)
		}
	}()

	_, err = teardownTx.Exec(context.Background(),
		fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", suite.schema))
	require.NoError(suite.t, err)

	// Fetch all the publications
	rows, err := teardownTx.Query(context.Background(),
		"SELECT pubname FROM pg_publication WHERE pubname LIKE $1", fmt.Sprintf("%%%s", "test_simple_slot_creation"))
	require.NoError(suite.t, err)

	// Iterate over the publications and drop them
	for rows.Next() {
		var pubname pgtype.Text
		err := rows.Scan(&pubname)
		require.NoError(suite.t, err)

		// Drop the publication in a new transaction
		_, err = suite.connector.pool.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION %s", pubname.String))
		require.NoError(suite.t, err)
	}

	_, err = teardownTx.Exec(context.Background(),
		"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE $1",
		fmt.Sprintf("%%%s", "test_simple_slot_creation"))
	require.NoError(suite.t, err)

	err = teardownTx.Commit(context.Background())
	require.NoError(suite.t, err)

	suite.True(suite.connector.ConnectionActive() == nil)

	err = suite.connector.Close()
	require.NoError(suite.t, err)

	suite.False(suite.connector.ConnectionActive() == nil)
}

func (suite PostgresReplicationSnapshotTestSuite) TestSimpleSlotCreation() {
	tables := map[string]string{
		suite.schema + ".test_1": "test_1_dst",
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
		require.NoError(suite.t, err)
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
	got.Each(t, e2eshared.GotSuite(setupSuite))
}
