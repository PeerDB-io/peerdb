package e2e_postgres

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
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

func TestPeerFlowE2ETestSuitePG(t *testing.T) {
	e2eshared.GotSuite(t, setupSuite, func(s PeerFlowE2ETestSuitePG) {
		err := e2e.TearDownPostgres(s.pool, s.suffix)
		if err != nil {
			s.t.Fatal("failed to drop Postgres schema", err)
		}
	})
}

func setupSuite(t *testing.T) PeerFlowE2ETestSuitePG {
	t.Helper()

	err := godotenv.Load()
	if err != nil {
		// it's okay if the .env file is not present
		// we will use the default values
		slog.Info("Unable to load .env file, using default values from env")
	}

	suffix := "pgtest_" + strings.ToLower(shared.RandomString(8))

	pool, err := e2e.SetupPostgres(suffix)
	if err != nil || pool == nil {
		t.Fatal("failed to setup postgres", err)
	}
	peer := generatePGPeer(e2e.GetTestPostgresConf())

	connector, err := connpostgres.NewPostgresConnector(context.Background(),
		&protos.PostgresConfig{
			Host:     "localhost",
			Port:     7132,
			User:     "postgres",
			Password: "postgres",
			Database: "postgres",
		}, false)
	require.NoError(t, err)
	return PeerFlowE2ETestSuitePG{
		t:         t,
		pool:      pool,
		peer:      peer,
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
	for {
		err := s.compareQuery(srcSchemaQualified, dstSchemaQualified, selector)
		// while testing, the prepared plan might break due to schema changes
		// solution is to retry, prepared statement should be evicted upon the first error
		if err != nil && !strings.Contains(err.Error(), "cached plan must not change result type") {
			return err
		}
		if err == nil {
			break
		}
	}

	for {
		err := s.compareQuery(dstSchemaQualified, srcSchemaQualified, selector)
		// while testing, the prepared plan might break due to schema changes
		// solution is to retry, prepared statement should be evicted upon the first error
		if err != nil && !strings.Contains(err.Error(), "cached plan must not change result type") {
			return err
		}
		if err == nil {
			break
		}
	}

	// If no error is returned, then the contents of the two tables are the same
	return nil
}

func (s PeerFlowE2ETestSuitePG) compareQuery(srcSchemaQualified, dstSchemaQualified, selector string) error {
	query := fmt.Sprintf("SELECT %s FROM %s EXCEPT SELECT %s FROM %s", selector, srcSchemaQualified,
		selector, dstSchemaQualified)
	rows, err := s.pool.Query(context.Background(), query)
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

func (s PeerFlowE2ETestSuitePG) Test_Complete_QRep_Flow_Multi_Insert_PG() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)
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
	if err != nil {
		require.FailNow(s.t, err.Error())
	}

	env.AssertExpectations(s.t)
}

func (s PeerFlowE2ETestSuitePG) Test_Setup_Destination_And_PeerDB_Columns_QRep_PG() {
	env := e2e.NewTemporalTestWorkflowEnvironment(s.t)
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
	if err != nil {
		require.FailNow(s.t, err.Error())
	}

	env.AssertExpectations(s.t)
}
