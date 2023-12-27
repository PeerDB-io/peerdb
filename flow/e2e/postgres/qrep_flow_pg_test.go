package e2e_postgres

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
)

const postgresSuffix = "postgres"

type PeerFlowE2ETestSuitePG struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	pool      *pgxpool.Pool
	peer      *protos.Peer
	connector *connpostgres.PostgresConnector
}

func TestPeerFlowE2ETestSuitePG(t *testing.T) {
	suite.Run(t, new(PeerFlowE2ETestSuitePG))
}

// Implement SetupAllSuite interface to setup the test suite
func (s *PeerFlowE2ETestSuitePG) SetupSuite() {
	err := godotenv.Load()
	if err != nil {
		// it's okay if the .env file is not present
		// we will use the default values
		slog.Info("Unable to load .env file, using default values from env")
	}

	pool, err := e2e.SetupPostgres(postgresSuffix)
	if err != nil || pool == nil {
		s.Fail("failed to setup postgres", err)
	}
	s.pool = pool
	s.peer = generatePGPeer(e2e.GetTestPostgresConf())

	s.connector, err = connpostgres.NewPostgresConnector(context.Background(),
		&protos.PostgresConfig{
			Host:     "localhost",
			Port:     7132,
			User:     "postgres",
			Password: "postgres",
			Database: "postgres",
		}, false)
	s.NoError(err)
}

// Implement TearDownAllSuite interface to tear down the test suite
func (s *PeerFlowE2ETestSuitePG) TearDownSuite() {
	err := e2e.TearDownPostgres(s.pool, postgresSuffix)
	if err != nil {
		s.Fail("failed to drop Postgres schema", err)
	}
}

func (s *PeerFlowE2ETestSuitePG) setupSourceTable(tableName string, rowCount int) {
	err := e2e.CreateTableForQRep(s.pool, postgresSuffix, tableName)
	s.NoError(err)
	err = e2e.PopulateSourceTable(s.pool, postgresSuffix, tableName, rowCount)
	s.NoError(err)
}

func (s *PeerFlowE2ETestSuitePG) comparePGTables(srcSchemaQualified, dstSchemaQualified, selector string) error {
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

func (s *PeerFlowE2ETestSuitePG) compareQuery(srcSchemaQualified, dstSchemaQualified, selector string) error {
	query := fmt.Sprintf("SELECT %s FROM %s EXCEPT SELECT %s FROM %s", selector, srcSchemaQualified,
		selector, dstSchemaQualified)
	rows, _ := s.pool.Query(context.Background(), query)
	rowsPresent := false

	defer rows.Close()
	for rows.Next() {
		rowsPresent = true
		values, err := rows.Values()
		if err != nil {
			return err
		}

		columns := rows.FieldDescriptions()

		for i, value := range values {
			fmt.Printf("%s: %v\n", columns[i].Name, value)
		}
		fmt.Println("---")
	}

	if rows.Err() != nil {
		return rows.Err()
	}
	if rowsPresent {
		return fmt.Errorf("comparison failed: rows are not equal")
	}
	return nil
}

func (s *PeerFlowE2ETestSuitePG) checkSyncedAt(dstSchemaQualified string) error {
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

func (s *PeerFlowE2ETestSuitePG) Test_Complete_QRep_Flow_Multi_Insert_PG() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.T(), env)

	numRows := 10

	srcTable := "test_qrep_flow_avro_pg_1"
	s.setupSourceTable(srcTable, numRows)

	dstTable := "test_qrep_flow_avro_pg_2"

	err := e2e.CreateTableForQRep(s.pool, postgresSuffix, dstTable)
	s.NoError(err)

	srcSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", postgresSuffix, srcTable)
	dstSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", postgresSuffix, dstTable)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		postgresSuffix, srcTable)

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
	s.NoError(err)

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())

	err = env.GetWorkflowError()
	s.NoError(err)

	err = s.comparePGTables(srcSchemaQualified, dstSchemaQualified, "*")
	if err != nil {
		s.FailNow(err.Error())
	}

	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuitePG) Test_Setup_Destination_And_PeerDB_Columns_QRep_PG() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(s.T(), env)

	numRows := 10

	srcTable := "test_qrep_columns_pg_1"
	s.setupSourceTable(srcTable, numRows)

	dstTable := "test_qrep_columns_pg_2"

	srcSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", postgresSuffix, srcTable)
	dstSchemaQualified := fmt.Sprintf("%s_%s.%s", "e2e_test", postgresSuffix, dstTable)

	query := fmt.Sprintf("SELECT * FROM e2e_test_%s.%s WHERE updated_at BETWEEN {{.start}} AND {{.end}}",
		postgresSuffix, srcTable)

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
	s.NoError(err)

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())

	err = env.GetWorkflowError()
	s.NoError(err)

	err = s.checkSyncedAt(dstSchemaQualified)
	if err != nil {
		s.FailNow(err.Error())
	}

	env.AssertExpectations(s.T())
}
