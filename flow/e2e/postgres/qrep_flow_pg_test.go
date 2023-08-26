package e2e_postgres

import (
	"context"
	"fmt"
	"testing"

	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
)

const postgresSuffix = "postgres"

type PeerFlowE2ETestSuitePG struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	pool *pgxpool.Pool
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
		log.Infof("Unable to load .env file, using default values from env")
	}

	log.SetReportCaller(true)

	pool, err := e2e.SetupPostgres(postgresSuffix)
	if err != nil {
		s.Fail("failed to setup postgres", err)
	}
	s.pool = pool
}

// Implement TearDownAllSuite interface to tear down the test suite
func (s *PeerFlowE2ETestSuitePG) TearDownSuite() {
	err := e2e.TearDownPostgres(s.pool, postgresSuffix)
	if err != nil {
		s.Fail("failed to drop Postgres schema", err)
	}
}

func (s *PeerFlowE2ETestSuitePG) setupSourceTable(tableName string, rowCount int) {
	e2e.CreateSourceTableQRep(s.pool, postgresSuffix, tableName)
	e2e.PopulateSourceTable(s.pool, postgresSuffix, tableName, rowCount)
}

func (s *PeerFlowE2ETestSuitePG) comparePGTables(srcSchemaQualified, dstSchemaQualified string) error {
	// Execute the two EXCEPT queries
	err := s.compareQuery(srcSchemaQualified, dstSchemaQualified)
	if err != nil {
		return err
	}

	err = s.compareQuery(dstSchemaQualified, srcSchemaQualified)
	if err != nil {
		return err
	}

	// If no error is returned, then the contents of the two tables are the same
	return nil
}

func (s *PeerFlowE2ETestSuitePG) compareQuery(schema1, schema2 string) error {
	query := fmt.Sprintf("SELECT * FROM %s EXCEPT SELECT * FROM %s", schema1, schema2)
	rows, _ := s.pool.Query(context.Background(), query)

	defer rows.Close()
	for rows.Next() {
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

	return rows.Err()
}

func (s *PeerFlowE2ETestSuitePG) Test_Complete_QRep_Flow_Multi_Insert_PG() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	numRows := 10

	srcTable := "test_qrep_flow_avro_pg_1"
	s.setupSourceTable(srcTable, numRows)

	dstTable := "test_qrep_flow_avro_pg_2"
	e2e.CreateSourceTableQRep(s.pool, postgresSuffix, dstTable) // the name is misleading, but this is the destination table

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
		protos.QRepSyncMode_QREP_SYNC_MODE_MULTI_INSERT,
		postgresPeer,
		"",
	)
	s.NoError(err)

	e2e.RunQrepFlowWorkflow(env, qrepConfig)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())

	// assert that error contains "invalid connection configs"
	err = env.GetWorkflowError()
	s.NoError(err)

	err = s.comparePGTables(srcSchemaQualified, dstSchemaQualified)
	if err != nil {
		s.FailNow(err.Error())
	}

	env.AssertExpectations(s.T())
}
