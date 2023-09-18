package e2e_eventhub

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/PeerDB-io/peer-flow/e2e"
	util "github.com/PeerDB-io/peer-flow/utils"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
)

const eventhubSuffix = "eventhub"

type PeerFlowE2ETestSuiteEH struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	pool     *pgxpool.Pool
	ehHelper *EventHubTestHelper
}

func TestPeerFlowE2ETestSuiteEH(t *testing.T) {
	suite.Run(t, new(PeerFlowE2ETestSuiteEH))
}

func (s *PeerFlowE2ETestSuiteEH) setupEventHub() error {
	enableEHT := os.Getenv("ENABLE_EVENT_HUB_TESTS")
	if enableEHT == "" {
		return nil
	}

	pgConf := e2e.GetTestPostgresConf()
	helper, err := NewEventHubTestHelper(pgConf)
	if err != nil {
		return err
	}

	s.ehHelper = helper
	return nil
}

func (s *PeerFlowE2ETestSuiteEH) SetupSuite() {
	err := godotenv.Load()
	if err != nil {
		// it's okay if the .env file is not present
		// we will use the default values
		log.Infof("Unable to load .env file, using default values from env")
	}

	log.SetReportCaller(true)

	pool, err := e2e.SetupPostgres(eventhubSuffix)
	if err != nil {
		s.Fail("failed to setup postgres", err)
	}
	s.pool = pool

	err = s.setupEventHub()
	if err != nil {
		s.Fail("failed to setup eventhub", err)
	}
}

// Implement TearDownAllSuite interface to tear down the test suite
func (s *PeerFlowE2ETestSuiteEH) TearDownSuite() {
	err := e2e.TearDownPostgres(s.pool, eventhubSuffix)
	if err != nil {
		s.Fail("failed to drop Postgres schema", err)
	}

	if s.ehHelper != nil {
		err = s.ehHelper.CleanUp()
		if err != nil {
			s.Fail("failed to clean up eventhub", err)
		}
	}
}

func (s *PeerFlowE2ETestSuiteEH) Test_Complete_Simple_Flow_EH() {
	if s.ehHelper == nil {
		s.T().Skip("Skipping EventHub test")
	}

	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	ru, err := util.RandomUInt64()
	s.NoError(err)

	jobName := fmt.Sprintf("test_complete_single_col_flow_eh_%d", ru)
	schemaQualifiedName := fmt.Sprintf("e2e_test.%s", jobName)
	_, err = s.pool.Exec(context.Background(), `
		CREATE TABLE `+schemaQualifiedName+` (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`)
	s.NoError(err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      jobName,
		TableNameMapping: map[string]string{schemaQualifiedName: jobName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.ehHelper.GetPeer(),
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	peerFlowInput := peerflow.CDCFlowLimits{
		TotalSyncFlows: 2,
		MaxBatchSize:   100,
	}

	// in a separate goroutine, wait for PeerFlowStatusQuery to finish setup
	// and then insert 10 rows into the source table
	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		// insert 10 rows into the source table
		for i := 0; i < 10; i++ {
			testKey := fmt.Sprintf("test_key_%d", i)
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.pool.Exec(context.Background(), `
			INSERT INTO `+schemaQualifiedName+` (key, value) VALUES ($1, $2)
		`, testKey, testValue)
			s.NoError(err)
		}
		fmt.Println("Inserted 10 rows into the source table")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &peerFlowInput, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	// Verify that the destination table has 10 rows
	// make context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msgs, err := s.ehHelper.ConsumeAllMessages(ctx, jobName, 10)

	require.NoError(s.T(), err)

	require.Equal(s.T(), 10, len(msgs))

	env.AssertExpectations(s.T())
}
