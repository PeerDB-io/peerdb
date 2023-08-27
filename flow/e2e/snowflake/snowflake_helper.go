package e2e_snowflake

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	connsnowflake "github.com/PeerDB-io/peer-flow/connectors/snowflake"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	util "github.com/PeerDB-io/peer-flow/utils"
)

type SnowflakeTestHelper struct {
	// config is the Snowflake config.
	Config *protos.SnowflakeConfig
	// peer struct holder Snowflake
	Peer *protos.Peer
	// connection to another database, to manage the test database
	adminClient *connsnowflake.SnowflakeClient
	// connection to the test database
	testClient *connsnowflake.SnowflakeClient
	// testSchemaName is the schema to use for testing.
	testSchemaName string
	// dbName is the database used for testing.
	testDatabaseName string
}

func NewSnowflakeTestHelper() (*SnowflakeTestHelper, error) {
	jsonPath := os.Getenv("TEST_SF_CREDS")
	if jsonPath == "" {
		return nil, fmt.Errorf("TEST_SF_CREDS env var not set")
	}

	content, err := e2e.ReadFileToBytes(jsonPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var config protos.SnowflakeConfig
	err = json.Unmarshal(content, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal json: %w", err)
	}

	peer := generateSFPeer(&config)
	runID, err := util.RandomUInt64()
	if err != nil {
		return nil, fmt.Errorf("failed to generate random uint64: %w", err)
	}

	testDatabaseName := fmt.Sprintf("e2e_test_%d", runID)

	adminClient, err := connsnowflake.NewSnowflakeClient(context.Background(), &config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Snowflake client: %w", err)
	}
	err = adminClient.ExecuteQuery(fmt.Sprintf("CREATE DATABASE %s", testDatabaseName))
	if err != nil {
		return nil, fmt.Errorf("failed to create Snowflake test database: %w", err)
	}

	config.Database = testDatabaseName
	testClient, err := connsnowflake.NewSnowflakeClient(context.Background(), &config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Snowflake client: %w", err)
	}

	return &SnowflakeTestHelper{
		Config:           &config,
		Peer:             peer,
		adminClient:      adminClient,
		testClient:       testClient,
		testSchemaName:   "PUBLIC",
		testDatabaseName: testDatabaseName,
	}, nil
}

func generateSFPeer(snowflakeConfig *protos.SnowflakeConfig) *protos.Peer {
	ret := &protos.Peer{}
	ret.Name = "test_sf_peer"
	ret.Type = protos.DBType_SNOWFLAKE

	ret.Config = &protos.Peer_SnowflakeConfig{
		SnowflakeConfig: snowflakeConfig,
	}

	return ret
}

// Cleanup drops the database.
func (s *SnowflakeTestHelper) Cleanup() error {
	err := s.testClient.Close()
	if err != nil {
		return err
	}
	err = s.adminClient.ExecuteQuery(fmt.Sprintf("DROP DATABASE %s", s.testDatabaseName))
	if err != nil {
		return err
	}
	return s.adminClient.Close()
}

// RunCommand runs the given command.
func (s *SnowflakeTestHelper) RunCommand(command string) error {
	return s.testClient.ExecuteQuery(command)
}

// CountRows(tableName) returns the number of rows in the given table.
func (s *SnowflakeTestHelper) CountRows(tableName string) (int, error) {
	res, err := s.testClient.CountRows(s.testSchemaName, tableName)
	if err != nil {
		return 0, err
	}

	return int(res), nil
}

func (s *SnowflakeTestHelper) CheckNull(tableName string, colNames []string) (bool, error) {
	return s.testClient.CheckNull(s.testSchemaName, tableName, colNames)
}

func (s *SnowflakeTestHelper) ExecuteAndProcessQuery(query string) (*model.QRecordBatch, error) {
	return s.testClient.ExecuteAndProcessQuery(query)
}

func (s *SnowflakeTestHelper) CreateTable(tableName string, schema *model.QRecordSchema) error {
	return s.testClient.CreateTable(schema, s.testSchemaName, tableName)
}
