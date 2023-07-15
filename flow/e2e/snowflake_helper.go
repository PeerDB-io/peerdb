package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	connsnowflake "github.com/PeerDB-io/peer-flow/connectors/snowflake"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
)

type SnowflakeTestHelper struct {
	// config is the Snowflake config.
	Config *protos.SnowflakeConfig
	// peer struct holder Snowflake
	Peer *protos.Peer
	// connection to Snowflake
	client *connsnowflake.SnowflakeClient
	// testSchemaName is the schema to use for testing.
	testSchemaName string
}

func NewSnowflakeTestHelper(testSchemaName string) (*SnowflakeTestHelper, error) {
	jsonPath := os.Getenv("TEST_SF_CREDS")
	if jsonPath == "" {
		return nil, fmt.Errorf("TEST_SF_CREDS env var not set")
	}

	content, err := readFileToBytes(jsonPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var config protos.SnowflakeConfig
	err = json.Unmarshal(content, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal json: %w", err)
	}

	peer := generateSFPeer(&config)

	client, err := connsnowflake.NewSnowflakeClient(context.Background(), &config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Snowflake client: %w", err)
	}

	return &SnowflakeTestHelper{
		Config:         &config,
		Peer:           peer,
		client:         client,
		testSchemaName: testSchemaName,
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

// RecreateSchema recreates the schema, i.e., drops it if exists and creates it again.
func (s *SnowflakeTestHelper) RecreateSchema() error {
	return s.client.RecreateSchema(s.testSchemaName)
}

// DropSchema drops the schema.
func (s *SnowflakeTestHelper) DropSchema() error {
	return s.client.DropSchema(s.testSchemaName)
}

// RunCommand runs the given command.
func (s *SnowflakeTestHelper) RunCommand(command string) error {
	return s.client.ExecuteQuery(command)
}

// CountRows(tableName) returns the number of rows in the given table.
func (s *SnowflakeTestHelper) CountRows(tableName string) (int, error) {
	res, err := s.client.CountRows(s.testSchemaName, tableName)
	if err != nil {
		return 0, err
	}

	return int(res), nil
}

func (s *SnowflakeTestHelper) CheckNull(tableName string, colNames []string) (bool, error) {
	return s.client.CheckNull(s.testSchemaName, tableName, colNames)
}

func (s *SnowflakeTestHelper) ExecuteAndProcessQuery(query string) (*model.QRecordBatch, error) {
	return s.client.ExecuteAndProcessQuery(query)
}

func (s *SnowflakeTestHelper) CreateTable(tableName string, schema *model.QRecordSchema) error {
	return s.client.CreateTable(schema, s.testSchemaName, tableName)
}
