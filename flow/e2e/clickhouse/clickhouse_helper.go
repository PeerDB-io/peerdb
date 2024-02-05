package e2e_clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"time"

	connclickhouse "github.com/PeerDB-io/peer-flow/connectors/clickhouse"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
)

type ClickhouseTestHelper struct {
	// config is the Clickhouse config.
	Config *protos.ClickhouseConfig
	// peer struct holder Clickhouse
	Peer *protos.Peer
	// connection to another database, to manage the test database
	adminClient *connclickhouse.ClickhouseClient
	// connection to the test database
	testClient *connclickhouse.ClickhouseClient
	// testSchemaName is the schema to use for testing.
	testSchemaName string
	// dbName is the database used for testing.
	testDatabaseName string
}

func NewClickhouseTestHelper() (*ClickhouseTestHelper, error) {
	jsonPath := os.Getenv("TEST_CH_CREDS")
	if jsonPath == "" {
		return nil, fmt.Errorf("TEST_CH_CREDS env var not set")
	}

	content, err := e2eshared.ReadFileToBytes(jsonPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var config *protos.ClickhouseConfig
	err = json.Unmarshal(content, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal json: %w", err)
	}

	peer := generateCHPeer(config)
	runID, err := shared.RandomUInt64()
	if err != nil {
		return nil, fmt.Errorf("failed to generate random uint64: %w", err)
	}

	testDatabaseName := fmt.Sprintf("e2e_test_%d", runID)

	adminClient, err := connclickhouse.NewClickhouseClient(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Clickhouse client: %w", err)
	}
	err = adminClient.ExecuteQuery(fmt.Sprintf("CREATE DATABASE %s", testDatabaseName))
	if err != nil {
		return nil, fmt.Errorf("failed to create Clickhouse test database: %w", err)
	}

	config.Database = testDatabaseName
	testClient, err := connclickhouse.NewClickhouseClient(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Clickhouse client: %w", err)
	}

	return &ClickhouseTestHelper{
		Config:      config,
		Peer:        peer,
		adminClient: adminClient,
		testClient:  testClient,
		//testSchemaName:   "PUBLIC",
		testSchemaName:   testDatabaseName,
		testDatabaseName: testDatabaseName,
	}, nil
}

func generateCHPeer(clickhouseConfig *protos.ClickhouseConfig) *protos.Peer {
	ret := &protos.Peer{}
	ret.Name = "test_ch_peer"
	ret.Type = protos.DBType_SNOWFLAKE

	ret.Config = &protos.Peer_ClickhouseConfig{
		ClickhouseConfig: clickhouseConfig,
	}

	return ret
}

// Cleanup drops the database.
func (s *ClickhouseTestHelper) Cleanup() error {
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
func (s *ClickhouseTestHelper) RunCommand(command string) error {
	return s.testClient.ExecuteQuery(command)
}

// CountRows(tableName) returns the number of rows in the given table.
func (s *ClickhouseTestHelper) CountRows(tableName string) (int, error) {
	res, err := s.testClient.CountRows(s.testSchemaName, tableName)
	if err != nil {
		return 0, err
	}

	return int(res), nil
}

// CountRows(tableName) returns the non-null number of rows in the given table.
func (s *ClickhouseTestHelper) CountNonNullRows(tableName string, columnName string) (int, error) {
	res, err := s.testClient.CountNonNullRows(s.testSchemaName, tableName, columnName)
	if err != nil {
		return 0, err
	}

	return int(res), nil
}

func (s *ClickhouseTestHelper) CheckNull(tableName string, colNames []string) (bool, error) {
	return s.testClient.CheckNull(s.testSchemaName, tableName, colNames)
}

func (s *ClickhouseTestHelper) ExecuteAndProcessQuery(query string) (*model.QRecordBatch, error) {
	return s.testClient.ExecuteAndProcessQuery(query)
}

func (s *ClickhouseTestHelper) CreateTable(tableName string, schema *model.QRecordSchema) error {
	return s.testClient.CreateTable(schema, s.testSchemaName, tableName)
}

// runs a query that returns an int result
func (s *ClickhouseTestHelper) RunIntQuery(query string) (int, error) {
	rows, err := s.testClient.ExecuteAndProcessQuery(query)
	if err != nil {
		return 0, err
	}

	numRecords := 0
	if rows == nil || len(rows.Records) != 1 {
		if rows != nil {
			numRecords = len(rows.Records)
		}
		return 0, fmt.Errorf("failed to execute query: %s, returned %d != 1 rows", query, numRecords)
	}

	rec := rows.Records[0]
	if len(rec) != 1 {
		return 0, fmt.Errorf("failed to execute query: %s, returned %d != 1 columns", query, len(rec))
	}

	switch rec[0].Kind {
	case qvalue.QValueKindInt32:
		return int(rec[0].Value.(int32)), nil
	case qvalue.QValueKindInt64:
		return int(rec[0].Value.(int64)), nil
	case qvalue.QValueKindNumeric:
		// get big.Rat and convert to int
		rat := rec[0].Value.(*big.Rat)
		return int(rat.Num().Int64() / rat.Denom().Int64()), nil
	default:
		return 0, fmt.Errorf("failed to execute query: %s, returned value of type %s", query, rec[0].Kind)
	}
}

// runs a query that returns an int result
func (s *ClickhouseTestHelper) checkSyncedAt(query string) error {
	recordBatch, err := s.testClient.ExecuteAndProcessQuery(query)
	if err != nil {
		return err
	}

	for _, record := range recordBatch.Records {
		for _, entry := range record {
			if entry.Kind != qvalue.QValueKindTimestamp {
				return fmt.Errorf("synced_at column check failed: _PEERDB_SYNCED_AT is not timestamp")
			}
			_, ok := entry.Value.(time.Time)
			if !ok {
				return fmt.Errorf("synced_at column failed: _PEERDB_SYNCED_AT is not valid")
			}
		}
	}

	return nil
}
