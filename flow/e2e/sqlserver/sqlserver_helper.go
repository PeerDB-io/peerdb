package e2e_sqlserver

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"

	peersql "github.com/PeerDB-io/peer-flow/connectors/sql"
	connsqlserver "github.com/PeerDB-io/peer-flow/connectors/sqlserver"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

type SQLServerHelper struct {
	config *protos.SqlServerConfig

	E          peersql.SQLQueryExecutor
	SchemaName string
	tables     []string
}

func NewSQLServerHelper() (*SQLServerHelper, error) {
	port, err := strconv.ParseUint(os.Getenv("SQLSERVER_PORT"), 10, 16)
	if err != nil {
		return nil, fmt.Errorf("invalid SQLSERVER_PORT: %s", os.Getenv("SQLSERVER_PORT"))
	}

	config := &protos.SqlServerConfig{
		Server:   os.Getenv("SQLSERVER_HOST"),
		Port:     uint32(port),
		User:     os.Getenv("SQLSERVER_USER"),
		Password: os.Getenv("SQLSERVER_PASSWORD"),
		Database: os.Getenv("SQLSERVER_DATABASE"),
	}

	connector, err := connsqlserver.NewSQLServerConnector(context.Background(), config)
	if err != nil {
		return nil, err
	}

	connErr := connector.ConnectionActive(context.Background())
	if connErr != nil {
		return nil, fmt.Errorf("invalid connection configs: %v", connErr)
	}

	//nolint:gosec // number has no cryptographic significance
	rndNum := rand.Uint64()
	testSchema := fmt.Sprintf("e2e_test_%d", rndNum)
	if err := connector.CreateSchema(context.Background(), testSchema); err != nil {
		return nil, err
	}

	return &SQLServerHelper{
		config:     config,
		E:          connector,
		SchemaName: testSchema,
	}, nil
}

func (h *SQLServerHelper) CreateTable(schema *qvalue.QRecordSchema, tableName string) error {
	err := h.E.CreateTable(context.Background(), schema, h.SchemaName, tableName)
	if err != nil {
		return err
	}

	h.tables = append(h.tables, tableName)
	return nil
}

func (h *SQLServerHelper) CleanUp() error {
	for _, tbl := range h.tables {
		err := h.E.ExecuteQuery(context.Background(), fmt.Sprintf("DROP TABLE %s.%s", h.SchemaName, tbl))
		if err != nil {
			return err
		}
	}

	if h.SchemaName != "" {
		return h.E.ExecuteQuery(context.Background(), "DROP SCHEMA "+h.SchemaName)
	}

	return nil
}
