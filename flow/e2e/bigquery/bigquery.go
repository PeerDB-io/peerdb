package e2e_bigquery

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peer-flow/connectors"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
)

type PeerFlowE2ETestSuiteBQ struct {
	t *testing.T

	bqSuffix string
	conn     *connpostgres.PostgresConnector
	bqHelper *BigQueryTestHelper
}

func (s PeerFlowE2ETestSuiteBQ) T() *testing.T {
	return s.t
}

func (s PeerFlowE2ETestSuiteBQ) Conn() *pgx.Conn {
	return s.conn.Conn()
}

func (s PeerFlowE2ETestSuiteBQ) Connector() *connpostgres.PostgresConnector {
	return s.conn
}

func (s PeerFlowE2ETestSuiteBQ) DestinationConnector() connectors.Connector {
	// TODO have BQ connector
	return nil
}

func (s PeerFlowE2ETestSuiteBQ) Suffix() string {
	return s.bqSuffix
}

func (s PeerFlowE2ETestSuiteBQ) Peer() *protos.Peer {
	return s.bqHelper.Peer
}

func (s PeerFlowE2ETestSuiteBQ) DestinationTable(table string) string {
	return table
}

func (s PeerFlowE2ETestSuiteBQ) GetRows(tableName string, colsString string) (*model.QRecordBatch, error) {
	s.t.Helper()
	qualifiedTableName := fmt.Sprintf("`%s.%s`", s.bqHelper.Config.DatasetId, tableName)
	bqSelQuery := fmt.Sprintf("SELECT %s FROM %s ORDER BY id", colsString, qualifiedTableName)
	s.t.Logf("running query on bigquery: %s", bqSelQuery)
	return s.bqHelper.ExecuteAndProcessQuery(bqSelQuery)
}

func (s PeerFlowE2ETestSuiteBQ) GetRowsWhere(tableName string, colsString string, where string) (*model.QRecordBatch, error) {
	s.t.Helper()
	qualifiedTableName := fmt.Sprintf("`%s.%s`", s.bqHelper.Config.DatasetId, tableName)
	bqSelQuery := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY id", colsString, qualifiedTableName, where)
	s.t.Logf("running query on bigquery: %s", bqSelQuery)
	return s.bqHelper.ExecuteAndProcessQuery(bqSelQuery)
}

func (s PeerFlowE2ETestSuiteBQ) Teardown() {
	e2e.TearDownPostgres(s)

	err := s.bqHelper.DropDataset(s.bqHelper.Config.DatasetId)
	if err != nil {
		s.t.Fatalf("failed to tear down bigquery: %v", err)
	}
}

func SetupSuite(t *testing.T) PeerFlowE2ETestSuiteBQ {
	t.Helper()

	suffix := shared.RandomString(8)
	tsSuffix := time.Now().Format("20060102150405")
	bqSuffix := fmt.Sprintf("bq_%s_%s", strings.ToLower(suffix), tsSuffix)
	conn, err := e2e.SetupPostgres(t, bqSuffix)
	if err != nil || conn == nil {
		t.Fatalf("failed to setup postgres: %v", err)
	}

	bqHelper, err := NewBigQueryTestHelper()
	if err != nil {
		t.Fatalf("Failed to create helper: %v", err)
	}

	err = bqHelper.RecreateDataset()
	if err != nil {
		t.Fatalf("Failed to recreate dataset: %v", err)
	}

	return PeerFlowE2ETestSuiteBQ{
		t:        t,
		bqSuffix: bqSuffix,
		conn:     conn,
		bqHelper: bqHelper,
	}
}
