package e2e_snowflake

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	connsnowflake "github.com/PeerDB-io/peerdb/flow/connectors/snowflake"
	"github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type PeerFlowE2ETestSuiteSF struct {
	t *testing.T

	conn      *connpostgres.PostgresConnector
	sfHelper  *SnowflakeTestHelper
	connector *connsnowflake.SnowflakeConnector
	pgSuffix  string
}

func (s PeerFlowE2ETestSuiteSF) T() *testing.T {
	return s.t
}

func (s PeerFlowE2ETestSuiteSF) Connector() *connpostgres.PostgresConnector {
	return s.conn
}

func (s PeerFlowE2ETestSuiteSF) Source() e2e.SuiteSource {
	return &e2e.PostgresSource{PostgresConnector: s.conn}
}

func (s PeerFlowE2ETestSuiteSF) DestinationConnector() connectors.Connector {
	return s.connector
}

func (s PeerFlowE2ETestSuiteSF) Conn() *pgx.Conn {
	return s.Connector().Conn()
}

func (s PeerFlowE2ETestSuiteSF) Suffix() string {
	return s.pgSuffix
}

func (s PeerFlowE2ETestSuiteSF) Peer() *protos.Peer {
	s.t.Helper()
	ret := &protos.Peer{
		Name: e2e.AddSuffix(s, "test_sf_peer"),
		Type: protos.DBType_SNOWFLAKE,
		Config: &protos.Peer_SnowflakeConfig{
			SnowflakeConfig: s.sfHelper.Config,
		},
	}
	e2e.CreatePeer(s.t, ret)
	return ret
}

func (s PeerFlowE2ETestSuiteSF) DestinationTable(table string) string {
	return fmt.Sprintf("%s.%s", s.sfHelper.testSchemaName, table)
}

func (s PeerFlowE2ETestSuiteSF) GetRows(tableName string, sfSelector string) (*model.QRecordBatch, error) {
	s.t.Helper()
	qualifiedTableName := fmt.Sprintf(`%s.%s.%s`, s.sfHelper.testDatabaseName, s.sfHelper.testSchemaName, tableName)
	sfSelQuery := fmt.Sprintf(`SELECT %s FROM %s ORDER BY id`, sfSelector, qualifiedTableName)
	s.t.Logf("running query on snowflake: %s", sfSelQuery)
	return s.sfHelper.ExecuteAndProcessQuery(s.t.Context(), sfSelQuery)
}

func SetupSuite(t *testing.T) PeerFlowE2ETestSuiteSF {
	t.Helper()

	suffix := shared.RandomString(8)
	tsSuffix := time.Now().Format("20060102150405")
	pgSuffix := fmt.Sprintf("sf_%s_%s", strings.ToLower(suffix), tsSuffix)

	conn, err := e2e.SetupPostgres(t, pgSuffix)
	if err != nil || conn == nil {
		t.Fatalf("failed to setup Postgres: %v", err)
	}

	sfHelper, err := NewSnowflakeTestHelper(t)
	if err != nil {
		t.Fatalf("failed to setup Snowflake: %v", err)
	}

	connector, err := connsnowflake.NewSnowflakeConnector(
		t.Context(),
		sfHelper.Config,
	)
	require.NoError(t, err)

	suite := PeerFlowE2ETestSuiteSF{
		t:         t,
		pgSuffix:  pgSuffix,
		conn:      conn.PostgresConnector,
		sfHelper:  sfHelper,
		connector: connector,
	}

	return suite
}

func (s PeerFlowE2ETestSuiteSF) Teardown(ctx context.Context) {
	e2e.TearDownPostgres(ctx, s)

	if s.sfHelper != nil {
		if err := s.sfHelper.Cleanup(ctx); err != nil {
			s.t.Fatalf("failed to tear down Snowflake: %v", err)
		}
	}

	if err := s.connector.Close(); err != nil {
		s.t.Fatalf("failed to close Snowflake connector: %v", err)
	}
}
