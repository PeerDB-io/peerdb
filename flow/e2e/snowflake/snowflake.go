package e2e_snowflake

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	connsnowflake "github.com/PeerDB-io/peer-flow/connectors/snowflake"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
)

type PeerFlowE2ETestSuiteSF struct {
	t *testing.T

	pgSuffix  string
	conn      *connpostgres.PostgresConnector
	sfHelper  *SnowflakeTestHelper
	connector *connsnowflake.SnowflakeConnector
}

func (s PeerFlowE2ETestSuiteSF) T() *testing.T {
	return s.t
}

func (s PeerFlowE2ETestSuiteSF) Connector() *connpostgres.PostgresConnector {
	return s.conn
}

func (s PeerFlowE2ETestSuiteSF) Conn() *pgx.Conn {
	return s.Connector().Conn()
}

func (s PeerFlowE2ETestSuiteSF) Suffix() string {
	return s.pgSuffix
}

func (s PeerFlowE2ETestSuiteSF) Peer() *protos.Peer {
	return s.sfHelper.Peer
}

func (s PeerFlowE2ETestSuiteSF) DestinationTable(table string) string {
	return e2e.AttachSchema(s, table)
}

func (s PeerFlowE2ETestSuiteSF) GetRows(tableName string, sfSelector string) (*model.QRecordBatch, error) {
	s.t.Helper()
	qualifiedTableName := fmt.Sprintf(`%s.%s.%s`, s.sfHelper.testDatabaseName, s.sfHelper.testSchemaName, tableName)
	sfSelQuery := fmt.Sprintf(`SELECT %s FROM %s ORDER BY id`, sfSelector, qualifiedTableName)
	s.t.Logf("running query on snowflake: %s", sfSelQuery)
	return s.sfHelper.ExecuteAndProcessQuery(sfSelQuery)
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

	sfHelper, err := NewSnowflakeTestHelper()
	if err != nil {
		t.Fatalf("failed to setup Snowflake: %v", err)
	}

	connector, err := connsnowflake.NewSnowflakeConnector(
		context.Background(),
		sfHelper.Config,
	)
	require.NoError(t, err)

	suite := PeerFlowE2ETestSuiteSF{
		t:         t,
		pgSuffix:  pgSuffix,
		conn:      conn,
		sfHelper:  sfHelper,
		connector: connector,
	}

	return suite
}

func (s PeerFlowE2ETestSuiteSF) Teardown() {
	e2e.TearDownPostgres(s)

	if s.sfHelper != nil {
		err := s.sfHelper.Cleanup()
		if err != nil {
			s.t.Fatalf("failed to tear down Snowflake: %v", err)
		}
	}

	err := s.connector.Close()
	if err != nil {
		s.t.Fatalf("failed to close Snowflake connector: %v", err)
	}
}
