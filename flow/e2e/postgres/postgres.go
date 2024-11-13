package e2e_postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peer-flow/connectors"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
)

type PeerFlowE2ETestSuitePG struct {
	t *testing.T

	conn   *connpostgres.PostgresConnector
	suffix string
}

func (s PeerFlowE2ETestSuitePG) T() *testing.T {
	return s.t
}

func (s PeerFlowE2ETestSuitePG) Connector() *connpostgres.PostgresConnector {
	return s.conn
}

func (s PeerFlowE2ETestSuitePG) DestinationConnector() connectors.Connector {
	return s.conn
}

func (s PeerFlowE2ETestSuitePG) Conn() *pgx.Conn {
	return s.conn.Conn()
}

func (s PeerFlowE2ETestSuitePG) Suffix() string {
	return s.suffix
}

func (s PeerFlowE2ETestSuitePG) Peer() *protos.Peer {
	return e2e.GeneratePostgresPeer(s.t)
}

func (s PeerFlowE2ETestSuitePG) DestinationTable(table string) string {
	return e2e.AttachSchema(s, table)
}

func (s PeerFlowE2ETestSuitePG) GetRows(table string, cols string) (*model.QRecordBatch, error) {
	s.t.Helper()
	pgQueryExecutor, err := s.conn.NewQRepQueryExecutor(context.Background(), "testflow", "testpart")
	if err != nil {
		return nil, err
	}

	return pgQueryExecutor.ExecuteAndProcessQuery(
		context.Background(),
		fmt.Sprintf(`SELECT %s FROM e2e_test_%s.%s ORDER BY id`, cols, s.suffix, connpostgres.QuoteIdentifier(table)),
	)
}

func SetupSuite(t *testing.T) PeerFlowE2ETestSuitePG {
	t.Helper()

	suffix := "pg_" + strings.ToLower(shared.RandomString(8))
	conn, err := e2e.SetupPostgres(t, suffix)
	require.NoError(t, err, "failed to setup postgres")

	return PeerFlowE2ETestSuitePG{
		t:      t,
		conn:   conn,
		suffix: suffix,
	}
}

func (s PeerFlowE2ETestSuitePG) Teardown() {
	e2e.TearDownPostgres(s)
}
