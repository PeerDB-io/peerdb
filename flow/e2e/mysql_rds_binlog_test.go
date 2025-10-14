package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// this test is not safe for parallel execution because it relies on a single table in a fixed place
// move it to its own suite
type MySQLRDSBinlogAPITestSuite struct {
	protos.FlowServiceClient
	t      *testing.T
	pg     *PostgresSource
	source *MySqlSource
	suffix string
	ch     ClickHouseSuite
}

func (s MySQLRDSBinlogAPITestSuite) Teardown(ctx context.Context) {
	s.pg.Teardown(s.t, ctx, s.suffix)
}

func (s MySQLRDSBinlogAPITestSuite) T() *testing.T {
	return s.t
}

func (s MySQLRDSBinlogAPITestSuite) Suffix() string {
	return s.suffix
}

func (s MySQLRDSBinlogAPITestSuite) Source() SuiteSource {
	return s.source
}

func (s MySQLRDSBinlogAPITestSuite) Connector() *connpostgres.PostgresConnector {
	return s.pg.PostgresConnector
}

func TestMySQLRDSBinlog(t *testing.T) {
	e2eshared.RunSuiteNoParallel(t, func(t *testing.T) MySQLRDSBinlogAPITestSuite {
		t.Helper()

		suffix := "api_" + strings.ToLower(shared.RandomString(8))
		pg, err := SetupPostgres(t, suffix)
		require.NoError(t, err)
		source, err := SetupMySQL(t, suffix)
		require.NoError(t, err)
		client, err := NewApiClient()
		require.NoError(t, err)
		return MySQLRDSBinlogAPITestSuite{
			FlowServiceClient: client,
			t:                 t,
			pg:                pg,
			source:            source,
			ch: SetupClickHouseSuite(t, false, func(*testing.T) (*MySqlSource, string, error) {
				return source, suffix, nil
			})(t),
			suffix: suffix,
		}
	})
}

func (s MySQLRDSBinlogAPITestSuite) TestMySQLRDSBinlogValidation() {
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "valid"))))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "my_validation_" + s.suffix,
		TableNameMapping: map[string]string{AttachSchema(s, "valid"): "valid"},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)

	require.NoError(s.t, s.source.Exec(s.t.Context(), "CREATE TABLE IF NOT EXISTS mysql.rds_configuration(name TEXT, value TEXT)"))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		"INSERT INTO mysql.rds_configuration(name, value) VALUES ('binlog retention hours', NULL)"))

	res, err := s.ValidateCDCMirror(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.Nil(s.t, res)
	require.Error(s.t, err)
	st, ok := status.FromError(err)
	require.True(s.t, ok)
	require.Equal(s.t, codes.FailedPrecondition, st.Code())
	require.Equal(s.t, "failed to validate source connector mysql: binlog configuration error: "+
		"RDS/Aurora setting 'binlog retention hours' should be at least 24, currently unset", st.Message())

	require.NoError(s.t, s.source.Exec(s.t.Context(), "UPDATE mysql.rds_configuration SET value = '1' WHERE name = 'binlog retention hours'"))
	res, err = s.ValidateCDCMirror(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.Nil(s.t, res)
	require.Error(s.t, err)
	st, ok = status.FromError(err)
	require.True(s.t, ok)
	require.Equal(s.t, codes.FailedPrecondition, st.Code())
	require.Equal(s.t, "failed to validate source connector mysql: binlog configuration error: "+
		"RDS/Aurora setting 'binlog retention hours' should be at least 24, currently 1", st.Message())

	require.NoError(s.t, s.source.Exec(s.t.Context(), "UPDATE mysql.rds_configuration SET value = '24' WHERE name = 'binlog retention hours';"))
	res, err = s.ValidateCDCMirror(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, res)

	require.NoError(s.t, s.source.Exec(s.t.Context(), "DROP TABLE IF EXISTS mysql.rds_configuration;"))
}
