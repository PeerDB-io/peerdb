//go:build tilt

package mysql_clickhouse

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

type MySQLRDSBinlogAPITestSuite struct {
	protos.FlowServiceClient
	t      *testing.T
	source *e2e.MySqlSource
	suffix string
	ch     e2e.ClickHouseSuite
}

func (s MySQLRDSBinlogAPITestSuite) Teardown(ctx context.Context) {
	s.source.Teardown(s.t, ctx, s.suffix)
}

func (s MySQLRDSBinlogAPITestSuite) T() *testing.T {
	return s.t
}

func (s MySQLRDSBinlogAPITestSuite) Suffix() string {
	return s.suffix
}

func (s MySQLRDSBinlogAPITestSuite) Source() e2e.SuiteSource {
	return s.source
}

func TestMySQLRDSBinlog(t *testing.T) {
	e2eshared.RunSuite(t, func(t *testing.T) MySQLRDSBinlogAPITestSuite {
		t.Helper()

		source, suffix := e2e.SetupMySQLTestContainerSource(t, "rdsbinlog", e2e.MySQLTestContainerConfig{
			Image:                "mysql:9.5",
			Flavor:               protos.MySqlFlavor_MYSQL_MYSQL,
			ReplicationMechanism: protos.MySqlReplicationMechanism_MYSQL_GTID,
		})
		client, err := e2e.NewApiClient()
		require.NoError(t, err)
		return MySQLRDSBinlogAPITestSuite{
			FlowServiceClient: client,
			t:                 t,
			source:            source,
			ch: e2e.SetupClickHouseSuite(t, false, func(*testing.T) (*e2e.MySqlSource, string, error) {
				return source, suffix, nil
			})(t),
			suffix: suffix,
		}
	})
}

func (s MySQLRDSBinlogAPITestSuite) TestMySQLRDSBinlogValidation() {
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", e2e.AttachSchema(s, "valid"))))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      "my_validation_" + s.suffix,
		TableNameMapping: map[string]string{e2e.AttachSchema(s, "valid"): "valid"},
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
	require.Equal(s.t, "failed to validate source connector "+s.source.Name+": binlog configuration error: "+
		"RDS/Aurora setting 'binlog retention hours' should be at least 24, currently unset", st.Message())

	require.NoError(s.t, s.source.Exec(s.t.Context(), "UPDATE mysql.rds_configuration SET value = '1' WHERE name = 'binlog retention hours'"))
	res, err = s.ValidateCDCMirror(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.Nil(s.t, res)
	require.Error(s.t, err)
	st, ok = status.FromError(err)
	require.True(s.t, ok)
	require.Equal(s.t, codes.FailedPrecondition, st.Code())
	require.Equal(s.t, "failed to validate source connector "+s.source.Name+": binlog configuration error: "+
		"RDS/Aurora setting 'binlog retention hours' should be at least 24, currently 1", st.Message())

	require.NoError(s.t, s.source.Exec(s.t.Context(), "UPDATE mysql.rds_configuration SET value = '24' WHERE name = 'binlog retention hours';"))
	res, err = s.ValidateCDCMirror(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, res)

	require.NoError(s.t, s.source.Exec(s.t.Context(), "DROP TABLE IF EXISTS mysql.rds_configuration;"))
}
