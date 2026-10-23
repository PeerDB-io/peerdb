package e2e

import (
	"fmt"
	"slices"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// TestResyncInitializesJSONFlag recreates a native JSON mirror through a RESYNC state change after stripping
// Flag_ClickHouseJSONEnabled from its stored config, as a mirror created before the flag existed looks. The
// recreation has to determine the flags again, or the resynced mirror would leave a scalar unwrapped and fail.
func (s APITestSuite) TestResyncInitializesJSONFlag() {
	switch source := s.source.(type) {
	case *PostgresSource:
	case *MySqlSource:
		if source.Config.Flavor == protos.MySqlFlavor_MYSQL_MARIA {
			s.t.Skip("skip maria, where JSON is stored as LONGTEXT and PeerDB does not map it to JSON yet")
		}
	default:
		s.t.Skip("JSON wrapping applies to Postgres and MySQL sources")
	}

	tableName := "resync_json_flag"
	srcFullName := AttachSchema(s, tableName)
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE %s(id INT PRIMARY KEY, j JSON)`, srcFullName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO %s(id, j) VALUES (1, '{"a": 1}')`, srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "resync_json_flag_" + s.suffix,
		TableNameMapping: map[string]string{srcFullName: tableName},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.InitialSnapshotOnly = true
	flowConnConfig.Env = map[string]string{"PEERDB_CLICKHOUSE_ENABLE_JSON": "true"}

	response, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)

	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), s.catalog, tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitForFinished(s.t, env, 3*time.Minute)
	EnvWaitForEqualTablesWithNamesFunc(env, s.ch, "initial snapshot", tableName, tableName, "id,j",
		jsonBatchesMatch([]string{"j"}, nil, true))

	// a mirror created before the flag existed has none of it in its stored config
	config, err := s.loadConfigFromCatalog(s.t.Context(), flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	require.Contains(s.t, config.Flags, shared.Flag_ClickHouseJSONEnabled)
	config.Flags = slices.DeleteFunc(config.Flags, func(flag string) bool { return flag == shared.Flag_ClickHouseJSONEnabled })
	configBytes, err := proto.Marshal(config)
	require.NoError(s.t, err)
	_, err = s.catalog.Exec(s.t.Context(), "UPDATE flows SET config_proto = $1 WHERE name = $2", configBytes, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)

	// a scalar only lands if the recreated mirror wraps it
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO %s(id, j) VALUES (2, '"dark"')`, srcFullName)))
	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RESYNC,
	})
	require.NoError(s.t, err)

	EnvWaitForEqualTablesWithNamesFunc(env, s.ch, "resynced snapshot with the scalar wrapped", tableName, tableName, "id,j",
		jsonBatchesMatch([]string{"j"}, nil, true))
	config, err = s.loadConfigFromCatalog(s.t.Context(), flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	require.Contains(s.t, config.Flags, shared.Flag_ClickHouseJSONEnabled)

	env, err = GetPeerflow(s.t.Context(), s.catalog, tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	EnvWaitForFinished(s.t, env, time.Minute)
}
