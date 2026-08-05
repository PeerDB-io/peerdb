package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	conncockroachdb "github.com/PeerDB-io/peerdb/flow/connectors/cockroachdb"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type CockroachDBSuite struct {
	t      *testing.T
	source *CockroachDBSource
	suffix string
}

func (s CockroachDBSuite) T() *testing.T {
	return s.t
}

func (s CockroachDBSuite) Suffix() string {
	return s.suffix
}

func (s CockroachDBSuite) Source() SuiteSource {
	return s.source
}

func (s CockroachDBSuite) Connector() *conncockroachdb.CockroachDBConnector {
	return s.source.conn
}

func (s CockroachDBSuite) Teardown(ctx context.Context) {
	s.source.Teardown(s.t, ctx, s.suffix)
}

func SetupCockroachDBSuite(t *testing.T) CockroachDBSuite {
	t.Helper()

	suffix := "crdb_" + strings.ToLower(common.RandomString(8))
	source, err := SetupCockroachDB(t, suffix)
	require.NoError(t, err, "failed to setup cockroachdb")

	return CockroachDBSuite{
		t:      t,
		source: source,
		suffix: suffix,
	}
}

func TestCockroachDBSuite(t *testing.T) {
	e2eshared.RunSuite(t, SetupCockroachDBSuite)
}

// crdbTypesExpectedQValueKinds is the expected QValueKind mapping for the test_types table,
// per crdbTypeToQValueKind in the cockroachdb connector
var crdbTypesExpectedQValueKinds = map[string]types.QValueKind{
	"id":            types.QValueKindInt64,
	"c_int8":        types.QValueKindInt64,
	"c_decimal":     types.QValueKindNumeric,
	"c_uuid":        types.QValueKindUUID,
	"c_jsonb":       types.QValueKindJSON,
	"c_timestamptz": types.QValueKindTimestampTZ,
	"c_bool":        types.QValueKindBoolean,
	"c_text_array":  types.QValueKindArrayString,
	"c_mood":        types.QValueKindEnum,
}

// setupCockroachDBTypesTable seeds a table covering interesting CockroachDB types,
// shared by schema introspection tests and QRep/CDC mirror tests
func setupCockroachDBTypesTable(s Suite, table string) {
	t := s.T()
	t.Helper()
	ctx := t.Context()
	schema := Schema(s)

	require.NoError(t, s.Source().Exec(ctx,
		fmt.Sprintf("CREATE TYPE %s.mood AS ENUM ('happy', 'sad', 'angry')", schema)))
	require.NoError(t, s.Source().Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.%s (
		id INT8 PRIMARY KEY DEFAULT unique_rowid(),
		c_int8 INT8,
		c_decimal DECIMAL(20,5),
		c_uuid UUID,
		c_jsonb JSONB,
		c_timestamptz TIMESTAMPTZ,
		c_bool BOOL,
		c_text_array TEXT[],
		c_mood %s.mood
	)`, schema, table, schema)))
	require.NoError(t, s.Source().Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.%s (c_int8, c_decimal, c_uuid, c_jsonb, c_timestamptz, c_bool, c_text_array, c_mood)
		VALUES (9223372036854775807, 123456789.54321, gen_random_uuid(), '{"key": "value"}',
			now(), true, ARRAY['one', 'two'], 'happy')`, schema, table)))
}

func (s CockroachDBSuite) Test_Rangefeed_Validation() {
	t := s.t
	ctx := t.Context()
	schema := "e2e_test_" + s.suffix

	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s.rf_check (id INT8 PRIMARY KEY, v TEXT)", schema)))

	cdcConfig := &protos.FlowConnectionConfigsCore{
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      schema + ".rf_check",
			DestinationTableIdentifier: "rf_check",
		}},
	}

	// peer without changefeeds enabled rejects CDC mirrors but allows snapshot-only
	require.ErrorContains(t, s.source.conn.ValidateMirrorSource(ctx, cdcConfig), "enable changefeeds")
	snapshotOnly := proto.CloneOf(cdcConfig)
	snapshotOnly.DoInitialSnapshot = true
	snapshotOnly.InitialSnapshotOnly = true
	require.NoError(t, s.source.conn.ValidateMirrorSource(ctx, snapshotOnly))

	// changefeed-enabled peer requires kv.rangefeed.enabled on the cluster
	cfPeerConfig := proto.CloneOf(s.source.config)
	cfPeerConfig.UseChangefeeds = true
	cfConn, err := conncockroachdb.NewCockroachDBConnector(ctx, nil, cfPeerConfig)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, cfConn.Close())
	}()

	require.NoError(t, s.source.Exec(ctx, "SET CLUSTER SETTING kv.rangefeed.enabled = false"))
	require.ErrorContains(t, cfConn.ValidateMirrorSource(ctx, cdcConfig), "rangefeed")
	require.NoError(t, s.source.Exec(ctx, "SET CLUSTER SETTING kv.rangefeed.enabled = true"))
	require.NoError(t, cfConn.ValidateMirrorSource(ctx, cdcConfig))
}

func (s CockroachDBSuite) Test_GC_TTL_Validation() {
	t := s.t
	ctx := t.Context()
	schema := "e2e_test_" + s.suffix

	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s.gcttl_check (id INT8 PRIMARY KEY, v TEXT)", schema)))

	cfg := &protos.FlowConnectionConfigsCore{
		DoInitialSnapshot:   true,
		InitialSnapshotOnly: true,
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      schema + ".gcttl_check",
			DestinationTableIdentifier: "gcttl_check",
		}},
	}

	// the default zone config is far above the floor
	require.NoError(t, s.source.conn.ValidateMirrorSource(ctx, cfg))

	// below the floor mirrors with an initial snapshot are rejected
	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s.gcttl_check CONFIGURE ZONE USING gc.ttlseconds = 300", schema)))
	err := s.source.conn.ValidateMirrorSource(ctx, cfg)
	require.ErrorContains(t, err, "gc.ttlseconds")
	require.ErrorContains(t, err, "below the 600 second minimum")

	// exactly at the floor passes again
	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s.gcttl_check CONFIGURE ZONE USING gc.ttlseconds = 600", schema)))
	require.NoError(t, s.source.conn.ValidateMirrorSource(ctx, cfg))

	// without an initial snapshot the check is skipped: validation proceeds to
	// the changefeed gate despite the low TTL
	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s.gcttl_check CONFIGURE ZONE USING gc.ttlseconds = 300", schema)))
	noSnapshot := proto.CloneOf(cfg)
	noSnapshot.DoInitialSnapshot = false
	noSnapshot.InitialSnapshotOnly = false
	require.ErrorContains(t, s.source.conn.ValidateMirrorSource(ctx, noSnapshot), "enable changefeeds")

	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s.gcttl_check CONFIGURE ZONE DISCARD", schema)))
	require.NoError(t, s.source.conn.ValidateMirrorSource(ctx, cfg))
}

func (s CockroachDBSuite) Test_History_Retention_Protection() {
	t := s.t
	ctx := t.Context()
	schema := "e2e_test_" + s.suffix
	flowName := "hr_" + s.suffix

	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s.hr_check (id INT8 PRIMARY KEY, v TEXT)", schema)))

	cfPeerConfig := proto.CloneOf(s.source.config)
	cfPeerConfig.UseChangefeeds = true
	cfConn, err := conncockroachdb.NewCockroachDBConnector(ctx, nil, cfPeerConfig)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, cfConn.Close())
	}()

	countRunning := func() int {
		var count int
		require.NoError(t, s.source.adminConn.QueryRow(ctx,
			"SELECT count(*) FROM [SHOW JOBS] WHERE job_type = 'HISTORY RETENTION'"+
				" AND status = 'running' AND right(description, length($1::text)) = $1::text",
			"peerdb initial load "+flowName).Scan(&count))
		return count
	}
	require.Equal(t, 0, countRunning())

	setupResult, err := cfConn.SetupReplication(ctx, shared.CatalogPool{}, &protos.SetupReplicationInput{
		FlowJobName:       flowName,
		DoInitialSnapshot: true,
		TableNameMapping:  map[string]string{schema + ".hr_check": "hr_check"},
	})
	require.NoError(t, err)
	require.Equal(t, 1, countRunning(), "SetupReplication should protect the snapshot timestamp")

	var jobID int64
	require.NoError(t, s.source.adminConn.QueryRow(ctx,
		"SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'HISTORY RETENTION'"+
			" AND status = 'running' AND right(description, length($1::text)) = $1::text LIMIT 1",
		"peerdb initial load "+flowName).Scan(&jobID))

	// the extension heartbeats the job: its expiry becomes heartbeat time +
	// expiration window. SHOW JOBS does not surface either, but the heartbeat
	// is visible in the job progress payload when running as an admin.
	readHeartbeat := func() (time.Time, bool) {
		if _, err := s.source.adminConn.Exec(ctx, "SET allow_unsafe_internals = on"); err != nil {
			t.Logf("could not set allow_unsafe_internals: %v", err)
		}
		var heartbeat string
		if err := s.source.adminConn.QueryRow(ctx,
			"SELECT COALESCE(crdb_internal.pb_to_json('cockroach.sql.jobs.jobspb.Progress', value)"+
				"->'HistoryRetentionProgress'->>'lastHeartbeatTime', '')"+
				" FROM system.job_info WHERE job_id = $1 AND info_key = 'legacy_progress'"+
				" ORDER BY written DESC LIMIT 1",
			jobID).Scan(&heartbeat); err != nil {
			t.Logf("job heartbeat not observable: %v", err)
			return time.Time{}, false
		}
		if heartbeat == "" {
			return time.Time{}, true
		}
		parsed, err := time.Parse(time.RFC3339Nano, heartbeat)
		require.NoError(t, err)
		return parsed, true
	}
	heartbeatBefore, beforeObservable := readHeartbeat()

	// a snapshot partition pull carrying the parent mirror name and the
	// captured HLC timestamp extends the protection
	pullConfig := &protos.QRepConfig{
		FlowJobName:      "clone_hr_check_" + s.suffix,
		ParentMirrorName: flowName,
		SnapshotName:     setupResult.SnapshotName,
		WatermarkTable:   schema + ".hr_check",
		Version:          shared.InternalVersion_Latest,
	}
	stream := model.NewQRecordStream(16)
	go func() {
		_, _, pullErr := cfConn.PullQRepRecords(ctx, shared.CatalogPool{}, nil, pullConfig, protos.DBType_CLICKHOUSE,
			&protos.QRepPartition{PartitionId: "hr_part", FullTablePartition: true}, stream)
		stream.Close(pullErr)
	}()
	_, err = stream.Schema()
	require.NoError(t, err)
	for range stream.Records {
	}
	require.NoError(t, stream.Err(), "partition pull with best-effort extension should succeed")
	require.Equal(t, 1, countRunning(), "the extension must leave the retention job running")

	if heartbeatAfter, afterObservable := readHeartbeat(); beforeObservable && afterObservable {
		require.False(t, heartbeatAfter.IsZero(), "the extension should have heartbeated the job")
		require.True(t, heartbeatAfter.After(heartbeatBefore),
			"the extension should move the job heartbeat (and with it the expiry) forward: before %s, after %s",
			heartbeatBefore, heartbeatAfter)
	}

	// a connector without changefeeds enabled never creates protection, so its
	// cleanup must not touch (or even look up) retention jobs
	require.NoError(t, s.source.conn.PullFlowCleanup(ctx, flowName))
	require.Equal(t, 1, countRunning(), "non-changefeed cleanup should leave the retention job alone")

	require.NoError(t, cfConn.PullFlowCleanup(ctx, flowName))
	require.Eventually(t, func() bool {
		return countRunning() == 0
	}, 30*time.Second, time.Second, "PullFlowCleanup should cancel the history retention job")
}

func (s CockroachDBSuite) Test_Peer_Creation_And_Validate() {
	t := s.t
	ctx := t.Context()

	peer := s.source.GeneratePeer(t)
	require.Equal(t, protos.DBType_COCKROACHDB, peer.Type)

	conn := s.Connector()
	require.NoError(t, conn.ConnectionActive(ctx), "cockroachdb connection should be active")
	require.NoError(t, conn.ValidateCheck(ctx), "cockroachdb should pass version gate")
}

func (s CockroachDBSuite) Test_Version() {
	t := s.t
	ctx := t.Context()

	conn := s.Connector()
	version, err := conn.GetVersion(ctx)
	require.NoError(t, err)
	require.Contains(t, version, "CockroachDB", "version string should identify CockroachDB")

	major, err := conn.GetMajorVersion(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, major, 22, "ValidateCheck requires version 22.1 or above")
}

func (s CockroachDBSuite) Test_Database_Variant() {
	t := s.t
	ctx := t.Context()

	variant, err := s.Connector().GetDatabaseVariant(ctx)
	require.NoError(t, err)
	require.Contains(t, []protos.DatabaseVariant{
		protos.DatabaseVariant_VARIANT_UNKNOWN,
		protos.DatabaseVariant_COCKROACHDB_CLOUD,
		protos.DatabaseVariant_COCKROACHDB_DEDICATED,
		protos.DatabaseVariant_COCKROACHDB_SERVERLESS,
	}, variant)
}

func (s CockroachDBSuite) Test_Schema_Introspection() {
	t := s.t
	ctx := t.Context()

	srcTable := "test_types"
	setupCockroachDBTypesTable(s, srcTable)
	conn := s.Connector()

	schemasResp, err := conn.GetSchemas(ctx)
	require.NoError(t, err)
	require.Contains(t, schemasResp.Schemas, Schema(s))

	tablesResp, err := conn.GetTablesInSchema(ctx, Schema(s), false)
	require.NoError(t, err)
	tableNames := make([]string, 0, len(tablesResp.Tables))
	for _, table := range tablesResp.Tables {
		// bare names (the UI schema-qualifies them) and always mirrorable
		require.True(t, table.CanMirror, "table %s should be mirrorable", table.TableName)
		require.NotContains(t, table.TableName, ".")
		tableNames = append(tableNames, table.TableName)
	}
	require.Contains(t, tableNames, srcTable)

	allTablesResp, err := conn.GetAllTables(ctx)
	require.NoError(t, err)
	require.Contains(t, allTablesResp.Tables, AttachSchema(s, srcTable))

	columnsResp, err := conn.GetColumns(ctx, shared.InternalVersion_Latest, Schema(s), srcTable)
	require.NoError(t, err)
	require.Len(t, columnsResp.Columns, len(crdbTypesExpectedQValueKinds))
	for _, col := range columnsResp.Columns {
		require.Contains(t, crdbTypesExpectedQValueKinds, col.Name)
		require.NotEmpty(t, col.Type, "column %s should have a type", col.Name)
	}
}

func (s CockroachDBSuite) Test_Table_Schema_QValue_Kinds() {
	t := s.t
	ctx := t.Context()

	srcTable := "test_types_qvalue"
	setupCockroachDBTypesTable(s, srcTable)

	tableMappings := []*protos.TableMapping{{
		SourceTableIdentifier:      AttachSchema(s, srcTable),
		DestinationTableIdentifier: srcTable + "_dst",
	}}

	schemas, err := s.Connector().GetTableSchema(ctx, nil, shared.InternalVersion_Latest, protos.TypeSystem_Q, tableMappings)
	require.NoError(t, err)
	require.Len(t, schemas, 1)

	tableSchema := schemas[AttachSchema(s, srcTable)]
	require.NotNil(t, tableSchema)
	require.Equal(t, protos.TypeSystem_Q, tableSchema.System)
	require.Equal(t, []string{"id"}, tableSchema.PrimaryKeyColumns)

	columnKinds := make(map[string]types.QValueKind, len(tableSchema.Columns))
	for _, col := range tableSchema.Columns {
		columnKinds[col.Name] = types.QValueKind(col.Type)
	}
	require.Len(t, columnKinds, len(crdbTypesExpectedQValueKinds))
	for colName, expectedKind := range crdbTypesExpectedQValueKinds {
		actualKind, exists := columnKinds[colName]
		require.True(t, exists, "column %s should exist", colName)
		require.Equal(t, expectedKind, actualKind, "column %s should map to %s", colName, expectedKind)
	}

	nullable := make(map[string]bool, len(tableSchema.Columns))
	for _, col := range tableSchema.Columns {
		nullable[col.Name] = col.Nullable
	}
	require.False(t, nullable["id"], "primary key should not be nullable")
	require.True(t, nullable["c_int8"], "c_int8 should be nullable")
	require.False(t, tableSchema.NullableEnabled, "NullableEnabled should be off by default")

	nullableSchemas, err := s.Connector().GetTableSchema(ctx, map[string]string{"PEERDB_NULLABLE": "true"},
		shared.InternalVersion_Latest, protos.TypeSystem_Q, tableMappings)
	require.NoError(t, err)
	require.True(t, nullableSchemas[AttachSchema(s, srcTable)].NullableEnabled,
		"PEERDB_NULLABLE should propagate to the table schema")
}

type CockroachDBClickHouseSuite struct {
	GenericSuite
}

func TestCockroachDBClickHouseSuite(t *testing.T) {
	e2eshared.RunSuite(t, SetupCockroachDBClickHouseSuite)
}

func SetupCockroachDBClickHouseSuite(t *testing.T) CockroachDBClickHouseSuite {
	t.Helper()
	return CockroachDBClickHouseSuite{SetupClickHouseSuite(t, false, func(t *testing.T) (*CockroachDBSource, string, error) {
		t.Helper()
		suffix := "crdbch_" + strings.ToLower(common.RandomString(8))
		source, err := SetupCockroachDB(t, suffix)
		return source, suffix, err
	})(t)}
}

func (s CockroachDBClickHouseSuite) Test_QRep_Simple() {
	t := s.T()
	ctx := t.Context()

	srcTable := "test_qrep_simple"
	srcQualified := AttachSchema(s, srcTable)
	dstTable := "test_qrep_simple_dst"

	require.NoError(t, s.Source().Exec(ctx,
		fmt.Sprintf("CREATE TABLE %s (id INT8 PRIMARY KEY, val TEXT)", srcQualified)))
	for i := range 10 {
		require.NoError(t, s.Source().Exec(ctx,
			fmt.Sprintf("INSERT INTO %s (id, val) VALUES (%d, 'val%d')", srcQualified, i+1, i+1)))
	}

	qrepConfig := CreateQRepWorkflowConfig(t,
		AddSuffix(s, srcTable),
		srcQualified,
		dstTable,
		fmt.Sprintf("SELECT * FROM %s WHERE id BETWEEN {{.start}} AND {{.end}}", srcQualified),
		s.Peer().Name,
		"",
		true,
		"",
		"",
	)
	qrepConfig.SourceName = s.Source().GeneratePeer(t).Name
	qrepConfig.WatermarkColumn = "id"
	qrepConfig.NumRowsPerPartition = 3
	qrepConfig.Env = map[string]string{"PEERDB_NULLABLE": "true"}

	tc := NewTemporalClient(t)
	env := RunQRepFlowWorkflow(t, tc, qrepConfig)
	EnvWaitForFinished(t, env, 3*time.Minute)
	require.NoError(t, env.Error(ctx))

	RequireEqualTablesWithNames(s, srcTable, dstTable, "id,val")
}

func (s CockroachDBClickHouseSuite) Test_QRep_Types() {
	t := s.T()
	ctx := t.Context()

	srcTable := "test_qrep_types"
	srcQualified := AttachSchema(s, srcTable)
	dstTable := "test_qrep_types_dst"

	setupCockroachDBTypesTable(s, srcTable)
	// row with NULLs in every nullable column
	require.NoError(t, s.Source().Exec(ctx, fmt.Sprintf("INSERT INTO %s DEFAULT VALUES", srcQualified)))

	qrepConfig := CreateQRepWorkflowConfig(t,
		AddSuffix(s, srcTable),
		srcQualified,
		dstTable,
		fmt.Sprintf("SELECT * FROM %s WHERE id BETWEEN {{.start}} AND {{.end}}", srcQualified),
		s.Peer().Name,
		"",
		true,
		"",
		"",
	)
	qrepConfig.SourceName = s.Source().GeneratePeer(t).Name
	qrepConfig.WatermarkColumn = "id"
	// nullable destination columns so the all-NULLs row round-trips as NULLs
	// instead of ClickHouse zero values
	qrepConfig.Env = map[string]string{"PEERDB_NULLABLE": "true"}

	tc := NewTemporalClient(t)
	env := RunQRepFlowWorkflow(t, tc, qrepConfig)
	EnvWaitForFinished(t, env, 3*time.Minute)
	require.NoError(t, env.Error(ctx))

	RequireEqualTablesWithNames(s, srcTable, dstTable,
		"id,c_int8,c_decimal,c_uuid,c_jsonb,c_timestamptz,c_bool,c_text_array,c_mood")
}
