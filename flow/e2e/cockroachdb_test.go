package e2e

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/protobuf/proto"

	conncockroachdb "github.com/PeerDB-io/peerdb/flow/connectors/cockroachdb"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
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

	// the setting is cluster wide and the suite runs in parallel on a shared
	// container: restore it even if the test dies mid-flip, so a failure here
	// cannot disable rangefeeds for every later test and run
	t.Cleanup(func() {
		_ = s.source.Exec(context.Background(), "SET CLUSTER SETTING kv.rangefeed.enabled = true")
	})
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
	require.ErrorContains(t, err, "below the 3600 second minimum")

	// exactly at the floor passes again
	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s.gcttl_check CONFIGURE ZONE USING gc.ttlseconds = 3600", schema)))
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

func (s CockroachDBSuite) Test_Generated_Columns_Introspection() {
	t := s.t
	ctx := t.Context()
	schema := "e2e_test_" + s.suffix
	table := "gen_cols"

	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.%s (
		id INT8 PRIMARY KEY,
		a INT8,
		stored_col INT8 AS (a * 2) STORED,
		virt_col INT8 AS (a * 3) VIRTUAL
	)`, schema, table)))

	// virtual computed columns are omitted by changefeeds, so introspection
	// must exclude them; stored computed columns are emitted and stay
	schemas, err := s.Connector().GetTableSchema(ctx, nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: schema + "." + table}})
	require.NoError(t, err)
	names := make([]string, 0, len(schemas[schema+"."+table].Columns))
	for _, col := range schemas[schema+"."+table].Columns {
		names = append(names, col.Name)
	}
	require.ElementsMatch(t, []string{"id", "a", "stored_col"}, names)

	columnsResp, err := s.Connector().GetColumns(ctx, shared.InternalVersion_Latest, schema, table)
	require.NoError(t, err)
	colNames := make([]string, 0, len(columnsResp.Columns))
	for _, col := range columnsResp.Columns {
		colNames = append(colNames, col.Name)
	}
	require.ElementsMatch(t, []string{"id", "a", "stored_col"}, colNames)
}

// newCrdbCdcOtelManager builds an OtelManager backed by a manual reader with
// every metric instrument PullRecords records to.
func newCrdbCdcOtelManager(t *testing.T) *otel_metrics.OtelManager {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	om := &otel_metrics.OtelManager{
		MetricsProvider:    provider,
		Meter:              provider.Meter("crdb_e2e"),
		Float64GaugesCache: make(map[string]metric.Float64Gauge),
		Int64GaugesCache:   make(map[string]metric.Int64Gauge),
		Int64CountersCache: make(map[string]metric.Int64Counter),
	}
	var err error
	om.Metrics.CockroachDBResolvedLagGauge, err = om.GetOrInitFloat64Gauge(
		otel_metrics.BuildMetricName(otel_metrics.CockroachDBResolvedLagGaugeName))
	require.NoError(t, err)
	om.Metrics.CockroachDBRecordsReceivedCounter, err = om.GetOrInitInt64Counter(
		otel_metrics.BuildMetricName(otel_metrics.CockroachDBRecordsReceivedName))
	require.NoError(t, err)
	om.Metrics.LatestConsumedLogEventGauge, err = om.GetOrInitInt64Gauge(
		otel_metrics.BuildMetricName(otel_metrics.LatestConsumedLogEventGaugeName))
	require.NoError(t, err)
	om.Metrics.SourceLagGauge, err = om.GetOrInitInt64Gauge(
		otel_metrics.BuildMetricName(otel_metrics.SourceLagGaugeName))
	require.NoError(t, err)
	om.Metrics.FetchedBytesCounter, err = om.GetOrInitInt64Counter(
		otel_metrics.BuildMetricName(otel_metrics.FetchedBytesCounterName))
	require.NoError(t, err)
	om.Metrics.AllFetchedBytesCounter, err = om.GetOrInitInt64Counter(
		otel_metrics.BuildMetricName(otel_metrics.AllFetchedBytesCounterName))
	require.NoError(t, err)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return om
}

// changefeedConnector builds a connector whose peer config has changefeeds
// enabled, as CDC mirrors require.
func (s CockroachDBSuite) changefeedConnector(t *testing.T) *conncockroachdb.CockroachDBConnector {
	t.Helper()
	cfPeerConfig := proto.CloneOf(s.source.config)
	cfPeerConfig.UseChangefeeds = true
	cfConn, err := conncockroachdb.NewCockroachDBConnector(t.Context(), nil, cfPeerConfig)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cfConn.Close()) })
	return cfConn
}

// pullOneBatch runs one PullRecords call and drains its stream. The suite
// runs in parallel with Test_Rangefeed_Validation on a shared cluster, so a
// pull failing inside that test's brief rangefeed-disabled window is retried.
func pullOneBatch(
	t *testing.T,
	ctx context.Context,
	conn *conncockroachdb.CockroachDBConnector,
	req *model.PullRecordsRequest[model.RecordItems],
) ([]model.Record[model.RecordItems], model.CdcCheckpoint, error) {
	t.Helper()
	for attempt := range 5 {
		stream := model.NewCDCStream[model.RecordItems](1024)
		req.RecordStream = stream
		om := newCrdbCdcOtelManager(t)
		var records []model.Record[model.RecordItems]
		pullErr := make(chan error, 1)
		go func() {
			pullErr <- conn.PullRecords(ctx, shared.CatalogPool{}, om, req)
		}()
		for record := range stream.GetRecords() {
			records = append(records, record)
		}
		err := <-pullErr
		if err != nil && strings.Contains(err.Error(), "kv.rangefeed.enabled") && attempt < 4 {
			t.Logf("retrying pull, rangefeeds were disabled by a concurrent test: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		return records, stream.GetLastCheckpoint(), err
	}
	panic("unreachable")
}

func (s CockroachDBSuite) Test_CDC_Live_PullRecords() {
	t := s.t
	ctx := t.Context()
	schema := "e2e_test_" + s.suffix
	flowName := "cdclive_" + s.suffix

	plainSrc := schema + ".cdc_rows"
	// PeerDB table identifiers are raw dotted names without SQL quoting;
	// only the SQL statements below quote the mixed-case name
	mixedSrc := schema + ".CdC Mixed"
	mixedSQL := schema + `."CdC Mixed"`
	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INT8 PRIMARY KEY, v TEXT, ip INET, g GEOGRAPHY, tm TIME)", plainSrc)))
	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INT8 PRIMARY KEY, v TEXT)", mixedSQL)))

	cfConn := s.changefeedConnector(t)
	_, err := cfConn.SetupReplication(ctx, shared.CatalogPool{}, &protos.SetupReplicationInput{
		FlowJobName: flowName,
		TableNameMapping: map[string]string{
			plainSrc: "cdc_rows_dst",
			mixedSrc: "cdc_mixed_dst",
		},
	})
	require.NoError(t, err)
	lastOffset, err := cfConn.GetLastOffset(ctx, flowName)
	require.NoError(t, err)
	require.NotEmpty(t, lastOffset.Text, "SetupReplication must seed the changefeed cursor")

	// all three operation types, plus rows in a mixed-case quoted table to
	// prove live full_table_name routing (emitted names are unquoted)
	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s VALUES (1, 'a', '192.168.1.5', 'POINT(-74 40.7)', '24:00:00'),
			(2, 'b', '10.0.0.0/24', NULL, '12:30:00'), (3, 'c', NULL, NULL, NULL)`, plainSrc)))
	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf("UPDATE %s SET v = 'b2' WHERE id = 2", plainSrc)))
	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = 3", plainSrc)))
	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (10, 'mixed')", mixedSQL)))

	tableMappings := []*protos.TableMapping{
		{SourceTableIdentifier: plainSrc, DestinationTableIdentifier: "cdc_rows_dst"},
		{SourceTableIdentifier: mixedSrc, DestinationTableIdentifier: "cdc_mixed_dst"},
	}
	schemas, err := cfConn.GetTableSchema(ctx, nil, shared.InternalVersion_Latest, protos.TypeSystem_Q, tableMappings)
	require.NoError(t, err)

	makeRequest := func(offset model.CdcCheckpoint) *model.PullRecordsRequest[model.RecordItems] {
		return &model.PullRecordsRequest[model.RecordItems]{
			FlowJobName: flowName,
			TableNameMapping: map[string]model.NameAndExclude{
				plainSrc: model.NewNameAndExclude("cdc_rows_dst", nil),
				mixedSrc: model.NewNameAndExclude("cdc_mixed_dst", nil),
			},
			TableNameSchemaMapping: map[string]*protos.TableSchema{
				"cdc_rows_dst":  schemas[plainSrc],
				"cdc_mixed_dst": schemas[mixedSrc],
			},
			LastOffset:      offset,
			MaxBatchSize:    1000,
			InternalVersion: shared.InternalVersion_Latest,
			IdleTimeout:     5 * time.Second,
		}
	}

	records, checkpoint, err := pullOneBatch(t, ctx, cfConn, makeRequest(lastOffset))
	require.NoError(t, err)
	require.Len(t, records, 6, "3 inserts + 1 update + 1 delete + 1 mixed-case insert")

	byTable := map[string]int{}
	var inserts, updates, deletes int
	for _, record := range records {
		byTable[record.GetDestinationTableName()]++
		switch typed := record.(type) {
		case *model.InsertRecord[model.RecordItems]:
			inserts++
			if typed.DestinationTableName == "cdc_rows_dst" && typed.Items.GetColumnValue("id") == (types.QValueInt64{Val: 1}) {
				require.Equal(t, types.QValueINET{Val: "192.168.1.5"}, typed.Items.GetColumnValue("ip"),
					"CDC INET must use the bare host form the snapshot path also produces")
				require.Equal(t, types.QValueGeography{Val: "SRID=4326;POINT (-74 40.7)"}, typed.Items.GetColumnValue("g"),
					"CDC geography must carry the default SRID like the snapshot path")
				require.Equal(t, types.QValueTime{Val: 23*time.Hour + 59*time.Minute + 59*time.Second + 999999*time.Microsecond},
					typed.Items.GetColumnValue("tm"), "TIME 24:00:00 must clamp like the snapshot path")
			}
		case *model.UpdateRecord[model.RecordItems]:
			updates++
			require.Equal(t, types.QValueString{Val: "b2"}, typed.NewItems.GetColumnValue("v"))
		case *model.DeleteRecord[model.RecordItems]:
			deletes++
		}
	}
	require.Equal(t, 4, inserts)
	require.Equal(t, 1, updates)
	require.Equal(t, 1, deletes)
	require.Equal(t, 5, byTable["cdc_rows_dst"])
	require.Equal(t, 1, byTable["cdc_mixed_dst"], "mixed-case quoted table must route through live full_table_name")

	require.NotEmpty(t, checkpoint.Text)
	require.NotEqual(t, lastOffset.Text, checkpoint.Text, "checkpoint must advance past the pulled records")
	require.Greater(t, checkpoint.ID, lastOffset.ID)

	// resume from the persisted checkpoint: every row written after it must
	// arrive exactly once. Rows between the batch's last resolved timestamp
	// and its end may be re-delivered (documented at-least-once behavior on
	// batch boundaries; destinations converge), but nothing may be lost and
	// nothing outside the first batch's tail may repeat.
	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (4, 'd', NULL, NULL, NULL)", plainSrc)))
	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (11, 'mixed2')", mixedSQL)))

	records2, checkpoint2, err := pullOneBatch(t, ctx, cfConn, makeRequest(checkpoint))
	require.NoError(t, err)
	newRows := map[string]int{}
	for _, record := range records2 {
		dst := record.GetDestinationTableName()
		var id int64
		switch typed := record.(type) {
		case *model.InsertRecord[model.RecordItems]:
			id = typed.Items.GetColumnValue("id").(types.QValueInt64).Val
		case *model.UpdateRecord[model.RecordItems]:
			id = typed.NewItems.GetColumnValue("id").(types.QValueInt64).Val
		case *model.DeleteRecord[model.RecordItems]:
			id = typed.Items.GetColumnValue("id").(types.QValueInt64).Val
		}
		switch {
		case dst == "cdc_rows_dst" && id == 4, dst == "cdc_mixed_dst" && id == 11:
			newRows[fmt.Sprintf("%s/%d", dst, id)]++
		case dst == "cdc_rows_dst" && (id == 1 || id == 2 || id == 3),
			dst == "cdc_mixed_dst" && id == 10:
			// tail replay from the first batch, acceptable at-least-once
		default:
			t.Fatalf("unexpected record in resumed batch: table %s id %d", dst, id)
		}
	}
	require.Equal(t, map[string]int{"cdc_rows_dst/4": 1, "cdc_mixed_dst/11": 1}, newRows,
		"rows written after the checkpoint must arrive exactly once")
	require.Greater(t, checkpoint2.ID, checkpoint.ID)
}

func (s CockroachDBSuite) Test_CDC_Truncate_Is_Terminal() {
	t := s.t
	ctx := t.Context()
	schema := "e2e_test_" + s.suffix
	flowName := "cdctrunc_" + s.suffix
	src := schema + ".trunc_victim"

	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id INT8 PRIMARY KEY, v TEXT)", src)))
	err := s.runPullAgainstBreakage(ctx, flowName, src, func() error {
		return s.source.Exec(ctx, "TRUNCATE "+src)
	})
	require.Error(t, err)
	var irrecoverable *exceptions.CockroachChangefeedIrrecoverableError
	require.ErrorAs(t, err, &irrecoverable, "TRUNCATE must classify as irrecoverable, not burn retries: %v", err)
	require.Equal(t, "TABLE_TRUNCATED", irrecoverable.Code)
	require.ErrorContains(t, err, "resync")
}

func (s CockroachDBSuite) Test_CDC_Drop_Is_Terminal() {
	t := s.t
	ctx := t.Context()
	schema := "e2e_test_" + s.suffix
	flowName := "cdcdrop_" + s.suffix
	src := schema + ".drop_victim"

	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id INT8 PRIMARY KEY, v TEXT)", src)))
	err := s.runPullAgainstBreakage(ctx, flowName, src, func() error {
		return s.source.Exec(ctx, "DROP TABLE "+src)
	})
	require.Error(t, err)
	var irrecoverable *exceptions.CockroachChangefeedIrrecoverableError
	require.ErrorAs(t, err, &irrecoverable, "DROP must classify as irrecoverable, not burn retries: %v", err)
	require.Equal(t, "TABLE_DROPPED", irrecoverable.Code)
}

// runPullAgainstBreakage starts a live PullRecords on the given table, fires
// the breakage mid-feed and returns PullRecords' error.
func (s CockroachDBSuite) runPullAgainstBreakage(
	ctx context.Context, flowName string, src string, breakage func() error,
) error {
	t := s.t
	t.Helper()

	cfConn := s.changefeedConnector(t)
	_, err := cfConn.SetupReplication(ctx, shared.CatalogPool{}, &protos.SetupReplicationInput{
		FlowJobName:      flowName,
		TableNameMapping: map[string]string{src: "breakage_dst"},
	})
	require.NoError(t, err)
	lastOffset, err := cfConn.GetLastOffset(ctx, flowName)
	require.NoError(t, err)

	schemas, err := cfConn.GetTableSchema(ctx, nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: src, DestinationTableIdentifier: "breakage_dst"}})
	require.NoError(t, err)

	req := &model.PullRecordsRequest[model.RecordItems]{
		FlowJobName:            flowName,
		TableNameMapping:       map[string]model.NameAndExclude{src: model.NewNameAndExclude("breakage_dst", nil)},
		TableNameSchemaMapping: map[string]*protos.TableSchema{"breakage_dst": schemas[src]},
		LastOffset:             lastOffset,
		MaxBatchSize:           1000,
		InternalVersion:        shared.InternalVersion_Latest,
		IdleTimeout:            time.Minute,
		RecordStream:           model.NewCDCStream[model.RecordItems](1024),
	}
	om := newCrdbCdcOtelManager(t)
	pullErr := make(chan error, 1)
	go func() {
		pullErr <- cfConn.PullRecords(ctx, shared.CatalogPool{}, om, req)
	}()
	go func() {
		for range req.RecordStream.GetRecords() {
		}
	}()

	// let the changefeed establish before breaking the table
	time.Sleep(3 * time.Second)
	require.NoError(t, breakage())

	select {
	case err := <-pullErr:
		return err
	case <-time.After(2 * time.Minute):
		t.Fatal("PullRecords did not return after the watched table broke")
		return nil
	}
}

func (s CockroachDBSuite) Test_CDC_Cursor_Past_GC_Is_Terminal() {
	t := s.t
	ctx := t.Context()
	schema := "e2e_test_" + s.suffix
	flowName := "cdcgc_" + s.suffix
	src := schema + ".gc_victim"

	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id INT8 PRIMARY KEY, v TEXT)", src)))
	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s CONFIGURE ZONE USING gc.ttlseconds = 1", src)))
	// force a split inside the table: forced MVCC GC below is then scoped to
	// a range that lies fully within this table, so it can never eat MVCC
	// history of the other tests running in parallel on the shared cluster
	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s SPLIT AT VALUES (9223372036854775807)", src)))
	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 'a')", src)))

	cfConn := s.changefeedConnector(t)
	_, err := cfConn.SetupReplication(ctx, shared.CatalogPool{}, &protos.SetupReplicationInput{
		FlowJobName:      flowName,
		TableNameMapping: map[string]string{src: "gc_dst"},
	})
	require.NoError(t, err)
	lastOffset, err := cfConn.GetLastOffset(ctx, flowName)
	require.NoError(t, err)
	require.NoError(t, s.source.Exec(ctx, fmt.Sprintf("UPDATE %s SET v = 'b' WHERE id = 1", src)))

	schemas, err := cfConn.GetTableSchema(ctx, nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: src, DestinationTableIdentifier: "gc_dst"}})
	require.NoError(t, err)

	// force MVCC GC past the stored cursor, but only on ranges lying fully
	// inside this table (the table-boundary split from the zone config takes
	// several seconds to reconcile). Needs the range leaseholder, so on
	// multi-node clusters (or without admin rights) this may never fire.
	forceGC := func() {
		_ = s.source.Exec(ctx, "SET allow_unsafe_internals = on")
		if err := s.source.Exec(ctx, fmt.Sprintf(
			`SELECT crdb_internal.kv_enqueue_replica(range_id, 'mvccGC', true) FROM [SHOW RANGES FROM TABLE %s]`+
				` WHERE start_key NOT LIKE '<before:%%' AND end_key NOT LIKE '<after:%%'`,
			src)); err != nil {
			t.Logf("could not enqueue MVCC GC: %v", err)
		}
	}

	deadline := time.Now().Add(75 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Skip("could not force MVCC GC past the cursor on this cluster (needs single node + admin), skipping")
		}
		time.Sleep(3 * time.Second)
		forceGC()

		attemptCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		req := &model.PullRecordsRequest[model.RecordItems]{
			FlowJobName:            flowName,
			TableNameMapping:       map[string]model.NameAndExclude{src: model.NewNameAndExclude("gc_dst", nil)},
			TableNameSchemaMapping: map[string]*protos.TableSchema{"gc_dst": schemas[src]},
			LastOffset:             lastOffset,
			MaxBatchSize:           1000,
			InternalVersion:        shared.InternalVersion_Latest,
			IdleTimeout:            2 * time.Second,
			RecordStream:           model.NewCDCStream[model.RecordItems](1024),
		}
		om := newCrdbCdcOtelManager(t)
		pullErr := make(chan error, 1)
		go func() {
			pullErr <- cfConn.PullRecords(attemptCtx, shared.CatalogPool{}, om, req)
		}()
		go func() {
			for range req.RecordStream.GetRecords() {
			}
		}()
		err := <-pullErr
		cancel()
		var irrecoverable *exceptions.CockroachChangefeedIrrecoverableError
		if errors.As(err, &irrecoverable) {
			require.Equal(t, "CURSOR_PAST_GC", irrecoverable.Code)
			require.ErrorContains(t, err, "resync")
			return
		}
		// cursor still inside the GC window: the pull just ran until the
		// attempt context expired, force GC again and retry
	}
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

// Test_CDC_Simple runs a full CDC mirror (initial snapshot + changefeed) from
// CockroachDB to ClickHouse through the real workflow stack, covering inserts,
// updates and deletes end to end.
func (s CockroachDBClickHouseSuite) Test_CDC_Simple() {
	t := s.T()
	ctx := t.Context()

	srcTable := "test_cdc_simple"
	dstTable := "test_cdc_simple_dst"
	srcQualified := AttachSchema(s, srcTable)

	require.NoError(t, s.Source().Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INT8 PRIMARY KEY, ky TEXT NOT NULL, val TEXT NOT NULL)", srcQualified)))
	for i := range 5 {
		require.NoError(t, s.Source().Exec(ctx, fmt.Sprintf(
			"INSERT INTO %s (id, ky, val) VALUES (%d, 'init_key_%d', 'init_value_%d')", srcQualified, i+1, i, i)))
	}

	// CDC mirrors require a changefeed-enabled peer
	crdbSource, sourceOk := s.Source().(*CockroachDBSource)
	require.True(t, sourceOk)
	cfPeerConfig := proto.CloneOf(crdbSource.config)
	cfPeerConfig.UseChangefeeds = true
	cfPeer := &protos.Peer{
		Name: AddSuffix(s, "cockroachdb_cf"),
		Type: protos.DBType_COCKROACHDB,
		Config: &protos.Peer_CockroachdbConfig{
			CockroachdbConfig: cfPeerConfig,
		},
	}
	CreatePeer(t, cfPeer)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.SourceName = cfPeer.Name
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "initial snapshot loaded", srcTable, dstTable, "id,ky,val")

	for i := range 5 {
		EnvNoError(t, env, s.Source().Exec(ctx, fmt.Sprintf(
			"INSERT INTO %s (id, ky, val) VALUES (%d, 'cdc_key_%d', 'cdc_value_%d')", srcQualified, i+6, i, i)))
	}
	EnvNoError(t, env, s.Source().Exec(ctx, fmt.Sprintf("UPDATE %s SET val = 'updated' WHERE id = 2", srcQualified)))
	EnvNoError(t, env, s.Source().Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = 3", srcQualified)))

	EnvWaitForEqualTablesWithNames(env, s, "normalizing cdc inserts, an update and a delete",
		srcTable, dstTable, "id,ky,val")

	env.Cancel(ctx)
	RequireEnvCanceled(t, env)
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
