package conncockroachdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestParseHLCWallNanos(t *testing.T) {
	testCases := []struct {
		name     string
		hlc      string
		expected int64
		wantErr  bool
	}{
		{"wall and logical", "1784758953157033880.0000000001", 1784758953157033880, false},
		{"wall only", "1784758953157033880", 1784758953157033880, false},
		{"zero", "0", 0, false},
		{"empty", "", 0, true},
		{"negative", "-1784758953157033880.0000000000", 0, true},
		{"letters", "not-a-timestamp", 0, true},
		{"trailing garbage", "1784758953157033880.0000000000; DROP TABLE t", 0, true},
		{"overflow", "97847589531570338800000", 0, true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wall, err := parseHLCWallNanos(tc.hlc)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, wall)
			}
		})
	}
}

func TestChangefeedResolvedInterval(t *testing.T) {
	testCases := []struct {
		idleTimeout time.Duration
		expected    time.Duration
	}{
		{0, time.Second},
		{time.Second, time.Second},
		{20 * time.Second, 5 * time.Second},
		{time.Minute, 10 * time.Second},
		{time.Hour, 10 * time.Second},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.expected, changefeedResolvedInterval(tc.idleTimeout), "idleTimeout=%s", tc.idleTimeout)
	}
}

func TestBuildChangefeedStatement(t *testing.T) {
	cursor := "1784758953157033880.0000000001"

	t.Run("single table", func(t *testing.T) {
		stmt, err := buildChangefeedStatement(
			[]*common.QualifiedTable{{Namespace: "public", Table: "users"}},
			changefeedOptions{Cursor: cursor, ResolvedInterval: 5 * time.Second},
		)
		require.NoError(t, err)
		require.Equal(t, `CREATE CHANGEFEED FOR TABLE "public"."users" WITH envelope = 'wrapped', updated, diff,`+
			` resolved = '5000ms', min_checkpoint_frequency = '5000ms', full_table_name, initial_scan = 'no',`+
			` cursor = '1784758953157033880.0000000001'`, stmt)
	})

	t.Run("multiple tables with quoting", func(t *testing.T) {
		stmt, err := buildChangefeedStatement(
			[]*common.QualifiedTable{
				{Namespace: "public", Table: "users"},
				{Namespace: "sales", Table: `we"ird`},
			},
			changefeedOptions{Cursor: cursor, ResolvedInterval: time.Second},
		)
		require.NoError(t, err)
		require.Contains(t, stmt, `FOR TABLE "public"."users", "sales"."we""ird" WITH`)
	})

	t.Run("interval floor", func(t *testing.T) {
		stmt, err := buildChangefeedStatement(
			[]*common.QualifiedTable{{Namespace: "public", Table: "users"}},
			changefeedOptions{Cursor: cursor},
		)
		require.NoError(t, err)
		require.Contains(t, stmt, "resolved = '1000ms'")
	})

	t.Run("no tables", func(t *testing.T) {
		_, err := buildChangefeedStatement(nil, changefeedOptions{Cursor: cursor})
		require.Error(t, err)
	})

	t.Run("invalid cursor", func(t *testing.T) {
		_, err := buildChangefeedStatement(
			[]*common.QualifiedTable{{Namespace: "public", Table: "users"}},
			changefeedOptions{Cursor: "1784758953157033880.0'; DROP TABLE users --"},
		)
		require.Error(t, err)
	})
}

func TestParseChangefeedEnvelopeOperation(t *testing.T) {
	testCases := []struct {
		name     string
		value    string
		expected changefeedOperation
		resolved string
	}{
		{
			// captured from a CockroachDB v25.4 sinkless changefeed
			name: "insert",
			value: `{"after": {"id": 2, "name": "carol"}, "before": null,` +
				` "updated": "1784758975071714277.0000000000"}`,
			expected: changefeedOpInsert,
		},
		{
			name: "update",
			value: `{"after": {"id": 1, "name": "bob"}, "before": {"id": 1, "name": "alice"},` +
				` "updated": "1784758975071714277.0000000000"}`,
			expected: changefeedOpUpdate,
		},
		{
			name: "delete",
			value: `{"after": null, "before": {"id": 1, "name": "alice"},` +
				` "updated": "1784758975071714277.0000000000"}`,
			expected: changefeedOpDelete,
		},
		{
			name:     "resolved",
			value:    `{"resolved":"1784759045000000000.0000000000"}`,
			expected: changefeedOpSkip,
			resolved: "1784759045000000000.0000000000",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			envelope, err := parseChangefeedEnvelope([]byte(tc.value))
			require.NoError(t, err)
			require.Equal(t, tc.expected, envelope.operation())
			require.Equal(t, tc.resolved, envelope.Resolved)
		})
	}

	t.Run("invalid json", func(t *testing.T) {
		_, err := parseChangefeedEnvelope([]byte("not json"))
		require.Error(t, err)
	})
}

func TestQValueFromChangefeedJSON(t *testing.T) {
	field := func(kind types.QValueKind) types.QField {
		return types.QField{Name: "col", Type: kind, Nullable: true}
	}
	mustDecimal := func(s string) types.QValue {
		num, ok := parseChangefeedNumeric(json.RawMessage(s))
		require.True(t, ok)
		return types.QValueNumeric{Val: num}
	}

	testCases := []struct {
		expected types.QValue
		name     string
		raw      string
		kind     types.QValueKind
	}{
		{types.QValueNull(types.QValueKindInt64), "null", `null`, types.QValueKindInt64},
		{types.QValueBoolean{Val: true}, "bool", `true`, types.QValueKindBoolean},
		{types.QValueInt16{Val: 42}, "int16", `42`, types.QValueKindInt16},
		{types.QValueInt32{Val: -7}, "int32", `-7`, types.QValueKindInt32},
		{types.QValueInt64{Val: 9007199254740993}, "int64 beyond float53", `9007199254740993`, types.QValueKindInt64},
		{types.QValueFloat32{Val: 1.5}, "float32", `1.5`, types.QValueKindFloat32},
		{types.QValueFloat64{Val: 1.5}, "float64", `1.5`, types.QValueKindFloat64},
		{mustDecimal("12345678901234567890.1234567890"), "big numeric", `12345678901234567890.1234567890`, types.QValueKindNumeric},
		{types.QValueNull(types.QValueKindNumeric), "numeric NaN", `"NaN"`, types.QValueKindNumeric},
		{types.QValueString{Val: "b brown"}, "string", `"b brown"`, types.QValueKindString},
		{types.QValueEnum{Val: "happy"}, "enum", `"happy"`, types.QValueKindEnum},
		{types.QValueBytes{Val: []byte{0x01, 0x02}}, "bytes", `"\\x0102"`, types.QValueKindBytes},
		{
			types.QValueUUID{Val: uuid.MustParse("e2cfe34d-4ca8-420b-ae70-64a9a799e282")},
			"uuid", `"e2cfe34d-4ca8-420b-ae70-64a9a799e282"`, types.QValueKindUUID,
		},
		{types.QValueJSON{Val: `{"a": 1}`}, "json", `{"a": 1}`, types.QValueKindJSON},
		{types.QValueJSON{Val: `[{"a": 1}, null]`, IsArray: true}, "json array", `[{"a": 1}, null]`, types.QValueKindArrayJSON},
		{
			types.QValueTimestamp{Val: time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)},
			"timestamp", `"2024-01-02T03:04:05.123456"`, types.QValueKindTimestamp,
		},
		{
			types.QValueTimestampTZ{Val: time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)},
			"timestamptz", `"2024-01-02T03:04:05.123456Z"`, types.QValueKindTimestampTZ,
		},
		{
			types.QValueTimestampTZ{Val: time.Date(2024, 1, 1, 14, 36, 34, 873000000, time.UTC)},
			"timestamptz hour-only offset", `"2024-01-01T14:36:34.873+00"`, types.QValueKindTimestampTZ,
		},
		{
			types.QValueTimestampTZ{Val: time.Date(2024, 1, 1, 14, 36, 34, 873000000, time.UTC)},
			"timestamptz zoneless is UTC", `"2024-01-01T14:36:34.873"`, types.QValueKindTimestampTZ,
		},
		{
			types.QValueDate{Val: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)},
			"date", `"2024-01-02"`, types.QValueKindDate,
		},
		{
			types.QValueTime{Val: 3*time.Hour + 4*time.Minute + 5*time.Second + 123*time.Millisecond},
			"time", `"03:04:05.123"`, types.QValueKindTime,
		},
		{types.QValueINET{Val: "192.168.1.1/24"}, "inet", `"192.168.1.1/24"`, types.QValueKindINET},
		{
			types.QValueInterval{Val: `{"hours":3,"minutes":4,"seconds":5,"days":2,"years":1,"valid":true}`},
			"interval", `"1 year 2 days 03:04:05"`, types.QValueKindInterval,
		},
		{types.QValueArrayInt64{Val: []int64{1, 2}}, "int64 array", `[1, 2]`, types.QValueKindArrayInt64},
		{types.QValueArrayInt32{Val: []int32{1, 2}}, "int32 array", `[1, 2]`, types.QValueKindArrayInt32},
		{types.QValueArrayInt16{Val: []int16{1, 2}}, "int16 array", `[1, 2]`, types.QValueKindArrayInt16},
		{types.QValueArrayFloat64{Val: []float64{1.25, 2.5}}, "float64 array", `[1.25, 2.5]`, types.QValueKindArrayFloat64},
		{types.QValueArrayBoolean{Val: []bool{true, false}}, "bool array", `[true, false]`, types.QValueKindArrayBoolean},
		{
			types.QValueArrayString{Val: []string{"a", "b brown", ""}},
			"string array with null", `["a", "b brown", null]`, types.QValueKindArrayString,
		},
		{
			types.QValueArrayUUID{Val: []uuid.UUID{uuid.MustParse("e2cfe34d-4ca8-420b-ae70-64a9a799e282"), uuid.Nil}},
			"uuid array with null", `["e2cfe34d-4ca8-420b-ae70-64a9a799e282", null]`, types.QValueKindArrayUUID,
		},
		{
			types.QValueArrayDate{Val: []time.Time{time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)}},
			"date array", `["2024-01-02"]`, types.QValueKindArrayDate,
		},
		{
			types.QValueArrayTimestamp{Val: []time.Time{time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)}},
			"timestamp array", `["2024-06-01T10:00:00"]`, types.QValueKindArrayTimestamp,
		},
		{
			types.QValueArrayTimestampTZ{Val: []time.Time{time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)}},
			"timestamptz array", `["2024-06-01T10:00:00Z"]`, types.QValueKindArrayTimestampTZ,
		},
		// unmapped kinds replicate as text
		{types.QValueString{Val: "[1,0,1,1]"}, "fallback to string", `"[1,0,1,1]"`, "vector"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			qv, err := qvalueFromChangefeedJSON(field(tc.kind), json.RawMessage(tc.raw))
			require.NoError(t, err)
			require.Equal(t, tc.expected, qv)
		})
	}

	t.Run("float NaN", func(t *testing.T) {
		qv, err := qvalueFromChangefeedJSON(field(types.QValueKindFloat64), json.RawMessage(`"NaN"`))
		require.NoError(t, err)
		require.True(t, math.IsNaN(qv.(types.QValueFloat64).Val))
	})

	t.Run("numeric NaN not nullable", func(t *testing.T) {
		qv, err := qvalueFromChangefeedJSON(
			types.QField{Name: "col", Type: types.QValueKindNumeric, Nullable: false},
			json.RawMessage(`"Infinity"`))
		require.NoError(t, err)
		require.Equal(t, types.QValueNumeric{}, qv)
	})

	t.Run("big numeric preserves precision", func(t *testing.T) {
		qv, err := qvalueFromChangefeedJSON(field(types.QValueKindNumeric), json.RawMessage(`12345678901234567890.1234567890`))
		require.NoError(t, err)
		require.Equal(t, "12345678901234567890.123456789", qv.(types.QValueNumeric).Val.String())
	})

	t.Run("geometry from geojson", func(t *testing.T) {
		qv, err := qvalueFromChangefeedJSON(field(types.QValueKindGeometry),
			json.RawMessage(`{"coordinates": [1, 2], "type": "Point"}`))
		require.NoError(t, err)
		require.Equal(t, "POINT (1 2)", qv.(types.QValueGeometry).Val)
	})

	t.Run("invalid int", func(t *testing.T) {
		_, err := qvalueFromChangefeedJSON(field(types.QValueKindInt64), json.RawMessage(`"abc"`))
		require.Error(t, err)
	})
}

func TestParseChangefeedTimestampTZ(t *testing.T) {
	expected := time.Date(2024, 1, 1, 14, 36, 34, 873000000, time.UTC)
	inputs := []string{
		"2024-01-01T14:36:34.873Z",
		"2024-01-01T14:36:34.873+00:00",
		"2024-01-01T14:36:34.873+00",
		"2024-01-01T14:36:34.873+0000",
		"2024-01-01 14:36:34.873Z",
		"2024-01-01 14:36:34.873+00:00",
		"2024-01-01 14:36:34.873+00",
		"2024-01-01 14:36:34.873+0000",
		// zoneless values (e.g. after ALTER between TIMESTAMP and TIMESTAMPTZ) are UTC
		"2024-01-01T14:36:34.873",
		"2024-01-01 14:36:34.873",
		"2024-01-01T20:06:34.873+05:30",
		"2024-01-01T20:06:34.873+0530",
		"2024-01-01 20:06:34.873+0530",
		"2024-01-01T09:36:34.873-05",
		"2024-01-01 09:36:34.873-05",
	}
	for _, s := range inputs {
		got, err := parseChangefeedTimestampTZ(s)
		require.NoError(t, err, s)
		require.True(t, expected.Equal(got), "%s parsed to %s", s, got)
	}

	_, err := parseChangefeedTimestampTZ("not a timestamp")
	require.Error(t, err)
}

func TestParsePgIntervalToJSON(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{"full", "1 year 2 days 03:04:05", `{"hours":3,"minutes":4,"seconds":5,"days":2,"years":1,"valid":true}`, false},
		{"months plural", "3 mons 04:05:06.5", `{"hours":4,"minutes":5,"seconds":6.5,"months":3,"valid":true}`, false},
		{"time only", "00:30:00", `{"minutes":30,"valid":true}`, false},
		{"negative time", "-00:30:00", `{"minutes":-30,"valid":true}`, false},
		{"negative days", "-2 days", `{"days":-2,"valid":true}`, false},
		{"single month", "1 mon", `{"months":1,"valid":true}`, false},
		{"unknown unit", "5 fortnights", "", true},
		{"dangling number", "5", "", true},
		{"time not last", "00:30:00 1 day", "", true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := parsePgIntervalToJSON(tc.input)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.JSONEq(t, tc.expected, out)
			}
		})
	}
}

func TestChangefeedRecordItems(t *testing.T) {
	schema := newChangefeedTableSchema(&protos.TableSchema{
		TableIdentifier: "public.users",
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: string(types.QValueKindInt64), Nullable: false},
			{Name: "name", Type: string(types.QValueKindString), Nullable: true},
			{Name: "secret", Type: string(types.QValueKindString), Nullable: true},
		},
	})
	row := map[string]json.RawMessage{
		"id":      json.RawMessage(`1`),
		"name":    json.RawMessage(`"alice"`),
		"secret":  json.RawMessage(`"hidden"`),
		"unknown": json.RawMessage(`5`),
	}

	items, err := changefeedRecordItems(row, schema, map[string]struct{}{"secret": {}})
	require.NoError(t, err)
	require.Equal(t, types.QValueInt64{Val: 1}, items.GetColumnValue("id"))
	require.Equal(t, types.QValueString{Val: "alice"}, items.GetColumnValue("name"))
	require.Nil(t, items.GetColumnValue("secret"))
	require.Nil(t, items.GetColumnValue("unknown"))
}

func TestUnknownColumns(t *testing.T) {
	schema := newChangefeedTableSchema(&protos.TableSchema{
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: string(types.QValueKindInt64)},
		},
	})
	schema.ignored["dropped"] = struct{}{}

	after := map[string]json.RawMessage{
		"id":      json.RawMessage(`1`),
		"b_col":   json.RawMessage(`2`),
		"dropped": json.RawMessage(`3`),
	}
	before := map[string]json.RawMessage{
		"id":    json.RawMessage(`1`),
		"a_col": json.RawMessage(`4`),
		"b_col": json.RawMessage(`5`),
	}

	require.Equal(t, []string{"a_col", "b_col"}, schema.unknownColumns(after, before))
	require.Empty(t, schema.unknownColumns(map[string]json.RawMessage{"id": json.RawMessage(`1`)}))
	require.Empty(t, schema.unknownColumns(nil))
}

func TestChangefeedTableRouting(t *testing.T) {
	state := &changefeedPullState{sourceByEmitted: make(map[string]string)}
	state.indexSource("defaultdb.public.MixedCase", "public.MixedCase")
	state.indexSource(`defaultdb.sales.we"ird`, `sales.we"ird`)
	state.indexSource("defaultdb.public.users", "public.users")

	testCases := []struct {
		name    string
		emitted string
		source  string
		found   bool
	}{
		{"exact", "defaultdb.public.users", "public.users", true},
		{"mixed case exact", "defaultdb.public.MixedCase", "public.MixedCase", true},
		{"mixed case quoted", `defaultdb.public."MixedCase"`, "public.MixedCase", true},
		{"mixed case lowered", "defaultdb.public.mixedcase", "public.MixedCase", true},
		{"quoted lowercase table", `defaultdb."public"."users"`, "public.users", true},
		{"special chars quoted", `defaultdb.sales."we""ird"`, `sales.we"ird`, true},
		{"special chars exact", `defaultdb.sales.we"ird`, `sales.we"ird`, true},
		{"unmapped", "defaultdb.public.missing", "", false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			source, ok := state.lookupSource(tc.emitted)
			require.Equal(t, tc.found, ok)
			require.Equal(t, tc.source, source)
		})
	}
}

func TestChangefeedKeyItems(t *testing.T) {
	schema := newChangefeedTableSchema(&protos.TableSchema{
		TableIdentifier:   "public.orders",
		PrimaryKeyColumns: []string{"id", "region"},
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: string(types.QValueKindInt64), Nullable: false},
			{Name: "region", Type: string(types.QValueKindString), Nullable: false},
			{Name: "name", Type: string(types.QValueKindString), Nullable: true},
		},
	})

	items, err := changefeedKeyItems([]byte(`[7, "us-east"]`), schema, nil)
	require.NoError(t, err)
	require.Equal(t, types.QValueInt64{Val: 7}, items.GetColumnValue("id"))
	require.Equal(t, types.QValueString{Val: "us-east"}, items.GetColumnValue("region"))
	require.Nil(t, items.GetColumnValue("name"))

	items, err = changefeedKeyItems([]byte(`[7, "us-east"]`), schema, map[string]struct{}{"region": {}})
	require.NoError(t, err)
	require.Equal(t, types.QValueInt64{Val: 7}, items.GetColumnValue("id"))
	require.Nil(t, items.GetColumnValue("region"))

	_, err = changefeedKeyItems([]byte(`[7]`), schema, nil)
	require.ErrorContains(t, err, "does not match primary key columns")

	_, err = changefeedKeyItems([]byte(`{"id": 7}`), schema, nil)
	require.ErrorContains(t, err, "failed to parse changefeed key")

	_, err = changefeedKeyItems(nil, schema, nil)
	require.Error(t, err)

	_, err = changefeedKeyItems([]byte(`["abc", "us-east"]`), schema, nil)
	require.ErrorContains(t, err, "primary key column id")

	noPKs := newChangefeedTableSchema(&protos.TableSchema{
		Columns: []*protos.FieldDescription{{Name: "id", Type: string(types.QValueKindInt64)}},
	})
	_, err = changefeedKeyItems([]byte(`[7]`), noPKs, nil)
	require.ErrorContains(t, err, "does not match primary key columns")
}

func TestChangefeedReplayDedup(t *testing.T) {
	logger := log.NewStructuredLogger(slog.Default())
	state := &changefeedPullState{
		req: &model.PullRecordsRequest[model.RecordItems]{
			RecordStream: model.NewCDCStream[model.RecordItems](32),
		},
		seenSinceResolved: make(map[changefeedDedupKey]struct{}),
		recordCount:       1,
		batchDeadline:     time.Now().Add(time.Hour),
	}
	rows := []struct {
		table   string
		key     string
		updated string
	}{
		{"defaultdb.public.users", `[1]`, "1784758975071714277.0000000000"},
		{"defaultdb.public.users", `[2]`, "1784758975071714277.0000000000"},
		{"defaultdb.public.orders", `[1]`, "1784758975071714300.0000000001"},
	}
	for _, row := range rows {
		require.False(t, state.dedupSeen(logger, row.table, []byte(row.key), row.updated))
	}
	// a reconnected session replays the same messages: none may emit again
	for _, row := range rows {
		require.True(t, state.dedupSeen(logger, row.table, []byte(row.key), row.updated))
	}

	c := &CockroachDBConnector{logger: logger}
	done, err := c.handleResolved(t.Context(), state, "1784758975071714400.0000000000")
	require.NoError(t, err)
	require.False(t, done)

	// past a resolved timestamp the same rows are new deliveries again
	for _, row := range rows {
		require.False(t, state.dedupSeen(logger, row.table, []byte(row.key), row.updated))
	}
}

func TestChangefeedRetryConfig(t *testing.T) {
	maxRetries, baseDelay := changefeedRetryConfig(&protos.CockroachDBConfig{})
	require.Equal(t, defaultChangefeedMaxRetries, maxRetries)
	require.Equal(t, defaultChangefeedRetryBaseDelay, baseDelay)

	configuredRetries := uint32(2)
	configuredDelay := uint32(1500)
	maxRetries, baseDelay = changefeedRetryConfig(&protos.CockroachDBConfig{
		MaxRetries:       &configuredRetries,
		RetryBaseDelayMs: &configuredDelay,
	})
	require.Equal(t, uint32(2), maxRetries)
	require.Equal(t, 1500*time.Millisecond, baseDelay)
}

func TestChangefeedBackoff(t *testing.T) {
	testCases := []struct {
		expected  time.Duration
		baseDelay time.Duration
		attempt   uint32
	}{
		{500 * time.Millisecond, 500 * time.Millisecond, 1},
		{time.Second, 500 * time.Millisecond, 2},
		{2 * time.Second, 500 * time.Millisecond, 3},
		{16 * time.Second, 500 * time.Millisecond, 6},
		{30 * time.Second, 500 * time.Millisecond, 7},
		{30 * time.Second, 500 * time.Millisecond, 64},
		{30 * time.Second, time.Minute, 1},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.expected, changefeedBackoff(tc.baseDelay, tc.attempt),
			"baseDelay=%s attempt=%d", tc.baseDelay, tc.attempt)
	}
}

func TestChangefeedErrorClassification(t *testing.T) {
	gcErr := errors.New(`batch timestamp 1700000000.0 must be after replica GC threshold 1700001000.0`)
	require.True(t, isCursorTooOldError(gcErr))
	require.True(t, isPermanentChangefeedError(gcErr))

	// GC threshold errors are matched on message even inside a PgError
	gcPgErr := &pgconn.PgError{
		Code:    pgerrcode.InvalidParameterValue,
		Message: "batch timestamp 1700000000.0 must be after replica GC threshold 1700001000.0",
	}
	require.True(t, isCursorTooOldError(gcPgErr))
	require.True(t, isPermanentChangefeedError(gcPgErr))

	undefinedTable := &pgconn.PgError{Code: pgerrcode.UndefinedTable, Message: `relation "public.users" does not exist`}
	require.True(t, isPermanentChangefeedError(undefinedTable))
	require.True(t, isPermanentChangefeedError(fmt.Errorf("failed to create changefeed: %w", undefinedTable)))
	require.True(t, isPermanentChangefeedError(&pgconn.PgError{
		Code: pgerrcode.SyntaxError, Message: "syntax error at or near \"CHANGEFEED\"",
	}))

	// serialization, deadlock, connection and unknown SQLSTATEs are retryable,
	// even when the message resembles a permanent-error substring
	require.False(t, isPermanentChangefeedError(&pgconn.PgError{
		Code: pgerrcode.SerializationFailure, Message: "restart transaction",
	}))
	require.False(t, isPermanentChangefeedError(&pgconn.PgError{
		Code: pgerrcode.DeadlockDetected, Message: "deadlock detected",
	}))
	require.False(t, isPermanentChangefeedError(&pgconn.PgError{
		Code: pgerrcode.ConnectionFailure, Message: "connection failure",
	}))
	require.False(t, isPermanentChangefeedError(&pgconn.PgError{
		Code: pgerrcode.InternalError, Message: "descriptor does not exist",
	}))

	// non-PgError fallback keeps the substring checks
	require.True(t, isPermanentChangefeedError(errors.New(`relation "public.users" does not exist`)))
	require.True(t, isPermanentChangefeedError(errors.New(
		"rangefeeds require the kv.rangefeed.enabled setting. See ...")))

	require.False(t, isCursorTooOldError(nil))
	require.False(t, isPermanentChangefeedError(nil))
	require.False(t, isPermanentChangefeedError(errors.New("unexpected EOF")))
	require.False(t, isPermanentChangefeedError(errors.New("connection reset by peer")))
}
