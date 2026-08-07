package conncockroachdb

import (
	"context"
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
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// newMetricsTestOtelManager builds an OtelManager backed by a ManualReader so
// tests can collect and assert recorded metric values without a real exporter.
func newMetricsTestOtelManager(t *testing.T) (*otel_metrics.OtelManager, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	om := &otel_metrics.OtelManager{
		MetricsProvider:    provider,
		Meter:              provider.Meter("test"),
		Float64GaugesCache: make(map[string]metric.Float64Gauge),
		Int64GaugesCache:   make(map[string]metric.Int64Gauge),
		Int64CountersCache: make(map[string]metric.Int64Counter),
	}
	gauge, err := om.GetOrInitFloat64Gauge(otel_metrics.BuildMetricName(otel_metrics.CockroachDBResolvedLagGaugeName))
	require.NoError(t, err)
	om.Metrics.CockroachDBResolvedLagGauge = gauge
	counter, err := om.GetOrInitInt64Counter(otel_metrics.BuildMetricName(otel_metrics.CockroachDBRecordsReceivedName))
	require.NoError(t, err)
	om.Metrics.CockroachDBRecordsReceivedCounter = counter
	logEventGauge, err := om.GetOrInitInt64Gauge(otel_metrics.BuildMetricName(otel_metrics.LatestConsumedLogEventGaugeName))
	require.NoError(t, err)
	om.Metrics.LatestConsumedLogEventGauge = logEventGauge
	sourceLagGauge, err := om.GetOrInitInt64Gauge(otel_metrics.BuildMetricName(otel_metrics.SourceLagGaugeName))
	require.NoError(t, err)
	om.Metrics.SourceLagGauge = sourceLagGauge
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return om, reader
}

func collectMetric(t *testing.T, ctx context.Context, reader *sdkmetric.ManualReader, name string) (metricdata.Metrics, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	wantName := otel_metrics.BuildMetricName(name)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == wantName {
				return m, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

func TestParseHLC(t *testing.T) {
	testCases := []struct {
		name      string
		hlc       string
		expected  crdbHLC
		roundTrip string
		wantErr   bool
	}{
		{
			name: "wall and logical", hlc: "1784758953157033880.0000000001",
			expected:  crdbHLC{WallNanos: 1784758953157033880, Logical: 1},
			roundTrip: "1784758953157033880.0000000001",
		},
		{
			name: "wall only", hlc: "1784758953157033880",
			expected:  crdbHLC{WallNanos: 1784758953157033880},
			roundTrip: "1784758953157033880.0000000000",
		},
		{name: "zero", hlc: "0", expected: crdbHLC{}, roundTrip: "0.0000000000"},
		{name: "empty", hlc: "", wantErr: true},
		{name: "negative", hlc: "-1784758953157033880.0000000000", wantErr: true},
		{name: "letters", hlc: "not-a-timestamp", wantErr: true},
		{name: "trailing garbage", hlc: "1784758953157033880.0000000000; DROP TABLE t", wantErr: true},
		{name: "overflow", hlc: "97847589531570338800000", wantErr: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ts, err := parseHLC(tc.hlc)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, ts)
				require.Equal(t, tc.roundTrip, ts.String())
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
	cursor := crdbHLC{WallNanos: 1784758953157033880, Logical: 1}

	t.Run("single table", func(t *testing.T) {
		stmt, err := buildChangefeedStatement(
			[]*common.QualifiedTable{{Namespace: "public", Table: "users"}},
			changefeedOptions{Cursor: cursor, ResolvedInterval: 5 * time.Second},
		)
		require.NoError(t, err)
		require.Equal(t, `CREATE CHANGEFEED FOR TABLE "public"."users" WITH envelope = 'wrapped', updated, diff,`+
			` resolved = '5000ms', min_checkpoint_frequency = '5000ms', full_table_name, initial_scan = 'no',`+
			` schema_change_policy = 'nobackfill', cursor = '1784758953157033880.0000000001'`, stmt)
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

	// cursors are typed HLC values whose rendering is always plain digits, so
	// hostile cursor text cannot reach the statement; parseHLC rejects it at
	// the string boundary (see TestParseHLC "trailing garbage")
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
		{types.QValueUInt32{Val: 4294967295}, "oid as uint32", `4294967295`, types.QValueKindUInt32},
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
			types.QValueTimestamp{Val: time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)},
			"timestamp with UTC Z suffix", `"2024-01-02T03:04:05.123456Z"`, types.QValueKindTimestamp,
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
		// extreme temporal values are legal in CockroachDB; emission formats
		// captured live from v25.4.13/v26.2.5 changefeeds
		{
			types.QValueDate{Val: time.Date(-1191, 9, 4, 0, 0, 0, 0, time.UTC)},
			"BC date", `"1192-09-04 BC"`, types.QValueKindDate,
		},
		{
			types.QValueDate{Val: time.Date(5874000, 1, 1, 0, 0, 0, 0, time.UTC)},
			"seven digit year date", `"5874000-01-01"`, types.QValueKindDate,
		},
		{
			types.QValueTimestamp{Val: time.Date(10000, 1, 1, 5, 6, 7, 0, time.UTC)},
			"five digit year timestamp", `"10000-01-01T05:06:07"`, types.QValueKindTimestamp,
		},
		{
			types.QValueTimestampTZ{Val: time.Date(-1191, 9, 4, 12, 30, 0, 0, time.UTC)},
			"BC timestamptz negative astronomical year", `"-1191-09-04T12:30:00Z"`, types.QValueKindTimestampTZ,
		},
		{
			types.QValueTime{Val: 3*time.Hour + 4*time.Minute + 5*time.Second + 123*time.Millisecond},
			"time", `"03:04:05.123"`, types.QValueKindTime,
		},
		{
			types.QValueTime{Val: 23*time.Hour + 59*time.Minute + 59*time.Second + 999999*time.Microsecond},
			"time 24:00 clamps into the day", `"24:00:00"`, types.QValueKindTime,
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
		// NULL elements map to the zero value, matching the snapshot path
		// (shared.ArrayCastElements) and the guarded array kinds below
		{types.QValueArrayInt64{Val: []int64{1, 0, 2}}, "int64 array with null", `[1, null, 2]`, types.QValueKindArrayInt64},
		{types.QValueArrayInt32{Val: []int32{1, 0, 2}}, "int32 array with null", `[1, null, 2]`, types.QValueKindArrayInt32},
		{types.QValueArrayInt16{Val: []int16{1, 0, 2}}, "int16 array with null", `[1, null, 2]`, types.QValueKindArrayInt16},
		{types.QValueArrayFloat32{Val: []float32{1.25, 2.5}}, "float32 array", `[1.25, 2.5]`, types.QValueKindArrayFloat32},
		{
			types.QValueArrayFloat32{Val: []float32{1.25, 0, 2.5}},
			"float32 array with null", `[1.25, null, 2.5]`, types.QValueKindArrayFloat32,
		},
		{
			types.QValueArrayFloat64{Val: []float64{1.25, 0, 2.5}},
			"float64 array with null", `[1.25, null, 2.5]`, types.QValueKindArrayFloat64,
		},
		{
			types.QValueArrayNumeric{Val: []decimal.Decimal{decimal.RequireFromString("1.5"), decimal.RequireFromString("2.5")}},
			"numeric array", `[1.5, 2.5]`, types.QValueKindArrayNumeric,
		},
		{
			types.QValueArrayNumeric{Val: []decimal.Decimal{decimal.RequireFromString("1.5"), {}}},
			"numeric array with null", `[1.5, null]`, types.QValueKindArrayNumeric,
		},
		{types.QValueArrayBoolean{Val: []bool{true, false}}, "bool array", `[true, false]`, types.QValueKindArrayBoolean},
		{
			types.QValueArrayBoolean{Val: []bool{true, false, false}},
			"bool array with null", `[true, null, false]`, types.QValueKindArrayBoolean,
		},
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

	t.Run("geography defaults to SRID 4326", func(t *testing.T) {
		// GeoJSON carries no SRID; the snapshot path emits SRID=4326;... from
		// EWKB, so the changefeed path must match
		qv, err := qvalueFromChangefeedJSON(field(types.QValueKindGeography),
			json.RawMessage(`{"coordinates": [-74, 40.7], "type": "Point"}`))
		require.NoError(t, err)
		require.Equal(t, "SRID=4326;POINT (-74 40.7)", qv.(types.QValueGeography).Val)
	})

	t.Run("invalid int", func(t *testing.T) {
		_, err := qvalueFromChangefeedJSON(field(types.QValueKindInt64), json.RawMessage(`"abc"`))
		require.Error(t, err)
	})

	t.Run("empty bytes", func(t *testing.T) {
		qv, err := qvalueFromChangefeedJSON(field(types.QValueKindBytes), json.RawMessage(`"\\x"`))
		require.NoError(t, err)
		require.Empty(t, qv.(types.QValueBytes).Val)
	})

	t.Run("bytes without hex prefix fail instead of reinterpreting text", func(t *testing.T) {
		// BYTES emissions always carry the \x prefix (verified live); bare
		// text means the column type drifted and must not be guessed at
		_, err := qvalueFromChangefeedJSON(field(types.QValueKindBytes), json.RawMessage(`"plain text"`))
		require.ErrorContains(t, err, "hex byte encoding")
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
	// non-public schemas: full_table_name emits db.schema.table without any
	// quoting, verified live on v25.4.13 and v26.2.5
	state.indexSource("defaultdb.app_data.orders", "app_data.orders")
	state.indexSource("defaultdb.Mixed Schema.Order Items", `"Mixed Schema"."Order Items"`)
	state.finishIndexing()

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
		{"non-public schema", "defaultdb.app_data.orders", "app_data.orders", true},
		{"mixed case schema with spaces", "defaultdb.Mixed Schema.Order Items", `"Mixed Schema"."Order Items"`, true},
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

func TestChangefeedTableRoutingCaseCollision(t *testing.T) {
	// public.data and public."Data" collide when lowercased; each emitted name
	// must route to its own source regardless of registration order
	for _, order := range [][2][2]string{
		{{"defaultdb.public.data", "public.data"}, {"defaultdb.public.Data", `public."Data"`}},
		{{"defaultdb.public.Data", `public."Data"`}, {"defaultdb.public.data", "public.data"}},
	} {
		state := &changefeedPullState{sourceByEmitted: make(map[string]string)}
		for _, pair := range order {
			state.indexSource(pair[0], pair[1])
		}
		state.finishIndexing()

		source, ok := state.lookupSource("defaultdb.public.data")
		require.True(t, ok)
		require.Equal(t, "public.data", source)
		source, ok = state.lookupSource("defaultdb.public.Data")
		require.True(t, ok)
		require.Equal(t, `public."Data"`, source)
		source, ok = state.lookupSource(`defaultdb.public."Data"`)
		require.True(t, ok)
		require.Equal(t, `public."Data"`, source)
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
		seenSinceResolved: make(map[changefeedDedupKey]crdbHLC),
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
		require.False(t, state.alreadySeen(row.table, []byte(row.key), row.updated))
		ts, err := parseHLC(row.updated)
		require.NoError(t, err)
		state.markSeen(logger, row.table, []byte(row.key), row.updated, ts)
	}
	// a reconnected session replays the same messages: none may emit again
	for _, row := range rows {
		require.True(t, state.alreadySeen(row.table, []byte(row.key), row.updated))
	}

	om, _ := newMetricsTestOtelManager(t)
	c := &CockroachDBConnector{logger: logger}

	// a resolved timestamp below some entries prunes only what it covers: the
	// newer rows can replay after a reconnect at that cursor and must stay
	// deduplicated (observed live on a multi-node cluster where resolved
	// timestamps lag behind delivered rows)
	done, err := c.handleResolved(t.Context(), om, state, "1784758975071714290.0000000000")
	require.NoError(t, err)
	require.False(t, done)
	require.True(t, state.alreadySeen(rows[2].table, []byte(rows[2].key), rows[2].updated),
		"entries above the resolved timestamp must survive the prune")
	require.False(t, state.alreadySeen(rows[0].table, []byte(rows[0].key), rows[0].updated))
	require.False(t, state.alreadySeen(rows[1].table, []byte(rows[1].key), rows[1].updated))

	// a resolved timestamp past every entry clears the set: the rows are new
	// deliveries again
	done, err = c.handleResolved(t.Context(), om, state, "1784758975071714400.0000000000")
	require.NoError(t, err)
	require.False(t, done)
	for _, row := range rows {
		require.False(t, state.alreadySeen(row.table, []byte(row.key), row.updated))
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

	// TRUNCATE and DROP of a watched table are terminal: the feed can never
	// resume at the same cursor. Message strings captured live on v25.4.13
	// and v26.2.5 (SQLSTATE XXUUU in every case).
	truncErr := &pgconn.PgError{Code: pgerrcode.InternalError, Message: `"probe_trunc" was truncated`}
	require.True(t, isTableTruncatedError(truncErr))
	require.True(t, isPermanentChangefeedError(truncErr))
	require.True(t, isPermanentChangefeedError(fmt.Errorf("changefeed failed: %w", truncErr)))

	dropErr := &pgconn.PgError{Code: pgerrcode.InternalError, Message: `"defaultdb.public.probe_drop" was dropped`}
	require.True(t, isTableDroppedError(dropErr))
	require.True(t, isPermanentChangefeedError(dropErr))
	descriptorErr := &pgconn.PgError{Code: pgerrcode.InternalError, Message: "descriptor is being dropped"}
	require.True(t, isTableDroppedError(descriptorErr))
	require.True(t, isPermanentChangefeedError(descriptorErr))

	// a watched table whose descriptor cannot be resolved at the cursor
	// (created or recreated after it) can never succeed at that cursor
	targetErr := &pgconn.PgError{
		Code: pgerrcode.InternalError,
		Message: `failed to resolve targets in the CHANGEFEED stmt:` +
			` table "e2e_test.\"CdC Mixed\"" does not exist: supplied backups do not cover requested time`,
	}
	require.True(t, isTargetNotAtCursorError(targetErr))
	require.True(t, isPermanentChangefeedError(targetErr))

	// deterministic client-side failures (row conversion, key decode, routing)
	// are terminal: replaying them cannot succeed
	require.True(t, isPermanentChangefeedError(newTerminalChangefeedError(errors.New("failed to convert insert"))))
	require.True(t, isPermanentChangefeedError(
		fmt.Errorf("session: %w", newTerminalChangefeedError(errors.New("failed to convert insert")))))

	// retryable SQLSTATEs, audited against CockroachDB's client-side error
	// semantics: serialization (40001), statement completion unknown (40003,
	// CockroachDB-specific ambiguous results), deadlock (40P01), class 08
	// connection errors, resource exhaustion (53xxx), query canceled (57014,
	// e.g. server-side CANCEL QUERY), admin shutdown (57P01, node drain
	// during rolling restarts) and unknown internal codes
	retryable := []*pgconn.PgError{
		{Code: pgerrcode.SerializationFailure, Message: "restart transaction"},
		{Code: pgerrcode.StatementCompletionUnknown, Message: "result is ambiguous"},
		{Code: pgerrcode.DeadlockDetected, Message: "deadlock detected"},
		{Code: pgerrcode.ConnectionException, Message: "connection exception"},
		{Code: pgerrcode.ConnectionDoesNotExist, Message: "connection does not exist"},
		{Code: pgerrcode.ConnectionFailure, Message: "connection failure"},
		{Code: pgerrcode.InsufficientResources, Message: "insufficient resources"},
		{Code: pgerrcode.DiskFull, Message: "disk full"},
		{Code: pgerrcode.OutOfMemory, Message: "out of memory"},
		{Code: pgerrcode.TooManyConnections, Message: "too many connections"},
		{Code: pgerrcode.QueryCanceled, Message: "query execution canceled"},
		{Code: pgerrcode.AdminShutdown, Message: "server is shutting down"},
		{Code: pgerrcode.InternalError, Message: "descriptor does not exist"},
	}
	for _, pgErr := range retryable {
		require.False(t, isPermanentChangefeedError(pgErr), "SQLSTATE %s should be retryable", pgErr.Code)
		require.False(t, isPermanentChangefeedError(fmt.Errorf("wrapped: %w", pgErr)),
			"wrapped SQLSTATE %s should be retryable", pgErr.Code)
	}

	// non-PgError fallback keeps the substring checks
	require.True(t, isPermanentChangefeedError(errors.New(`relation "public.users" does not exist`)))
	require.True(t, isPermanentChangefeedError(errors.New(
		"rangefeeds require the kv.rangefeed.enabled setting. See ...")))
	require.True(t, isPermanentChangefeedError(errors.New(`"probe_trunc" was truncated`)))
	require.True(t, isPermanentChangefeedError(errors.New(`"defaultdb.public.probe_drop" was dropped`)))

	require.False(t, isCursorTooOldError(nil))
	require.False(t, isTableTruncatedError(nil))
	require.False(t, isTableDroppedError(nil))
	require.False(t, isPermanentChangefeedError(nil))
	require.False(t, isPermanentChangefeedError(errors.New("unexpected EOF")))
	require.False(t, isPermanentChangefeedError(errors.New("connection reset by peer")))
}

func TestClassifySessionEnd(t *testing.T) {
	streamErr := errors.New("stream broke")

	t.Run("parent cancellation is an error, not a watchdog completion", func(t *testing.T) {
		parent, cancelParent := context.WithCancel(t.Context())
		queryCtx, cancelQuery := context.WithCancelCause(parent)
		defer cancelQuery(nil)
		cancelParent()
		// the child context is canceled too, but the batch must NOT complete
		done, err := classifySessionEnd(parent, queryCtx, context.Canceled, true)
		require.False(t, done)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("watchdog completes the batch when records exist", func(t *testing.T) {
		queryCtx, cancelQuery := context.WithCancelCause(t.Context())
		cancelQuery(errChangefeedWatchdog)
		done, err := classifySessionEnd(t.Context(), queryCtx, context.Canceled, true)
		require.True(t, done)
		require.NoError(t, err)
	})

	t.Run("watchdog without records asks for a reconnect", func(t *testing.T) {
		queryCtx, cancelQuery := context.WithCancelCause(t.Context())
		cancelQuery(errChangefeedWatchdog)
		done, err := classifySessionEnd(t.Context(), queryCtx, context.Canceled, false)
		require.False(t, done)
		require.NoError(t, err)
	})

	t.Run("plain cancel of the query context is not treated as a watchdog", func(t *testing.T) {
		queryCtx, cancelQuery := context.WithCancelCause(t.Context())
		cancelQuery(nil)
		done, err := classifySessionEnd(t.Context(), queryCtx, streamErr, true)
		require.False(t, done)
		require.ErrorIs(t, err, streamErr)
	})

	t.Run("stream error propagates", func(t *testing.T) {
		queryCtx, cancelQuery := context.WithCancelCause(t.Context())
		defer cancelQuery(nil)
		done, err := classifySessionEnd(t.Context(), queryCtx, streamErr, false)
		require.False(t, done)
		require.ErrorIs(t, err, streamErr)
	})

	t.Run("clean end without error is unexpected", func(t *testing.T) {
		queryCtx, cancelQuery := context.WithCancelCause(t.Context())
		defer cancelQuery(nil)
		done, err := classifySessionEnd(t.Context(), queryCtx, nil, false)
		require.False(t, done)
		require.ErrorContains(t, err, "ended unexpectedly")
	})
}

func TestSplitExtendedYear(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		rest     string
		year     int
		extended bool
	}{
		{"normal date", "2024-01-02", "2024-01-02", 0, false},
		{"normal timestamp", "2024-01-02T03:04:05", "2024-01-02T03:04:05", 0, false},
		{"BC date", "1192-09-04 BC", "2000-09-04", -1191, true},
		{"seven digit year date", "5874000-01-01", "2000-01-01", 5874000, true},
		{"five digit year timestamp", "10000-01-01T05:06:07Z", "2000-01-01T05:06:07Z", 10000, true},
		{"negative astronomical year", "-1191-09-04T12:30:00Z", "2000-09-04T12:30:00Z", -1191, true},
		{"not a date", "not a date", "not a date", 0, false},
		{"bare number", "12345", "12345", 0, false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rest, year, extended := splitExtendedYear(tc.input)
			require.Equal(t, tc.extended, extended)
			require.Equal(t, tc.rest, rest)
			if extended {
				require.Equal(t, tc.year, year)
			}
		})
	}
}

// newProcessRowTestState builds a single-table pull state for exercising
// processChangefeedRow directly.
func newProcessRowTestState() *changefeedPullState {
	state := &changefeedPullState{
		req: &model.PullRecordsRequest[model.RecordItems]{
			FlowJobName: "test_mirror",
			TableNameMapping: map[string]model.NameAndExclude{
				"public.users": model.NewNameAndExclude("users_dst", nil),
			},
			RecordStream: model.NewCDCStream[model.RecordItems](32),
			IdleTimeout:  time.Minute,
			MaxBatchSize: 100,
		},
		sourceByEmitted:   make(map[string]string),
		schemas:           make(map[string]*changefeedTableSchema),
		seenSinceResolved: make(map[changefeedDedupKey]crdbHLC),
	}
	state.indexSource("defaultdb.public.users", "public.users")
	state.finishIndexing()
	state.schemas["public.users"] = newChangefeedTableSchema(&protos.TableSchema{
		Columns:           []*protos.FieldDescription{{Name: "id", Type: string(types.QValueKindInt64)}},
		PrimaryKeyColumns: []string{"id"},
	})
	return state
}

func TestProcessChangefeedRowFailureDoesNotMarkSeen(t *testing.T) {
	ctx := t.Context()
	om, _ := newMetricsTestOtelManager(t)
	c := &CockroachDBConnector{logger: log.NewStructuredLogger(slog.Default())}
	state := newProcessRowTestState()

	updated := fmt.Sprintf("%d.0000000000", time.Now().UnixNano())
	poisoned := &changefeedEnvelope{
		After:   map[string]json.RawMessage{"id": json.RawMessage(`"not an int"`)},
		Updated: updated,
	}
	err := c.processChangefeedRow(ctx, om, state, "defaultdb.public.users", []byte("[1]"), poisoned)
	require.Error(t, err)
	require.True(t, isPermanentChangefeedError(err), "conversion failures recur on replay and must be permanent")
	// the failed message must not be in the dedup set: a replay after a
	// reconnect has to process it again instead of silently skipping it
	require.False(t, state.alreadySeen("defaultdb.public.users", []byte("[1]"), updated))
	require.Zero(t, state.recordCount)

	// the replay delivers a processable version: it must go through
	valid := &changefeedEnvelope{
		After:   map[string]json.RawMessage{"id": json.RawMessage(`1`)},
		Updated: updated,
	}
	require.NoError(t, c.processChangefeedRow(ctx, om, state, "defaultdb.public.users", []byte("[1]"), valid))
	require.Equal(t, uint32(1), state.recordCount)
	require.True(t, state.alreadySeen("defaultdb.public.users", []byte("[1]"), updated))

	// a second replay of the processed message is deduplicated
	require.NoError(t, c.processChangefeedRow(ctx, om, state, "defaultdb.public.users", []byte("[1]"), valid))
	require.Equal(t, uint32(1), state.recordCount)
}

func TestProcessChangefeedRowDropPathsFailLoudly(t *testing.T) {
	ctx := t.Context()
	om, _ := newMetricsTestOtelManager(t)
	c := &CockroachDBConnector{logger: log.NewStructuredLogger(slog.Default())}
	updated := fmt.Sprintf("%d.0000000000", time.Now().UnixNano())

	t.Run("unmapped emitted table", func(t *testing.T) {
		state := newProcessRowTestState()
		envelope := &changefeedEnvelope{
			After:   map[string]json.RawMessage{"id": json.RawMessage(`1`)},
			Updated: updated,
		}
		err := c.processChangefeedRow(ctx, om, state, "defaultdb.public.renamed", []byte("[1]"), envelope)
		require.ErrorContains(t, err, "maps to no source table")
		require.True(t, isPermanentChangefeedError(err))
	})

	t.Run("missing cached schema", func(t *testing.T) {
		state := newProcessRowTestState()
		delete(state.schemas, "public.users")
		envelope := &changefeedEnvelope{
			After:   map[string]json.RawMessage{"id": json.RawMessage(`1`)},
			Updated: updated,
		}
		err := c.processChangefeedRow(ctx, om, state, "defaultdb.public.users", []byte("[1]"), envelope)
		require.ErrorContains(t, err, "no cached schema")
		require.True(t, isPermanentChangefeedError(err))
	})

	t.Run("delete with unusable key", func(t *testing.T) {
		state := newProcessRowTestState()
		envelope := &changefeedEnvelope{Updated: updated}
		err := c.processChangefeedRow(ctx, om, state, "defaultdb.public.users", []byte(`[1, 2]`), envelope)
		require.ErrorContains(t, err, "key is unusable")
		require.True(t, isPermanentChangefeedError(err))
	})
}

func TestChangefeedMetricsRecording(t *testing.T) {
	ctx := t.Context()
	om, reader := newMetricsTestOtelManager(t)
	c := &CockroachDBConnector{logger: log.NewStructuredLogger(slog.Default())}

	state := &changefeedPullState{
		req: &model.PullRecordsRequest[model.RecordItems]{
			TableNameMapping: map[string]model.NameAndExclude{
				"public.users": model.NewNameAndExclude("users_dst", nil),
			},
			RecordStream: model.NewCDCStream[model.RecordItems](32),
			IdleTimeout:  time.Minute,
			MaxBatchSize: 100,
		},
		sourceByEmitted:   make(map[string]string),
		schemas:           make(map[string]*changefeedTableSchema),
		seenSinceResolved: make(map[changefeedDedupKey]crdbHLC),
	}
	state.indexSource("defaultdb.public.users", "public.users")
	state.finishIndexing()
	state.schemas["public.users"] = newChangefeedTableSchema(&protos.TableSchema{
		Columns:           []*protos.FieldDescription{{Name: "id", Type: string(types.QValueKindInt64)}},
		PrimaryKeyColumns: []string{"id"},
	})

	updated := fmt.Sprintf("%d.0000000000", time.Now().Add(-time.Minute).UnixNano())
	envelope := &changefeedEnvelope{
		After:   map[string]json.RawMessage{"id": json.RawMessage("1")},
		Updated: updated,
	}
	require.NoError(t, c.processChangefeedRow(ctx, om, state, "defaultdb.public.users", []byte("[1]"), envelope))

	recordsMetric, found := collectMetric(t, ctx, reader, otel_metrics.CockroachDBRecordsReceivedName)
	require.True(t, found, "records received counter should be collected")
	sum, ok := recordsMetric.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 1)
	require.Equal(t, int64(1), sum.DataPoints[0].Value)

	resolved := fmt.Sprintf("%d.0000000000", time.Now().Add(-90*time.Second).UnixNano())
	done, err := c.handleResolved(ctx, om, state, resolved)
	require.NoError(t, err)
	require.False(t, done)

	lagMetric, found := collectMetric(t, ctx, reader, otel_metrics.CockroachDBResolvedLagGaugeName)
	require.True(t, found, "resolved lag gauge should be collected")
	gauge, ok := lagMetric.Data.(metricdata.Gauge[float64])
	require.True(t, ok)
	require.Len(t, gauge.DataPoints, 1)
	require.InDelta(t, 90, gauge.DataPoints[0].Value, 30, "lag should be about 90 seconds behind wall clock")
}
