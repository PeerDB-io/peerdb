//nolint:gosec // test-only golden file
package model

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

var updateAvroSchemaGolden = flag.Bool("update-avro-schema-golden", false, "rewrite testdata/avro_schema_golden.json")

const avroSchemaGoldenPath = "testdata/avro_schema_golden.json"

var avroSchemaGoldenKinds = []types.QValueKind{
	types.QValueKindInvalid,
	types.QValueKindFloat32,
	types.QValueKindFloat64,
	types.QValueKindInt8,
	types.QValueKindInt16,
	types.QValueKindInt32,
	types.QValueKindInt64,
	types.QValueKindInt256,
	types.QValueKindUInt8,
	types.QValueKindUInt16,
	types.QValueKindUInt32,
	types.QValueKindUInt64,
	types.QValueKindUInt256,
	types.QValueKindBoolean,
	types.QValueKindQChar,
	types.QValueKindString,
	types.QValueKindEnum,
	types.QValueKindUint16Enum,
	types.QValueKindUint64Set,
	types.QValueKindTimestamp,
	types.QValueKindTimestampTZ,
	types.QValueKindDate,
	types.QValueKindTime,
	types.QValueKindTimeTZ,
	types.QValueKindInterval,
	types.QValueKindBytes,
	types.QValueKindUUID,
	types.QValueKindJSON,
	types.QValueKindJSONB,
	types.QValueKindHStore,
	types.QValueKindGeography,
	types.QValueKindGeometry,
	types.QValueKindPoint,
	types.QValueKindCIDR,
	types.QValueKindINET,
	types.QValueKindMacaddr,
	types.QValueKindArrayFloat32,
	types.QValueKindArrayFloat64,
	types.QValueKindArrayInt16,
	types.QValueKindArrayInt32,
	types.QValueKindArrayInt64,
	types.QValueKindArrayString,
	types.QValueKindArrayEnum,
	types.QValueKindArrayDate,
	types.QValueKindArrayTime,
	types.QValueKindArrayInterval,
	types.QValueKindArrayTimestamp,
	types.QValueKindArrayTimestampTZ,
	types.QValueKindArrayBoolean,
	types.QValueKindArrayJSON,
	types.QValueKindArrayJSONB,
	types.QValueKindArrayUUID,
}

// precision/scale combinations: unbounded, regular, ClickHouse max, beyond ClickHouse max
var avroSchemaGoldenNumerics = [][2]int16{{0, 0}, {18, 4}, {76, 38}, {90, 10}}

// avroSchemaGoldenRecordSchema has a nullable and a non-nullable column for every kind,
// so named types such as int256 appear more than once in the record.
func avroSchemaGoldenRecordSchema() types.QRecordSchema {
	var fields []types.QField
	for _, nullable := range []bool{false, true} {
		suffix := "nn"
		if nullable {
			suffix = "n"
		}
		for _, kind := range avroSchemaGoldenKinds {
			fields = append(fields, types.QField{Name: fmt.Sprintf("%s_%s", kind, suffix), Type: kind, Nullable: nullable})
		}
		for _, kind := range []types.QValueKind{types.QValueKindNumeric, types.QValueKindArrayNumeric} {
			for _, ps := range avroSchemaGoldenNumerics {
				fields = append(fields, types.QField{
					Name:      fmt.Sprintf("%s_%d_%d_%s", kind, ps[0], ps[1], suffix),
					Type:      kind,
					Nullable:  nullable,
					Precision: ps[0],
					Scale:     ps[1],
				})
			}
		}
	}
	return types.QRecordSchema{Fields: fields}
}

// TestAvroSchemaGolden pins the Avro schema JSON written into OCF headers for every destination,
// destinations read it back to infer column types.
func TestAvroSchemaGolden(t *testing.T) {
	schema := avroSchemaGoldenRecordSchema()
	cases := []struct {
		name      string
		targetDWH protos.DBType
		env       map[string]string
	}{
		{"clickhouse", protos.DBType_CLICKHOUSE, map[string]string{
			"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": "false",
			"PEERDB_CLICKHOUSE_BINARY_FORMAT":               "raw",
		}},
		{"clickhouse_numeric_as_string_base64", protos.DBType_CLICKHOUSE, map[string]string{
			"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": "true",
			"PEERDB_CLICKHOUSE_BINARY_FORMAT":               "base64",
		}},
		{"snowflake", protos.DBType_SNOWFLAKE, map[string]string{
			"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": "false",
			"PEERDB_CLICKHOUSE_BINARY_FORMAT":               "raw",
		}},
		{"bigquery", protos.DBType_BIGQUERY, map[string]string{
			"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": "false",
			"PEERDB_CLICKHOUSE_BINARY_FORMAT":               "raw",
		}},
		{"s3", protos.DBType_S3, map[string]string{
			"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": "false",
			"PEERDB_CLICKHOUSE_BINARY_FORMAT":               "raw",
		}},
	}

	got := make(map[string]json.RawMessage, len(cases))
	for _, tc := range cases {
		var avroNameMap map[string]string
		if tc.targetDWH == protos.DBType_CLICKHOUSE {
			avroNameMap = ConstructColumnNameAvroFieldMap(schema.Fields)
		}
		avroSchema, err := GetAvroSchemaDefinition(t.Context(), tc.env, "golden_dst_table", schema, tc.targetDWH, avroNameMap)
		require.NoError(t, err, tc.name)
		got[tc.name] = json.RawMessage(avroSchema.Schema.String())
	}

	if *updateAvroSchemaGolden {
		out, err := json.MarshalIndent(got, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(avroSchemaGoldenPath, append(out, '\n'), 0o644))
		return
	}

	raw, err := os.ReadFile(avroSchemaGoldenPath)
	require.NoError(t, err, "run with -update-avro-schema-golden to create it")
	var want map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &want))
	require.Len(t, got, len(want))
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.JSONEq(t, string(want[tc.name]), string(got[tc.name]))
		})
	}
}
