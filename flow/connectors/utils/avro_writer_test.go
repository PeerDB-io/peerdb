package utils

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"github.com/twmb/avro/ocf"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func writeTestOCF(t *testing.T, schema types.QRecordSchema, records [][]types.QValue) []byte {
	t.Helper()
	avroSchema, err := model.GetAvroSchemaDefinition(t.Context(), benchAvroEnv, "test_table",
		schema, protos.DBType_CLICKHOUSE, model.ConstructColumnNameAvroFieldMap(schema.Fields))
	require.NoError(t, err)

	stream := model.NewQRecordStream(len(records))
	stream.SetSchema(schema)
	for _, record := range records {
		stream.Records <- record
	}
	stream.Close(nil)

	var buf bytes.Buffer
	writer := NewPeerDBOCFWriter(stream, avroSchema, protos.AvroCodec_ZStandard, protos.DBType_CLICKHOUSE, nil)
	numRows, err := writer.WriteOCF(t.Context(), benchAvroEnv, &buf, nil, nil)
	require.NoError(t, err)
	require.Equal(t, int64(len(records)), numRows)
	return buf.Bytes()
}

func readTestOCF(t *testing.T, data []byte) []map[string]any {
	t.Helper()
	reader, err := ocf.NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer reader.Close()

	var records []map[string]any
	for {
		var record map[string]any
		if err := reader.Decode(&record); errors.Is(err, io.EOF) {
			return records
		} else {
			require.NoError(t, err)
		}
		records = append(records, record)
	}
}

// A typed nil slice in a nullable array column must be written as null, and an empty slice as an empty array
func TestWriteOCFNullableArrays(t *testing.T) {
	schema := types.QRecordSchema{Fields: []types.QField{
		{Name: "a_int16", Type: types.QValueKindArrayInt16, Nullable: true},
		{Name: "a_int32", Type: types.QValueKindArrayInt32, Nullable: true},
		{Name: "a_int64", Type: types.QValueKindArrayInt64, Nullable: true},
		{Name: "a_float32", Type: types.QValueKindArrayFloat32, Nullable: true},
		{Name: "a_float64", Type: types.QValueKindArrayFloat64, Nullable: true},
		{Name: "a_bool", Type: types.QValueKindArrayBoolean, Nullable: true},
		{Name: "a_string", Type: types.QValueKindArrayString, Nullable: true},
		{Name: "a_timestamp", Type: types.QValueKindArrayTimestamp, Nullable: true},
		{Name: "a_date", Type: types.QValueKindArrayDate, Nullable: true},
		{Name: "a_time", Type: types.QValueKindArrayTime, Nullable: true},
		{Name: "a_uuid", Type: types.QValueKindArrayUUID, Nullable: true},
		{Name: "a_numeric", Type: types.QValueKindArrayNumeric, Nullable: true, Precision: 18, Scale: 4},
	}}
	nilRecord := []types.QValue{
		types.QValueArrayInt16{Val: []int16(nil)},
		types.QValueArrayInt32{Val: []int32(nil)},
		types.QValueArrayInt64{Val: []int64(nil)},
		types.QValueArrayFloat32{Val: []float32(nil)},
		types.QValueArrayFloat64{Val: []float64(nil)},
		types.QValueArrayBoolean{Val: []bool(nil)},
		types.QValueArrayString{Val: []string(nil)},
		types.QValueArrayTimestamp{Val: []time.Time(nil)},
		types.QValueArrayDate{Val: []time.Time(nil)},
		types.QValueArrayTime{Val: []time.Duration(nil)},
		types.QValueArrayUUID{Val: []uuid.UUID(nil)},
		types.QValueArrayNumeric{Val: []decimal.Decimal(nil), Precision: 18, Scale: 4},
	}
	emptyRecord := []types.QValue{
		types.QValueArrayInt16{Val: []int16{}},
		types.QValueArrayInt32{Val: []int32{}},
		types.QValueArrayInt64{Val: []int64{}},
		types.QValueArrayFloat32{Val: []float32{}},
		types.QValueArrayFloat64{Val: []float64{}},
		types.QValueArrayBoolean{Val: []bool{}},
		types.QValueArrayString{Val: []string{}},
		types.QValueArrayTimestamp{Val: []time.Time{}},
		types.QValueArrayDate{Val: []time.Time{}},
		types.QValueArrayTime{Val: []time.Duration{}},
		types.QValueArrayUUID{Val: []uuid.UUID{}},
		types.QValueArrayNumeric{Val: []decimal.Decimal{}, Precision: 18, Scale: 4},
	}

	records := readTestOCF(t, writeTestOCF(t, schema, [][]types.QValue{nilRecord, emptyRecord}))
	require.Len(t, records, 2)
	avroFieldNames := model.ConstructColumnNameAvroFieldMap(schema.Fields)
	for _, field := range schema.Fields {
		name := avroFieldNames[field.Name]
		require.Contains(t, records[0], name)
		if field.Type == types.QValueKindArrayNumeric {
			// FIXME: https://linear.app/clickhouse/issue/DBI-1307
			// processArrayNumeric lacks the nil guard the other array helpers have,
			// so a typed nil slice is written as an empty array instead of null.
			// We should align this with the behavior of the other array helpers but making sure
			// existing functionality doesn't rely on the current behavior.
			require.Equal(t, []any{}, records[0][name], "typed nil slice in %s", field.Name)
		} else {
			require.Nil(t, records[0][name], "typed nil slice in %s", field.Name)
		}
		require.Equal(t, []any{}, records[1][name], "empty slice in %s", field.Name)
	}
}

// ClickHouse loads the staged file even for an empty partition, so a file without records must still be valid Avro
func TestWriteOCFZeroRecords(t *testing.T) {
	data := writeTestOCF(t, benchAvroSchema, nil)

	reader, err := ocf.NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer reader.Close()
	var record map[string]any
	require.ErrorIs(t, reader.Decode(&record), io.EOF)
}
