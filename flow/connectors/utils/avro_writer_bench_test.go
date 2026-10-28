//nolint:gosec // deterministic test-only random data
package utils

import (
	"io"
	"log/slog"
	"math/big"
	"math/rand/v2"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hamba/avro/v2/ocf"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

const benchAvroNumRecords = 10_000

var benchAvroEnv = map[string]string{
	"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": "false",
	"PEERDB_CLICKHOUSE_BINARY_FORMAT":               "raw",
}

// benchAvroSchema mimics a typical ClickHouse destination table
var benchAvroSchema = types.QRecordSchema{Fields: []types.QField{
	{Name: "id", Type: types.QValueKindInt64},
	{Name: "name", Type: types.QValueKindString, Nullable: true},
	{Name: "status", Type: types.QValueKindEnum},
	{Name: "qty", Type: types.QValueKindInt32, Nullable: true},
	{Name: "score", Type: types.QValueKindFloat64, Nullable: true},
	{Name: "active", Type: types.QValueKindBoolean},
	{Name: "created_at", Type: types.QValueKindTimestampTZ},
	{Name: "updated_at", Type: types.QValueKindTimestamp, Nullable: true},
	{Name: "birthday", Type: types.QValueKindDate, Nullable: true},
	{Name: "amount", Type: types.QValueKindNumeric, Nullable: true, Precision: 18, Scale: 4},
	{Name: "big_amount", Type: types.QValueKindNumeric, Nullable: true},
	{Name: "ext_id", Type: types.QValueKindUUID, Nullable: true},
	{Name: "payload", Type: types.QValueKindBytes, Nullable: true},
	{Name: "attrs", Type: types.QValueKindJSON, Nullable: true},
	{Name: "tags", Type: types.QValueKindArrayString, Nullable: true},
	{Name: "counters", Type: types.QValueKindArrayInt64, Nullable: true},
}}

func benchAvroRecords() [][]types.QValue {
	rng := rand.New(rand.NewPCG(1, 2))
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	records := make([][]types.QValue, benchAvroNumRecords)
	for i := range records {
		ts := base.Add(time.Duration(rng.Int64N(int64(365 * 24 * time.Hour))))
		var name types.QValue = types.QValueNull(types.QValueKindString)
		if rng.IntN(10) != 0 {
			name = types.QValueString{Val: "customer-" + strconv.FormatInt(rng.Int64N(1_000_000), 10)}
		}
		payload := make([]byte, 16+rng.IntN(48))
		for j := range payload {
			payload[j] = byte(rng.IntN(256))
		}
		records[i] = []types.QValue{
			types.QValueInt64{Val: int64(i)},
			name,
			types.QValueEnum{Val: []string{"new", "paid", "shipped", "cancelled"}[rng.IntN(4)]},
			types.QValueInt32{Val: rng.Int32N(10_000)},
			types.QValueFloat64{Val: rng.Float64() * 1000},
			types.QValueBoolean{Val: rng.IntN(2) == 0},
			types.QValueTimestampTZ{Val: ts},
			types.QValueTimestamp{Val: ts.Add(time.Hour)},
			types.QValueDate{Val: ts.Truncate(24 * time.Hour)},
			types.QValueNumeric{Val: decimal.New(rng.Int64N(1_000_000_000), -4)},
			types.QValueNumeric{Val: decimal.NewFromBigInt(new(big.Int).SetUint64(rng.Uint64()), -6)},
			types.QValueUUID{Val: uuid.UUID(rngBytes16(rng))},
			types.QValueBytes{Val: payload},
			types.QValueJSON{Val: `{"source":"web","visits":` + strconv.Itoa(rng.IntN(100)) + `,"flags":["a","b"]}`},
			types.QValueArrayString{Val: []string{"alpha", "beta", "gamma"}[:1+rng.IntN(3)]},
			types.QValueArrayInt64{Val: []int64{rng.Int64(), rng.Int64(), rng.Int64()}},
		}
	}
	return records
}

func rngBytes16(rng *rand.Rand) [16]byte {
	var b [16]byte
	for i := range b {
		b[i] = byte(rng.IntN(256))
	}
	return b
}

// BenchmarkWriteOCF measures QRecord to Avro conversion plus OCF encoding for a ClickHouse destination,
// including size tracking.
func BenchmarkWriteOCF(b *testing.B) {
	avroSchema, err := model.GetAvroSchemaDefinition(b.Context(), benchAvroEnv, "bench_table",
		benchAvroSchema, protos.DBType_CLICKHOUSE, model.ConstructColumnNameAvroFieldMap(benchAvroSchema.Fields))
	require.NoError(b, err)
	records := benchAvroRecords()

	// keep WriteOCF progress logs out of the measurement and the benchmark output
	defaultLogger := slog.Default()
	slog.SetDefault(slog.New(slog.DiscardHandler))
	b.Cleanup(func() { slog.SetDefault(defaultLogger) })

	for _, codec := range []ocf.CodecName{ocf.ZStandard, ocf.Null} {
		b.Run(string(codec), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				stream := model.NewQRecordStream(len(records))
				for _, record := range records {
					stream.Records <- record
				}
				stream.Close(nil)
				b.StartTimer()

				writer := NewPeerDBOCFWriter(stream, avroSchema, codec, protos.DBType_CLICKHOUSE, &model.QRecordAvroChunkSizeTracker{})
				numRows, err := writer.WriteOCF(b.Context(), benchAvroEnv, io.Discard, nil, nil)
				require.NoError(b, err)
				require.Equal(b, int64(len(records)), numRows)
			}
		})
	}
}
