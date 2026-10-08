package utils

import (
	"bytes"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/internal/benchfixtures"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestRawTableAvroCompatibility(t *testing.T) {
	ctx := t.Context()
	env := map[string]string{"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": "false"}
	schema, err := model.GetAvroSchemaDefinition(ctx, env, "raw", rawTableSchema(), protos.DBType_CLICKHOUSE, nil)
	require.NoError(t, err)
	names := make([]string, len(schema.Fields))
	for i, field := range schema.Schema.Fields() {
		names[i] = field.Name()
	}
	converter, err := model.NewQRecordAvroConverter(ctx, env, schema, protos.DBType_CLICKHOUSE, names, internal.LoggerFromCtx(ctx))
	require.NoError(t, err)
	truncator := model.NewStreamNumericTruncator([]*protos.TableMapping{
		{DestinationTableIdentifier: "first"}, {DestinationTableIdentifier: "second"},
	}, nil)
	var oldOCF, newOCF bytes.Buffer
	opts := []ocf.EncoderFunc{ocf.WithCodec(ocf.ZStandard), ocf.WithBlockLength(2), ocf.WithSyncBlock([16]byte{1})}
	oldWriter, err := ocf.NewEncoderWithSchema(schema.Schema, &oldOCF, opts...)
	require.NoError(t, err)
	newWriter, err := ocf.NewEncoderWithSchema(schema.Schema, &newOCF, opts...)
	require.NoError(t, err)
	// OCF header metadata is a map with nondeterministic ordering. Compare the
	// compressed data blocks; both writers above use the exact same schema.
	oldOCF.Reset()
	newOCF.Reset()
	for i := range 12 {
		items, old := benchfixtures.Record(i), benchfixtures.Record(i+1)
		items.AddColumn(benchfixtures.Names()[19], types.QValueString{Val: "<>&雪\x00\xff"})
		dst := []string{"first", "second"}[i%2]
		for _, record := range []model.Record[model.RecordItems]{
			&model.InsertRecord[model.RecordItems]{Items: items, DestinationTableName: dst},
			&model.UpdateRecord[model.RecordItems]{
				NewItems: items, OldItems: old, DestinationTableName: dst,
				UnchangedToastColumns: map[string]struct{}{"toast": {}},
			},
			&model.DeleteRecord[model.RecordItems]{
				Items: items, DestinationTableName: dst, UnchangedToastColumns: map[string]struct{}{"toast": {}},
			},
			&model.MessageRecord[model.RecordItems]{},
		} {
			batch := []int64{0, -1, math.MaxInt64, math.MinInt64}[i%4]
			row, err := recordToRawTableRow(batch, record, protos.DBType_CLICKHOUSE, false, truncator)
			require.NoError(t, err)
			legacy, err := legacyRecordToQRecord(batch, record, protos.DBType_CLICKHOUSE, false, truncator)
			require.NoError(t, err)
			if row == nil {
				require.Nil(t, legacy)
				continue
			}
			// Both paths generate fresh metadata. Hold only those two fields constant.
			legacy[0] = types.QValueUUID{Val: uuid.MustParse(row.UID)}
			legacy[1] = types.QValueInt64{Val: row.Timestamp}

			converted := row.QRecord()
			// JSON member order is unspecified. Check payload content, then
			// give both Avro encoders identical payload bytes for comparison.
			for _, idx := range []int{3, 5} {
				want, got := legacy[idx].Value().(string), converted[idx].Value().(string)
				if want == "" {
					require.Empty(t, got)
				} else {
					require.JSONEq(t, want, got)
				}
				legacy[idx] = types.QValueString{Val: got}
			}
			require.Equal(t, legacy, converted)
			m, _, err := converter.Convert(ctx, env, legacy, nil, nil, internal.BinaryFormatRaw, false)
			require.NoError(t, err)
			want, err := avro.Marshal(schema.Schema, m)
			require.NoError(t, err)
			got, err := avro.Marshal(schema.Schema, row)
			require.NoError(t, err)
			require.Equal(t, want, got)
			require.NoError(t, oldWriter.Encode(m))
			require.NoError(t, newWriter.Encode(row))
		}
	}
	require.NoError(t, oldWriter.Close())
	require.NoError(t, newWriter.Close())
	require.Equal(t, oldOCF.Bytes(), newOCF.Bytes())
}

// Frozen staged implementation: independent oracle for the envelope refactor.
func legacyRecordToQRecord(
	batchID int64, record model.Record[model.RecordItems], targetDWH protos.DBType, unboundedNumericAsString bool,
	numericTruncator model.StreamNumericTruncator,
) ([]types.QValue, error) {
	var entries [8]types.QValue
	jsonOpts := rawTableJSONOptions(targetDWH)
	switch typedRecord := record.(type) {
	case *model.InsertRecord[model.RecordItems]:
		tableNumericTruncator := numericTruncator.Get(typedRecord.DestinationTableName)
		preprocessedItems := truncateNumerics(
			typedRecord.Items, targetDWH, unboundedNumericAsString, tableNumericTruncator,
		)
		itemsJSON, err := preprocessedItems.ToJSONWithOptions(jsonOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize insert record items to JSON: %w", err)
		}

		entries[3] = types.QValueString{Val: itemsJSON}
		entries[4] = types.QValueInt64{Val: 0}
		entries[5] = types.QValueString{Val: ""}
		entries[7] = types.QValueString{Val: ""}
	case *model.UpdateRecord[model.RecordItems]:
		tableNumericTruncator := numericTruncator.Get(typedRecord.DestinationTableName)
		preprocessedItems := truncateNumerics(
			typedRecord.NewItems, targetDWH, unboundedNumericAsString, tableNumericTruncator,
		)
		newItemsJSON, err := preprocessedItems.ToJSONWithOptions(jsonOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize update record new items to JSON: %w", err)
		}
		oldItemsJSON, err := typedRecord.OldItems.ToJSONWithOptions(jsonOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize update record old items to JSON: %w", err)
		}

		entries[3] = types.QValueString{Val: newItemsJSON}
		entries[4] = types.QValueInt64{Val: 1}
		entries[5] = types.QValueString{Val: oldItemsJSON}
		entries[7] = types.QValueString{Val: KeysToString(typedRecord.UnchangedToastColumns)}

	case *model.DeleteRecord[model.RecordItems]:
		itemsJSON, err := typedRecord.Items.ToJSONWithOptions(jsonOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize delete record items to JSON: %w", err)
		}

		entries[3] = types.QValueString{Val: itemsJSON}
		entries[4] = types.QValueInt64{Val: 2}
		entries[5] = types.QValueString{Val: itemsJSON}
		entries[7] = types.QValueString{Val: KeysToString(typedRecord.UnchangedToastColumns)}

	case *model.MessageRecord[model.RecordItems]:
		return nil, nil

	default:
		return nil, fmt.Errorf("unknown record type: %T", typedRecord)
	}

	entries[0] = types.QValueUUID{Val: uuid.New()}
	entries[1] = types.QValueInt64{Val: time.Now().UnixNano()}
	entries[2] = types.QValueString{Val: record.GetDestinationTableName()}
	entries[6] = types.QValueInt64{Val: batchID}

	return entries[:], nil
}
