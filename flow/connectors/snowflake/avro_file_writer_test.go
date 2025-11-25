package connsnowflake

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hamba/avro/v2/ocf"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// createQValue creates a QValue of the appropriate kind for a given placeholder.
func createQValue(t *testing.T, kind types.QValueKind, placeholder int) types.QValue {
	t.Helper()

	switch kind {
	case types.QValueKindInt16:
		return types.QValueInt16{Val: int16(placeholder)}
	case types.QValueKindInt32:
		return types.QValueInt32{Val: int32(placeholder)}
	case types.QValueKindInt64:
		return types.QValueInt64{Val: int64(placeholder)}
	case types.QValueKindFloat32:
		return types.QValueFloat32{Val: float32(placeholder) / 4.0}
	case types.QValueKindFloat64:
		return types.QValueFloat64{Val: float64(placeholder) / 4.0}
	case types.QValueKindBoolean:
		return types.QValueBoolean{Val: placeholder%2 == 0}
	case types.QValueKindString:
		return types.QValueString{Val: fmt.Sprintf("string%d", placeholder)}
	case types.QValueKindTimestamp:
		return types.QValueTimestamp{Val: time.Now()}
	case types.QValueKindTimestampTZ:
		return types.QValueTimestampTZ{Val: time.Now()}
	case types.QValueKindTime:
		return types.QValueTime{Val: 21600000000}
	case types.QValueKindTimeTZ:
		return types.QValueTimeTZ{Val: 21600000000}
	case types.QValueKindDate:
		return types.QValueDate{Val: time.Now()}
	case types.QValueKindNumeric:
		return types.QValueNumeric{Val: decimal.New(int64(placeholder), 1)}
	case types.QValueKindUUID:
		return types.QValueUUID{Val: uuid.New()} // assuming you have the github.com/google/uuid package
	case types.QValueKindQChar:
		return types.QValueQChar{Val: uint8(48 + placeholder%10)}
		// case types.QValueKindArray:
		// 	value = []int{1, 2, 3} // placeholder array, replace with actual logic
		// case types.QValueKindJSON:
		// 	value = `{"key": "value"}` // placeholder JSON, replace with actual logic
	case types.QValueKindBytes:
		return types.QValueBytes{Val: []byte("sample bytes")} // placeholder bytes, replace with actual logic
	case types.QValueKindArrayNumeric:
		return types.QValueArrayNumeric{Val: []decimal.Decimal{
			decimal.New(int64(placeholder), 1),
		}}
	default:
		require.Failf(t, "unsupported QValueKind", "unsupported QValueKind: %s", kind)
		return types.QValueNull(kind)
	}
}

//nolint:unparam
func generateRecords(
	t *testing.T,
	nullable bool,
	numRows uint32,
	allnulls bool,
) (*model.QRecordStream, types.QRecordSchema) {
	t.Helper()

	allQValueKinds := []types.QValueKind{
		types.QValueKindFloat32,
		types.QValueKindFloat64,
		types.QValueKindInt16,
		types.QValueKindInt32,
		types.QValueKindInt64,
		types.QValueKindBoolean,
		// types.QValueKindArray,
		types.QValueKindString,
		types.QValueKindTimestamp,
		types.QValueKindTimestampTZ,
		types.QValueKindTime,
		types.QValueKindTimeTZ,
		types.QValueKindDate,
		types.QValueKindNumeric,
		types.QValueKindBytes,
		types.QValueKindUUID,
		types.QValueKindQChar,
		// types.QValueKindJSON,
		types.QValueKindArrayNumeric,
	}

	numKinds := len(allQValueKinds)

	schema := types.QRecordSchema{
		Fields: make([]types.QField, numKinds),
	}
	for i, kind := range allQValueKinds {
		schema.Fields[i] = types.QField{
			Name:     string(kind),
			Type:     kind,
			Nullable: nullable,
		}
	}

	// Create sample records
	records := &model.QRecordBatch{
		Schema:  schema,
		Records: make([][]types.QValue, numRows),
	}

	for row := range numRows {
		entries := make([]types.QValue, len(allQValueKinds))

		for i, kind := range allQValueKinds {
			if allnulls {
				entries[i] = types.QValueNull(kind)
			} else {
				entries[i] = createQValue(t, kind, int(row)*i)
			}
		}

		records.Records[row] = entries
	}

	return records.ToQRecordStream(1024), schema
}

func TestWriteRecordsToAvroFileHappyPath(t *testing.T) {
	// Create temporary file
	tmpfile, err := os.CreateTemp(t.TempDir(), "example_*.avro")
	require.NoError(t, err)

	defer os.Remove(tmpfile.Name()) // clean up
	defer tmpfile.Close()           // close file after test ends

	// Define sample data
	records, schema := generateRecords(t, true, 10, false)

	avroSchema, err := model.GetAvroSchemaDefinition(t.Context(), nil, "not_applicable", schema, protos.DBType_SNOWFLAKE, nil)
	require.NoError(t, err)

	t.Logf("[test] avroSchema: %v", avroSchema)

	// Call function
	writer := utils.NewPeerDBOCFWriter(records, avroSchema, ocf.Null, protos.DBType_SNOWFLAKE, nil)
	_, err = writer.WriteRecordsToAvroFile(t.Context(), nil, tmpfile.Name())
	require.NoError(t, err, "expected WriteRecordsToAvroFile to complete without errors")

	// Check file is not empty
	info, err := tmpfile.Stat()
	require.NoError(t, err)
	require.NotZero(t, info.Size(), "expected file to not be empty")
}

func TestWriteRecordsToZstdAvroFileHappyPath(t *testing.T) {
	// Create temporary file
	tmpfile, err := os.CreateTemp(t.TempDir(), "example_*.avro")
	require.NoError(t, err)

	defer os.Remove(tmpfile.Name()) // clean up
	defer tmpfile.Close()           // close file after test ends

	// Define sample data
	records, schema := generateRecords(t, true, 10, false)

	avroSchema, err := model.GetAvroSchemaDefinition(t.Context(), nil, "not_applicable", schema, protos.DBType_SNOWFLAKE, nil)
	require.NoError(t, err)

	t.Logf("[test] avroSchema: %v", avroSchema)

	// Call function
	writer := utils.NewPeerDBOCFWriter(records, avroSchema, ocf.ZStandard, protos.DBType_SNOWFLAKE, nil)
	_, err = writer.WriteRecordsToAvroFile(t.Context(), nil, tmpfile.Name())
	require.NoError(t, err, "expected WriteRecordsToAvroFile to complete without errors")

	// Check file is not empty
	info, err := tmpfile.Stat()
	require.NoError(t, err)
	require.NotZero(t, info.Size(), "expected file to not be empty")
}

func TestWriteRecordsToDeflateAvroFileHappyPath(t *testing.T) {
	// Create temporary file
	tmpfile, err := os.CreateTemp(t.TempDir(), "example_*.avro")
	require.NoError(t, err)

	defer os.Remove(tmpfile.Name()) // clean up
	defer tmpfile.Close()           // close file after test ends

	// Define sample data
	records, schema := generateRecords(t, true, 10, false)

	avroSchema, err := model.GetAvroSchemaDefinition(t.Context(), nil, "not_applicable", schema, protos.DBType_SNOWFLAKE, nil)
	require.NoError(t, err)

	t.Logf("[test] avroSchema: %v", avroSchema)

	// Call function
	writer := utils.NewPeerDBOCFWriter(records, avroSchema, ocf.Deflate, protos.DBType_SNOWFLAKE, nil)
	_, err = writer.WriteRecordsToAvroFile(t.Context(), nil, tmpfile.Name())
	require.NoError(t, err, "expected WriteRecordsToAvroFile to complete without errors")

	// Check file is not empty
	info, err := tmpfile.Stat()
	require.NoError(t, err)
	require.NotZero(t, info.Size(), "expected file to not be empty")
}

func TestWriteRecordsToAvroFileNonNull(t *testing.T) {
	// Create temporary file
	tmpfile, err := os.CreateTemp(t.TempDir(), "example_*.avro")
	require.NoError(t, err)

	defer os.Remove(tmpfile.Name()) // clean up
	defer tmpfile.Close()           // close file after test ends

	records, schema := generateRecords(t, false, 10, false)

	avroSchema, err := model.GetAvroSchemaDefinition(t.Context(), nil, "not_applicable", schema, protos.DBType_SNOWFLAKE, nil)
	require.NoError(t, err)

	t.Logf("[test] avroSchema: %v", avroSchema)

	// Call function
	writer := utils.NewPeerDBOCFWriter(records, avroSchema, ocf.Null, protos.DBType_SNOWFLAKE, nil)
	_, err = writer.WriteRecordsToAvroFile(t.Context(), nil, tmpfile.Name())
	require.NoError(t, err, "expected WriteRecordsToAvroFile to complete without errors")

	// Check file is not empty
	info, err := tmpfile.Stat()
	require.NoError(t, err)
	require.NotZero(t, info.Size(), "expected file to not be empty")
}

func TestWriteRecordsToAvroFileAllNulls(t *testing.T) {
	// Create temporary file
	tmpfile, err := os.CreateTemp(t.TempDir(), "example_*.avro")
	require.NoError(t, err)

	defer os.Remove(tmpfile.Name()) // clean up
	defer tmpfile.Close()           // close file after test ends

	// Define sample data
	records, schema := generateRecords(t, true, 10, true)

	avroSchema, err := model.GetAvroSchemaDefinition(t.Context(), nil, "not_applicable", schema, protos.DBType_SNOWFLAKE, nil)
	require.NoError(t, err)

	t.Logf("[test] avroSchema: %v", avroSchema)

	// Call function
	writer := utils.NewPeerDBOCFWriter(records, avroSchema, ocf.Null, protos.DBType_SNOWFLAKE, nil)
	_, err = writer.WriteRecordsToAvroFile(t.Context(), nil, tmpfile.Name())
	require.NoError(t, err, "expected WriteRecordsToAvroFile to complete without errors")

	// Check file is not empty
	info, err := tmpfile.Stat()
	require.NoError(t, err)
	require.NotZero(t, info.Size(), "expected file to not be empty")
}
