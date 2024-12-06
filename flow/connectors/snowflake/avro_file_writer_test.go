package connsnowflake

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	avro "github.com/PeerDB-io/peer-flow/connectors/utils/avro"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

// createQValue creates a QValue of the appropriate kind for a given placeholder.
func createQValue(t *testing.T, kind qvalue.QValueKind, placeholder int) qvalue.QValue {
	t.Helper()

	switch kind {
	case qvalue.QValueKindInt16:
		return qvalue.QValueInt16{Val: int16(placeholder)}
	case qvalue.QValueKindInt32:
		return qvalue.QValueInt32{Val: int32(placeholder)}
	case qvalue.QValueKindInt64:
		return qvalue.QValueInt64{Val: int64(placeholder)}
	case qvalue.QValueKindFloat32:
		return qvalue.QValueFloat32{Val: float32(placeholder) / 4.0}
	case qvalue.QValueKindFloat64:
		return qvalue.QValueFloat64{Val: float64(placeholder) / 4.0}
	case qvalue.QValueKindBoolean:
		return qvalue.QValueBoolean{Val: placeholder%2 == 0}
	case qvalue.QValueKindString:
		return qvalue.QValueString{Val: fmt.Sprintf("string%d", placeholder)}
	case qvalue.QValueKindTimestamp:
		return qvalue.QValueTimestamp{Val: time.Now()}
	case qvalue.QValueKindTimestampTZ:
		return qvalue.QValueTimestampTZ{Val: time.Now()}
	case qvalue.QValueKindTime:
		return qvalue.QValueTime{Val: time.Now()}
	case qvalue.QValueKindTimeTZ:
		return qvalue.QValueTimeTZ{Val: time.Now()}
	case qvalue.QValueKindDate:
		return qvalue.QValueDate{Val: time.Now()}
	case qvalue.QValueKindNumeric:
		return qvalue.QValueNumeric{Val: decimal.New(int64(placeholder), 1)}
	case qvalue.QValueKindUUID:
		return qvalue.QValueUUID{Val: uuid.New()} // assuming you have the github.com/google/uuid package
	case qvalue.QValueKindQChar:
		return qvalue.QValueQChar{Val: uint8(48 + placeholder%10)} // assuming you have the github.com/google/uuid package
		// case qvalue.QValueKindArray:
		// 	value = []int{1, 2, 3} // placeholder array, replace with actual logic
		// case qvalue.QValueKindStruct:
		// 	value = map[string]interface{}{"key": "value"} // placeholder struct, replace with actual logic
		// case qvalue.QValueKindJSON:
		// 	value = `{"key": "value"}` // placeholder JSON, replace with actual logic
	case qvalue.QValueKindBytes:
		return qvalue.QValueBytes{Val: []byte("sample bytes")} // placeholder bytes, replace with actual logic
	default:
		require.Failf(t, "unsupported QValueKind", "unsupported QValueKind: %s", kind)
		return qvalue.QValueNull(kind)
	}
}

//nolint:unparam
func generateRecords(
	t *testing.T,
	nullable bool,
	numRows uint32,
	allnulls bool,
) (*model.QRecordStream, qvalue.QRecordSchema) {
	t.Helper()

	allQValueKinds := []qvalue.QValueKind{
		qvalue.QValueKindFloat32,
		qvalue.QValueKindFloat64,
		qvalue.QValueKindInt16,
		qvalue.QValueKindInt32,
		qvalue.QValueKindInt64,
		qvalue.QValueKindBoolean,
		// qvalue.QValueKindArray,
		// qvalue.QValueKindStruct,
		qvalue.QValueKindString,
		qvalue.QValueKindTimestamp,
		qvalue.QValueKindTimestampTZ,
		qvalue.QValueKindTime,
		qvalue.QValueKindTimeTZ,
		qvalue.QValueKindDate,
		qvalue.QValueKindNumeric,
		qvalue.QValueKindBytes,
		qvalue.QValueKindUUID,
		qvalue.QValueKindQChar,
		// qvalue.QValueKindJSON,
	}

	numKinds := len(allQValueKinds)

	schema := qvalue.QRecordSchema{
		Fields: make([]qvalue.QField, numKinds),
	}
	for i, kind := range allQValueKinds {
		schema.Fields[i] = qvalue.QField{
			Name:     string(kind),
			Type:     kind,
			Nullable: nullable,
		}
	}

	// Create sample records
	records := &model.QRecordBatch{
		Schema:  schema,
		Records: make([][]qvalue.QValue, numRows),
	}

	for row := range numRows {
		entries := make([]qvalue.QValue, len(allQValueKinds))

		for i, kind := range allQValueKinds {
			if allnulls {
				entries[i] = qvalue.QValueNull(kind)
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
	tmpfile, err := os.CreateTemp("", "example_*.avro")
	require.NoError(t, err)

	defer os.Remove(tmpfile.Name()) // clean up
	defer tmpfile.Close()           // close file after test ends

	// Define sample data
	records, schema := generateRecords(t, true, 10, false)

	avroSchema, err := model.GetAvroSchemaDefinition(context.Background(), nil, "not_applicable", schema, protos.DBType_SNOWFLAKE)
	require.NoError(t, err)

	t.Logf("[test] avroSchema: %v", avroSchema)

	// Call function
	writer := avro.NewPeerDBOCFWriter(records, avroSchema, avro.CompressNone, protos.DBType_SNOWFLAKE)
	_, err = writer.WriteRecordsToAvroFile(context.Background(), nil, tmpfile.Name())
	require.NoError(t, err, "expected WriteRecordsToAvroFile to complete without errors")

	// Check file is not empty
	info, err := tmpfile.Stat()
	require.NoError(t, err)
	require.NotZero(t, info.Size(), "expected file to not be empty")
}

func TestWriteRecordsToZstdAvroFileHappyPath(t *testing.T) {
	// Create temporary file
	tmpfile, err := os.CreateTemp("", "example_*.avro.zst")
	require.NoError(t, err)

	defer os.Remove(tmpfile.Name()) // clean up
	defer tmpfile.Close()           // close file after test ends

	// Define sample data
	records, schema := generateRecords(t, true, 10, false)

	avroSchema, err := model.GetAvroSchemaDefinition(context.Background(), nil, "not_applicable", schema, protos.DBType_SNOWFLAKE)
	require.NoError(t, err)

	t.Logf("[test] avroSchema: %v", avroSchema)

	// Call function
	writer := avro.NewPeerDBOCFWriter(records, avroSchema, avro.CompressZstd, protos.DBType_SNOWFLAKE)
	_, err = writer.WriteRecordsToAvroFile(context.Background(), nil, tmpfile.Name())
	require.NoError(t, err, "expected WriteRecordsToAvroFile to complete without errors")

	// Check file is not empty
	info, err := tmpfile.Stat()
	require.NoError(t, err)
	require.NotZero(t, info.Size(), "expected file to not be empty")
}

func TestWriteRecordsToDeflateAvroFileHappyPath(t *testing.T) {
	// Create temporary file
	tmpfile, err := os.CreateTemp("", "example_*.avro.zz")
	require.NoError(t, err)

	defer os.Remove(tmpfile.Name()) // clean up
	defer tmpfile.Close()           // close file after test ends

	// Define sample data
	records, schema := generateRecords(t, true, 10, false)

	avroSchema, err := model.GetAvroSchemaDefinition(context.Background(), nil, "not_applicable", schema, protos.DBType_SNOWFLAKE)
	require.NoError(t, err)

	t.Logf("[test] avroSchema: %v", avroSchema)

	// Call function
	writer := avro.NewPeerDBOCFWriter(records, avroSchema, avro.CompressDeflate, protos.DBType_SNOWFLAKE)
	_, err = writer.WriteRecordsToAvroFile(context.Background(), nil, tmpfile.Name())
	require.NoError(t, err, "expected WriteRecordsToAvroFile to complete without errors")

	// Check file is not empty
	info, err := tmpfile.Stat()
	require.NoError(t, err)
	require.NotZero(t, info.Size(), "expected file to not be empty")
}

func TestWriteRecordsToAvroFileNonNull(t *testing.T) {
	// Create temporary file
	tmpfile, err := os.CreateTemp("", "example_*.avro")
	require.NoError(t, err)

	defer os.Remove(tmpfile.Name()) // clean up
	defer tmpfile.Close()           // close file after test ends

	records, schema := generateRecords(t, false, 10, false)

	avroSchema, err := model.GetAvroSchemaDefinition(context.Background(), nil, "not_applicable", schema, protos.DBType_SNOWFLAKE)
	require.NoError(t, err)

	t.Logf("[test] avroSchema: %v", avroSchema)

	// Call function
	writer := avro.NewPeerDBOCFWriter(records, avroSchema, avro.CompressNone, protos.DBType_SNOWFLAKE)
	_, err = writer.WriteRecordsToAvroFile(context.Background(), nil, tmpfile.Name())
	require.NoError(t, err, "expected WriteRecordsToAvroFile to complete without errors")

	// Check file is not empty
	info, err := tmpfile.Stat()
	require.NoError(t, err)
	require.NotZero(t, info.Size(), "expected file to not be empty")
}

func TestWriteRecordsToAvroFileAllNulls(t *testing.T) {
	// Create temporary file
	tmpfile, err := os.CreateTemp("", "example_*.avro")
	require.NoError(t, err)

	defer os.Remove(tmpfile.Name()) // clean up
	defer tmpfile.Close()           // close file after test ends

	// Define sample data
	records, schema := generateRecords(t, true, 10, true)

	avroSchema, err := model.GetAvroSchemaDefinition(context.Background(), nil, "not_applicable", schema, protos.DBType_SNOWFLAKE)
	require.NoError(t, err)

	t.Logf("[test] avroSchema: %v", avroSchema)

	// Call function
	writer := avro.NewPeerDBOCFWriter(records, avroSchema, avro.CompressNone, protos.DBType_SNOWFLAKE)
	_, err = writer.WriteRecordsToAvroFile(context.Background(), nil, tmpfile.Name())
	require.NoError(t, err, "expected WriteRecordsToAvroFile to complete without errors")

	// Check file is not empty
	info, err := tmpfile.Stat()
	require.NoError(t, err)
	require.NotZero(t, info.Size(), "expected file to not be empty")
}
