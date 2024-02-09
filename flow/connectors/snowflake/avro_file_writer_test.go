package connsnowflake

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	avro "github.com/PeerDB-io/peer-flow/connectors/utils/avro"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

// createQValue creates a QValue of the appropriate kind for a given placeholder.
func createQValue(t *testing.T, kind qvalue.QValueKind, placeHolder int) qvalue.QValue {
	t.Helper()

	var value interface{}
	switch kind {
	case qvalue.QValueKindInt16, qvalue.QValueKindInt32, qvalue.QValueKindInt64:
		value = int64(placeHolder)
	case qvalue.QValueKindFloat32:
		value = float32(placeHolder)
	case qvalue.QValueKindFloat64:
		value = float64(placeHolder)
	case qvalue.QValueKindBoolean:
		value = placeHolder%2 == 0
	case qvalue.QValueKindString:
		value = fmt.Sprintf("string%d", placeHolder)
	case qvalue.QValueKindTimestamp, qvalue.QValueKindTimestampTZ, qvalue.QValueKindTime,
		qvalue.QValueKindTimeTZ, qvalue.QValueKindDate:
		value = time.Now()
	case qvalue.QValueKindNumeric:
		// create a new big.Rat for numeric data
		value = big.NewRat(int64(placeHolder), 1)
	case qvalue.QValueKindUUID:
		value = uuid.New() // assuming you have the github.com/google/uuid package
	// case qvalue.QValueKindArray:
	// 	value = []int{1, 2, 3} // placeholder array, replace with actual logic
	// case qvalue.QValueKindStruct:
	// 	value = map[string]interface{}{"key": "value"} // placeholder struct, replace with actual logic
	// case qvalue.QValueKindJSON:
	// 	value = `{"key": "value"}` // placeholder JSON, replace with actual logic
	case qvalue.QValueKindBytes, qvalue.QValueKindBit:
		value = []byte("sample bytes") // placeholder bytes, replace with actual logic
	default:
		require.Failf(t, "unsupported QValueKind", "unsupported QValueKind: %s", kind)
	}

	return qvalue.QValue{
		Kind:  kind,
		Value: value,
	}
}

//nolint:unparam
func generateRecords(
	t *testing.T,
	nullable bool,
	numRows uint32,
	allnulls bool,
) (*model.QRecordStream, *model.QRecordSchema) {
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
		// qvalue.QValueKindJSON,
		qvalue.QValueKindBit,
	}

	numKinds := len(allQValueKinds)

	schema := &model.QRecordSchema{
		Fields: make([]model.QField, numKinds),
	}

	// Create sample records
	records := &model.QRecordBatch{
		Records: make([][]qvalue.QValue, numRows),
		Schema:  schema,
	}

	for i, kind := range allQValueKinds {
		schema.Fields[i] = model.QField{
			Name:     string(kind),
			Type:     kind,
			Nullable: nullable,
		}
	}

	for row := uint32(0); row < numRows; row++ {
		entries := make([]qvalue.QValue, len(allQValueKinds))

		for i, kind := range allQValueKinds {
			placeHolder := int(row) * i
			entries[i] = createQValue(t, kind, placeHolder)
			if allnulls {
				entries[i].Value = nil
			}
		}

		records.Records[row] = entries
	}

	stream, err := records.ToQRecordStream(1024)
	require.NoError(t, err)

	return stream, schema
}

func TestWriteRecordsToAvroFileHappyPath(t *testing.T) {
	// Create temporary file
	tmpfile, err := os.CreateTemp("", "example_*.avro")
	require.NoError(t, err)

	defer os.Remove(tmpfile.Name()) // clean up
	defer tmpfile.Close()           // close file after test ends

	// Define sample data
	records, schema := generateRecords(t, true, 10, false)

	avroSchema, err := model.GetAvroSchemaDefinition("not_applicable", schema, qvalue.QDWHTypeSnowflake)
	require.NoError(t, err)

	t.Logf("[test] avroSchema: %v", avroSchema)

	// Call function
	writer := avro.NewPeerDBOCFWriter(records, avroSchema, avro.CompressNone, qvalue.QDWHTypeSnowflake)
	_, err = writer.WriteRecordsToAvroFile(context.Background(), tmpfile.Name())
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

	avroSchema, err := model.GetAvroSchemaDefinition("not_applicable", schema, qvalue.QDWHTypeSnowflake)
	require.NoError(t, err)

	t.Logf("[test] avroSchema: %v", avroSchema)

	// Call function
	writer := avro.NewPeerDBOCFWriter(records, avroSchema, avro.CompressZstd, qvalue.QDWHTypeSnowflake)
	_, err = writer.WriteRecordsToAvroFile(context.Background(), tmpfile.Name())
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

	avroSchema, err := model.GetAvroSchemaDefinition("not_applicable", schema, qvalue.QDWHTypeSnowflake)
	require.NoError(t, err)

	t.Logf("[test] avroSchema: %v", avroSchema)

	// Call function
	writer := avro.NewPeerDBOCFWriter(records, avroSchema, avro.CompressDeflate, qvalue.QDWHTypeSnowflake)
	_, err = writer.WriteRecordsToAvroFile(context.Background(), tmpfile.Name())
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

	avroSchema, err := model.GetAvroSchemaDefinition("not_applicable", schema, qvalue.QDWHTypeSnowflake)
	require.NoError(t, err)

	t.Logf("[test] avroSchema: %v", avroSchema)

	// Call function
	writer := avro.NewPeerDBOCFWriter(records, avroSchema, avro.CompressNone, qvalue.QDWHTypeSnowflake)
	_, err = writer.WriteRecordsToAvroFile(context.Background(), tmpfile.Name())
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

	avroSchema, err := model.GetAvroSchemaDefinition("not_applicable", schema, qvalue.QDWHTypeSnowflake)
	require.NoError(t, err)

	t.Logf("[test] avroSchema: %v", avroSchema)

	// Call function
	writer := avro.NewPeerDBOCFWriter(records, avroSchema, avro.CompressNone, qvalue.QDWHTypeSnowflake)
	_, err = writer.WriteRecordsToAvroFile(context.Background(), tmpfile.Name())
	require.NoError(t, err, "expected WriteRecordsToAvroFile to complete without errors")

	// Check file is not empty
	info, err := tmpfile.Stat()
	require.NoError(t, err)
	require.NotZero(t, info.Size(), "expected file to not be empty")
}
