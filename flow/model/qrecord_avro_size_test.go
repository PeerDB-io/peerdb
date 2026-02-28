package model

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/hamba/avro/v2/ocf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// This test verifies that avro size computation is sufficiently accurate for MongoDB documents
// by comparing them against actual uncompressed Avro file sizes. MongoDB CDC connector
// produces records with _id (string) and doc (JSON). We will need additional test cases
// for broader schema coverage when we are introducing uncompressed bytes for other databases.
func TestMongoDBAvroSizeComputation(t *testing.T) {
	testCases := []struct {
		name       string
		numRecords int
	}{
		{
			name:       "small_file",
			numRecords: 5_000,
		},
		{
			name:       "medium_file",
			numRecords: 50_000,
		},
		{
			name:       "large_file",
			numRecords: 500_000,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create temporary file
			tmpfile, err := os.CreateTemp(t.TempDir(), fmt.Sprintf("test_avro_size_%s.avro", tc.name))
			require.NoError(t, err)
			defer os.Remove(tmpfile.Name())
			defer tmpfile.Close()

			schema := types.QRecordSchema{
				Fields: []types.QField{
					{Name: "_id", Type: types.QValueKindString, Nullable: false},
					{Name: "doc", Type: types.QValueKindJSON, Nullable: false},
				},
			}
			avroSchema, computedSize := writeAvroFileCompressed(t, schema, tc.numRecords, tmpfile.Name())

			t.Logf("Total computed size for %d records: %d bytes (%.2f MiB)",
				tc.numRecords, computedSize, float64(computedSize)/(1024*1024))

			actualSize := getActualUncompressedSize(t, tmpfile.Name(), avroSchema, tc.numRecords)
			t.Logf("Actual uncompressed Avro size for %d records: %d bytes (%.2f MiB)",
				tc.numRecords, actualSize, float64(actualSize)/(1024*1024))

			diff := actualSize - computedSize
			diffPercent := float64(diff) / float64(actualSize) * 100
			t.Logf("Difference: %d bytes (%.2f%%)", diff, diffPercent)

			lowerBound := float64(actualSize) * 0.95
			upperBound := float64(actualSize) * 1.05
			assert.GreaterOrEqual(t, float64(computedSize), lowerBound,
				"Estimated size should be within 5% of actual (lower bound)")
			assert.LessOrEqual(t, float64(computedSize), upperBound,
				"Estimated size should be within 5% of actual (upper bound)")

			// for completion, print out the compression ratio
			fileInfo, err := os.Stat(tmpfile.Name())
			require.NoError(t, err)
			compressedSize := fileInfo.Size()
			compressionRatio := float64(actualSize) / float64(compressedSize)
			t.Logf("Compressed file size: %d bytes (%.2f MiB), compression ratio: %.2fx",
				compressedSize, float64(compressedSize)/(1024*1024), compressionRatio)
		})
	}
}

func writeAvroFileCompressed(
	t *testing.T,
	schema types.QRecordSchema,
	numRecords int,
	filePath string,
) (*QRecordAvroSchemaDefinition, int64) {
	t.Helper()

	env := map[string]string{
		"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": "false", // avoids db lookups
	}

	avroSchema, err := GetAvroSchemaDefinition(
		context.Background(),
		env,
		"mongo_avro_size_dst_table",
		schema,
		protos.DBType_CLICKHOUSE,
		ConstructColumnNameAvroFieldMap(schema.Fields),
	)
	require.NoError(t, err)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	defer file.Close()

	encoder, err := ocf.NewEncoderWithSchema(
		avroSchema.Schema,
		file,
		ocf.WithCodec(ocf.ZStandard),
		ocf.WithBlockLength(8192),
		ocf.WithBlockSize(1<<26),
	)
	require.NoError(t, err)
	defer encoder.Close()

	avroFieldNames := make([]string, len(avroSchema.Schema.Fields()))
	for i, field := range avroSchema.Schema.Fields() {
		avroFieldNames[i] = field.Name()
	}

	avroConverter, err := NewQRecordAvroConverter(
		context.Background(),
		env,
		avroSchema,
		protos.DBType_CLICKHOUSE,
		avroFieldNames,
		nil,
	)
	require.NoError(t, err)

	docContentTemplate := `{
		"user_id":"%s",
		"email":"%s",
		"age":%d,
		"is_active":%t,
		"is_verified":%t,
		"count_logins":%d,
		"created_at":"%s",
		"updated_at":"%s",
		"session_id":"%s",
		"ip_address":"%s",
		"tags":["%s","%s","%s"],
		"preferences":{"theme":"%s","language":"%s"},
		"notes":"%s"
	}`

	bytes := int64(0)
	//nolint:gosec // tests, not security-sensitive
	for range numRecords {
		randomizedDocContent := fmt.Sprintf(docContentTemplate,
			fmt.Sprintf("usr_%x", rand.Int64()),
			fmt.Sprintf("user%d@example.com", rand.IntN(100000)),
			rand.IntN(80)+18,
			rand.IntN(2) == 1,
			rand.IntN(2) == 1,
			rand.IntN(10000),
			time.Now(),
			time.Now(),
			fmt.Sprintf("sess_%x", rand.Int64()),
			fmt.Sprintf("192.168.%d.%d", rand.IntN(256), rand.IntN(256)),
			randomChoice([]string{"premium", "basic", "trial", "enterprise"}),
			randomChoice([]string{"web", "mobile", "api"}),
			randomChoice([]string{"google", "facebook", "twitter", "direct"}),
			randomChoice([]string{"dark", "light", "auto"}),
			randomChoice([]string{"en", "es", "fr", "de", "ja"}),
			randomText(100),
		)

		record := []types.QValue{
			types.QValueString{Val: fmt.Sprintf("%x", rand.Int64())},
			types.QValueJSON{Val: randomizedDocContent},
		}

		avroMap, size, err := avroConverter.Convert(
			context.Background(),
			env,
			record,
			nil,
			nil,
			internal.BinaryFormatRaw,
			true,
		)
		require.NoError(t, err)
		err = encoder.Encode(avroMap)
		require.NoError(t, err)

		bytes += size
	}

	err = encoder.Flush()
	require.NoError(t, err)
	err = encoder.Close()
	require.NoError(t, err)
	err = file.Close()
	require.NoError(t, err)

	return avroSchema, bytes
}

func randomChoice(choices []string) string {
	//nolint:gosec // tests, not security-sensitive
	return choices[rand.IntN(len(choices))]
}

func randomText(length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
	result := make([]byte, length)
	for i := range result {
		//nolint:gosec // tests, not security-sensitive
		result[i] = chars[rand.IntN(len(chars))]
	}
	return string(result)
}

type MeteredWriter struct {
	bytesWritten int64
}

func (w *MeteredWriter) Write(p []byte) (int, error) {
	w.bytesWritten += int64(len(p))
	return len(p), nil
}

// getActualUncompressedSize reads a compressed Avro file and measures the actual
// uncompressed data size by re-encoding without compression to a metered writer
func getActualUncompressedSize(t *testing.T, filePath string, avroSchema *QRecordAvroSchemaDefinition, numRecords int) int64 {
	t.Helper()

	file, err := os.Open(filePath)
	require.NoError(t, err)
	defer file.Close()
	decoder, err := ocf.NewDecoder(file)
	require.NoError(t, err)

	meteredWriter := &MeteredWriter{}
	encoder, err := ocf.NewEncoderWithSchema(
		avroSchema.Schema,
		meteredWriter,
		ocf.WithCodec(ocf.Null),
		ocf.WithBlockLength(8192),
		ocf.WithBlockSize(1<<26),
	)
	require.NoError(t, err)
	defer encoder.Close()
	defer encoder.Close()

	n := 0
	for decoder.HasNext() {
		var record map[string]any
		err := decoder.Decode(&record)
		require.NoError(t, err)
		err = encoder.Encode(record)
		require.NoError(t, err)
		n += 1
	}
	require.NoError(t, decoder.Error())
	assert.Equal(t, numRecords, n)

	err = encoder.Flush()
	require.NoError(t, err)

	return meteredWriter.bytesWritten
}
