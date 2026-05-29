//nolint:gosec // test-only random data and temp files
package model

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math/big"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hamba/avro/v2/ocf"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// This test verifies that avro size computation is sufficiently accurate for MongoDB documents
// by comparing them against actual uncompressed Avro file sizes. MongoDB CDC connector
// produces records with _id (string) and doc (JSON).
func TestMongoDBAvroSizeComputation(t *testing.T) {
	testCases := []struct {
		name       string
		numRecords int
	}{
		{name: "small_file", numRecords: 5_000},
		{name: "medium_file", numRecords: 50_000},
		{name: "large_file", numRecords: 500_000},
	}

	schema := types.QRecordSchema{
		Fields: []types.QField{
			{Name: "_id", Type: types.QValueKindString, Nullable: false},
			{Name: "doc", Type: types.QValueKindJSON, Nullable: false},
		},
	}

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

	genRecord := func() []types.QValue {
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
		return []types.QValue{
			types.QValueString{Val: fmt.Sprintf("%x", rand.Int64())},
			types.QValueJSON{Val: randomizedDocContent},
		}
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpfile, err := os.CreateTemp(t.TempDir(), fmt.Sprintf("test_avro_size_%s.avro", tc.name))
			require.NoError(t, err)
			defer os.Remove(tmpfile.Name())
			defer tmpfile.Close()

			avroSchema, computedSize := writeAvroFileCompressed(t, schema, tc.numRecords, tmpfile.Name(), internal.NewSettings(nil), genRecord)

			t.Logf("Total computed size for %d records: %d bytes (%.2f MiB)",
				tc.numRecords, computedSize, float64(computedSize)/(1024*1024))

			actualSize := getActualUncompressedSize(t, tmpfile.Name(), avroSchema, tc.numRecords)
			t.Logf("Actual uncompressed Avro size for %d records: %d bytes (%.2f MiB)",
				tc.numRecords, actualSize, float64(actualSize)/(1024*1024))

			diff := actualSize - computedSize
			diffPercent := float64(diff) / float64(actualSize) * 100
			t.Logf("Difference: %d bytes (%.2f%%)", diff, diffPercent)

			assertWithinTolerance(t, computedSize, actualSize, 0.05)

			fileInfo, err := os.Stat(tmpfile.Name())
			require.NoError(t, err)
			compressedSize := fileInfo.Size()
			compressionRatio := float64(actualSize) / float64(compressedSize)
			t.Logf("Compressed file size: %d bytes (%.2f MiB), compression ratio: %.2fx",
				compressedSize, float64(compressedSize)/(1024*1024), compressionRatio)
		})
	}
}

func TestAvroSizeComputation(t *testing.T) {
	//nolint:govet // fieldalignment: readability over struct packing in test definitions
	type subtestDef struct {
		name       string
		kind       types.QValueKind
		precision  int16
		scale      int16
		numRecords int
		settings   *internal.Settings
		genValue   func() types.QValue
	}

	subtests := []subtestDef{
		// Scalar types
		{
			name:       "invalid",
			kind:       types.QValueKindInvalid,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueInvalid{Val: randomText(20 + rand.IntN(200))}
			},
		},
		{
			name:       "float32",
			kind:       types.QValueKindFloat32,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueFloat32{Val: rand.Float32() * 1000}
			},
		},
		{
			name:       "float64",
			kind:       types.QValueKindFloat64,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueFloat64{Val: rand.Float64() * 1e6}
			},
		},
		{
			name:       "int8",
			kind:       types.QValueKindInt8,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueInt8{Val: int8(rand.IntN(256) - 128)}
			},
		},
		{
			name:       "int16",
			kind:       types.QValueKindInt16,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueInt16{Val: int16(rand.Int32())}
			},
		},
		{
			name:       "int32",
			kind:       types.QValueKindInt32,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueInt32{Val: rand.Int32()}
			},
		},
		{
			name:       "int64",
			kind:       types.QValueKindInt64,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueInt64{Val: rand.Int64()}
			},
		},
		{
			name:       "int256",
			kind:       types.QValueKindInt256,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueInt256{Val: randomBigInt(31)}
			},
		},
		{
			name:       "uint8",
			kind:       types.QValueKindUInt8,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueUInt8{Val: uint8(rand.IntN(256))}
			},
		},
		{
			name:       "uint16",
			kind:       types.QValueKindUInt16,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueUInt16{Val: uint16(rand.IntN(65536))}
			},
		},
		{
			name:       "uint32",
			kind:       types.QValueKindUInt32,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueUInt32{Val: rand.Uint32()}
			},
		},
		{
			name:       "uint64",
			kind:       types.QValueKindUInt64,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueUInt64{Val: rand.Uint64()}
			},
		},
		{
			name:       "uint256",
			kind:       types.QValueKindUInt256,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueUInt256{Val: randomBigInt(31)}
			},
		},
		{
			name:       "boolean",
			kind:       types.QValueKindBoolean,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueBoolean{Val: rand.IntN(2) == 1}
			},
		},
		{
			name:       "qchar",
			kind:       types.QValueKindQChar,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueQChar{Val: uint8(rand.IntN(128))}
			},
		},
		{
			name:       "string",
			kind:       types.QValueKindString,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueString{Val: randomText(20 + rand.IntN(200))}
			},
		},
		{
			name:       "enum",
			kind:       types.QValueKindEnum,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueEnum{Val: randomText(10 + rand.IntN(50))}
			},
		},
		{
			name:       "uint16enum",
			kind:       types.QValueKindUint16Enum,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueUint16Enum{Val: uint16(rand.IntN(65536))}
			},
		},
		{
			name:       "timestamp",
			kind:       types.QValueKindTimestamp,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueTimestamp{Val: randomTimestamp()}
			},
		},
		{
			name:       "timestamptz",
			kind:       types.QValueKindTimestampTZ,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueTimestampTZ{Val: randomTimestamp()}
			},
		},
		{
			name:       "date",
			kind:       types.QValueKindDate,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueDate{Val: randomDate()}
			},
		},
		{
			name:       "time",
			kind:       types.QValueKindTime,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueTime{
					Val: time.Duration(rand.Int64N(int64(24 * time.Hour))),
				}
			},
		},
		{
			name:       "timetz",
			kind:       types.QValueKindTimeTZ,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueTimeTZ{
					Val: time.Duration(rand.Int64N(int64(24 * time.Hour))),
				}
			},
		},
		{
			name:       "interval",
			kind:       types.QValueKindInterval,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueInterval{
					Val: randomText(10 + rand.IntN(30)),
				}
			},
		},
		{
			name:       "numeric_76_38",
			kind:       types.QValueKindNumeric,
			precision:  76,
			scale:      38,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueNumeric{
					Val: randomDecimal(38, 38), Precision: 76, Scale: 38,
				}
			},
		},
		{
			name:       "numeric_38_38",
			kind:       types.QValueKindNumeric,
			precision:  38,
			scale:      38,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueNumeric{
					Val: randomFractionalDecimal(38), Precision: 38, Scale: 38,
				}
			},
		},
		{
			name:       "numeric_38_0",
			kind:       types.QValueKindNumeric,
			precision:  38,
			scale:      0,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueNumeric{
					Val: randomIntegerDecimal(38), Precision: 38, Scale: 0,
				}
			},
		},
		{
			name:       "bytes_raw",
			kind:       types.QValueKindBytes,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueBytes{Val: randomBytes(50 + rand.IntN(200))}
			},
		},
		{
			name:       "bytes_base64",
			kind:       types.QValueKindBytes,
			numRecords: 10_000,
			settings:   internal.NewSettings(map[string]string{"PEERDB_CLICKHOUSE_BINARY_FORMAT": "base64"}),
			genValue: func() types.QValue {
				return types.QValueBytes{Val: randomBytes(50 + rand.IntN(200))}
			},
		},
		{
			name:       "uuid",
			kind:       types.QValueKindUUID,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueUUID{Val: uuid.New()}
			},
		},
		{
			name:       "json_small",
			kind:       types.QValueKindJSON,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueJSON{Val: smallJSON()}
			},
		},
		{
			name:       "hstore",
			kind:       types.QValueKindHStore,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueHStore{Val: randomHStore(3 + rand.IntN(5))}
			},
		},
		{
			name:       "geography",
			kind:       types.QValueKindGeography,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueGeography{Val: randomText(20 + rand.IntN(200))}
			},
		},
		{
			name:       "geometry",
			kind:       types.QValueKindGeometry,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueGeometry{Val: randomText(20 + rand.IntN(200))}
			},
		},
		{
			name:       "point",
			kind:       types.QValueKindPoint,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValuePoint{Val: randomText(20 + rand.IntN(50))}
			},
		},
		{
			name:       "cidr",
			kind:       types.QValueKindCIDR,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueCIDR{Val: randomText(10 + rand.IntN(30))}
			},
		},
		{
			name:       "inet",
			kind:       types.QValueKindINET,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueINET{Val: randomText(10 + rand.IntN(30))}
			},
		},
		{
			name:       "macaddr",
			kind:       types.QValueKindMacaddr,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueMacaddr{Val: randomText(17)}
			},
		},
		// Array types
		{
			name:       "array_float32",
			kind:       types.QValueKindArrayFloat32,
			numRecords: 5_000,
			genValue: func() types.QValue {
				return types.QValueArrayFloat32{
					Val: randomArray(func() float32 { return rand.Float32() * 1000 }),
				}
			},
		},
		{
			name:       "array_float64",
			kind:       types.QValueKindArrayFloat64,
			numRecords: 5_000,
			genValue: func() types.QValue {
				return types.QValueArrayFloat64{
					Val: randomArray(func() float64 { return rand.Float64() * 1e6 }),
				}
			},
		},
		{
			name:       "array_int16",
			kind:       types.QValueKindArrayInt16,
			numRecords: 5_000,
			genValue: func() types.QValue {
				return types.QValueArrayInt16{
					Val: randomArray(func() int16 { return int16(rand.Int32()) }),
				}
			},
		},
		{
			name:       "array_int32",
			kind:       types.QValueKindArrayInt32,
			numRecords: 5_000,
			genValue: func() types.QValue {
				return types.QValueArrayInt32{Val: randomArray(rand.Int32)}
			},
		},
		{
			name:       "array_int64",
			kind:       types.QValueKindArrayInt64,
			numRecords: 5_000,
			genValue: func() types.QValue {
				return types.QValueArrayInt64{Val: randomArray(rand.Int64)}
			},
		},
		{
			name:       "array_string",
			kind:       types.QValueKindArrayString,
			numRecords: 5_000,
			genValue: func() types.QValue {
				return types.QValueArrayString{
					Val: randomArray(func() string { return randomText(10 + rand.IntN(50)) }),
				}
			},
		},
		{
			name:       "array_enum",
			kind:       types.QValueKindArrayEnum,
			numRecords: 5_000,
			genValue: func() types.QValue {
				return types.QValueArrayEnum{
					Val: randomArray(func() string { return randomText(10 + rand.IntN(50)) }),
				}
			},
		},
		{
			name:       "array_date",
			kind:       types.QValueKindArrayDate,
			numRecords: 5_000,
			genValue: func() types.QValue {
				return types.QValueArrayDate{Val: randomArray(randomDate)}
			},
		},
		{
			name:       "array_interval",
			kind:       types.QValueKindArrayInterval,
			numRecords: 5_000,
			genValue: func() types.QValue {
				return types.QValueArrayInterval{
					Val: randomArray(func() string { return randomText(10 + rand.IntN(30)) }),
				}
			},
		},
		{
			name:       "array_timestamp",
			kind:       types.QValueKindArrayTimestamp,
			numRecords: 5_000,
			genValue: func() types.QValue {
				return types.QValueArrayTimestamp{Val: randomArray(randomTimestamp)}
			},
		},
		{
			name:       "array_timestamptz",
			kind:       types.QValueKindArrayTimestampTZ,
			numRecords: 5_000,
			genValue: func() types.QValue {
				return types.QValueArrayTimestampTZ{Val: randomArray(randomTimestamp)}
			},
		},
		{
			name:       "array_boolean",
			kind:       types.QValueKindArrayBoolean,
			numRecords: 5_000,
			genValue: func() types.QValue {
				return types.QValueArrayBoolean{
					Val: randomArray(func() bool { return rand.IntN(2) == 1 }),
				}
			},
		},
		{
			name:       "array_uuid",
			kind:       types.QValueKindArrayUUID,
			numRecords: 5_000,
			genValue: func() types.QValue {
				return types.QValueArrayUUID{Val: randomArray(uuid.New)}
			},
		},
		{
			name:       "array_numeric",
			kind:       types.QValueKindArrayNumeric,
			precision:  38,
			scale:      6,
			numRecords: 5_000,
			genValue: func() types.QValue {
				return types.QValueArrayNumeric{
					Val:       randomArray(func() decimal.Decimal { return randomDecimal(32, 6) }),
					Precision: 38,
					Scale:     6,
				}
			},
		},
	}

	for _, tc := range subtests {
		t.Run(tc.name, func(t *testing.T) {
			schema := homogeneousSchema(tc.kind, false, tc.precision, tc.scale)
			genRecord := func() []types.QValue {
				vals := make([]types.QValue, 10)
				for i := range vals {
					vals[i] = tc.genValue()
				}
				return vals
			}
			settings := tc.settings
			if settings == nil {
				settings = internal.NewSettings(nil)
			}
			runSizeSubtest(t, settings, schema, tc.numRecords, tc.name, genRecord)
		})
	}
}

func TestAvroSizeComputationSpecial(t *testing.T) {
	t.Run("nullable_mixed", func(t *testing.T) {
		schema := types.QRecordSchema{
			Fields: []types.QField{
				{Name: "col_0", Type: types.QValueKindBoolean, Nullable: true},
				{Name: "col_1", Type: types.QValueKindInt32, Nullable: true},
				{Name: "col_2", Type: types.QValueKindInt64, Nullable: true},
				{Name: "col_3", Type: types.QValueKindFloat64, Nullable: true},
				{Name: "col_4", Type: types.QValueKindString, Nullable: true},
				{Name: "col_5", Type: types.QValueKindNumeric, Nullable: true, Precision: 38, Scale: 6},
				{Name: "col_6", Type: types.QValueKindUUID, Nullable: true},
				{Name: "col_7", Type: types.QValueKindDate, Nullable: true},
				{Name: "col_8", Type: types.QValueKindTimestamp, Nullable: true},
				{Name: "col_9", Type: types.QValueKindJSON, Nullable: true},
			},
		}
		genRecord := func() []types.QValue {
			vals := make([]types.QValue, 10)
			if rand.IntN(100) < 30 {
				vals[0] = types.QValueNull(types.QValueKindBoolean)
			} else {
				vals[0] = types.QValueBoolean{Val: rand.IntN(2) == 1}
			}
			if rand.IntN(100) < 30 {
				vals[1] = types.QValueNull(types.QValueKindInt32)
			} else {
				vals[1] = types.QValueInt32{Val: rand.Int32()}
			}
			if rand.IntN(100) < 30 {
				vals[2] = types.QValueNull(types.QValueKindInt64)
			} else {
				vals[2] = types.QValueInt64{Val: rand.Int64()}
			}
			if rand.IntN(100) < 30 {
				vals[3] = types.QValueNull(types.QValueKindFloat64)
			} else {
				vals[3] = types.QValueFloat64{Val: rand.Float64() * 1e6}
			}
			if rand.IntN(100) < 30 {
				vals[4] = types.QValueNull(types.QValueKindString)
			} else {
				vals[4] = types.QValueString{Val: randomText(20 + rand.IntN(200))}
			}
			if rand.IntN(100) < 30 {
				vals[5] = types.QValueNull(types.QValueKindNumeric)
			} else {
				vals[5] = types.QValueNumeric{Val: randomDecimal(32, 6), Precision: 38, Scale: 6}
			}
			if rand.IntN(100) < 30 {
				vals[6] = types.QValueNull(types.QValueKindUUID)
			} else {
				vals[6] = types.QValueUUID{Val: uuid.New()}
			}
			if rand.IntN(100) < 30 {
				vals[7] = types.QValueNull(types.QValueKindDate)
			} else {
				vals[7] = types.QValueDate{Val: randomDate()}
			}
			if rand.IntN(100) < 30 {
				vals[8] = types.QValueNull(types.QValueKindTimestamp)
			} else {
				vals[8] = types.QValueTimestamp{Val: randomTimestamp()}
			}
			if rand.IntN(100) < 30 {
				vals[9] = types.QValueNull(types.QValueKindJSON)
			} else {
				vals[9] = types.QValueJSON{Val: smallJSON()}
			}
			return vals
		}
		runSizeSubtest(t, internal.NewSettings(nil), schema, 10_000, "nullable_mixed", genRecord)
	})

	t.Run("mixed_all_scalar", func(t *testing.T) {
		schema := types.QRecordSchema{
			Fields: []types.QField{
				{Name: "col_0", Type: types.QValueKindBoolean, Nullable: false},
				{Name: "col_1", Type: types.QValueKindInt16, Nullable: false},
				{Name: "col_2", Type: types.QValueKindInt32, Nullable: false},
				{Name: "col_3", Type: types.QValueKindInt64, Nullable: false},
				{Name: "col_4", Type: types.QValueKindFloat32, Nullable: false},
				{Name: "col_5", Type: types.QValueKindFloat64, Nullable: false},
				{Name: "col_6", Type: types.QValueKindString, Nullable: false},
				{Name: "col_7", Type: types.QValueKindNumeric, Nullable: false, Precision: 38, Scale: 6},
				{Name: "col_8", Type: types.QValueKindUUID, Nullable: false},
				{Name: "col_9", Type: types.QValueKindBytes, Nullable: false},
				{Name: "col_10", Type: types.QValueKindDate, Nullable: false},
				{Name: "col_11", Type: types.QValueKindTimestamp, Nullable: false},
				{Name: "col_12", Type: types.QValueKindTimestampTZ, Nullable: false},
				{Name: "col_13", Type: types.QValueKindTime, Nullable: false},
				{Name: "col_14", Type: types.QValueKindJSON, Nullable: false},
			},
		}
		genRecord := func() []types.QValue {
			return []types.QValue{
				types.QValueBoolean{Val: rand.IntN(2) == 1},
				types.QValueInt16{Val: int16(rand.Int32())},
				types.QValueInt32{Val: rand.Int32()},
				types.QValueInt64{Val: rand.Int64()},
				types.QValueFloat32{Val: rand.Float32() * 1000},
				types.QValueFloat64{Val: rand.Float64() * 1e6},
				types.QValueString{Val: randomText(20 + rand.IntN(200))},
				types.QValueNumeric{Val: randomDecimal(32, 6), Precision: 38, Scale: 6},
				types.QValueUUID{Val: uuid.New()},
				types.QValueBytes{Val: randomBytes(50 + rand.IntN(200))},
				types.QValueDate{Val: randomDate()},
				types.QValueTimestamp{Val: randomTimestamp()},
				types.QValueTimestampTZ{Val: randomTimestamp()},
				types.QValueTime{Val: time.Duration(rand.Int64N(int64(24 * time.Hour)))},
				types.QValueJSON{Val: smallJSON()},
			}
		}
		runSizeSubtest(t, internal.NewSettings(nil), schema, 10_000, "mixed_all_scalar", genRecord)
	})
}

// Schema-only kinds with no QValue struct — untestable for size
var sizeTestSkippedKinds = []string{
	"QValueKindJSONB",
	"QValueKindArrayJSON",
	"QValueKindArrayJSONB",
}

// Parses QValueKind constants from kind.go via AST and asserts every kind
// is either directly tested (by parsing subtestDef.kind from this file) or acknowledged as skipped.
func TestAllQValueKindsSizeCovered(t *testing.T) {
	coveredKinds := make(map[string]struct{})

	// Parse this test file to extract all types.QValueKind* references:
	// - kind: fields in subtestDef literals (directly tested kinds)
	// - keys in the aliases map below (validated via schema comparison)
	selfFset := token.NewFileSet()
	selfNode, err := parser.ParseFile(selfFset, "qrecord_avro_size_test.go", nil, 0)
	require.NoError(t, err)
	collectSelector := func(expr ast.Expr) {
		sel, ok := expr.(*ast.SelectorExpr)
		if !ok {
			return
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok || pkg.Name != "types" {
			return
		}
		coveredKinds[sel.Sel.Name] = struct{}{}
	}
	ast.Inspect(selfNode, func(n ast.Node) bool {
		kv, ok := n.(*ast.KeyValueExpr)
		if !ok {
			return true
		}
		// kind: types.QValueKindX in subtestDef literals
		if ident, ok := kv.Key.(*ast.Ident); ok && ident.Name == "kind" {
			collectSelector(kv.Value)
		}
		// types.QValueKindX: types.QValueKindY in aliases map
		collectSelector(kv.Key)
		return true
	})
	require.NotEmpty(t, coveredKinds, "should have parsed tested kinds from this file")

	for _, skip := range sizeTestSkippedKinds {
		coveredKinds[skip] = struct{}{}
	}

	// Parse kind.go to get all QValueKind constant identifier names
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, "../shared/types/kind.go", nil, 0)
	require.NoError(t, err)

	var allKinds []string
	ast.Inspect(node, func(n ast.Node) bool {
		genDecl, ok := n.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.CONST {
			return true
		}
		for _, spec := range genDecl.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			if vs.Type == nil {
				continue
			}
			ident, ok := vs.Type.(*ast.Ident)
			if !ok || ident.Name != "QValueKind" {
				continue
			}
			for _, name := range vs.Names {
				if name.Name != "_" {
					allKinds = append(allKinds, name.Name)
				}
			}
		}
		return true
	})

	require.NotEmpty(t, allKinds, "should have parsed QValueKind constants from kind.go")

	for _, kind := range allKinds {
		_, covered := coveredKinds[kind]
		assert.Truef(t, covered,
			"%s is not covered by size tests — add a test or mark it as an alias", kind)
	}
}

func runSizeSubtest(
	t *testing.T,
	settings *internal.Settings,
	schema types.QRecordSchema,
	numRecords int,
	name string,
	genRecord func() []types.QValue,
) {
	t.Helper()

	tmpfile, err := os.CreateTemp(t.TempDir(), fmt.Sprintf("test_avro_%s.avro", name))
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	avroSchema, computedSize := writeAvroFileCompressed(t, schema, numRecords, tmpfile.Name(), settings, genRecord)

	t.Logf("Computed size for %d records: %d bytes (%.2f MiB)",
		numRecords, computedSize, float64(computedSize)/(1024*1024))

	actualSize := getActualUncompressedSize(t, tmpfile.Name(), avroSchema, numRecords)
	t.Logf("Actual uncompressed size for %d records: %d bytes (%.2f MiB)",
		numRecords, actualSize, float64(actualSize)/(1024*1024))

	diff := actualSize - computedSize
	diffPercent := float64(diff) / float64(actualSize) * 100
	t.Logf("Difference: %d bytes (%.2f%%)", diff, diffPercent)

	assertWithinTolerance(t, computedSize, actualSize, 0.05)
}

func writeAvroFileCompressed(
	t *testing.T,
	schema types.QRecordSchema,
	numRecords int,
	filePath string,
	settings *internal.Settings,
	genRecord func() []types.QValue,
) (*QRecordAvroSchemaDefinition, int64) {
	t.Helper()

	avroSchema, err := GetAvroSchemaDefinition(
		context.Background(),
		settings,
		"avro_size_dst_table",
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

	avroConverter := NewQRecordAvroConverter(
		settings,
		avroSchema,
		protos.DBType_CLICKHOUSE,
		avroFieldNames,
		log.NewStructuredLogger(nil),
	)

	binaryFormat := settings.ClickHouseBinaryFormat

	bytes := int64(0)
	for range numRecords {
		record := genRecord()

		avroMap, size, err := avroConverter.Convert(
			context.Background(),
			settings,
			record,
			nil,
			nil,
			binaryFormat,
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

func randomHStore(numKeys int) string {
	var b []byte
	for i := range numKeys {
		if i > 0 {
			b = append(b, ',', ' ')
		}
		b = fmt.Appendf(b, `"%s"=>"%s"`, randomAlphaNum(5+rand.IntN(10)), randomAlphaNum(5+rand.IntN(20)))
	}
	return string(b)
}

func randomAlphaNum(length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = chars[rand.IntN(len(chars))]
	}
	return string(b)
}

func randomBigInt(byteLen int) *big.Int {
	b := make([]byte, byteLen)
	_, _ = crand.Read(b)
	return new(big.Int).SetBytes(b)
}

func randomChoice(choices []string) string {
	return choices[rand.IntN(len(choices))]
}

func randomText(length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
	result := make([]byte, length)
	for i := range result {
		result[i] = chars[rand.IntN(len(chars))]
	}
	return string(result)
}

func randomArray[T any](gen func() T) []T {
	arr := make([]T, 3+rand.IntN(8))
	for i := range arr {
		arr[i] = gen()
	}
	return arr
}

func randomBytes(length int) []byte {
	b := make([]byte, length)
	for i := range b {
		b[i] = byte(rand.IntN(256))
	}
	return b
}

func randomDate() time.Time {
	year := 2000 + rand.IntN(31)
	month := 1 + rand.IntN(12)
	day := 1 + rand.IntN(28)
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
}

func randomTimestamp() time.Time {
	return randomDate().Add(time.Duration(rand.Int64N(int64(24 * time.Hour))))
}

func randomDecimal(intDigits, fracDigits int) decimal.Decimal {
	s := randomDigitString(intDigits) + "." + randomDigitString(fracDigits)
	if rand.IntN(2) == 1 {
		s = "-" + s
	}
	d, _ := decimal.NewFromString(s)
	return d
}

func randomFractionalDecimal(fracDigits int) decimal.Decimal {
	s := "0." + randomDigitString(fracDigits)
	if rand.IntN(2) == 1 {
		s = "-" + s
	}
	d, _ := decimal.NewFromString(s)
	return d
}

func randomIntegerDecimal(digits int) decimal.Decimal {
	s := randomDigitString(digits)
	if rand.IntN(2) == 1 {
		s = "-" + s
	}
	d, _ := decimal.NewFromString(s)
	return d
}

func randomDigitString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = '0' + byte(rand.IntN(10))
	}
	if b[0] == '0' && length > 1 {
		b[0] = '1' + byte(rand.IntN(9))
	}
	return string(b)
}

func smallJSON() string {
	return fmt.Sprintf(
		`{"name":"%s","age":%d,"active":%t,"score":%.2f,"tag":"%s"}`,
		randomText(20+rand.IntN(30)),
		rand.IntN(100),
		rand.IntN(2) == 1,
		rand.Float64()*100,
		randomText(10+rand.IntN(20)),
	)
}

func homogeneousSchema(kind types.QValueKind, nullable bool, precision, scale int16) types.QRecordSchema {
	fields := make([]types.QField, 10)
	for i := range fields {
		fields[i] = types.QField{
			Name:      fmt.Sprintf("col_%d", i),
			Type:      kind,
			Nullable:  nullable,
			Precision: precision,
			Scale:     scale,
		}
	}
	return types.QRecordSchema{Fields: fields}
}

func assertWithinTolerance(t *testing.T, computed, actual int64, tolerance float64) {
	t.Helper()
	lowerBound := float64(actual) * (1 - tolerance)
	upperBound := float64(actual) * (1 + tolerance)
	assert.GreaterOrEqual(t, float64(computed), lowerBound,
		"Estimated size should be within %.0f%% of actual (lower bound): computed=%d actual=%d", tolerance*100, computed, actual)
	assert.LessOrEqual(t, float64(computed), upperBound,
		"Estimated size should be within %.0f%% of actual (upper bound): computed=%d actual=%d", tolerance*100, computed, actual)
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
