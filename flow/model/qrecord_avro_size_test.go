//nolint:gosec // test-only random data and temp files
package model

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hamba/avro/v2/ocf"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// TODO: remove
func TestMain(m *testing.M) {
	os.Setenv("PEERDB_CATALOG_HOST", "localhost")
	os.Setenv("PEERDB_CATALOG_PORT", "9901")
	os.Setenv("PEERDB_CATALOG_USER", "postgres")
	os.Setenv("PEERDB_CATALOG_PASSWORD", "postgres")
	os.Setenv("PEERDB_CATALOG_DATABASE", "postgres")
	os.Exit(m.Run())
}

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

			avroSchema, computedSize := writeAvroFileCompressed(t, schema, tc.numRecords, tmpfile.Name(), nil, genRecord)

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
		env        map[string]string
		genValue   func() types.QValue
	}

	subtests := []subtestDef{
		{
			name:       "boolean",
			kind:       types.QValueKindBoolean,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueBoolean{Val: rand.IntN(2) == 1}
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
			name:       "string",
			kind:       types.QValueKindString,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueString{Val: randomText(20 + rand.IntN(200))}
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
			name:       "uuid",
			kind:       types.QValueKindUUID,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueUUID{Val: uuid.New()}
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
			env:        map[string]string{"PEERDB_CLICKHOUSE_BINARY_FORMAT": "base64"},
			genValue: func() types.QValue {
				return types.QValueBytes{Val: randomBytes(50 + rand.IntN(200))}
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
			name:       "json_small",
			kind:       types.QValueKindJSON,
			numRecords: 10_000,
			genValue: func() types.QValue {
				return types.QValueJSON{Val: smallJSON()}
			},
		},
		// Array types
		{
			name:       "array_int16",
			kind:       types.QValueKindArrayInt16,
			numRecords: 50_000,
			genValue: func() types.QValue {
				return types.QValueArrayInt16{
					Val: randomArray(func() int16 { return int16(rand.Int32()) }),
				}
			},
		},
		{
			name:       "array_int32",
			kind:       types.QValueKindArrayInt32,
			numRecords: 50_000,
			genValue: func() types.QValue {
				return types.QValueArrayInt32{Val: randomArray(rand.Int32)}
			},
		},
		{
			name:       "array_int64",
			kind:       types.QValueKindArrayInt64,
			numRecords: 50_000,
			genValue: func() types.QValue {
				return types.QValueArrayInt64{Val: randomArray(rand.Int64)}
			},
		},
		{
			name:       "array_float32",
			kind:       types.QValueKindArrayFloat32,
			numRecords: 50_000,
			genValue: func() types.QValue {
				return types.QValueArrayFloat32{
					Val: randomArray(func() float32 { return rand.Float32() * 1000 }),
				}
			},
		},
		{
			name:       "array_float64",
			kind:       types.QValueKindArrayFloat64,
			numRecords: 50_000,
			genValue: func() types.QValue {
				return types.QValueArrayFloat64{
					Val: randomArray(func() float64 { return rand.Float64() * 1e6 }),
				}
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
			name:       "array_boolean",
			kind:       types.QValueKindArrayBoolean,
			numRecords: 50_000,
			genValue: func() types.QValue {
				return types.QValueArrayBoolean{
					Val: randomArray(func() bool { return rand.IntN(2) == 1 }),
				}
			},
		},
		{
			name:       "array_timestamp",
			kind:       types.QValueKindArrayTimestamp,
			numRecords: 50_000,
			genValue: func() types.QValue {
				return types.QValueArrayTimestamp{Val: randomArray(randomTimestamp)}
			},
		},
		{
			name:       "array_date",
			kind:       types.QValueKindArrayDate,
			numRecords: 50_000,
			genValue: func() types.QValue {
				return types.QValueArrayDate{Val: randomArray(randomDate)}
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
			runSizeSubtest(t, tc.env, schema, tc.numRecords, tc.name, genRecord)
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
		runSizeSubtest(t, nil, schema, 10_000, "nullable_mixed", genRecord)
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
		runSizeSubtest(t, nil, schema, 10_000, "mixed_all_scalar", genRecord)
	})
}

func TestAllQValueKindsSizeCovered(t *testing.T) {
	testedKinds := map[types.QValueKind]string{
		// Tested in TestAvroSizeComputation
		types.QValueKindBoolean:     "boolean",
		types.QValueKindInt16:       "int16",
		types.QValueKindInt32:       "int32",
		types.QValueKindInt64:       "int64",
		types.QValueKindFloat32:     "float32",
		types.QValueKindFloat64:     "float64",
		types.QValueKindString:      "string",
		types.QValueKindNumeric:     "numeric_*",
		types.QValueKindUUID:        "uuid",
		types.QValueKindBytes:       "bytes_*",
		types.QValueKindDate:        "date",
		types.QValueKindTimestamp:   "timestamp",
		types.QValueKindTimestampTZ: "timestamptz",
		types.QValueKindTime:        "time",
		types.QValueKindJSON:        "json_*",

		types.QValueKindArrayInt16:     "array_int16",
		types.QValueKindArrayInt32:     "array_int32",
		types.QValueKindArrayInt64:     "array_int64",
		types.QValueKindArrayFloat32:   "array_float32",
		types.QValueKindArrayFloat64:   "array_float64",
		types.QValueKindArrayString:    "array_string",
		types.QValueKindArrayBoolean:   "array_boolean",
		types.QValueKindArrayTimestamp: "array_timestamp",
		types.QValueKindArrayDate:      "array_date",
		types.QValueKindArrayUUID:      "array_uuid",
		types.QValueKindArrayNumeric:   "array_numeric",

		// Aliases or types that map to already-tested Avro representations
		types.QValueKindInvalid:          "alias: serialized as string",
		types.QValueKindJSONB:            "alias: same Avro encoding as JSON",
		types.QValueKindEnum:             "alias: serialized as string",
		types.QValueKindQChar:            "alias: serialized as string",
		types.QValueKindHStore:           "alias: serialized as string",
		types.QValueKindGeography:        "alias: serialized as string",
		types.QValueKindGeometry:         "alias: serialized as string",
		types.QValueKindPoint:            "alias: serialized as string",
		types.QValueKindCIDR:             "alias: serialized as string",
		types.QValueKindINET:             "alias: serialized as string",
		types.QValueKindMacaddr:          "alias: serialized as string",
		types.QValueKindInterval:         "alias: serialized as string",
		types.QValueKindTimeTZ:           "alias: same Avro encoding as Time",
		types.QValueKindInt8:             "alias: serialized as int32",
		types.QValueKindUInt8:            "alias: serialized as int32",
		types.QValueKindUInt16:           "alias: serialized as int32",
		types.QValueKindUInt32:           "alias: serialized as int64",
		types.QValueKindUInt64:           "alias: serialized as string/int64",
		types.QValueKindInt256:           "alias: serialized as string",
		types.QValueKindUInt256:          "alias: serialized as string",
		types.QValueKindArrayEnum:        "alias: same as ArrayString",
		types.QValueKindArrayInterval:    "alias: same as ArrayString",
		types.QValueKindArrayTimestampTZ: "alias: same as ArrayTimestamp",
		types.QValueKindArrayJSON:        "alias: serialized as string",
		types.QValueKindArrayJSONB:       "alias: serialized as string",
	}

	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, "../shared/types/kind.go", nil, 0)
	require.NoError(t, err)

	var parsedKinds []string
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
			for i, name := range vs.Names {
				if name.Name == "_" {
					continue
				}
				if i >= len(vs.Values) {
					continue
				}
				bl, ok := vs.Values[i].(*ast.BasicLit)
				if !ok {
					continue
				}
				val := bl.Value[1 : len(bl.Value)-1]
				parsedKinds = append(parsedKinds, val)
			}
		}
		return true
	})

	require.NotEmpty(t, parsedKinds, "should have parsed QValueKind constants from kind.go")

	for _, kindStr := range parsedKinds {
		kind := types.QValueKind(kindStr)
		_, covered := testedKinds[kind]
		assert.Truef(t, covered,
			"QValueKind %q is not covered by size tests — add a test or mark it as an alias in testedKinds", kindStr)
	}
}

func runSizeSubtest(
	t *testing.T,
	env map[string]string,
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

	avroSchema, computedSize := writeAvroFileCompressed(t, schema, numRecords, tmpfile.Name(), env, genRecord)

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
	env map[string]string,
	genRecord func() []types.QValue,
) (*QRecordAvroSchemaDefinition, int64) {
	t.Helper()

	avroSchema, err := GetAvroSchemaDefinition(
		context.Background(),
		env,
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

	avroConverter, err := NewQRecordAvroConverter(
		context.Background(),
		env,
		avroSchema,
		protos.DBType_CLICKHOUSE,
		avroFieldNames,
		nil,
	)
	require.NoError(t, err)

	binaryFormat, err := internal.PeerDBBinaryFormat(context.Background(), env)
	require.NoError(t, err)

	bytes := int64(0)
	for range numRecords {
		record := genRecord()

		avroMap, size, err := avroConverter.Convert(
			context.Background(),
			env,
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
