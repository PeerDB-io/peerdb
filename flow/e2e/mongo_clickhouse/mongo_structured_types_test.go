//go:build tilt

package mongo_clickhouse

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	connclickhouse "github.com/PeerDB-io/peerdb/flow/connectors/clickhouse"
	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// inferredSchema is the response of discovery's /source/schema?format=JSONEachRow for a MongoDB collection.
type inferredSchema struct {
	Fields []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"fields"`
}

// bsonField is a source field and how structured ingestion must land it, as ClickHouse renders the destination
// column with toString ("NULL" for NULL).
type bsonField struct {
	value    any
	name     string
	expected string
	// expectedCDC is what CDC lands instead, when it differs from the initial load.
	expectedCDC string
}

type bsonDocument struct {
	fields []bsonField
	id     int32
}

func (d bsonDocument) withID(t *testing.T, id int32) bson.Raw {
	t.Helper()
	doc := bson.D{{Key: "_id", Value: id}}
	for _, field := range d.fields {
		doc = append(doc, bson.E{Key: field.name, Value: field.value})
	}
	raw, err := bson.Marshal(doc)
	require.NoError(t, err)
	return raw
}

// Test_Structured_Ingestion_Inferred_Types replicates, on the initial load and on CDC, a collection holding
// every non-deprecated BSON type, plus the deprecated DBPointer, JavaScript with scope and Symbol, including
// values at the extremes of their type domains (allTypesDocuments)
// and using the type that should have been inferred for them (testdata/mongodb_all_types_inferred_schema.json).
// Checks every ingested value against the one the example expects.
func (s MongoClickhouseSuite) Test_Structured_Ingestion_Inferred_Types() {
	t := s.T()
	srcDatabase := e2e.GetTestDatabase(s.Suffix())
	srcTable := "test_structured_inferred_types"
	dstTable := "test_structured_inferred_types_dst"
	const cdcIDOffset = 100

	schemaData, err := os.ReadFile("testdata/mongodb_all_types_inferred_schema.json")
	require.NoError(t, err)
	var schema inferredSchema
	require.NoError(t, json.Unmarshal(schemaData, &schema))

	// the CDC copies are the same documents under another _id
	documents := allTypesDocuments(t)
	initialDocuments := make([]bson.Raw, 0, len(documents))
	cdcDocuments := make([]bson.Raw, 0, len(documents))
	for _, document := range documents {
		initialDocuments = append(initialDocuments, document.withID(t, document.id))
		cdcDocuments = append(cdcDocuments, document.withID(t, document.id+cdcIDOffset))
	}

	// Do not include `_id` in the selected column set.
	columns := make([]*protos.ColumnSetting, 0, len(schema.Fields))
	columnNames := make([]string, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		if field.Name == "_id" {
			continue
		}
		columns = append(columns, &protos.ColumnSetting{SourceName: field.Name, DestinationType: field.Type})
		columnNames = append(columnNames, field.Name)
	}
	tableMappings := e2e.TableMappings(s, srcTable, dstTable)
	tableMappings[0].StructuredIngestionConfig = &protos.StructuredIngestionTableConfig{Enabled: true}
	tableMappings[0].Columns = columns

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, srcTable),
		TableMappings: tableMappings,
		Destination:   s.Peer().Name,
	}
	flowConnConfig := s.generateFlowConnectionConfigsDefaultEnv(connectionGen)
	flowConnConfig.DoInitialSnapshot = true

	collection := s.Source().(*e2e.MongoSource).AdminClient().Database(srcDatabase).Collection(srcTable)
	insertDocuments := func(documents []bson.Raw) {
		for _, raw := range documents {
			_, err := collection.InsertOne(t.Context(), raw)
			require.NoError(t, err)
		}
	}
	insertDocuments(initialDocuments)

	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t, tc, flowConnConfig)
	e2e.EnvWaitForCount(env, s, "initial load", dstTable, "_id", len(initialDocuments))

	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	insertDocuments(cdcDocuments)
	e2e.EnvWaitForCount(env, s, "cdc", dstTable, "_id", len(initialDocuments)+len(cdcDocuments))

	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)

	peer := s.Peer()
	database := peer.GetClickhouseConfig().Database
	ch, err := connclickhouse.Connect(t.Context(), nil, peer.GetClickhouseConfig())
	require.NoError(t, err)
	defer ch.Close()

	// destination column types, for the report
	columnTypes := map[string]string{}
	typeRows, err := ch.Query(t.Context(), fmt.Sprintf(
		"SELECT name, type FROM system.columns WHERE database = '%s' AND table = '%s'", database, dstTable))
	require.NoError(t, err)
	for typeRows.Next() {
		var name, columnType string
		require.NoError(t, typeRows.Scan(&name, &columnType))
		columnTypes[name] = columnType
	}
	require.NoError(t, typeRows.Err())
	for _, field := range schema.Fields {
		t.Logf("column %s: inferred %s, created %s", field.Name, field.Type, columnTypes[field.Name])
	}

	// ingested[_id][column] is the value landed, rendered with toString
	ingested := map[string]map[string]string{}
	for _, column := range append([]string{"_peerdb_malformed_data"}, columnNames...) {
		rows, err := ch.Query(t.Context(), fmt.Sprintf(
			`SELECT _id, ifNull(toString("%s"), 'NULL') FROM "%s"."%s" FINAL`, column, database, dstTable))
		require.NoError(t, err)
		for rows.Next() {
			var id, value string
			require.NoError(t, rows.Scan(&id, &value))
			if ingested[id] == nil {
				ingested[id] = map[string]string{}
			}
			ingested[id][column] = value
		}
		require.NoError(t, rows.Err())
	}

	var problems []string
	for _, document := range documents {
		for _, phase := range []struct {
			name string
			id   int32
		}{{"initial load", document.id}, {"cdc", document.id + cdcIDOffset}} {
			row, ok := ingested[strconv.Itoa(int(phase.id))]
			if !ok {
				problems = append(problems, fmt.Sprintf("[%s] _id %d: not ingested", phase.name, phase.id))
				continue
			}
			if malformed := row["_peerdb_malformed_data"]; malformed != "NULL" {
				problems = append(problems, fmt.Sprintf("[%s] _id %d: reported as malformed %s",
					phase.name, phase.id, truncate(malformed)))
			}
			for _, field := range document.fields {
				expected := field.expected
				if phase.name == "cdc" && field.expectedCDC != "" {
					expected = field.expectedCDC
				}
				if actual := row[field.name]; actual != expected {
					problems = append(problems, fmt.Sprintf("[%s] _id %d, %s (%s): ingested %s, expected %s",
						phase.name, phase.id, field.name, columnTypes[field.name], truncate(actual), truncate(expected)))
				}
			}
		}
	}

	require.Empty(t, problems, "structured ingestion does not land the expected values:\n%s", strings.Join(problems, "\n"))
}

// allTypesDocuments track every non-deprecated BSON type with instances of type domain extremes
// (minimum, maximum, smallest and special values, non-finite numbers), then ordinary values.
func allTypesDocuments(t *testing.T) []bsonDocument {
	t.Helper()
	objectID := func(hex string) bson.ObjectID {
		id, err := bson.ObjectIDFromHex(hex)
		require.NoError(t, err)
		return id
	}
	decimal := func(s string) bson.Decimal128 {
		d, err := bson.ParseDecimal128(s)
		require.NoError(t, err)
		return d
	}
	uuidBinary := func(s string) bson.Binary {
		u := uuid.MustParse(s)
		return bson.Binary{Subtype: bson.TypeBinaryUUID, Data: u[:]}
	}
	// binaries land as JSON objects, ClickHouse escapes the forward slashes of their base64
	binaryJSON := func(subtype byte, data []byte) string {
		encoded := strings.ReplaceAll(base64.StdEncoding.EncodeToString(data), "/", `\/`)
		return fmt.Sprintf(`{"Data":"%s","Subtype":%d}`, encoded, subtype)
	}
	allBytes := make([]byte, 256)
	for i := range allBytes {
		allBytes[i] = byte(i)
	}
	// types landing the same for every document
	constants := []bsonField{
		{name: "f_null", value: nil, expected: "NULL"},
		{name: "f_minkey", value: bson.MinKey{}, expected: "{}"},
		{name: "f_maxkey", value: bson.MaxKey{}, expected: "{}"},
	}

	documents := []bsonDocument{
		{id: 0, fields: []bsonField{
			{name: "f_double", value: -math.MaxFloat64, expected: "-1.7976931348623157e308"},
			{name: "f_string", value: "", expected: ""},
			{
				name: "f_object",
				value: bson.D{
					{Key: "a", Value: int32(math.MinInt32)}, {Key: "b", Value: ""}, {Key: "c", Value: bson.D{{Key: "d", Value: false}}},
				},
				expected: `{"a":-2147483648,"b":"","c":{"d":false}}`,
			},
			{
				name:     "f_array",
				value:    bson.A{int32(math.MinInt32), int32(math.MinInt32)},
				expected: "[-2147483648,-2147483648]",
			},
			{
				name:     "f_binary",
				value:    bson.Binary{Subtype: bson.TypeBinaryGeneric, Data: []byte{}},
				expected: `{"Data":"","Subtype":0}`,
			},
			{
				name:     "f_uuid",
				value:    uuidBinary("00000000-0000-0000-0000-000000000000"),
				expected: `{"Data":"AAAAAAAAAAAAAAAAAAAAAA==","Subtype":4}`,
			},
			{name: "f_objectid", value: objectID("000000000000000000000000"), expected: "000000000000000000000000"},
			{name: "f_bool", value: false, expected: "false"},
			// the minimum BSON date, milliseconds since the epoch as a signed 64-bit integer, clamped to the
			// minimum ClickHouse date keeping the time of day
			{name: "f_date", value: bson.DateTime(math.MinInt64), expected: "1900-01-01 16:47:04.192000"},
			{name: "f_regex", value: bson.Regex{Pattern: "(?:)", Options: ""}, expected: `{"Options":"","Pattern":"(?:)"}`},
			{name: "f_javascript", value: bson.JavaScript(""), expected: ""},
			{name: "f_int", value: int32(math.MinInt32), expected: "-2147483648"},
			// a top level Timestamp(0, 0) would be replaced by the server with the current time
			{name: "f_timestamp", value: bson.Timestamp{T: 1791062347, I: 2}, expected: `{"I":2,"T":1791062347}`},
			{name: "f_long", value: int64(math.MinInt64), expected: "-9223372036854775808"},
			{
				name:     "f_decimal",
				value:    decimal("-9.999999999999999999999999999999999E+6144"),
				expected: "-9.999999999999999999999999999999999E+6144",
			},
		}},
		{id: 1, fields: []bsonField{
			{name: "f_double", value: math.MaxFloat64, expected: "1.7976931348623157e308"},
			{name: "f_string", value: strings.Repeat("x", 65536), expected: strings.Repeat("x", 65536)},
			{
				name: "f_object",
				value: bson.D{
					{Key: "a", Value: int32(math.MaxInt32)},
					{Key: "b", Value: strings.Repeat("z", 1024)},
					{Key: "c", Value: bson.D{{Key: "d", Value: true}}},
				},
				expected: `{"a":2147483647,"b":"` + strings.Repeat("z", 1024) + `","c":{"d":true}}`,
			},
			{
				name:     "f_array",
				value:    bson.A{int32(math.MaxInt32), int32(math.MaxInt32)},
				expected: "[2147483647,2147483647]",
			},
			{
				name:     "f_binary",
				value:    bson.Binary{Subtype: bson.TypeBinaryGeneric, Data: allBytes},
				expected: binaryJSON(bson.TypeBinaryGeneric, allBytes),
			},
			{
				name:     "f_uuid",
				value:    uuidBinary("ffffffff-ffff-ffff-ffff-ffffffffffff"),
				expected: `{"Data":"\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/w==","Subtype":4}`,
			},
			{name: "f_objectid", value: objectID("ffffffffffffffffffffffff"), expected: "ffffffffffffffffffffffff"},
			{name: "f_bool", value: true, expected: "true"},
			// the maximum BSON date, clamped to the maximum ClickHouse date keeping the time of day
			{name: "f_date", value: bson.DateTime(math.MaxInt64), expected: "2299-12-31 07:12:55.807000"},
			{
				name:     "f_regex",
				value:    bson.Regex{Pattern: `^[\w.+-]+@[^\s@]+\.[a-z]{2,}$ # every option`, Options: "imsx"},
				expected: `{"Options":"imsx","Pattern":"^[\\w.+-]+@[^\\s@]+\\.[a-z]{2,}$ # every option"}`,
			},
			{
				name:     "f_javascript",
				value:    bson.JavaScript(`function () { return "\u0000\n\t'\""; }`),
				expected: `function () { return "\u0000\n\t'\""; }`,
			},
			{name: "f_int", value: int32(math.MaxInt32), expected: "2147483647"},
			{
				name:     "f_timestamp",
				value:    bson.Timestamp{T: math.MaxUint32, I: math.MaxUint32},
				expected: `{"I":4294967295,"T":4294967295}`,
			},
			{name: "f_long", value: int64(math.MaxInt64), expected: "9223372036854775807"},
			{
				name:     "f_decimal",
				value:    decimal("9.999999999999999999999999999999999E+6144"),
				expected: "9.999999999999999999999999999999999E+6144",
			},
		}},
		{id: 2, fields: []bsonField{
			// the smallest subnormal
			{name: "f_double", value: math.SmallestNonzeroFloat64, expected: "5e-324"},
			{
				name:     "f_string",
				value:    "tab\tnewline\nquote\"backslash\\nul\x00 héllo 世界 🚀",
				expected: "tab\tnewline\nquote\"backslash\\nul\x00 héllo 世界 🚀",
			},
			{
				name: "f_object",
				value: bson.D{
					{Key: "a", Value: int32(0)}, {Key: "b", Value: "\x00"}, {Key: "c", Value: bson.D{{Key: "d", Value: false}}},
				},
				expected: `{"a":0,"b":"\u0000","c":{"d":false}}`,
			},
			{name: "f_array", value: bson.A{int32(0), int32(0)}, expected: "[0,0]"},
			{
				name:     "f_binary",
				value:    bson.Binary{Subtype: bson.TypeBinaryGeneric, Data: []byte{0x00}},
				expected: `{"Data":"AA==","Subtype":0}`,
			},
			{
				name:     "f_uuid",
				value:    uuidBinary("123e4567-e89b-12d3-a456-426614174000"),
				expected: `{"Data":"Ej5FZ+ibEtOkVkJmFBdAAA==","Subtype":4}`,
			},
			{name: "f_objectid", value: objectID("000000000000000000000001"), expected: "000000000000000000000001"},
			{name: "f_bool", value: false, expected: "false"},
			{name: "f_date", value: bson.DateTime(0), expected: "1970-01-01 00:00:00.000000"},
			{name: "f_regex", value: bson.Regex{Pattern: `\x00|/`, Options: "su"}, expected: `{"Options":"su","Pattern":"\\x00|\/"}`},
			{name: "f_javascript", value: bson.JavaScript("0"), expected: "0"},
			{name: "f_int", value: int32(0), expected: "0"},
			{name: "f_timestamp", value: bson.Timestamp{T: 1, I: 0}, expected: `{"I":0,"T":1}`},
			{name: "f_long", value: int64(0), expected: "0"},
			{name: "f_decimal", value: decimal("1E-6176"), expected: "1E-6176"},
		}},
		{id: 3, fields: []bsonField{
			// FIXME: CDC goes through the raw table's JSON, which cannot hold non-finite numbers: PeerDB lands them as NULL,
			// as for any source producing them (Postgres, BQ), while the initial load keeps them.
			{name: "f_double", value: math.Inf(1), expected: "inf", expectedCDC: "NULL"},
			{name: "f_string", value: "infinity", expected: "infinity"},
			{
				name: "f_object",
				value: bson.D{
					{Key: "a", Value: int32(1)}, {Key: "b", Value: "inf"}, {Key: "c", Value: bson.D{{Key: "d", Value: true}}},
				},
				expected: `{"a":1,"b":"inf","c":{"d":true}}`,
			},
			{name: "f_array", value: bson.A{int32(1), int32(-1)}, expected: "[1,-1]"},
			{
				name:     "f_binary",
				value:    bson.Binary{Subtype: bson.TypeBinaryGeneric, Data: []byte{0x01}},
				expected: `{"Data":"AQ==","Subtype":0}`,
			},
			{
				name:     "f_uuid",
				value:    uuidBinary("00000000-0000-0000-0000-000000000001"),
				expected: `{"Data":"AAAAAAAAAAAAAAAAAAAAAQ==","Subtype":4}`,
			},
			{name: "f_objectid", value: objectID("000000000000000000000002"), expected: "000000000000000000000002"},
			{name: "f_bool", value: true, expected: "true"},
			// the lower bound of DateTime64
			{
				name:     "f_date",
				value:    bson.NewDateTimeFromTime(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)),
				expected: "1900-01-01 00:00:00.000000",
			},
			{name: "f_regex", value: bson.Regex{Pattern: "inf", Options: ""}, expected: `{"Options":"","Pattern":"inf"}`},
			{name: "f_javascript", value: bson.JavaScript("Infinity"), expected: "Infinity"},
			{name: "f_int", value: int32(1), expected: "1"},
			{name: "f_timestamp", value: bson.Timestamp{T: math.MaxUint32, I: 0}, expected: `{"I":0,"T":4294967295}`},
			{name: "f_long", value: int64(1), expected: "1"},
			{name: "f_decimal", value: decimal("Infinity"), expected: "Infinity"},
		}},
		{id: 4, fields: []bsonField{
			{name: "f_double", value: math.Inf(-1), expected: "-inf", expectedCDC: "NULL"},
			{name: "f_string", value: "-infinity", expected: "-infinity"},
			{
				name: "f_object",
				value: bson.D{
					{Key: "a", Value: int32(-1)}, {Key: "b", Value: "-inf"}, {Key: "c", Value: bson.D{{Key: "d", Value: false}}},
				},
				expected: `{"a":-1,"b":"-inf","c":{"d":false}}`,
			},
			{name: "f_array", value: bson.A{int32(-1), int32(1)}, expected: "[-1,1]"},
			{
				name:     "f_binary",
				value:    bson.Binary{Subtype: bson.TypeBinaryGeneric, Data: []byte{0xff}},
				expected: `{"Data":"\/w==","Subtype":0}`,
			},
			{
				name:     "f_uuid",
				value:    uuidBinary("80000000-0000-0000-0000-000000000000"),
				expected: `{"Data":"gAAAAAAAAAAAAAAAAAAAAA==","Subtype":4}`,
			},
			{name: "f_objectid", value: objectID("000000000000000000000003"), expected: "000000000000000000000003"},
			{name: "f_bool", value: false, expected: "false"},
			// the upper bound of DateTime64 at millisecond precision
			{
				name:     "f_date",
				value:    bson.NewDateTimeFromTime(time.Date(2299, 12, 31, 23, 59, 59, 999*int(time.Millisecond), time.UTC)),
				expected: "2299-12-31 23:59:59.999000",
			},
			{name: "f_regex", value: bson.Regex{Pattern: "-inf", Options: "i"}, expected: `{"Options":"i","Pattern":"-inf"}`},
			{name: "f_javascript", value: bson.JavaScript("-Infinity"), expected: "-Infinity"},
			{name: "f_int", value: int32(-1), expected: "-1"},
			{name: "f_timestamp", value: bson.Timestamp{T: 0, I: math.MaxUint32}, expected: `{"I":4294967295,"T":0}`},
			{name: "f_long", value: int64(-1), expected: "-1"},
			{name: "f_decimal", value: decimal("-Infinity"), expected: "-Infinity"},
		}},
		{id: 5, fields: []bsonField{
			{name: "f_double", value: math.NaN(), expected: "nan", expectedCDC: "NULL"},
			{name: "f_string", value: "NaN", expected: "NaN"},
			{
				name: "f_object",
				value: bson.D{
					{Key: "a", Value: int32(42)}, {Key: "b", Value: "nan"}, {Key: "c", Value: bson.D{{Key: "d", Value: true}}},
				},
				expected: `{"a":42,"b":"nan","c":{"d":true}}`,
			},
			{name: "f_array", value: bson.A{int32(42), int32(-42)}, expected: "[42,-42]"},
			{
				name:     "f_binary",
				value:    bson.Binary{Subtype: bson.TypeBinaryGeneric, Data: []byte{0x00, 0x01}},
				expected: `{"Data":"AAE=","Subtype":0}`,
			},
			{
				name:     "f_uuid",
				value:    uuidBinary("7fffffff-ffff-ffff-ffff-ffffffffffff"),
				expected: `{"Data":"f\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/w==","Subtype":4}`,
			},
			{name: "f_objectid", value: objectID("7fffffffffffffffffffffff"), expected: "7fffffffffffffffffffffff"},
			{name: "f_bool", value: true, expected: "true"},
			// one millisecond before the epoch
			{name: "f_date", value: bson.DateTime(-1), expected: "1969-12-31 23:59:59.999000"},
			{name: "f_regex", value: bson.Regex{Pattern: "nan", Options: "m"}, expected: `{"Options":"m","Pattern":"nan"}`},
			{name: "f_javascript", value: bson.JavaScript("NaN"), expected: "NaN"},
			{name: "f_int", value: int32(42), expected: "42"},
			{name: "f_timestamp", value: bson.Timestamp{T: math.MaxInt32 + 1, I: 1}, expected: `{"I":1,"T":2147483648}`},
			// 2^53 + 1, the first integer a double cannot hold
			{name: "f_long", value: int64(1<<53 + 1), expected: "9007199254740993"},
			{name: "f_decimal", value: decimal("NaN"), expected: "NaN"},
		}},
	}

	dateBase := time.Date(2023, 12, 31, 12, 30, 45, 0, time.UTC)
	for i := int32(6); i < 20; i++ {
		// integral doubles come as int32, like from a client that does not tell them apart
		double := float64(i-6) * 1.25
		var doubleValue any = double
		if (i-6)%4 == 0 {
			doubleValue = int32(double)
		}
		var uuidBytes [16]byte
		uuidBytes[6], uuidBytes[8], uuidBytes[15] = 0x40, 0x80, byte(i)
		binary := fmt.Appendf(nil, "bytes_%d", i)
		date := dateBase.Add(time.Duration(i) * (24*time.Hour + time.Millisecond))
		documents = append(documents, bsonDocument{id: i, fields: []bsonField{
			{name: "f_double", value: doubleValue, expected: strconv.FormatFloat(double, 'g', -1, 64)},
			{name: "f_string", value: fmt.Sprintf("value_%d", i), expected: fmt.Sprintf("value_%d", i)},
			{
				name: "f_object",
				value: bson.D{
					{Key: "a", Value: i},
					{Key: "b", Value: fmt.Sprintf("nested_%d", i)},
					{Key: "c", Value: bson.D{{Key: "d", Value: i%2 == 0}}},
				},
				expected: fmt.Sprintf(`{"a":%d,"b":"nested_%d","c":{"d":%t}}`, i, i, i%2 == 0),
			},
			{name: "f_array", value: bson.A{i, i * 10}, expected: fmt.Sprintf("[%d,%d]", i, i*10)},
			{
				name:     "f_binary",
				value:    bson.Binary{Subtype: bson.TypeBinaryGeneric, Data: binary},
				expected: binaryJSON(bson.TypeBinaryGeneric, binary),
			},
			{
				name:     "f_uuid",
				value:    bson.Binary{Subtype: bson.TypeBinaryUUID, Data: uuidBytes[:]},
				expected: binaryJSON(bson.TypeBinaryUUID, uuidBytes[:]),
			},
			{
				name:     "f_objectid",
				value:    objectID(fmt.Sprintf("65a1b2c3d4e5f6a7b8c9d0%02x", i)),
				expected: fmt.Sprintf("65a1b2c3d4e5f6a7b8c9d0%02x", i),
			},
			{name: "f_bool", value: i%2 == 0, expected: strconv.FormatBool(i%2 == 0)},
			{name: "f_date", value: bson.NewDateTimeFromTime(date), expected: date.Format("2006-01-02 15:04:05.000000")},
			{
				name:     "f_regex",
				value:    bson.Regex{Pattern: fmt.Sprintf("^value_%d$", i), Options: "i"},
				expected: fmt.Sprintf(`{"Options":"i","Pattern":"^value_%d$"}`, i),
			},
			{
				name:     "f_javascript",
				value:    bson.JavaScript(fmt.Sprintf("function () { return %d; }", i)),
				expected: fmt.Sprintf("function () { return %d; }", i),
			},
			{name: "f_int", value: i * 1000, expected: strconv.Itoa(int(i) * 1000)},
			{
				name:     "f_timestamp",
				value:    bson.Timestamp{T: 1700000000 + uint32(i), I: uint32(i)},
				expected: fmt.Sprintf(`{"I":%d,"T":%d}`, i, 1700000000+i),
			},
			{name: "f_long", value: int64(i) * 1_000_000_000_000, expected: strconv.FormatInt(int64(i)*1_000_000_000_000, 10)},
			{name: "f_decimal", value: decimal(fmt.Sprintf("%d.%04d", i, i)), expected: fmt.Sprintf("%d.%04d", i, i)},
		}})
	}

	// deprecated types that inference and ingestion still agree on
	pointerID := objectID("65a1b2c3d4e5f6a7b8c9d0e1")
	for i := range documents {
		id := documents[i].id
		documents[i].fields = append(documents[i].fields, constants...)
		documents[i].fields = append(documents[i].fields,
			bsonField{
				name:     "f_dbpointer",
				value:    bson.DBPointer{DB: fmt.Sprintf("db.coll_%d", id), Pointer: pointerID},
				expected: fmt.Sprintf(`{"DB":"db.coll_%d","Pointer":"%s"}`, id, pointerID.Hex()),
			},
			bsonField{
				name: "f_codewithscope",
				value: bson.CodeWithScope{
					Code:  bson.JavaScript(fmt.Sprintf("function () { return x + %d; }", id)),
					Scope: bson.D{{Key: "x", Value: id}},
				},
				expected: fmt.Sprintf(`{"Code":"function () { return x + %d; }","Scope":{"x":%d}}`, id, id),
			},
			bsonField{name: "f_symbol", value: bson.Symbol(fmt.Sprintf("sym_%d", id)), expected: fmt.Sprintf("sym_%d", id)},
		)
	}
	return documents
}

func truncate(value string) string {
	const limit = 120
	if len(value) <= limit {
		return value
	}
	return fmt.Sprintf("%s... (%d bytes)", value[:limit], len(value))
}
