package connmongo

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils/structured"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func structuredTestColumns() []*protos.ColumnSetting {
	return []*protos.ColumnSetting{
		// as inferred schemas declare them: all nullable, a document may lack any field
		{SourceName: "name", DestinationType: "Nullable(String)"},
		{SourceName: "age", DestinationType: "Nullable(Int64)"},
		{SourceName: "score", DestinationType: "Nullable(Float64)"},
		{SourceName: "address", DestinationType: "Nullable(JSON)"},
	}
}

func TestGetStructuredSchema(t *testing.T) {
	projector, err := newStructuredSchemaProjector(structuredTestColumns(), true)
	require.NoError(t, err)

	require.Equal(t, []types.QField{
		{Name: DefaultDocumentKeyColumnName, Type: types.QValueKindString, Nullable: false},
		// schema columns in declaration order, all nullable
		{Name: "name", Type: types.QValueKindString, Nullable: true},
		{Name: "age", Type: types.QValueKindInt64, Nullable: true},
		{Name: "score", Type: types.QValueKindFloat64, Nullable: true},
		{Name: "address", Type: types.QValueKindJSON, Nullable: true},
		{Name: structured.MalformedDataColumn, Type: types.QValueKindJSON, Nullable: true},
	}, GetStructuredSchema(projector).Fields)
}

// TestGetTableSchemaStructured checks the table schema of a structured mapping carries the kinds the
// records are projected to, so the destination types come from the mapping's `destination_type` override,
// and that every column but the document key is nullable.
// TestStructuredProjectorPathsAgree checks the two projector construction paths cannot drift: QRep builds
// it from the mapping's columns, CDC from the table schema GetTableSchema emits for that same mapping.
// Both must project records with the same layout and kinds.
func TestStructuredProjectorPathsAgree(t *testing.T) {
	fromMapping, err := newStructuredSchemaProjector(structuredTestColumns(), true)
	require.NoError(t, err)

	schemas, err := (&MongoConnector{}).GetTableSchema(t.Context(), nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: "test.t", StructuredIngestion: true, Columns: structuredTestColumns()}})
	require.NoError(t, err)
	fromTableSchema, err := newStructuredSchemaProjectorFromTableSchema(schemas["test.t"], true)
	require.NoError(t, err)

	require.Equal(t, fromMapping.QRecordSchema(), fromTableSchema.QRecordSchema())
}

func TestGetTableSchemaStructured(t *testing.T) {
	structuredTable, plainTable := "test.structured", "test.plain"
	schemas, err := (&MongoConnector{}).GetTableSchema(t.Context(), nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{
			{SourceTableIdentifier: structuredTable, StructuredIngestion: true, Columns: structuredTestColumns()},
			{SourceTableIdentifier: plainTable},
		})
	require.NoError(t, err)

	fieldNamesAndTypes := func(schema *protos.TableSchema) [][2]string {
		out := make([][2]string, 0, len(schema.Columns))
		for _, column := range schema.Columns {
			out = append(out, [2]string{column.Name, column.Type})
		}
		return out
	}

	require.Equal(t, protos.TypeSystem_Q, schemas[structuredTable].System)
	require.True(t, schemas[structuredTable].NullableEnabled)
	require.Equal(t, [][2]string{
		{DefaultDocumentKeyColumnName, "string"},
		{"name", "string"},
		{"age", "int64"},
		{"score", "float64"},
		{"address", "json"},
		{structured.MalformedDataColumn, "json"},
	}, fieldNamesAndTypes(schemas[structuredTable]))
	for _, column := range schemas[structuredTable].Columns {
		// every column but the document key is nullable
		require.Equal(t, column.Name != DefaultDocumentKeyColumnName, column.Nullable, column.Name)
	}

	// a plain mapping is unaffected
	require.Equal(t, protos.TypeSystem_Q, schemas[plainTable].System)
	require.False(t, schemas[plainTable].NullableEnabled)
	require.Equal(t, [][2]string{
		{DefaultDocumentKeyColumnName, "string"},
		{DefaultFullDocumentColumnName, "json"},
	}, fieldNamesAndTypes(schemas[plainTable]))

	// an unsupported destination type fails schema resolution
	_, err = (&MongoConnector{}).GetTableSchema(t.Context(), nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{{
			SourceTableIdentifier: structuredTable, StructuredIngestion: true,
			Columns: []*protos.ColumnSetting{{SourceName: "location", DestinationType: "Point"}},
		}})
	require.ErrorContains(t, err, structuredTable)
	require.ErrorContains(t, err, "Point")
}

func TestNewStructuredSchemaProjectorRejects(t *testing.T) {
	for name, tc := range map[string]struct {
		columns  []*protos.ColumnSetting
		offender string
	}{
		"unknown type": {
			[]*protos.ColumnSetting{{SourceName: "location", DestinationType: "Point"}}, "location",
		},
		"duplicate column": {
			[]*protos.ColumnSetting{{SourceName: "age", DestinationType: "String"}, {SourceName: "age", DestinationType: "Int64"}}, "age",
		},
		"malformed data column": {
			[]*protos.ColumnSetting{{SourceName: structured.MalformedDataColumn, DestinationType: "JSON"}}, structured.MalformedDataColumn,
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := newStructuredSchemaProjector(tc.columns, true)
			require.ErrorContains(t, err, tc.offender)
		})
	}
}

func TestStructuredQValuesFromBsonRaw(t *testing.T) {
	oid, err := bson.ObjectIDFromHex("507f1f77bcf86cd799439011")
	require.NoError(t, err)
	projector, err := newStructuredSchemaProjector(structuredTestColumns(), true)
	require.NoError(t, err)
	schema := GetStructuredSchema(projector)
	converter := NewDirectBsonConverter()

	toRecord := func(t *testing.T, doc bson.D) []types.QValue {
		raw, err := bson.Marshal(doc)
		require.NoError(t, err)
		record, err := StructuredQValuesFromBsonRaw(raw, shared.InternalVersion_Latest, converter, projector, "db.coll")
		require.NoError(t, err)
		require.Len(t, record, len(schema.Fields))
		return record
	}
	// record values by schema field name, for readable assertions
	byName := func(record []types.QValue) map[string]types.QValue {
		values := make(map[string]types.QValue, len(record))
		for i, field := range schema.Fields {
			values[field.Name] = record[i]
		}
		return values
	}

	t.Run("well formed document", func(t *testing.T) {
		values := byName(toRecord(t, bson.D{
			{Key: "_id", Value: oid},
			{Key: "name", Value: "Ada"},
			{Key: "age", Value: int32(36)},
			{Key: "score", Value: nil},
			{Key: "address", Value: bson.D{{Key: "city", Value: "London"}}},
		}))
		require.Equal(t, types.QValueString{Val: "507f1f77bcf86cd799439011"}, values["_id"])
		require.Equal(t, types.QValueString{Val: "Ada"}, values["name"])
		require.Equal(t, types.QValueInt64{Val: 36}, values["age"])
		// a BSON null takes the kind of its column
		require.Equal(t, types.QValueNull(types.QValueKindFloat64), values["score"])
		require.Equal(t, types.QValueJSON{Val: `{"city":"London"}`}, values["address"])
		// nothing malformed, so the column is null
		require.Equal(t, types.QValueNull(types.QValueKindJSON), values[structured.MalformedDataColumn])
	})

	t.Run("absent columns are null", func(t *testing.T) {
		values := byName(toRecord(t, bson.D{{Key: "_id", Value: oid}}))
		require.Equal(t, types.QValueNull(types.QValueKindString), values["name"])
		require.Equal(t, types.QValueNull(types.QValueKindInt64), values["age"])
		require.Equal(t, types.QValueNull(types.QValueKindFloat64), values["score"])
		require.Equal(t, types.QValueNull(types.QValueKindJSON), values["address"])
		require.Equal(t, types.QValueNull(types.QValueKindJSON), values[structured.MalformedDataColumn])
	})

	t.Run("malformed fields", func(t *testing.T) {
		values := byName(toRecord(t, bson.D{
			{Key: "_id", Value: oid},
			{Key: "name", Value: "Ada"},
			{Key: "age", Value: "thirty six"},        // type mismatch: String into Int64
			{Key: "email", Value: "ada@example.com"}, // not in the schema
		}))
		require.Equal(t, types.QValueString{Val: "Ada"}, values["name"])
		// a mismatched column is left null and reported instead
		require.Equal(t, types.QValueNull(types.QValueKindInt64), values["age"])

		malformed, ok := values[structured.MalformedDataColumn].(types.QValueJSON)
		require.True(t, ok, "malformed data should be recorded as JSON")
		var reported map[string]map[string]map[string]any
		require.NoError(t, json.Unmarshal([]byte(malformed.Val), &reported))
		require.Equal(t, map[string]map[string]any{
			"age":   {"type_mismatch": true, "value": "thirty six"},
			"email": {"unexpected": true, "value": "ada@example.com"},
		}, reported["malformed_data"])
	})

	t.Run("missing id", func(t *testing.T) {
		raw, err := bson.Marshal(bson.D{{Key: "name", Value: "Ada"}})
		require.NoError(t, err)
		_, err = StructuredQValuesFromBsonRaw(raw, shared.InternalVersion_Latest, converter, projector, "db.coll")
		require.Error(t, err)
	})
}

// collectDocumentQValues walks doc, returning the yielded fields in order and the walk error.
func collectDocumentQValues(t *testing.T, doc bson.D) ([]string, map[string]types.QValue, error) {
	t.Helper()
	raw, err := bson.Marshal(doc)
	require.NoError(t, err)

	fields, walkErr := DocumentQValueIterator(raw, NewDirectBsonConverter())
	names := []string{}
	values := map[string]types.QValue{}
	for field, value := range fields {
		names = append(names, field)
		values[field] = value
	}
	return names, values, walkErr()
}

func TestDocumentQValueIterator(t *testing.T) {
	oid, err := bson.ObjectIDFromHex("507f1f77bcf86cd799439011")
	require.NoError(t, err)

	names, values, err := collectDocumentQValues(t, bson.D{
		{Key: "_id", Value: oid},
		{Key: "name", Value: "Ada"},
		{Key: "age", Value: int32(36)},
		{Key: "big", Value: int64(1) << 40},
		{Key: "score", Value: 9.5},
		{Key: "active", Value: true},
		{Key: "nickname", Value: nil},
		{Key: "address", Value: bson.D{{Key: "city", Value: "London"}}},
		{Key: "tags", Value: bson.A{"math", "cs"}},
	})
	require.NoError(t, err)

	// the document key is excluded, the rest is yielded in document order
	require.Equal(t, []string{"name", "age", "big", "score", "active", "nickname", "address", "tags"}, names)
	require.Equal(t, types.QValueString{Val: "Ada"}, values["name"])
	require.Equal(t, types.QValueInt64{Val: 36}, values["age"])
	require.Equal(t, types.QValueInt64{Val: 1 << 40}, values["big"])
	require.Equal(t, types.QValueFloat64{Val: 9.5}, values["score"])
	require.Equal(t, types.QValueBoolean{Val: true}, values["active"])
	// nulls carry no kind, the consumer gives them the kind of the column they land in
	require.Equal(t, types.QValueNull(types.QValueKindInvalid), values["nickname"])
	// embedded documents and arrays are yielded whole, as JSON
	require.Equal(t, types.QValueJSON{Val: `{"city":"London"}`}, values["address"])
	require.Equal(t, types.QValueJSON{Val: `["math","cs"]`, IsArray: true}, values["tags"])
}

func TestDocumentQValueIteratorEmptyAndKeyOnly(t *testing.T) {
	for name, doc := range map[string]bson.D{
		"empty document": {},
		"key only":       {{Key: "_id", Value: "the-key"}},
	} {
		t.Run(name, func(t *testing.T) {
			names, _, err := collectDocumentQValues(t, doc)
			require.NoError(t, err)
			require.Empty(t, names)
		})
	}
}

// TestDocumentQValueIteratorStopsEarly checks the walk honours a consumer breaking out of the range loop, which
// ProjectRecord does not do but iter.Seq2 allows.
func TestDocumentQValueIteratorStopsEarly(t *testing.T) {
	raw, err := bson.Marshal(bson.D{
		{Key: "first", Value: 1},
		{Key: "second", Value: 2},
		{Key: "third", Value: 3},
	})
	require.NoError(t, err)

	fields, walkErr := DocumentQValueIterator(raw, NewDirectBsonConverter())
	var seen []string
	for field := range fields {
		seen = append(seen, field)
		if len(seen) == 2 {
			break
		}
	}
	require.Equal(t, []string{"first", "second"}, seen)
	require.NoError(t, walkErr())
}

func TestDocumentQValueIteratorMalformedDocument(t *testing.T) {
	// a length header longer than the buffer: elements cannot be read
	names, _, err := func() ([]string, map[string]types.QValue, error) {
		fields, walkErr := DocumentQValueIterator(bson.Raw{0xff, 0x00, 0x00, 0x00, 0x00}, NewDirectBsonConverter())
		names := []string{}
		values := map[string]types.QValue{}
		for field, value := range fields {
			names = append(names, field)
			values[field] = value
		}
		return names, values, walkErr()
	}()
	require.ErrorContains(t, err, "failed to read document fields")
	require.Empty(t, names)
}

// TestDocumentQValueIteratorConversionError checks a converter failure stops the walk and is reported, rather
// than being swallowed by the iterator.
func TestDocumentQValueIteratorConversionError(t *testing.T) {
	raw, err := bson.Marshal(bson.D{
		{Key: "ok", Value: "value"},
		{Key: "boom", Value: "value"},
		{Key: "unreached", Value: "value"},
	})
	require.NoError(t, err)

	converter := &failingConverter{BsonToQValueConverter: NewDirectBsonConverter(), failOnCall: 2}
	fields, walkErr := DocumentQValueIterator(raw, converter)
	var seen []string
	for field := range fields {
		seen = append(seen, field)
	}
	require.Equal(t, []string{"ok"}, seen)
	require.ErrorIs(t, walkErr(), errConversionFailed)
	require.ErrorContains(t, walkErr(), "boom")
}

// failingConverter fails the nth call to QValueFromBsonValue, delegating everything else.
type failingConverter struct {
	BsonToQValueConverter
	failOnCall int
	calls      int
}

func (c *failingConverter) QValueFromBsonValue(rv bson.RawValue, nullKind types.QValueKind) (types.QValue, error) {
	c.calls += 1
	if c.calls == c.failOnCall {
		return nil, errConversionFailed
	}
	return c.BsonToQValueConverter.QValueFromBsonValue(rv, nullKind)
}

var errConversionFailed = errors.New("conversion failed")
