package connmongo

import (
	"encoding/json"
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
		{SourceName: "name", DestinationType: "String"},
		{SourceName: "age", DestinationType: "Int64"},
		{SourceName: "score", DestinationType: "Nullable(Float64)"},
		{SourceName: "address", DestinationType: "JSON"},
	}
}

func TestGetStructuredSchema(t *testing.T) {
	projector, err := newStructuredSchemaProjector(structuredTestColumns())
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
			_, err := newStructuredSchemaProjector(tc.columns)
			require.ErrorContains(t, err, tc.offender)
		})
	}
}

func TestStructuredQValuesFromBsonRaw(t *testing.T) {
	oid, err := bson.ObjectIDFromHex("507f1f77bcf86cd799439011")
	require.NoError(t, err)
	projector, err := newStructuredSchemaProjector(structuredTestColumns())
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
