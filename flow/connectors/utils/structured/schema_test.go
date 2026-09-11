package structured

import (
	"fmt"
	"iter"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// testSchemaToQKind resolves the fake schema types these tests declare columns with, failing on
// anything else.
func testSchemaToQKind(schemaType string) (types.QValueKind, error) {
	kind, ok := map[string]types.QValueKind{
		"str": types.QValueKindString,
		"i64": types.QValueKindInt64,
		"f64": types.QValueKindFloat64,
	}[schemaType]
	if !ok {
		return types.QValueKindInvalid, fmt.Errorf("unknown schema type %s", schemaType)
	}
	return kind, nil
}

func testProjectorColumns() []*protos.ColumnSetting {
	return []*protos.ColumnSetting{
		{SourceName: "name", DestinationType: "str"},
		{SourceName: "age", DestinationType: "i64"},
		{SourceName: "score", DestinationType: "f64"},
	}
}

type recordField struct {
	name  string
	value types.QValue
}

// recordOf is a record yielding the given fields in order, as ProjectRecord consumes them.
func recordOf(fields ...recordField) iter.Seq2[string, types.QValue] {
	return func(yield func(string, types.QValue) bool) {
		for _, field := range fields {
			if !yield(field.name, field.value) {
				return
			}
		}
	}
}

func TestNewSchemaProjector(t *testing.T) {
	projector, err := NewSchemaProjector(testSchemaToQKind, testProjectorColumns(), true)
	require.NoError(t, err)

	// the record schema is the columns in their declared order with the kinds schemaToQKind resolved,
	// all nullable as a record may lack any of them, followed by the malformed data column
	recordFields := []types.QField{
		{Name: "name", Type: types.QValueKindString, Nullable: true},
		{Name: "age", Type: types.QValueKindInt64, Nullable: true},
		{Name: "score", Type: types.QValueKindFloat64, Nullable: true},
		{Name: MalformedDataColumn, Type: types.QValueKindJSON, Nullable: true},
	}
	require.Equal(t, recordFields, projector.QRecordSchema().Fields)
}

func TestNewSchemaProjectorRejects(t *testing.T) {
	for name, tc := range map[string]struct {
		columns  []*protos.ColumnSetting
		offender string
	}{
		"unresolvable type": {
			[]*protos.ColumnSetting{{SourceName: "location", DestinationType: "point"}}, "schema column location",
		},
		"duplicate column": {
			[]*protos.ColumnSetting{{SourceName: "age", DestinationType: "str"}, {SourceName: "age", DestinationType: "i64"}}, "age",
		},
		"collision with malformed data column": {
			[]*protos.ColumnSetting{{SourceName: MalformedDataColumn, DestinationType: "str"}}, MalformedDataColumn,
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewSchemaProjector(testSchemaToQKind, tc.columns, true)
			require.ErrorContains(t, err, tc.offender)
		})
	}
}

func TestProjectRecord(t *testing.T) {
	projector, err := NewSchemaProjector(testSchemaToQKind, testProjectorColumns(), true)
	require.NoError(t, err)

	t.Run("complete record", func(t *testing.T) {
		// record field order does not matter: values land in their column's slot
		values, err := projector.ProjectRecord(recordOf(
			recordField{"score", types.QValueFloat64{Val: 9.75}},
			recordField{"name", types.QValueString{Val: "Ada"}},
			recordField{"age", types.QValueInt64{Val: 36}},
		))
		require.NoError(t, err)
		require.Equal(t, []types.QValue{
			types.QValueString{Val: "Ada"},
			types.QValueInt64{Val: 36},
			types.QValueFloat64{Val: 9.75},
			types.QValueNull(types.QValueKindJSON), // nothing malformed
		}, values)
	})

	t.Run("absent columns are null of their kind", func(t *testing.T) {
		values, err := projector.ProjectRecord(recordOf(recordField{"name", types.QValueString{Val: "Ada"}}))
		require.NoError(t, err)
		require.Equal(t, []types.QValue{
			types.QValueString{Val: "Ada"},
			types.QValueNull(types.QValueKindInt64),
			types.QValueNull(types.QValueKindFloat64),
			types.QValueNull(types.QValueKindJSON),
		}, values)
	})

	t.Run("a null fits any column, keeping the column's kind", func(t *testing.T) {
		values, err := projector.ProjectRecord(recordOf(recordField{"age", types.QValueNull(types.QValueKindString)}))
		require.NoError(t, err)
		require.Equal(t, types.QValueNull(types.QValueKindInt64), values[1])
		// and is not reported as a mismatch
		require.Equal(t, types.QValueNull(types.QValueKindJSON), values[3])
	})

	t.Run("mismatched and unexpected fields are malformed data", func(t *testing.T) {
		values, err := projector.ProjectRecord(recordOf(
			recordField{"age", types.QValueString{Val: "thirty six"}},        // type mismatch: string into an int64 column
			recordField{"email", types.QValueString{Val: "ada@example.com"}}, // not in the schema
		))
		require.NoError(t, err)
		// the mismatched column is left null and reported instead
		require.Equal(t, types.QValueNull(types.QValueKindInt64), values[1])
		malformed, ok := values[3].(types.QValueJSON)
		require.True(t, ok, "malformed data should be recorded as JSON")
		require.JSONEq(t, `{
			"age": {"type_mismatch": true, "value": "thirty six"},
			"email": {"unexpected": true, "value": "ada@example.com"}
		}`, malformed.Val)
	})

	t.Run("shouldRecordValues=false omits mismatched, unexpected and duplicated values", func(t *testing.T) {
		blind, err := NewSchemaProjector(testSchemaToQKind, testProjectorColumns(), false)
		require.NoError(t, err)
		values, err := blind.ProjectRecord(recordOf(
			recordField{"age", types.QValueString{Val: "thirty six"}},
			recordField{"email", types.QValueString{Val: "ada@example.com"}},
			recordField{"name", types.QValueString{Val: "Ada"}},
			recordField{"name", types.QValueString{Val: "Lovelace"}},
		))
		require.NoError(t, err)
		malformed, ok := values[3].(types.QValueJSON)
		require.True(t, ok)
		// none of the mismatched, unexpected or duplicated fields leaks its source value
		require.JSONEq(t, `{
			"age": {"type_mismatch": true},
			"email": {"unexpected": true},
			"name": {"duplicated_fields": true}
		}`, malformed.Val)
	})

	t.Run("a duplicated field keeps its first value and reports the later one", func(t *testing.T) {
		values, err := projector.ProjectRecord(recordOf(
			recordField{"name", types.QValueString{Val: "Ada"}},
			recordField{"age", types.QValueInt64{Val: 36}},
			recordField{"name", types.QValueString{Val: "Lovelace"}},
		))
		require.NoError(t, err)
		require.Equal(t, types.QValueString{Val: "Ada"}, values[0])
		require.Equal(t, types.QValueInt64{Val: 36}, values[1])
		malformed, ok := values[3].(types.QValueJSON)
		require.True(t, ok, "malformed data should be recorded as JSON")
		require.JSONEq(t, `{"name": {"duplicated_fields": true, "value": "Lovelace"}}`, malformed.Val)
	})

	t.Run("a field repeated more than twice is reported with its last value", func(t *testing.T) {
		values, err := projector.ProjectRecord(recordOf(
			recordField{"name", types.QValueString{Val: "Ada"}},
			recordField{"name", types.QValueString{Val: "Lovelace"}},
			recordField{"name", types.QValueString{Val: "Byron"}},
		))
		require.NoError(t, err)
		require.Equal(t, types.QValueString{Val: "Ada"}, values[0])
		malformed, ok := values[3].(types.QValueJSON)
		require.True(t, ok)
		require.JSONEq(t, `{"name": {"duplicated_fields": true, "value": "Byron"}}`, malformed.Val)
	})

	t.Run("a duplicate of a null first occurrence is reported and does not fill the column", func(t *testing.T) {
		values, err := projector.ProjectRecord(recordOf(
			recordField{"age", types.QValueNull(types.QValueKindInt64)},
			recordField{"age", types.QValueInt64{Val: 36}},
		))
		require.NoError(t, err)
		// the first occurrence, a null, is the one projected
		require.Equal(t, types.QValueNull(types.QValueKindInt64), values[1])
		malformed, ok := values[3].(types.QValueJSON)
		require.True(t, ok)
		require.JSONEq(t, `{"age": {"duplicated_fields": true, "value": 36}}`, malformed.Val)
	})

	t.Run("a duplicate of a mismatched first occurrence is reported and does not fill the column", func(t *testing.T) {
		values, err := projector.ProjectRecord(recordOf(
			recordField{"age", types.QValueString{Val: "thirty six"}},
			recordField{"age", types.QValueInt64{Val: 36}},
		))
		require.NoError(t, err)
		// the column stays null: the first occurrence was mismatched and the second is a duplicate
		require.Equal(t, types.QValueNull(types.QValueKindInt64), values[1])
		malformed, ok := values[3].(types.QValueJSON)
		require.True(t, ok)
		// malformed data holds a single reason per field, so the duplicate supersedes the mismatch
		require.JSONEq(t, `{"age": {"duplicated_fields": true, "value": 36}}`, malformed.Val)
	})

	t.Run("a duplicated unexpected field is reported as duplicated", func(t *testing.T) {
		values, err := projector.ProjectRecord(recordOf(
			recordField{"email", types.QValueString{Val: "ada@example.com"}},
			recordField{"email", types.QValueString{Val: "lovelace@example.com"}},
		))
		require.NoError(t, err)
		malformed, ok := values[3].(types.QValueJSON)
		require.True(t, ok)
		require.JSONEq(t, `{"email": {"duplicated_fields": true, "value": "lovelace@example.com"}}`, malformed.Val)
	})
}
