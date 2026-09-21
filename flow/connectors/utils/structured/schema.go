package structured

import (
	"fmt"
	"iter"
	"slices"

	connclickhouse "github.com/PeerDB-io/peerdb/flow/connectors/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// schemaColumn is a schema column resolved once at construction: the kind its values must have and its
// position in the records ProjectRecord produces.
type schemaColumn struct {
	kind  types.QValueKind
	index int
}

type SchemaProjector struct {
	// Schema columns by the record field they read from.
	columns map[string]schemaColumn
	// Record fields in order: the schema columns as declared, then the malformed data column.
	fields []types.QField
	// Position of the malformed data column in fields, and so in the records ProjectRecord produces.
	malformedDataIndex int
	// Whether malformed data entries carry the offending source value, both for mismatched and for
	// unexpected fields
	shouldRecordValues bool
}

// NewSchemaProjector resolves the schema columns' kinds through schemaToQKind, failing on a type it does
// not know, a column declared twice or one named as the malformed data column.
func NewSchemaProjector(
	schemaToQKind func(schemaType string) (types.QValueKind, error),
	schemaColumns []*protos.ColumnSetting,
	shouldRecordValues bool,
) (*SchemaProjector, error) {
	schemaFields := make([]types.QField, 0, len(schemaColumns))

	for _, column := range schemaColumns {
		kind, err := schemaToQKind(column.DestinationType)
		if err != nil {
			return nil, fmt.Errorf("schema column %s: %w", column.SourceName, err)
		}
		schemaFields = append(schemaFields, types.QField{Name: column.SourceName, Type: kind})
	}

	return NewSchemaProjectorFromQFields(schemaFields, shouldRecordValues)
}

// NewSchemaProjectorFromQFields builds a projector for schema columns whose QKinds are already resolved.
func NewSchemaProjectorFromQFields(schemaFields []types.QField, shouldRecordValues bool) (*SchemaProjector, error) {
	fields := make([]types.QField, 0, len(schemaFields)+1)
	columns := make(map[string]schemaColumn, len(schemaFields))

	for _, field := range schemaFields {
		if _, duplicate := columns[field.Name]; duplicate {
			return nil, fmt.Errorf("schema column %s is declared more than once", field.Name)
		}
		if field.Name == MalformedDataColumn {
			return nil, fmt.Errorf("schema column %s clashes with the malformed data column", field.Name)
		}
		columns[field.Name] = schemaColumn{kind: field.Type, index: len(fields)}
		// All structured columns are nullable as a record may lack any of the columns
		fields = append(fields, types.QField{Name: field.Name, Type: field.Type, Nullable: true})
	}

	malformedData := MalformedDataFieldDescription()
	malformedDataIndex := len(fields)
	fields = append(fields, types.QField{
		Name:     malformedData.Name,
		Type:     types.QValueKind(malformedData.Type),
		Nullable: malformedData.Nullable,
	})

	return &SchemaProjector{
		fields:             fields,
		malformedDataIndex: malformedDataIndex,
		columns:            columns,
		shouldRecordValues: shouldRecordValues,
	}, nil
}

// NewSchemaProjectorFromCHtoQValue is NewSchemaProjector with connclickhouse.QValueKindForType as the
// schema type to QValueKind conversion, i.e. for schemas declared with ClickHouse column types. It is
// the same resolution connclickhouse.GetTableSchemaForTable applies when reading a table's schema.
func NewSchemaProjectorFromCHtoQValue(
	schemaColumns []*protos.ColumnSetting,
	shouldRecordValues bool,
) (*SchemaProjector, error) {
	return NewSchemaProjector(connclickhouse.QValueKindForType, schemaColumns, shouldRecordValues)
}

// QRecordSchema is the schema of the records ProjectRecord produces: the schema columns as declared
// followed by the malformed data column.
func (sc *SchemaProjector) QRecordSchema() types.QRecordSchema {
	return types.NewQRecordSchema(slices.Clone(sc.fields))
}

// ProjectRecord projects a record onto the schema, laid out as QRecordSchema. Every schema column gets
// the record's value, or a null of the column's kind when the record lacks the field or its value does
// not match the column's kind. Mismatched values, record fields absent from the schema and repeated
// record fields are reported in the malformed data column.
// When a field is repeated, its first occurrence is the one projected and the later ones are the ones
// reported as malformed, this is an indication of broken upstream data at source or bugs in the
// connector.
func (sc *SchemaProjector) ProjectRecord(record iter.Seq2[string, types.QValue]) ([]types.QValue, error) {
	values := make([]types.QValue, len(sc.fields))
	seenFields := make(map[string]struct{}, len(sc.fields))
	for i, field := range sc.fields {
		values[i] = types.QValueNull(field.Type)
	}
	malformedData := NewMalformedData()

	for field, value := range record {
		// Only the first occurrence of a field is projected, whatever became of it: any later one is
		// recorded as malformed data.
		if _, seen := seenFields[field]; seen {
			var recordedValue types.QValue
			if sc.shouldRecordValues {
				recordedValue = value
			}
			malformedData.AddField(field, ReasonDuplicatedFields, recordedValue)
			continue
		}
		seenFields[field] = struct{}{}

		column, isSchemaColumn := sc.columns[field]

		// Record fields not present in the schema are recorded as malformed data.
		if !isSchemaColumn {
			var recordedValue types.QValue
			if sc.shouldRecordValues {
				recordedValue = value
			}
			malformedData.AddField(field, ReasonUnexpected, recordedValue)
			continue
		}

		// A null fits any column, and the slot already holds one of the column's kind.
		if _, isNull := value.(types.QValueNull); isNull {
			continue
		}

		// Otherwise the kinds must match, or the value is recorded as malformed data.
		// NOTE: The equals comparison canbe replaced by a call to a equivalence function.
		if column.kind != value.Kind() {
			var recordedValue types.QValue
			if sc.shouldRecordValues {
				recordedValue = value
			}
			malformedData.AddField(field, ReasonTypeMismatch, recordedValue)
			continue
		}

		values[column.index] = value
	}

	if !malformedData.IsEmpty() {
		qValue, err := malformedData.AsQValue()
		if err != nil {
			return nil, err
		}
		values[sc.malformedDataIndex] = qValue
	}

	return values, nil
}

// ApplyRecordSchema is ProjectRecord for consumers taking records as RecordItems, such as CDC.
func (sc *SchemaProjector) ApplyRecordSchema(record iter.Seq2[string, types.QValue]) (model.RecordItems, error) {
	values, err := sc.ProjectRecord(record)
	if err != nil {
		return model.RecordItems{}, err
	}
	result := model.NewRecordItems(len(values))
	for i, field := range sc.fields {
		result.AddColumn(field.Name, values[i])
	}
	return result, nil
}
