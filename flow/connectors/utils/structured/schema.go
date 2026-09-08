package structured

import (
	"fmt"
	"iter"
	"slices"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// defaultSchemaToQKind maps a ClickHouse column type to the QValueKind expected for its values.
//
// TODO: this is a temporary copy of the conversion in the switch at
// connclickhouse.GetTableSchemaForTable (flow/connectors/clickhouse/clickhouse.go), which is not
// reusable from here without importing the whole ClickHouse connector. Both need to be unified.
func defaultSchemaToQKind(schemaType string) (types.QValueKind, error) {
	switch schemaType {
	case "String", "Nullable(String)", "LowCardinality(String)", "LowCardinality(Nullable(String))":
		return types.QValueKindString, nil
	case "Bool", "Nullable(Bool)":
		return types.QValueKindBoolean, nil
	case "Int8", "Nullable(Int8)":
		return types.QValueKindInt8, nil
	case "Int16", "Nullable(Int16)":
		return types.QValueKindInt16, nil
	case "Int32", "Nullable(Int32)":
		return types.QValueKindInt32, nil
	case "Int64", "Nullable(Int64)":
		return types.QValueKindInt64, nil
	case "Int256", "Nullable(Int256)":
		return types.QValueKindInt256, nil
	case "UInt8", "Nullable(UInt8)":
		return types.QValueKindUInt8, nil
	case "UInt16", "Nullable(UInt16)":
		return types.QValueKindUInt16, nil
	case "UInt32", "Nullable(UInt32)":
		return types.QValueKindUInt32, nil
	case "UInt64", "Nullable(UInt64)":
		return types.QValueKindUInt64, nil
	case "UInt256", "Nullable(UInt256)":
		return types.QValueKindUInt256, nil
	case "UUID", "Nullable(UUID)":
		return types.QValueKindUUID, nil
	case "DateTime64(6)", "Nullable(DateTime64(6))", "DateTime64(9)", "Nullable(DateTime64(9))":
		return types.QValueKindTimestamp, nil
	case "Time64(6)", "Nullable(Time64(6))":
		return types.QValueKindTime, nil
	case "Date32", "Nullable(Date32)":
		return types.QValueKindDate, nil
	case "Float32", "Nullable(Float32)":
		return types.QValueKindFloat32, nil
	case "Float64", "Nullable(Float64)":
		return types.QValueKindFloat64, nil
	case "Array(Int32)":
		return types.QValueKindArrayInt32, nil
	case "Array(Float32)":
		return types.QValueKindArrayFloat32, nil
	case "Array(Float64)":
		return types.QValueKindArrayFloat64, nil
	case "Array(String)", "Array(LowCardinality(String))":
		return types.QValueKindArrayString, nil
	case "Array(UUID)":
		return types.QValueKindArrayUUID, nil
	case "Array(DateTime64(6))":
		return types.QValueKindArrayTimestamp, nil
	case "Array(Int64)":
		return types.QValueKindArrayInt64, nil
	case "Array(Bool)":
		return types.QValueKindArrayBoolean, nil
	case "Array(Date)":
		return types.QValueKindArrayDate, nil
	case "JSON", "Nullable(JSON)":
		return types.QValueKindJSON, nil
	default:
		if strings.Contains(schemaType, "Decimal") {
			if strings.HasPrefix(schemaType, "Array(") {
				return types.QValueKindArrayNumeric, nil
			}
			return types.QValueKindNumeric, nil
		}
		return types.QValueKindInvalid, fmt.Errorf("failed to resolve QValueKind for %s", schemaType)
	}
}

// schemaColumn is a schema column resolved once at construction: the kind its values must have and its
// position in the records ProjectRecord produces.
type schemaColumn struct {
	kind  types.QValueKind
	index int
}

type SchemaProjector struct {
	// Record fields in order: the schema columns as declared, then the malformed data column.
	fields []types.QField
	// Schema columns by the record field they read from.
	columns            map[string]schemaColumn
	shouldRecordValues bool
}

// NewSchemaProjector resolves the schema columns' kinds through schemaToQKind, failing on a type it does
// not know, a column declared twice or one named as the malformed data column.
func NewSchemaProjector(
	schemaToQKind func(schemaType string) (types.QValueKind, error),
	schemaColumns []*protos.ColumnSetting,
	shouldRecordValues bool,
) (*SchemaProjector, error) {
	fields := make([]types.QField, 0, len(schemaColumns)+1)
	columns := make(map[string]schemaColumn, len(schemaColumns))

	// TODO: Potentially handle columns renames.

	for _, column := range schemaColumns {
		kind, err := schemaToQKind(column.DestinationType)
		if err != nil {
			return nil, fmt.Errorf("schema column %s: %w", column.SourceName, err)
		}
		if _, duplicate := columns[column.SourceName]; duplicate {
			return nil, fmt.Errorf("schema column %s is declared more than once", column.SourceName)
		}
		if column.SourceName == MalformedDataColumn {
			return nil, fmt.Errorf("schema column %s clashes with the malformed data column", column.SourceName)
		}
		columns[column.SourceName] = schemaColumn{kind: kind, index: len(fields)}
		// nullable as a record may lack any of the columns
		fields = append(fields, types.QField{Name: column.SourceName, Type: kind, Nullable: true})
	}

	malformedData := MalformedDataFieldDescription()
	fields = append(fields, types.QField{
		Name:     malformedData.Name,
		Type:     types.QValueKind(malformedData.Type),
		Nullable: malformedData.Nullable,
	})

	return &SchemaProjector{
		fields:             fields,
		columns:            columns,
		shouldRecordValues: shouldRecordValues,
	}, nil
}

// NewSchemaProjectorWithDefaultSchemaToKind is NewSchemaProjector with defaultSchemaToQKind as the
// schema type to QValueKind conversion, i.e. for schemas declared with ClickHouse column types.
func NewSchemaProjectorWithDefaultSchemaToKind(
	schemaColumns []*protos.ColumnSetting,
	shouldRecordValues bool,
) (*SchemaProjector, error) {
	return NewSchemaProjector(defaultSchemaToQKind, schemaColumns, shouldRecordValues)
}

// Columns are the structured schema (order is relevant).
func (sc *SchemaProjector) Columns() []types.QField {
	return slices.Clone(sc.fields[:len(sc.fields)-1])
}

// QRecordSchema is the schema of the records ProjectRecord produces: Columns followed by the malformed
// data column.
func (sc *SchemaProjector) QRecordSchema() types.QRecordSchema {
	return types.NewQRecordSchema(slices.Clone(sc.fields))
}

// ProjectRecord projects a record onto the schema, laid out as QRecordSchema. Every schema column gets
// the record's value, or a null of the column's kind when the record lacks the field or its value does
// not match the column's kind. Mismatched values and record fields absent from the schema are reported
// in the malformed data column.
func (sc *SchemaProjector) ProjectRecord(record iter.Seq2[string, types.QValue]) ([]types.QValue, error) {
	values := make([]types.QValue, len(sc.fields))
	for i, field := range sc.fields {
		values[i] = types.QValueNull(field.Type)
	}
	malformedData := NewMalformedData()

	for field, value := range record {
		column, isSchemaColumn := sc.columns[field]

		// Record fields not present in the schema are recorded as malformed data.
		if !isSchemaColumn {
			malformedData.AddField(field, ReasonUnexpected, &value)
			continue
		}

		// A null fits any column, and the slot already holds one of the column's kind.
		if _, isNull := value.(types.QValueNull); isNull {
			continue
		}

		// Otherwise the kinds must match, or the value is recorded as malformed data.
		// NOTE: The equals comparison canbe replaced by a call to a equivalence function.
		if column.kind != value.Kind() {
			var recordedValue *types.QValue
			if sc.shouldRecordValues {
				recordedValue = &value
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
		values[len(values)-1] = qValue
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
