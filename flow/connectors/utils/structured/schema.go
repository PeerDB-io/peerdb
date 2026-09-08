package structured

import (
	"fmt"
	"iter"
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
	case "JSON":
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

type SchemaProjector struct {
	schemaToQKind      func(schemaType string) (types.QValueKind, error)
	schemaColumns      map[string]protos.ColumnSetting
	shouldRecordValues bool
}

func NewSchemaProjector(
	schemaToQKind func(schemaType string) (types.QValueKind, error),
	schemaColumns map[string]protos.ColumnSetting,
	shouldRecordValues bool,
) *SchemaProjector {
	return &SchemaProjector{
		schemaToQKind:      schemaToQKind,
		schemaColumns:      schemaColumns,
		shouldRecordValues: shouldRecordValues,
	}
}

// NewSchemaProjectorWithDefaultSchemaToKind is NewSchemaProjector with defaultSchemaToQKind as the
// schema type to QValueKind conversion, i.e. for schemas declared with ClickHouse column types.
func NewSchemaProjectorWithDefaultSchemaToKind(schemaColumns map[string]protos.ColumnSetting, shouldRecordValues bool) *SchemaProjector {
	return NewSchemaProjector(defaultSchemaToQKind, schemaColumns, shouldRecordValues)
}

func (sc *SchemaProjector) ApplyRecordSchema(record iter.Seq2[string, types.QValue]) (model.RecordItems, error) {
	// Input record columns plus malformed data column, it might not be added.
	result := model.NewRecordItems(len(sc.schemaColumns) + 1)
	malformedData := NewMalformedData()

	// TODO: Potentially handle columns renames.

	for field, value := range record {
		schemaColumn, foundSchemaColumn := sc.schemaColumns[field]

		// Record columns not present in the schema are recorded as malformed data.
		if !foundSchemaColumn {
			malformedData.AddField(field, ReasonUnexpected, &value)
			continue
		}

		// Then we check if the types match...
		schemaDerivedQKind, err := sc.schemaToQKind(schemaColumn.DestinationType)
		if err != nil {
			return result, err
		}

		//... if not, we record it as malformed data.
		if schemaDerivedQKind != value.Kind() {
			var recordedValue *types.QValue
			if sc.shouldRecordValues {
				recordedValue = &value
			}
			malformedData.AddField(field, ReasonTypeMismatch, recordedValue)
			continue
		}

		// At this point, the value matches the expected schema type.
		result.AddColumn(field, value)
	}

	// If malformed data exists, we add it as a special column.
	if !malformedData.IsEmpty() {
		if qValue, err := malformedData.AsQValue(); err == nil {
			result.AddColumn(MalformedDataColumn, qValue)
		} else {
			return result, err
		}
	}

	return result, nil
}
