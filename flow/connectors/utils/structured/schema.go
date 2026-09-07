package structured

import (
	"iter"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type SchemaProjector struct {
	schemaToQKind      func(schemaType string) (types.QValueKind, error)
	schemaColumns      map[string]protos.ColumnSetting
	shouldRecordValues bool
}

func NewSchemaProjector(schemaToQKind func(schemaType string) (types.QValueKind, error), schemaColumns map[string]protos.ColumnSetting, shouldRecordValues bool) *SchemaProjector {
	return &SchemaProjector{
		schemaToQKind:      schemaToQKind,
		schemaColumns:      schemaColumns,
		shouldRecordValues: shouldRecordValues,
	}
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
