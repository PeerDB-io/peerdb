package connmongo

import (
	"fmt"
	"iter"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils/structured"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// This file functions wire SchemaProjection abstractions to MongoDB realm so it can be used both
// by CDC and QRep flows.

// emptyBsonDocument is the canonical encoding of `{}`: a 5-byte length header and the terminator.
var emptyBsonDocument = bson.Raw{0x05, 0x00, 0x00, 0x00, 0x00}

// DocumentQValueIterator returns an iterator that lazily walks the top-level fields of a document excluding document key and
// yielding each as a QValue.
// The walk stops at the first failure, which the returned function reports once the walk is over.
func DocumentQValueIterator(raw bson.Raw, converter BsonToQValueConverter) (iter.Seq2[string, types.QValue], func() error) {
	var walkErr error
	return func(yield func(string, types.QValue) bool) {
		elements, err := raw.Elements()
		if err != nil {
			walkErr = fmt.Errorf("failed to read document fields: %w", err)
			return
		}
		for _, element := range elements {
			field, err := element.KeyErr()
			if err != nil {
				walkErr = fmt.Errorf("failed to read document field name: %w", err)
				return
			}
			if field == DefaultDocumentKeyColumnName {
				continue
			}
			value, err := converter.QValueFromBsonValue(element.Value(), types.QValueKindInvalid)
			if err != nil {
				walkErr = fmt.Errorf("failed to convert document field to QValue %s: %w", field, err)
				return
			}
			if !yield(field, value) {
				return
			}
		}
	}, func() error { return walkErr }
}

// newStructuredSchemaProjector builds the projector for the columns of a structured ingestion table
// mapping. recordMalformedValues controls whether the projector records the offending values.
func newStructuredSchemaProjector(
	columns []*protos.ColumnSetting, recordMalformedValues bool,
) (*structured.SchemaProjector, error) {
	return structured.NewSchemaProjectorFromCHtoQValue(columns, recordMalformedValues)
}

// newStructuredSchemaProjectorFromTableSchema builds the projector for a structured ingestion table from
// the table schema GetTableSchema emitted for it (persisted at setup), whose column types are already
// QValueKinds.
func newStructuredSchemaProjectorFromTableSchema(
	schema *protos.TableSchema, recordMalformedValues bool,
) (*structured.SchemaProjector, error) {
	fields := make([]types.QField, 0, len(schema.Columns))
	for _, column := range schema.Columns {
		if column.Name == DefaultDocumentKeyColumnName || column.Name == structured.MalformedDataColumn {
			continue
		}
		fields = append(fields, types.QField{Name: column.Name, Type: types.QValueKind(column.Type)})
	}
	return structured.NewSchemaProjectorFromQFields(fields, recordMalformedValues)
}

// GetStructuredSchema is the record schema of a structured ingestion table: the document key followed by
// the columns projector projects documents onto, the layout StructuredQValuesFromBsonRaw produces.
func GetStructuredSchema(projector *structured.SchemaProjector) types.QRecordSchema {
	projected := projector.QRecordSchema()
	schema := make([]types.QField, 0, len(projected.Fields)+1)
	schema = append(schema, documentKeyQField())
	schema = append(schema, projected.Fields...)
	return types.QRecordSchema{Fields: schema}
}

// StructuredQValuesFromBsonRaw converts a document into a record laid out as GetStructuredSchema for
// projector: the document key, then the document fields projected by projector onto the schema columns.
func StructuredQValuesFromBsonRaw(
	raw bson.Raw,
	version uint32,
	converter BsonToQValueConverter,
	projector *structured.SchemaProjector,
	tableName string,
) ([]types.QValue, error) {
	rv := raw.Lookup(DefaultDocumentKeyColumnName)
	if rv.IsZero() || rv.Type == bson.TypeNull {
		return nil, exceptions.NewInvalidIdValueError(tableName)
	}
	idQValue, err := converter.QValueStringFromId(rv, version)
	if err != nil {
		return nil, fmt.Errorf("failed to convert key %s: %w", DefaultDocumentKeyColumnName, err)
	}

	fields, walkErr := DocumentQValueIterator(raw, converter)
	values, err := projector.ProjectRecord(fields)
	if err != nil {
		return nil, fmt.Errorf("failed to project document onto schema: %w", err)
	}
	if err := walkErr(); err != nil {
		return nil, err
	}

	record := make([]types.QValue, 0, len(values)+1)
	record = append(record, idQValue)
	record = append(record, values...)
	return record, nil
}
