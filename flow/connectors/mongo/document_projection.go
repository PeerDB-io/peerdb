package connmongo

import (
	"fmt"
	"iter"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils/structured"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// This file functions wire SchemaProjection abstractions to MongoDB realm so it can be used both
// by CDC and QRep flows.

// emptyBsonDocument is the canonical encoding of `{}`: a 5-byte length header and the terminator.
var emptyBsonDocument = bson.Raw{0x05, 0x00, 0x00, 0x00, 0x00}

const bsonDocumentHeaderSize = 4

// DocumentQValueIterator lazily walks the fields of a document, the document key excluded, yielding each as
// a QValue. The walk stops at the first failure, which the returned function reports once the walk is over.
type DocumentQValueIterator func(raw bson.Raw, converter BsonToQValueConverter) (iter.Seq2[string, types.QValue], func() error)

// NewDocumentQValueIterator builds the iterator for the documents of a table, once for all of them.
// `expectedColumnKind` holds the schema columns, by name, and the kind each one expects. It drives two things:
//   - Arrays are brought into the kind their column expects instead of inferring an uniform element type.
//   - Embedded documents are flattened only where a column asks for it: a document is walked into, its
//     fields named by their path with keys joined by dots, only when some column is named under its path
//     (a and a.b for a column a.b.c) and it is not a column itself. Any other is yielded whole, as JSON:
//     a column receives its document as it is, and a document no column reads is one field. Arrays are
//     never flattened.
//
// The returned iterator only reads what it was built with, so it is safe for concurrent use.
func NewDocumentQValueIterator(expectedColumnKind map[string]types.QValueKind) DocumentQValueIterator {
	// Every path a column is named under, its name cut at each of its dots: resolved here once so the
	// walk decides whether to flatten a document with a lookup, whatever the number of columns.
	flattenedPaths := make(map[string]struct{})
	for column := range expectedColumnKind {
		for i, char := range column {
			if char == '.' {
				flattenedPaths[column[:i]] = struct{}{}
			}
		}
	}

	return func(raw bson.Raw, converter BsonToQValueConverter) (iter.Seq2[string, types.QValue], func() error) {
		var walkErr error
		return func(yield func(string, types.QValue) bool) {
			walker := documentWalker{
				converter:          converter,
				expectedColumnKind: expectedColumnKind,
				flattenedPaths:     flattenedPaths,
				yield:              yield,
			}
			_, walkErr = walker.walk(bsoncore.Document(raw), "")
		}, func() error { return walkErr }
	}
}

// documentWalker holds what a DocumentQValueIterator walks every (nested) document with.
type documentWalker struct {
	converter          BsonToQValueConverter
	expectedColumnKind map[string]types.QValueKind
	flattenedPaths     map[string]struct{}
	yield              func(string, types.QValue) bool
}

func (w *documentWalker) isFlattenedPath(field string) bool {
	_, isFlattened := w.flattenedPaths[field]
	return isFlattened
}

// walk yields the fields of doc, named by prefix followed by their key, recursing into the embedded documents
// found at flattened paths. It reports whether the walk must go on: false when the consumer stopped it or
// it failed.
func (w *documentWalker) walk(doc bsoncore.Document, prefix string) (bool, error) {
	// Walk the elements in place rather than through raw.Elements(), which builds two slices of all
	// elements and validates each one before the conversion below parses it again.
	if len(doc) == 0 {
		return true, nil
	}
	length, rem, ok := bsoncore.ReadLength(doc)
	if !ok {
		return false, fmt.Errorf("failed to read document fields: %w", bsoncore.NewInsufficientBytesError(doc, rem))
	}
	for length -= bsonDocumentHeaderSize; length > 1; {
		elem, next, ok := bsoncore.ReadElement(rem)
		if !ok {
			return false, fmt.Errorf("failed to read document fields: %w", bsoncore.NewInsufficientBytesError(doc, rem))
		}
		length -= int32(len(elem))
		rem = next
		element := bson.RawElement(elem)
		key, err := element.KeyErr()
		if err != nil {
			return false, fmt.Errorf("failed to read document field name: %w", err)
		}
		// Only the top level key is the document key, a nested _id is a field as any other.
		if prefix == "" && key == DefaultDocumentKeyColumnName {
			continue
		}
		field := prefix + key
		rawValue := element.Value()
		maybeExpectedKind, isSchemaColumn := w.expectedColumnKind[field]
		// A column takes its document whole, even when other columns are named under it.
		if !isSchemaColumn && rawValue.Type == bson.TypeEmbeddedDocument && w.isFlattenedPath(field) {
			nested, ok := rawValue.DocumentOK()
			if !ok {
				return false, fmt.Errorf("failed to read embedded document %s", field)
			}
			if goOn, err := w.walk(bsoncore.Document(nested), field+"."); !goOn {
				return false, err
			}
			continue
		}
		value, err := w.converter.QValueFromBsonValue(rawValue, maybeExpectedKind)
		if err != nil && maybeExpectedKind.IsArray() && rawValue.Type == bson.TypeArray {
			// An array that does not fit its typed column is yielded as JSON: the projector then
			// reports it as a type mismatch, its JSON form as the value, instead of the record failing.
			value, err = w.converter.QValueJSONFromArray(rawValue.Array())
		}
		if err != nil {
			return false, fmt.Errorf("failed to convert document field to QValue %s: %w", field, err)
		}
		if !w.yield(field, value) {
			return false, nil
		}
	}
	return true, nil
}

// DocumentProjector projects the documents of a structured ingestion table onto its schema columns: the
// generic schema projector together with the iterator reading documents for it, built once per table.
type DocumentProjector struct {
	*structured.SchemaProjector
	// DocumentFields walks a document's fields as the schema columns ask for them.
	DocumentFields DocumentQValueIterator
}

func newDocumentProjector(projector *structured.SchemaProjector, err error) (*DocumentProjector, error) {
	if err != nil {
		return nil, err
	}
	return &DocumentProjector{
		SchemaProjector: projector,
		DocumentFields:  NewDocumentQValueIterator(projector.ColumnKinds()),
	}, nil
}

// newStructuredSchemaProjector builds the projector for the columns of a structured ingestion table
// mapping. recordMalformedValues controls whether the projector records the offending values.
func newStructuredSchemaProjector(
	columns []*protos.ColumnSetting, excludedColumnsNames []string, recordMalformedValues bool,
) (*DocumentProjector, error) {
	filteredColumns := make([]*protos.ColumnSetting, 0, len(columns))
	for _, column := range columns {
		if isProjectedColumn(column.SourceName) {
			filteredColumns = append(filteredColumns, column)
		}
	}

	excludedColumnSet := make(map[string]struct{}, len(excludedColumnsNames))
	for _, col := range excludedColumnsNames {
		excludedColumnSet[col] = struct{}{}
	}

	return newDocumentProjector(structured.NewSchemaProjectorFromCHtoQValue(filteredColumns, excludedColumnSet, recordMalformedValues))
}

// newStructuredSchemaProjectorFromTableSchema builds the projector for a structured ingestion table from
// the table schema GetTableSchema emitted for it (persisted at setup), whose column types are already
// QValueKinds.
func newStructuredSchemaProjectorFromTableSchema(
	schema *protos.TableSchema, excludedColumnSet map[string]struct{}, recordMalformedValues bool,
) (*DocumentProjector, error) {
	fields := make([]types.QField, 0, len(schema.Columns))
	for _, column := range schema.Columns {
		if isProjectedColumn(column.Name) {
			fields = append(fields, types.QField{Name: column.Name, Type: types.QValueKind(column.Type)})
		}
	}
	return newDocumentProjector(structured.NewSchemaProjectorFromQFields(fields, excludedColumnSet, recordMalformedValues))
}

func isProjectedColumn(columnName string) bool {
	return columnName != DefaultDocumentKeyColumnName && columnName != structured.MalformedDataColumn
}

// GetStructuredSchema is the record schema of a structured ingestion table: the document key followed by
// the columns projector projects documents onto, the layout StructuredQValuesFromBsonRaw produces.
func GetStructuredSchema(projector *DocumentProjector) types.QRecordSchema {
	projected := projector.QRecordSchema()
	schema := make([]types.QField, 0, len(projected.Fields)+1)
	schema = append(schema, documentKeyQField())
	schema = append(schema, projected.Fields...)
	return types.QRecordSchema{Fields: schema}
}

// StructuredQValuesFromBsonRaw converts a document into a record laid out as GetStructuredSchema for
// the projector: the document key, then the document fields projected onto the schema columns, arrays
// converted toward their columns' typed array kinds.
func StructuredQValuesFromBsonRaw(
	raw bson.Raw,
	version uint32,
	converter BsonToQValueConverter,
	projector *DocumentProjector,
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

	fields, walkErr := projector.DocumentFields(raw, converter)
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
