package connmongo

import (
	"fmt"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"

	shared_mongo "github.com/PeerDB-io/peerdb/flow/pkg/mongo"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// BsonToQValueConverter converts BSON values into QValues following the MongoDB ClickPipes type mapping
// (https://clickhouse.com/docs/integrations/clickpipes/mongodb/datatypes).
//
// Arrays correspond to typed array QValues when the destination column kind is one of the array kinds
// QValueFromBsonValue dispatches: each element must map, under this same mapping, to the kind's element
// type, e.g. an array of 32/64-bit integers into an Int64 array, or one mixing ObjectIds, Dates and
// Decimal128s into a String array. Arrays of JSON take the elements mapping to JSON: embedded documents
// and nested arrays included. An element mapping elsewhere, nulls included, fails the conversion.
// For any other destination kind, arrays land whole as JSON.
type BsonToQValueConverter interface {
	// QValueStringFromId converts a raw _id value to a QValueString.
	QValueStringFromId(id bson.RawValue, version uint32) (types.QValueString, error)
	// QValueJSONFromDocument converts a raw BSON document (Object) to a QValueJSON.
	QValueJSONFromDocument(raw bson.Raw) (types.QValueJSON, error)
	// QValueJSONFromArray converts a raw BSON array to a QValueJSON.
	QValueJSONFromArray(arr bson.RawArray) (types.QValueJSON, error)

	// QValueFromBsonValue converts any BSON value to the QValue prescribed by the type mapping above,
	// dispatching on the BSON type. columnKind is the kind of the destination column: BSON null (and a
	// missing value) yields a QValueNull of it, and an array converts to its corresponding typed array
	// QValue when it names an array kind.
	QValueFromBsonValue(v bson.RawValue, columnKind types.QValueKind) (types.QValue, error)

	QValueStringFromObjectID(oid bson.ObjectID) types.QValueString
	QValueStringFromString(s string) types.QValueString
}

// DirectBsonConverter converts BSON directly to JSON string without intermediate deserialization,
// it uses jsoniter.Stream to build JSON output incrementally into a reusable buffer (to avoid allocation)
type DirectBsonConverter struct {
	stream *jsoniter.Stream
}

func NewDirectBsonConverter() *DirectBsonConverter {
	return &DirectBsonConverter{
		// technically we write JSON directly via raw stream methods and do not use jsoniter's
		// config-driven serialization specified here, but this config is applied consistent with our
		// custom serialization so specifying it here for consistency
		stream: jsoniter.NewStream(jsoniter.ConfigCompatibleWithStandardLibrary, nil, 512),
	}
}

func (c *DirectBsonConverter) QValueJSONFromDocument(raw bson.Raw) (types.QValueJSON, error) {
	c.stream.Reset(nil)
	if err := shared_mongo.RawDocumentToJSON(bsoncore.Document(raw), c.stream); err != nil {
		return types.QValueJSON{}, fmt.Errorf("failed to convert document: %w", err)
	}
	return types.QValueJSON{Val: string(c.stream.Buffer())}, nil
}

func (c *DirectBsonConverter) QValueJSONFromArray(arr bson.RawArray) (types.QValueJSON, error) {
	c.stream.Reset(nil)
	if err := shared_mongo.RawArrayToJSON(bsoncore.Array(arr), c.stream); err != nil {
		return types.QValueJSON{}, fmt.Errorf("failed to convert array: %w", err)
	}
	return types.QValueJSON{Val: string(c.stream.Buffer()), IsArray: true}, nil
}

func typedArrayFromBson[Q types.QValue, T any](
	c *DirectBsonConverter, arr bson.RawArray, element func(Q) T,
) ([]T, error) {
	rawValues, err := arr.Values()
	if err != nil {
		return nil, fmt.Errorf("failed to read array elements: %w", err)
	}
	var want Q
	values := make([]T, 0, len(rawValues))
	for i, rawValue := range rawValues {
		qValue, err := c.QValueFromBsonValue(rawValue, types.QValueKindInvalid)
		if err != nil {
			return nil, fmt.Errorf("failed to convert array element %d: %w", i, err)
		}
		typed, ok := qValue.(Q)
		if !ok {
			if _, isNull := qValue.(types.QValueNull); isNull {
				return nil, fmt.Errorf("array element %d is null, not %s", i, want.Kind())
			}
			return nil, fmt.Errorf("array element %d maps to %s, not %s", i, qValue.Kind(), want.Kind())
		}
		values = append(values, element(typed))
	}
	return values, nil
}

func (c *DirectBsonConverter) QValueStringFromId(id bson.RawValue, version uint32) (types.QValueString, error) {
	if version >= shared.InternalVersion_MongoDBIdWithoutRedundantQuotes {
		switch id.Type {
		case bson.TypeObjectID:
			return c.QValueStringFromObjectID(id.ObjectID()), nil
		case bson.TypeString:
			return c.QValueStringFromString(id.StringValue()), nil
		}
	}
	c.stream.Reset(nil)
	if err := shared_mongo.RawValueToJSON(bsoncore.Value{Type: bsoncore.Type(id.Type), Data: id.Value}, c.stream); err != nil {
		return types.QValueString{}, fmt.Errorf("failed to convert %s: %w", DefaultDocumentKeyColumnName, err)
	}
	return types.QValueString{Val: string(c.stream.Buffer())}, nil
}

func (c *DirectBsonConverter) QValueFromBsonValue(rv bson.RawValue, columnKind types.QValueKind) (types.QValue, error) {
	if rv.IsZero() {
		return types.QValueNull(columnKind), nil
	}
	v := bsoncore.Value{Type: bsoncore.Type(rv.Type), Data: rv.Value}
	switch v.Type {
	case bsoncore.TypeDouble:
		return types.QValueFloat64{Val: v.Double()}, nil

	case bsoncore.TypeString:
		return c.QValueStringFromString(v.StringValue()), nil

	case bsoncore.TypeEmbeddedDocument:
		// Nested documents are encoded as `QValueJSON` ...
		return c.QValueJSONFromDocument(bson.Raw(v.Document()))

	case bsoncore.TypeArray:
		// ... as well as Arrays, unless the destination column is of an array kind, with
		// corresponding QValue s corresponding to typed arrays.
		arr := bson.RawArray(v.Array())
		switch columnKind {
		case types.QValueKindArrayString:
			values, err := typedArrayFromBson(c, arr, func(q types.QValueString) string { return q.Val })
			return types.QValueArrayString{Val: values}, err
		case types.QValueKindArrayInt64:
			values, err := typedArrayFromBson(c, arr, func(q types.QValueInt64) int64 { return q.Val })
			return types.QValueArrayInt64{Val: values}, err
		case types.QValueKindArrayFloat64:
			values, err := typedArrayFromBson(c, arr, func(q types.QValueFloat64) float64 { return q.Val })
			return types.QValueArrayFloat64{Val: values}, err
		case types.QValueKindArrayBoolean:
			values, err := typedArrayFromBson(c, arr, func(q types.QValueBoolean) bool { return q.Val })
			return types.QValueArrayBoolean{Val: values}, err
		case types.QValueKindArrayJSON, types.QValueKindArrayJSONB:
			// The array kinds of JSON have no dedicated QValue struct: as the other connectors
			// producing JSON arrays, this yield a QValueJSON holding the whole array serialized in
			// Val and IsArray: true.
			values, err := typedArrayFromBson(c, arr, func(q types.QValueJSON) string { return q.Val })
			if err != nil {
				return nil, err
			}
			return types.QValueJSON{Val: "[" + strings.Join(values, ",") + "]", IsArray: true}, nil
		default:
			return c.QValueJSONFromArray(arr)
		}

	case bsoncore.TypeBinary:
		subtype, data := v.Binary()
		c.stream.Reset(nil)
		shared_mongo.WriteBinaryJSON(c.stream, subtype, data)
		return types.QValueJSON{Val: string(c.stream.Buffer())}, nil

	case bsoncore.TypeObjectID:
		return c.QValueStringFromObjectID(v.ObjectID()), nil

	case bsoncore.TypeBoolean:
		return types.QValueBoolean{Val: v.Boolean()}, nil

	case bsoncore.TypeDateTime:
		return types.QValueString{Val: v.Time().UTC().Format(time.RFC3339Nano)}, nil

	case bsoncore.TypeNull:
		return types.QValueNull(columnKind), nil

	case bsoncore.TypeRegex:
		pattern, options := v.Regex()
		c.stream.Reset(nil)
		shared_mongo.WriteRegexJSON(c.stream, pattern, options)
		return types.QValueJSON{Val: string(c.stream.Buffer())}, nil

	case bsoncore.TypeJavaScript:
		// Code is interpreted as a string.
		return types.QValueString{Val: v.JavaScript()}, nil

	case bsoncore.TypeSymbol: // deprecated type, kept for backwards-compatibility
		return c.QValueStringFromString(v.Symbol()), nil

	case bsoncore.TypeInt32:
		return types.QValueInt64{Val: int64(v.Int32())}, nil

	case bsoncore.TypeTimestamp:
		t, i := v.Timestamp()
		c.stream.Reset(nil)
		shared_mongo.WriteTimestampJSON(c.stream, t, i)
		return types.QValueJSON{Val: string(c.stream.Buffer())}, nil

	case bsoncore.TypeInt64:
		return types.QValueInt64{Val: v.Int64()}, nil

	case bsoncore.TypeDecimal128:
		h, l := v.Decimal128()
		return types.QValueString{Val: bson.NewDecimal128(h, l).String()}, nil

	default:
		// Undefined, MinKey, MaxKey, DBPointer and CodeWithScope are deprecated and not part of the documented
		// mapping; they are rendered as JSON exactly as they are inside a full document.
		c.stream.Reset(nil)
		if err := shared_mongo.RawValueToJSON(v, c.stream); err != nil {
			return nil, fmt.Errorf("failed to convert %s value: %w", v.Type.String(), err)
		}
		return types.QValueJSON{Val: string(c.stream.Buffer())}, nil
	}
}

func (c *DirectBsonConverter) QValueStringFromObjectID(oid bson.ObjectID) types.QValueString {
	return types.QValueString{Val: oid.Hex()}
}

func (c *DirectBsonConverter) QValueStringFromString(s string) types.QValueString {
	return types.QValueString{Val: s}
}
