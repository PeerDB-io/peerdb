package connmongo

import (
	"fmt"
	"time"

	jsoniter "github.com/json-iterator/go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"

	shared_mongo "github.com/PeerDB-io/peerdb/flow/pkg/mongo"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// BsonToQValueConverter converts BSON values into QValues following the MongoDB ClickPipes type mapping
// (https://clickhouse.com/docs/integrations/clickpipes/mongodb/datatypes)
type BsonToQValueConverter interface {
	// QValueStringFromId converts a raw _id value to a QValueString.
	QValueStringFromId(id bson.RawValue, version uint32) (types.QValueString, error)
	// QValueJSONFromDocument converts a raw BSON document (Object) to a QValueJSON.
	QValueJSONFromDocument(raw bson.Raw) (types.QValueJSON, error)
	// QValueJSONFromArray converts a raw BSON array to a QValueJSON.
	QValueJSONFromArray(arr bson.RawArray) (types.QValueJSON, error)

	// QValueFromBsonValue converts any BSON value to the QValue prescribed by the type mapping above,
	// dispatching on the BSON type to the per-type converters below. BSON null (and a missing value)
	// yields a QValueNull of nullKind, the kind of the destination column.
	QValueFromBsonValue(v bson.RawValue, nullKind types.QValueKind) (types.QValue, error)

	QValueStringFromObjectID(oid bson.ObjectID) types.QValueString
	QValueStringFromString(s string) types.QValueString
	QValueInt64FromInt32(i int32) types.QValueInt64
	QValueInt64FromInt64(i int64) types.QValueInt64
	QValueFloat64FromDouble(f float64) types.QValueFloat64
	QValueBooleanFromBoolean(b bool) types.QValueBoolean
	QValueStringFromDateTime(t time.Time) types.QValueString
	QValueJSONFromRegex(pattern string, options string) types.QValueJSON
	QValueJSONFromTimestamp(t uint32, i uint32) types.QValueJSON
	QValueStringFromDecimal128(d bson.Decimal128) types.QValueString
	QValueJSONFromBinary(subtype byte, data []byte) types.QValueJSON
	QValueStringFromJavaScript(code string) types.QValueString
	QValueNullFromNull(kind types.QValueKind) types.QValueNull
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

func (c *DirectBsonConverter) QValueFromBsonValue(rv bson.RawValue, nullKind types.QValueKind) (types.QValue, error) {
	if rv.IsZero() {
		return c.QValueNullFromNull(nullKind), nil
	}
	v := bsoncore.Value{Type: bsoncore.Type(rv.Type), Data: rv.Value}
	switch v.Type {
	case bsoncore.TypeDouble:
		return c.QValueFloat64FromDouble(v.Double()), nil

	case bsoncore.TypeString:
		return c.QValueStringFromString(v.StringValue()), nil

	case bsoncore.TypeEmbeddedDocument:
		// Nested documents are are encoded as `QValueJSON` ...
		return c.QValueJSONFromDocument(bson.Raw(v.Document()))

	case bsoncore.TypeArray:
		// ... as well as Arrays.
		return c.QValueJSONFromArray(bson.RawArray(v.Array()))

	case bsoncore.TypeBinary:
		subtype, data := v.Binary()
		return c.QValueJSONFromBinary(subtype, data), nil

	case bsoncore.TypeObjectID:
		return c.QValueStringFromObjectID(v.ObjectID()), nil

	case bsoncore.TypeBoolean:
		return c.QValueBooleanFromBoolean(v.Boolean()), nil

	case bsoncore.TypeDateTime:
		return c.QValueStringFromDateTime(v.Time()), nil

	case bsoncore.TypeNull:
		return c.QValueNullFromNull(nullKind), nil

	case bsoncore.TypeRegex:
		pattern, options := v.Regex()
		return c.QValueJSONFromRegex(pattern, options), nil

	case bsoncore.TypeJavaScript:
		// Code is interpreted as a string.
		return c.QValueStringFromJavaScript(v.JavaScript()), nil

	case bsoncore.TypeSymbol: // deprecated type, kept for backwards-compatibility
		return c.QValueStringFromString(v.Symbol()), nil

	case bsoncore.TypeInt32:
		return c.QValueInt64FromInt32(v.Int32()), nil

	case bsoncore.TypeTimestamp:
		t, i := v.Timestamp()
		return c.QValueJSONFromTimestamp(t, i), nil

	case bsoncore.TypeInt64:
		return c.QValueInt64FromInt64(v.Int64()), nil

	case bsoncore.TypeDecimal128:
		h, l := v.Decimal128()
		return c.QValueStringFromDecimal128(bson.NewDecimal128(h, l)), nil

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

func (c *DirectBsonConverter) QValueInt64FromInt32(i int32) types.QValueInt64 {
	return types.QValueInt64{Val: int64(i)}
}

func (c *DirectBsonConverter) QValueInt64FromInt64(i int64) types.QValueInt64 {
	return types.QValueInt64{Val: i}
}

func (c *DirectBsonConverter) QValueFloat64FromDouble(f float64) types.QValueFloat64 {
	return types.QValueFloat64{Val: f}
}

func (c *DirectBsonConverter) QValueBooleanFromBoolean(b bool) types.QValueBoolean {
	return types.QValueBoolean{Val: b}
}

func (c *DirectBsonConverter) QValueStringFromDateTime(t time.Time) types.QValueString {
	return types.QValueString{Val: t.UTC().Format(time.RFC3339Nano)}
}

func (c *DirectBsonConverter) QValueJSONFromRegex(pattern string, options string) types.QValueJSON {
	c.stream.Reset(nil)
	shared_mongo.WriteRegexJSON(c.stream, pattern, options)
	return types.QValueJSON{Val: string(c.stream.Buffer())}
}

func (c *DirectBsonConverter) QValueJSONFromTimestamp(t uint32, i uint32) types.QValueJSON {
	c.stream.Reset(nil)
	shared_mongo.WriteTimestampJSON(c.stream, t, i)
	return types.QValueJSON{Val: string(c.stream.Buffer())}
}

func (c *DirectBsonConverter) QValueStringFromDecimal128(d bson.Decimal128) types.QValueString {
	return types.QValueString{Val: d.String()}
}

func (c *DirectBsonConverter) QValueJSONFromBinary(subtype byte, data []byte) types.QValueJSON {
	c.stream.Reset(nil)
	shared_mongo.WriteBinaryJSON(c.stream, subtype, data)
	return types.QValueJSON{Val: string(c.stream.Buffer())}
}

func (c *DirectBsonConverter) QValueStringFromJavaScript(code string) types.QValueString {
	return types.QValueString{Val: code}
}

func (c *DirectBsonConverter) QValueNullFromNull(kind types.QValueKind) types.QValueNull {
	return types.QValueNull(kind)
}
