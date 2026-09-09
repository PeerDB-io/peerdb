package connmongo

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"iter"
	"math"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"

	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// BsonToQValueConverter converts BSON values into QValues following the MongoDB ClickPipes type mapping
// (https://clickhouse.com/docs/integrations/clickpipes/mongodb/datatypes):
//
//	ObjectId           -> String (hex)
//	String             -> String
//	32-bit integer     -> Int64
//	64-bit integer     -> Int64
//	Double             -> Float64
//	Boolean            -> Bool
//	Date               -> String (ISO 8601)
//	Regular Expression -> JSON {Pattern: String, Options: String}
//	Timestamp          -> JSON {T: Int64, I: Int64}
//	Decimal128         -> String
//	Binary data        -> JSON {Subtype: Int64, Data: String (base64)}
//	JavaScript         -> String
//	Null               -> Null
//	Array              -> JSON (Dynamic on the destination)
//	Object             -> JSON (Dynamic on the destination)
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
	if err := rawDocToJSON(bsoncore.Document(raw), c.stream); err != nil {
		return types.QValueJSON{}, fmt.Errorf("failed to convert document: %w", err)
	}
	return types.QValueJSON{Val: string(c.stream.Buffer())}, nil
}

func (c *DirectBsonConverter) QValueJSONFromArray(arr bson.RawArray) (types.QValueJSON, error) {
	c.stream.Reset(nil)
	if err := rawArrayToJSON(bsoncore.Array(arr), c.stream); err != nil {
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
	if err := rawValueToJSON(bsoncore.Value{Type: bsoncore.Type(id.Type), Data: id.Value}, c.stream); err != nil {
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
		if err := rawValueToJSON(v, c.stream); err != nil {
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
	writeRegexJSON(c.stream, pattern, options)
	return types.QValueJSON{Val: string(c.stream.Buffer())}
}

func (c *DirectBsonConverter) QValueJSONFromTimestamp(t uint32, i uint32) types.QValueJSON {
	c.stream.Reset(nil)
	writeTimestampJSON(c.stream, t, i)
	return types.QValueJSON{Val: string(c.stream.Buffer())}
}

func (c *DirectBsonConverter) QValueStringFromDecimal128(d bson.Decimal128) types.QValueString {
	return types.QValueString{Val: d.String()}
}

func (c *DirectBsonConverter) QValueJSONFromBinary(subtype byte, data []byte) types.QValueJSON {
	c.stream.Reset(nil)
	writeBinaryJSON(c.stream, subtype, data)
	return types.QValueJSON{Val: string(c.stream.Buffer())}
}

func (c *DirectBsonConverter) QValueStringFromJavaScript(code string) types.QValueString {
	return types.QValueString{Val: code}
}

func (c *DirectBsonConverter) QValueNullFromNull(kind types.QValueKind) types.QValueNull {
	return types.QValueNull(kind)
}

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

func rawDocToJSON(doc bsoncore.Document, stream *jsoniter.Stream) error {
	length, rem, ok := bsoncore.ReadLength(doc)
	if !ok {
		return fmt.Errorf("failed to read document length")
	}
	length -= 4

	stream.WriteRaw("{")
	first := true
	for length > 1 {
		elem, next, ok := bsoncore.ReadElement(rem)
		if !ok {
			return fmt.Errorf("failed to read document element")
		}
		length -= int32(len(elem))
		rem = next

		if !first {
			stream.WriteRaw(",")
		}
		first = false

		stream.WriteStringWithHTMLEscaped(elem.Key())
		stream.WriteRaw(":")
		if err := rawValueToJSON(elem.Value(), stream); err != nil {
			return err
		}
	}
	stream.WriteRaw("}")
	return nil
}

func rawArrayToJSON(arr bsoncore.Array, stream *jsoniter.Stream) error {
	length, rem, ok := bsoncore.ReadLength(arr)
	if !ok {
		return fmt.Errorf("failed to read array length")
	}
	length -= 4

	stream.WriteRaw("[")
	first := true
	for length > 1 {
		elem, next, ok := bsoncore.ReadElement(rem)
		if !ok {
			return fmt.Errorf("failed to read array element")
		}
		length -= int32(len(elem))
		rem = next

		if !first {
			stream.WriteRaw(",")
		}
		first = false

		if err := rawValueToJSON(elem.Value(), stream); err != nil {
			return err
		}
	}
	stream.WriteRaw("]")
	return nil
}

func rawValueToJSON(v bsoncore.Value, stream *jsoniter.Stream) error {
	switch v.Type {
	case bsoncore.TypeDouble:
		writeFloat64JSON(stream, v.Double())

	case bsoncore.TypeString:
		stream.WriteStringWithHTMLEscaped(v.StringValue())

	case bsoncore.TypeEmbeddedDocument:
		return rawDocToJSON(v.Document(), stream)

	case bsoncore.TypeArray:
		return rawArrayToJSON(v.Array(), stream)

	case bsoncore.TypeBinary:
		subtype, data := v.Binary()
		writeBinaryJSON(stream, subtype, data)

	case bsoncore.TypeUndefined:
		stream.WriteEmptyObject()

	case bsoncore.TypeObjectID:
		oid := v.ObjectID()
		stream.WriteRaw(`"`)
		stream.SetBuffer(hex.AppendEncode(stream.Buffer(), oid[:]))
		stream.WriteRaw(`"`)

	case bsoncore.TypeBoolean:
		stream.WriteBool(v.Boolean())

	case bsoncore.TypeDateTime:
		stream.WriteRaw(`"`)
		stream.SetBuffer(v.Time().UTC().AppendFormat(stream.Buffer(), time.RFC3339Nano))
		stream.WriteRaw(`"`)

	case bsoncore.TypeNull:
		stream.WriteNil()

	case bsoncore.TypeRegex:
		pattern, options := v.Regex()
		writeRegexJSON(stream, pattern, options)

	case bsoncore.TypeJavaScript:
		stream.WriteStringWithHTMLEscaped(v.JavaScript())

	case bsoncore.TypeSymbol:
		stream.WriteStringWithHTMLEscaped(v.Symbol())

	case bsoncore.TypeInt32:
		stream.WriteInt32(v.Int32())

	case bsoncore.TypeTimestamp:
		t, i := v.Timestamp()
		writeTimestampJSON(stream, t, i)

	case bsoncore.TypeInt64:
		stream.WriteInt64(v.Int64())

	case bsoncore.TypeDecimal128:
		h, l := v.Decimal128()
		stream.WriteString(bson.NewDecimal128(h, l).String())

	case bsoncore.TypeMinKey, bsoncore.TypeMaxKey:
		stream.WriteEmptyObject()

	case bsoncore.TypeDBPointer: // deprecated type, kept for backwards-compatibility
		ns, oid := v.DBPointer()
		stream.WriteRaw(`{"DB":`)
		stream.WriteStringWithHTMLEscaped(ns)
		stream.WriteRaw(`,"Pointer":"`)
		stream.SetBuffer(hex.AppendEncode(stream.Buffer(), oid[:]))
		stream.WriteRaw(`"}`)

	case bsoncore.TypeCodeWithScope: // deprecated type, kept for backwards-compatibility
		code, scope := v.CodeWithScope()
		stream.WriteRaw(`{"Code":`)
		stream.WriteStringWithHTMLEscaped(code)
		stream.WriteRaw(`,"Scope":`)
		if err := rawDocToJSON(scope, stream); err != nil {
			return err
		}
		stream.WriteRaw("}")

	default:
		return fmt.Errorf("unknown type: %v", v.Type.String())
	}
	return nil
}

// writeBinaryJSON encodes BSON binary data as {"Subtype": <int>, "Data": "<base64>"}.
func writeBinaryJSON(stream *jsoniter.Stream, subtype byte, data []byte) {
	stream.WriteRaw(`{"Subtype":`)
	stream.WriteUint8(subtype)
	stream.WriteRaw(`,"Data":"`)
	stream.SetBuffer(base64.StdEncoding.AppendEncode(stream.Buffer(), data))
	stream.WriteRaw(`"}`)
}

// writeRegexJSON encodes a BSON regular expression as {"Pattern": "<pattern>", "Options": "<flags>"}.
func writeRegexJSON(stream *jsoniter.Stream, pattern string, options string) {
	stream.WriteRaw(`{"Pattern":`)
	stream.WriteStringWithHTMLEscaped(pattern)
	stream.WriteRaw(`,"Options":`)
	stream.WriteStringWithHTMLEscaped(options)
	stream.WriteRaw("}")
}

// writeTimestampJSON encodes a BSON (internal) timestamp as {"T": <seconds>, "I": <increment>}.
func writeTimestampJSON(stream *jsoniter.Stream, t uint32, i uint32) {
	stream.WriteRaw(`{"T":`)
	stream.WriteUint32(t)
	stream.WriteRaw(`,"I":`)
	stream.WriteUint32(i)
	stream.WriteRaw("}")
}

// Assume (and test) that values outside of these limits will come out in scientific notation
// and will be parsed as floats either way
var (
	floatLimit    = math.Pow10(21)
	floatNegLimit = -floatLimit
)

// writeFloat64JSON encodes NaN/Inf as quoted strings, integer-valued floats with an explicit
// ".0" suffix (to hint ClickHouse to parse as float), and other values in standard notation.
func writeFloat64JSON(stream *jsoniter.Stream, v float64) {
	if math.IsNaN(v) {
		stream.WriteRaw(`"NaN"`)
	} else if math.IsInf(v, 1) {
		stream.WriteRaw(`"+Inf"`)
	} else if math.IsInf(v, -1) {
		stream.WriteRaw(`"-Inf"`)
	} else if v < floatLimit && v > floatNegLimit && v == math.Trunc(v) {
		// use explicit decimal to hint ClickHouse to parse as float
		stream.SetBuffer(strconv.AppendFloat(stream.Buffer(), v, 'f', 1, 64))
	} else {
		// standard notation, with implementation copied from json-iterator's WriteFloat64
		abs := math.Abs(v)
		format := byte('f')
		if abs != 0 && (abs < 1e-6 || abs >= 1e21) {
			format = 'e'
		}
		stream.SetBuffer(strconv.AppendFloat(stream.Buffer(), v, format, -1, 64))
	}
}
