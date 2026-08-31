package connmongo

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"

	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type BsonToQValueConverter interface {
	// QValueStringFromId converts a raw _id value to a QValueString.
	QValueStringFromId(id bson.RawValue, version uint32) (types.QValueString, error)
	// QValueJSONFromDocument converts a raw BSON document to a QValueJSON.
	QValueJSONFromDocument(raw bson.Raw) (types.QValueJSON, error)
	// QValueFromBsonValue converts a single BSON value to a typed QValue of the
	// requested kind, coercing across compatible types and returning a typed NULL
	// for missing/null values or incompatible types.
	QValueFromBsonValue(v bson.RawValue, kind types.QValueKind) (types.QValue, error)
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

// QValueFromBsonValue converts a single BSON value to the requested QValueKind.
// Missing/null/undefined values and values whose type cannot be coerced yield a
// typed NULL so that heterogeneous MongoDB data cannot stall replication.
func (c *DirectBsonConverter) QValueFromBsonValue(v bson.RawValue, kind types.QValueKind) (types.QValue, error) {
	if v.IsZero() || v.Type == bson.TypeNull || v.Type == bson.TypeUndefined {
		return types.QValueNull(kind), nil
	}

	switch kind {
	case types.QValueKindString:
		s, ok := c.bsonToString(v)
		if !ok {
			return types.QValueNull(kind), nil
		}
		return types.QValueString{Val: s}, nil
	case types.QValueKindBoolean:
		b, ok := bsonToBool(v)
		if !ok {
			return types.QValueNull(kind), nil
		}
		return types.QValueBoolean{Val: b}, nil
	case types.QValueKindInt8, types.QValueKindInt16, types.QValueKindInt32, types.QValueKindInt64:
		n, ok := bsonToInt64(v)
		if !ok {
			return types.QValueNull(kind), nil
		}
		switch kind {
		case types.QValueKindInt8:
			return types.QValueInt8{Val: int8(n)}, nil
		case types.QValueKindInt16:
			return types.QValueInt16{Val: int16(n)}, nil
		case types.QValueKindInt32:
			return types.QValueInt32{Val: int32(n)}, nil
		default:
			return types.QValueInt64{Val: n}, nil
		}
	case types.QValueKindFloat32:
		f, ok := bsonToFloat64(v)
		if !ok {
			return types.QValueNull(kind), nil
		}
		return types.QValueFloat32{Val: float32(f)}, nil
	case types.QValueKindFloat64:
		f, ok := bsonToFloat64(v)
		if !ok {
			return types.QValueNull(kind), nil
		}
		return types.QValueFloat64{Val: f}, nil
	case types.QValueKindNumeric:
		d, ok := bsonToDecimal(v)
		if !ok {
			return types.QValueNull(kind), nil
		}
		return types.QValueNumeric{Val: d}, nil
	case types.QValueKindTimestamp:
		t, ok := bsonToTime(v)
		if !ok {
			return types.QValueNull(kind), nil
		}
		return types.QValueTimestamp{Val: t}, nil
	case types.QValueKindTimestampTZ:
		t, ok := bsonToTime(v)
		if !ok {
			return types.QValueNull(kind), nil
		}
		return types.QValueTimestampTZ{Val: t}, nil
	case types.QValueKindDate:
		t, ok := bsonToTime(v)
		if !ok {
			return types.QValueNull(kind), nil
		}
		return types.QValueDate{Val: t}, nil
	case types.QValueKindUUID:
		u, ok := bsonToUUID(v)
		if !ok {
			return types.QValueNull(kind), nil
		}
		return types.QValueUUID{Val: u}, nil
	case types.QValueKindBytes:
		b, ok := bsonToBytes(v)
		if !ok {
			return types.QValueNull(kind), nil
		}
		return types.QValueBytes{Val: b}, nil
	case types.QValueKindJSON, types.QValueKindJSONB:
		c.stream.Reset(nil)
		if err := rawValueToJSON(bsoncore.Value{Type: bsoncore.Type(v.Type), Data: v.Value}, c.stream); err != nil {
			return nil, fmt.Errorf("failed to convert value to JSON: %w", err)
		}
		return types.QValueJSON{Val: string(c.stream.Buffer()), IsArray: v.Type == bson.TypeArray}, nil
	default:
		return nil, fmt.Errorf("unsupported destination type %q for MongoDB typed projection", kind)
	}
}

// bsonToString renders any scalar BSON value as text; documents/arrays fall back
// to their JSON serialization.
func (c *DirectBsonConverter) bsonToString(v bson.RawValue) (string, bool) {
	switch v.Type {
	case bson.TypeString:
		return v.StringValue(), true
	case bson.TypeObjectID:
		return v.ObjectID().Hex(), true
	case bson.TypeInt32:
		return strconv.FormatInt(int64(v.Int32()), 10), true
	case bson.TypeInt64:
		return strconv.FormatInt(v.Int64(), 10), true
	case bson.TypeDouble:
		return strconv.FormatFloat(v.Double(), 'f', -1, 64), true
	case bson.TypeBoolean:
		return strconv.FormatBool(v.Boolean()), true
	case bson.TypeDateTime:
		return v.Time().UTC().Format(time.RFC3339Nano), true
	case bson.TypeDecimal128:
		return v.Decimal128().String(), true
	default:
		c.stream.Reset(nil)
		if err := rawValueToJSON(bsoncore.Value{Type: bsoncore.Type(v.Type), Data: v.Value}, c.stream); err != nil {
			return "", false
		}
		return string(c.stream.Buffer()), true
	}
}

func bsonToInt64(v bson.RawValue) (int64, bool) {
	switch v.Type {
	case bson.TypeInt32:
		return int64(v.Int32()), true
	case bson.TypeInt64:
		return v.Int64(), true
	case bson.TypeDouble:
		return int64(v.Double()), true
	case bson.TypeBoolean:
		if v.Boolean() {
			return 1, true
		}
		return 0, true
	case bson.TypeString:
		if n, err := strconv.ParseInt(strings.TrimSpace(v.StringValue()), 10, 64); err == nil {
			return n, true
		}
	}
	return 0, false
}

func bsonToFloat64(v bson.RawValue) (float64, bool) {
	switch v.Type {
	case bson.TypeDouble:
		return v.Double(), true
	case bson.TypeInt32:
		return float64(v.Int32()), true
	case bson.TypeInt64:
		return float64(v.Int64()), true
	case bson.TypeString:
		if f, err := strconv.ParseFloat(strings.TrimSpace(v.StringValue()), 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

func bsonToBool(v bson.RawValue) (bool, bool) {
	switch v.Type {
	case bson.TypeBoolean:
		return v.Boolean(), true
	case bson.TypeInt32:
		return v.Int32() != 0, true
	case bson.TypeInt64:
		return v.Int64() != 0, true
	case bson.TypeDouble:
		return v.Double() != 0, true
	case bson.TypeString:
		if b, err := strconv.ParseBool(strings.TrimSpace(v.StringValue())); err == nil {
			return b, true
		}
	}
	return false, false
}

func bsonToTime(v bson.RawValue) (time.Time, bool) {
	switch v.Type {
	case bson.TypeDateTime:
		return v.Time().UTC(), true
	case bson.TypeTimestamp:
		t, _ := v.Timestamp()
		return time.Unix(int64(t), 0).UTC(), true
	case bson.TypeString:
		if t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(v.StringValue())); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func bsonToUUID(v bson.RawValue) (uuid.UUID, bool) {
	switch v.Type {
	case bson.TypeString:
		if u, err := uuid.Parse(v.StringValue()); err == nil {
			return u, true
		}
	case bson.TypeBinary:
		_, data := v.Binary()
		if u, err := uuid.FromBytes(data); err == nil {
			return u, true
		}
	}
	return uuid.UUID{}, false
}

func bsonToBytes(v bson.RawValue) ([]byte, bool) {
	switch v.Type {
	case bson.TypeBinary:
		_, data := v.Binary()
		return data, true
	case bson.TypeString:
		return []byte(v.StringValue()), true
	}
	return nil, false
}

func bsonToDecimal(v bson.RawValue) (decimal.Decimal, bool) {
	switch v.Type {
	case bson.TypeDecimal128:
		if d, err := decimal.NewFromString(v.Decimal128().String()); err == nil {
			return d, true
		}
	case bson.TypeInt32:
		return decimal.NewFromInt(int64(v.Int32())), true
	case bson.TypeInt64:
		return decimal.NewFromInt(v.Int64()), true
	case bson.TypeDouble:
		return decimal.NewFromFloat(v.Double()), true
	case bson.TypeString:
		if d, err := decimal.NewFromString(strings.TrimSpace(v.StringValue())); err == nil {
			return d, true
		}
	}
	return decimal.Decimal{}, false
}

func (c *DirectBsonConverter) QValueStringFromId(id bson.RawValue, version uint32) (types.QValueString, error) {
	if version >= shared.InternalVersion_MongoDBIdWithoutRedundantQuotes {
		switch id.Type {
		case bson.TypeObjectID:
			return types.QValueString{Val: id.ObjectID().Hex()}, nil
		case bson.TypeString:
			return types.QValueString{Val: id.StringValue()}, nil
		}
	}
	c.stream.Reset(nil)
	if err := rawValueToJSON(bsoncore.Value{Type: bsoncore.Type(id.Type), Data: id.Value}, c.stream); err != nil {
		return types.QValueString{}, fmt.Errorf("failed to convert %s: %w", DefaultDocumentKeyColumnName, err)
	}
	return types.QValueString{Val: string(c.stream.Buffer())}, nil
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
		stream.WriteRaw(`{"Subtype":`)
		stream.WriteUint8(subtype)
		stream.WriteRaw(`,"Data":"`)
		stream.SetBuffer(base64.StdEncoding.AppendEncode(stream.Buffer(), data))
		stream.WriteRaw(`"}`)

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
		stream.WriteRaw(`{"Pattern":`)
		stream.WriteStringWithHTMLEscaped(pattern)
		stream.WriteRaw(`,"Options":`)
		stream.WriteStringWithHTMLEscaped(options)
		stream.WriteRaw("}")

	case bsoncore.TypeJavaScript:
		stream.WriteStringWithHTMLEscaped(v.JavaScript())

	case bsoncore.TypeSymbol:
		stream.WriteStringWithHTMLEscaped(v.Symbol())

	case bsoncore.TypeInt32:
		stream.WriteInt32(v.Int32())

	case bsoncore.TypeTimestamp:
		t, i := v.Timestamp()
		stream.WriteRaw(`{"T":`)
		stream.WriteUint32(t)
		stream.WriteRaw(`,"I":`)
		stream.WriteUint32(i)
		stream.WriteRaw("}")

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
