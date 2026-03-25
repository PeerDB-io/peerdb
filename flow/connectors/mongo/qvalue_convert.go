package connmongo

import (
	"encoding/base64"
	"fmt"
	"math"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"

	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type BsonToQValueConverter interface {
	// QValueStringFromKey looks up the _id field in the given raw BSON document
	// and converts it to a QValueString representation.
	QValueStringFromKey(raw bson.Raw, version uint32) (types.QValueString, error)
	// QValueJSONFromDocument converts a raw BSON document to a QValueJSON.
	QValueJSONFromDocument(raw bson.Raw) (types.QValueJSON, error)
}

// QValuesFromBsonRaw converts a raw BSON document to QValues, extracting the _id
// and producing JSON for the full document using the provided converter.
func QValuesFromBsonRaw(raw bson.Raw, version uint32, converter BsonToQValueConverter) ([]types.QValue, error) {
	idQValue, err := converter.QValueStringFromKey(raw, version)
	if err != nil {
		return nil, fmt.Errorf("failed to convert key %s: %w", DefaultDocumentKeyColumnName, err)
	}

	docQValue, err := converter.QValueJSONFromDocument(raw)
	if err != nil {
		return nil, fmt.Errorf("failed to convert document to JSON: %w", err)
	}

	return []types.QValue{idQValue, docQValue}, nil
}

// LegacyBsonConverter converts BSON to JSON via bson.D deserialization + json-iterator.
type LegacyBsonConverter struct {
	api jsoniter.API
}

func NewLegacyBsonConverter() *LegacyBsonConverter {
	return &LegacyBsonConverter{
		api: CreateExtendedJSONMarshaler(),
	}
}

func (c *LegacyBsonConverter) QValueJSONFromDocument(raw bson.Raw) (types.QValueJSON, error) {
	var d bson.D
	if err := bson.Unmarshal(raw, &d); err != nil {
		return types.QValueJSON{}, fmt.Errorf("failed to unmarshal BSON document: %w", err)
	}
	jsonb, err := c.api.Marshal(d)
	if err != nil {
		return types.QValueJSON{}, fmt.Errorf("failed to marshal document to JSON: %w", err)
	}
	return types.QValueJSON{Val: string(jsonb)}, nil
}

func (c *LegacyBsonConverter) QValueStringFromKey(raw bson.Raw, version uint32) (types.QValueString, error) {
	rv := raw.Lookup(DefaultDocumentKeyColumnName)
	if rv.IsZero() {
		return types.QValueString{}, fmt.Errorf("key %s not found", DefaultDocumentKeyColumnName)
	}
	if version >= shared.InternalVersion_MongoDBIdWithoutRedundantQuotes {
		switch rv.Type {
		case bson.TypeObjectID:
			return types.QValueString{Val: rv.ObjectID().Hex()}, nil
		case bson.TypeString:
			return types.QValueString{Val: rv.StringValue()}, nil
		}
	}
	var val any
	if err := rv.Unmarshal(&val); err != nil {
		return types.QValueString{}, fmt.Errorf("failed to unmarshal _id: %w", err)
	}
	jsonb, err := c.api.Marshal(val)
	if err != nil {
		return types.QValueString{}, fmt.Errorf("failed to marshal _id to JSON: %w", err)
	}
	return types.QValueString{Val: string(jsonb)}, nil
}

// DirectBsonConverter converts BSON directly to JSON without intermediate deserialization.
// It reuses a json-iterator Stream as the output buffer.
type DirectBsonConverter struct {
	stream *jsoniter.Stream
}

func NewDirectBsonConverter() *DirectBsonConverter {
	return &DirectBsonConverter{
		stream: jsoniter.NewStream(jsoniter.ConfigDefault, nil, 512),
	}
}

func (c *DirectBsonConverter) QValueJSONFromDocument(raw bson.Raw) (types.QValueJSON, error) {
	// Reset stream buffer length to 0 while keeping the backing array for reuse
	c.stream.SetBuffer(c.stream.Buffer()[:0])
	if err := rawDocToJSON(bsoncore.Document(raw), c.stream); err != nil {
		return types.QValueJSON{}, err
	}
	return types.QValueJSON{Val: string(c.stream.Buffer())}, nil
}

func (c *DirectBsonConverter) QValueStringFromKey(raw bson.Raw, version uint32) (types.QValueString, error) {
	rv := raw.Lookup(DefaultDocumentKeyColumnName)
	if rv.IsZero() {
		return types.QValueString{}, fmt.Errorf("key %s not found", DefaultDocumentKeyColumnName)
	}
	if version >= shared.InternalVersion_MongoDBIdWithoutRedundantQuotes {
		switch rv.Type {
		case bson.TypeObjectID:
			return types.QValueString{Val: rv.ObjectID().Hex()}, nil
		case bson.TypeString:
			return types.QValueString{Val: rv.StringValue()}, nil
		}
	}
	// Reset stream buffer length to 0 while keeping the backing array for reuse
	c.stream.SetBuffer(c.stream.Buffer()[:0])
	if err := coreValueToJSON(bsoncore.Value{Type: bsoncore.Type(rv.Type), Data: rv.Value}, c.stream); err != nil {
		return types.QValueString{}, fmt.Errorf("failed to convert _id to JSON: %w", err)
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
		if err := coreValueToJSON(elem.Value(), stream); err != nil {
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

		if err := coreValueToJSON(elem.Value(), stream); err != nil {
			return err
		}
	}
	stream.WriteRaw("]")
	return nil
}

const hextable = "0123456789abcdef"

// writeHexObjectID writes a 12-byte ObjectID as 24 hex characters directly into the stream.
func writeHexObjectID(stream *jsoniter.Stream, oid [12]byte) {
	buf := stream.Buffer()
	for _, b := range oid {
		buf = append(buf, hextable[b>>4], hextable[b&0xf])
	}
	stream.SetBuffer(buf)
}

func coreValueToJSON(v bsoncore.Value, stream *jsoniter.Stream) error {
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
		stream.WriteRaw(`"`)
		writeHexObjectID(stream, v.ObjectID())
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
		stream.WriteStringWithHTMLEscaped(bson.NewDecimal128(h, l).String())

	case bsoncore.TypeMinKey, bsoncore.TypeMaxKey:
		stream.WriteEmptyObject()

	case bsoncore.TypeDBPointer: // deprecated type, kept for backwards-compatibility
		ns, oid := v.DBPointer()
		stream.WriteRaw(`{"DB":`)
		stream.WriteStringWithHTMLEscaped(ns)
		stream.WriteRaw(`,"Pointer":"`)
		writeHexObjectID(stream, oid)
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

// writeFloat64JSON matches the encoding behavior of encodeCustom + writeFloat64WithExplicitDecimal:
// NaN/Inf → quoted strings, integer-valued floats → explicit ".0", others → standard notation
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
