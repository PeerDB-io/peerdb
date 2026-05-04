package connmongo

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"

	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type BsonToQValueConverter interface {
	// QValueStringFromId QValueStringFromId converts a raw _id value to a QValueString.
	QValueStringFromId(id bson.RawValue, version uint32) (types.QValueString, error)
	// QValueJSONFromDocument converts a raw BSON document to a QValueJSON.
	QValueJSONFromDocument(raw bson.Raw) (types.QValueJSON, error)
}

func NewBsonConverter(ctx context.Context, settings *internal.Settings) (BsonToQValueConverter, error) {
	direct, err := internal.PeerDBMongoDBDirectBsonConverter(ctx, settings.Env)
	if err != nil {
		return nil, err
	}

	if !direct {
		return &LegacyBsonConverter{
			api: CreateExtendedJSONMarshaler(),
		}, nil
	}

	return &DirectBsonConverter{
		// technically we write JSON directly via raw stream methods and do not use jsoniter's
		// config-driven serialization specified here, but this config is applied consistent with our
		// custom serialization (and used by LegacyBsonConverter) so specifying it here for consistency
		stream: jsoniter.NewStream(jsoniter.ConfigCompatibleWithStandardLibrary, nil, 512),
	}, nil
}

// LegacyBsonConverter converts BSON to JSON by first deserializing BSON to bson.D,
// and then serialize to JSON string via json-iterator.
type LegacyBsonConverter struct {
	api jsoniter.API
}

func (c *LegacyBsonConverter) QValueJSONFromDocument(raw bson.Raw) (types.QValueJSON, error) {
	var d bson.D
	if err := bson.Unmarshal(raw, &d); err != nil {
		return types.QValueJSON{}, fmt.Errorf("failed to unmarshal document: %w", err)
	}
	jsonb, err := c.api.Marshal(d)
	if err != nil {
		return types.QValueJSON{}, fmt.Errorf("failed to marshal document: %w", err)
	}
	return types.QValueJSON{Val: string(jsonb)}, nil
}

func (c *LegacyBsonConverter) QValueStringFromId(id bson.RawValue, version uint32) (types.QValueString, error) {
	if version >= shared.InternalVersion_MongoDBIdWithoutRedundantQuotes {
		switch id.Type {
		case bson.TypeObjectID:
			return types.QValueString{Val: id.ObjectID().Hex()}, nil
		case bson.TypeString:
			return types.QValueString{Val: id.StringValue()}, nil
		}
	}
	var val any
	if err := id.Unmarshal(&val); err != nil {
		return types.QValueString{}, fmt.Errorf("failed to unmarshal %s: %w", DefaultDocumentKeyColumnName, err)
	}
	jsonb, err := c.api.Marshal(val)
	if err != nil {
		return types.QValueString{}, fmt.Errorf("failed to marshal %s: %w", DefaultDocumentKeyColumnName, err)
	}
	return types.QValueString{Val: string(jsonb)}, nil
}

// DirectBsonConverter converts BSON directly to JSON string without intermediate deserialization,
// it uses jsoniter.Stream to build JSON output incrementally into a reusable buffer (to avoid allocation)
type DirectBsonConverter struct {
	stream *jsoniter.Stream
}

func (c *DirectBsonConverter) QValueJSONFromDocument(raw bson.Raw) (types.QValueJSON, error) {
	c.stream.Reset(nil)
	if err := rawDocToJSON(bsoncore.Document(raw), c.stream); err != nil {
		return types.QValueJSON{}, fmt.Errorf("failed to convert document: %w", err)
	}
	return types.QValueJSON{Val: string(c.stream.Buffer())}, nil
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
