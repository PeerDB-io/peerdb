package mongo

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

// Filter out system databases after listing all databases.
// Some MongoDB Atlas tiers (e.g., free tier) don't support regex filters in ListDatabases.
func GetDatabaseNames(ctx context.Context, client *mongo.Client) ([]string, error) {
	dbs, err := client.ListDatabases(ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	filteredDbNames := make([]string, 0, len(dbs.Databases))
	for _, db := range dbs.Databases {
		if db.Name == "admin" || db.Name == "local" || db.Name == "config" {
			continue
		}
		filteredDbNames = append(filteredDbNames, db.Name)
	}
	slices.Sort(filteredDbNames)
	return filteredDbNames, nil
}

// Filter out views and system collections after listing all collections.
// Some MongoDB Atlas tiers (e.g., free tier) don't support regex filters in ListCollections.
func GetCollectionNames(ctx context.Context, client *mongo.Client, databaseName string) ([]string, error) {
	db := client.Database(databaseName)
	cur, err := db.ListCollections(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	type CollectionSpec struct {
		Name string `bson:"name"`
		Type string `bson:"type"` // "collection" | "view" | etc
	}

	filteredCollNames := make([]string, 0, 100)
	for cur.Next(ctx) {
		if err := cur.Err(); err != nil {
			return nil, err
		}

		var coll CollectionSpec
		if err := cur.Decode(&coll); err != nil {
			return nil, err
		}

		if strings.HasPrefix(coll.Name, "system.") {
			continue
		}
		if strings.EqualFold(coll.Type, "view") {
			continue
		}
		filteredCollNames = append(filteredCollNames, coll.Name)
	}
	slices.Sort(filteredCollNames)
	return filteredCollNames, nil
}

func RawDocumentToJSON(doc bsoncore.Document, stream *jsoniter.Stream) error {
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
		if err := RawValueToJSON(elem.Value(), stream); err != nil {
			return err
		}
	}
	stream.WriteRaw("}")
	return nil
}

func RawArrayToJSON(arr bsoncore.Array, stream *jsoniter.Stream) error {
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

		if err := RawValueToJSON(elem.Value(), stream); err != nil {
			return err
		}
	}
	stream.WriteRaw("]")
	return nil
}

// RawValueToJSON writes a BSON value to stream as the JSON PeerDB lands MongoDB documents with, following the
// MongoDB ClickPipes type mapping (https://clickhouse.com/docs/integrations/clickpipes/mongodb/datatypes).
//
// The deprecated Undefined, MinKey and MaxKey render as {}, DBPointer as {"DB": "<ns>", "Pointer": "<hex>"}
// and CodeWithScope as {"Code": "<code>", "Scope": {...}}.
//
// RawDocumentToJSON and RawArrayToJSON are its entry points for a whole document or array; WriteBinaryJSON,
// WriteRegexJSON and WriteTimestampJSON expose the encodings of the BSON types rendered as JSON objects.
func RawValueToJSON(v bsoncore.Value, stream *jsoniter.Stream) error {
	switch v.Type {
	case bsoncore.TypeDouble:
		writeFloat64JSON(stream, v.Double())

	case bsoncore.TypeString:
		stream.WriteStringWithHTMLEscaped(v.StringValue())

	case bsoncore.TypeEmbeddedDocument:
		return RawDocumentToJSON(v.Document(), stream)

	case bsoncore.TypeArray:
		return RawArrayToJSON(v.Array(), stream)

	case bsoncore.TypeBinary:
		subtype, data := v.Binary()
		WriteBinaryJSON(stream, subtype, data)

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
		WriteRegexJSON(stream, pattern, options)

	case bsoncore.TypeJavaScript:
		stream.WriteStringWithHTMLEscaped(v.JavaScript())

	case bsoncore.TypeSymbol:
		stream.WriteStringWithHTMLEscaped(v.Symbol())

	case bsoncore.TypeInt32:
		stream.WriteInt32(v.Int32())

	case bsoncore.TypeTimestamp:
		t, i := v.Timestamp()
		WriteTimestampJSON(stream, t, i)

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
		if err := RawDocumentToJSON(scope, stream); err != nil {
			return err
		}
		stream.WriteRaw("}")

	default:
		return fmt.Errorf("unknown type: %v", v.Type.String())
	}
	return nil
}

// WriteBinaryJSON encodes BSON binary data as {"Subtype": <int>, "Data": "<base64>"}.
func WriteBinaryJSON(stream *jsoniter.Stream, subtype byte, data []byte) {
	stream.WriteRaw(`{"Subtype":`)
	stream.WriteUint8(subtype)
	stream.WriteRaw(`,"Data":"`)
	stream.SetBuffer(base64.StdEncoding.AppendEncode(stream.Buffer(), data))
	stream.WriteRaw(`"}`)
}

// WriteRegexJSON encodes a BSON regular expression as {"Pattern": "<pattern>", "Options": "<flags>"}.
func WriteRegexJSON(stream *jsoniter.Stream, pattern string, options string) {
	stream.WriteRaw(`{"Pattern":`)
	stream.WriteStringWithHTMLEscaped(pattern)
	stream.WriteRaw(`,"Options":`)
	stream.WriteStringWithHTMLEscaped(options)
	stream.WriteRaw("}")
}

// WriteTimestampJSON encodes a BSON (internal) timestamp as {"T": <seconds>, "I": <increment>}.
func WriteTimestampJSON(stream *jsoniter.Stream, t uint32, i uint32) {
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
